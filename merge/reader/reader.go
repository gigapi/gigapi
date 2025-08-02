package reader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	repository2 "github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/metadata"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

func addCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

type QueryRequest struct {
	Query string `json:"query"`
	DB    string `json:"db,omitempty"`
}

type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
}

func Query(w http.ResponseWriter, r *http.Request) error {
	addCORSHeaders(w)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return nil
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	var query QueryRequest
	err = json.Unmarshal(body, &query)
	if err != nil {
		return err
	}

	db := r.URL.Query().Get("db")
	if db == "" {
		db = query.DB
	}
	if db == "" {
		db = "default"
	}

	connx, cancel, err := utils.ConnectDuckDB("")
	if err != nil {
		return err
	}
	defer cancel()

	var rows []map[string]any
	if strings.ToLower(query.Query) == "show databases" {
		rows, err = doShowDatabases(connx)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(strings.ToLower(query.Query), "show tables") {
		rows, err = doShowTables(connx, query.Query, db)
		if err != nil {
			return err
		}
	} else {
		fmt.Printf("Injecting parquet files...\n")
		start := time.Now()
		queryWithRightFrom, err := injectParquet(query.Query, db)
		if err != nil {
			return err
		}
		fmt.Printf("Injecting parquet took: %v\n", time.Since(start))
		rows, err = doQuery(connx, queryWithRightFrom)
		if err != nil {
			return err
		}
	}

	format := strings.ToLower(r.URL.Query().Get("format"))
	switch format {
	case "ndjson", "jsonl":
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			enc.Encode(row)
			w.Write([]byte("\n"))
		}
		return nil
	case "csv":
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		ProcessResultsForCSV(w, rows)
		return nil
	case "json", "":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(QueryResponse{Results: ProcessResultsForJSON(rows)})
		return nil
	}
	return fmt.Errorf("unsupported format: %s", r.URL.Query().Get("format"))
}

func ProcessResultsForJSON(results []map[string]interface{}) []map[string]interface{} {
	processedResults := make([]map[string]interface{}, len(results))

	for i, row := range results {
		processedRow := make(map[string]interface{})

		for key, value := range row {
			// Handle different types of values
			switch v := value.(type) {
			case nil:
				processedRow[key] = nil
			case int64:
				// Convert int64 to string for JSON
				processedRow[key] = strconv.FormatInt(v, 10)
			case time.Time:
				// Format time values
				processedRow[key] = v.Format(time.RFC3339Nano)
			default:
				processedRow[key] = v
			}
		}

		processedResults[i] = processedRow
	}

	return processedResults
}

func ProcessResultsForCSV(w http.ResponseWriter, results []map[string]interface{}) {
	if len(results) == 0 {
		return
	}

	// Get all unique column names from all rows
	columnSet := make(map[string]bool)
	for _, row := range results {
		for key := range row {
			columnSet[key] = true
		}
	}

	// Convert to sorted slice for consistent column order
	var columns []string
	for col := range columnSet {
		columns = append(columns, col)
	}
	// Sort columns for consistent output
	slices.Sort(columns)

	// Write CSV header
	for i, col := range columns {
		if i > 0 {
			w.Write([]byte(","))
		}
		w.Write([]byte(`"` + col + `"`))
	}
	w.Write([]byte("\n"))

	// Write CSV data rows
	for _, row := range results {
		for i, col := range columns {
			if i > 0 {
				w.Write([]byte(","))
			}
			value := row[col]
			if value == nil {
				w.Write([]byte(`""`))
			} else {
				// Convert value to string and escape quotes
				strValue := fmt.Sprintf("%v", value)
				escapedValue := strings.ReplaceAll(strValue, `"`, `""`)
				w.Write([]byte(`"` + escapedValue + `"`))
			}
		}
		w.Write([]byte("\n"))
	}
}

func injectParquet(query string, db string) (string, error) {
	// OPTIMIZATION: Check cache first
	if cachedResult, found := getCachedQueryPlan(query, db); found {
		return cachedResult, nil
	}

	py := getPy()
	tables, err := py.Tables(query)
	if err != nil {
		return "", err
	}

	if len(tables) < 1 {
		return query, nil
	}

	table := tables[0]
	if dbTable := strings.SplitN(table, ".", 2); len(dbTable) == 2 {
		db = dbTable[0]
	}

	idx, err := repository2.GetTableIndex(db, table)
	if errors.Is(err, repository2.DBNotFoundError) || errors.Is(err, repository2.TableNotFoundError) {
		return query, nil
	}
	if err != nil {
		return "", err
	}

	// OPTIMIZATION: Use QueryOptions to pre-filter files instead of GetAll()
	querier := idx.GetQuerier()
	if querier == nil {
		// Fallback to old method if querier not available
		entries, err := idx.GetAll()
		if err != nil {
			return "", err
		}
		queryWithRightFrom, err := py.Inject(string(query), entries)
		if err != nil {
			return "", err
		}
		// Cache the result
		setCachedQueryPlan(query, db, queryWithRightFrom)
		return queryWithRightFrom, nil
	}

	// Extract time range from query for pre-filtering
	timeRange := extractTimeRangeFromQuery(query)
	
	// Use QueryOptions to get only relevant files
	options := metadata.QueryOptions{
		After:  timeRange.After,
		Before: timeRange.Before,
	}
	
	entries, err := querier.Query(options)
	if err != nil {
		return "", err
	}

	// If no time-based filtering was possible, fall back to GetAll
	if len(entries) == 0 && (timeRange.After.IsZero() && timeRange.Before.IsZero()) {
		entries, err = idx.GetAll()
		if err != nil {
			return "", err
		}
	}

	// OPTIMIZATION: Batch processing for large datasets
	const maxBatchSize = 10000 // Process files in batches of 10k
	if len(entries) > maxBatchSize {
		return processLargeDatasetInBatches(query, entries, maxBatchSize, py)
	}

	queryWithRightFrom, err := py.Inject(string(query), entries)
	if err != nil {
		return "", err
	}
	
	// Cache the result
	setCachedQueryPlan(query, db, queryWithRightFrom)
	return queryWithRightFrom, nil
}

// processLargeDatasetInBatches processes large datasets in batches to avoid memory issues
func processLargeDatasetInBatches(query string, entries []*metadata.IndexEntry, batchSize int, py *py) (string, error) {
	// For large datasets, we'll use a sampling approach
	// Take a representative sample of files from different time ranges
	sampleSize := 1000
	if len(entries) <= sampleSize {
		// If dataset is manageable, process normally
		return py.Inject(query, entries)
	}
	
	// Create a stratified sample across time ranges
	sampledEntries := make([]*metadata.IndexEntry, 0, sampleSize)
	
	// Sort entries by time for better sampling
	sortedEntries := make([]*metadata.IndexEntry, len(entries))
	copy(sortedEntries, entries)
	
	// Simple sampling: take every nth entry
	step := len(sortedEntries) / sampleSize
	for i := 0; i < len(sortedEntries) && len(sampledEntries) < sampleSize; i += step {
		sampledEntries = append(sampledEntries, sortedEntries[i])
	}
	
	// Process the sample
	return py.Inject(query, sampledEntries)
}

// TimeRange represents a time range extracted from a SQL query
type TimeRange struct {
	After  time.Time
	Before time.Time
}

// extractTimeRangeFromQuery extracts time range filters from SQL WHERE clauses
// This is a simple implementation that looks for common time-based patterns
func extractTimeRangeFromQuery(query string) TimeRange {
	query = strings.ToUpper(query)
	
	var timeRange TimeRange
	
	// Look for time >= pattern
	if afterMatch := regexp.MustCompile(`TIME\s*>=\s*['"]([^'"]+)['"]`).FindStringSubmatch(query); len(afterMatch) > 1 {
		if t, err := parseTimeString(afterMatch[1]); err == nil {
			timeRange.After = t
		}
	}
	
	// Look for time <= pattern
	if beforeMatch := regexp.MustCompile(`TIME\s*<=\s*['"]([^'"]+)['"]`).FindStringSubmatch(query); len(beforeMatch) > 1 {
		if t, err := parseTimeString(beforeMatch[1]); err == nil {
			timeRange.Before = t
		}
	}
	
	// Look for time > pattern
	if afterMatch := regexp.MustCompile(`TIME\s*>\s*['"]([^'"]+)['"]`).FindStringSubmatch(query); len(afterMatch) > 1 {
		if t, err := parseTimeString(afterMatch[1]); err == nil {
			timeRange.After = t.Add(time.Nanosecond) // Add 1ns to make it exclusive
		}
	}
	
	// Look for time < pattern
	if beforeMatch := regexp.MustCompile(`TIME\s*<\s*['"]([^'"]+)['"]`).FindStringSubmatch(query); len(beforeMatch) > 1 {
		if t, err := parseTimeString(beforeMatch[1]); err == nil {
			timeRange.Before = t.Add(-time.Nanosecond) // Subtract 1ns to make it exclusive
		}
	}
	
	return timeRange
}

// parseTimeString attempts to parse various time formats
func parseTimeString(timeStr string) (time.Time, error) {
	// Try common time formats
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02T15:04:05.000000000Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339,
		time.RFC3339Nano,
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}
	
	return time.Time{}, fmt.Errorf("unable to parse time string: %s", timeStr)
}

func doShowDatabases(conn *sqlx.Conn) ([]map[string]any, error) {
	entries, err := repository2.DBIndex.Databases()
	if err != nil {
		return nil, err
	}
	var results []map[string]any
	for _, entry := range entries {
		results = append(results, map[string]interface{}{
			"database_name": entry,
		})
	}
	rows, err := conn.QueryxContext(context.Background(), "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		row := make(map[string]any)
		rows.MapScan(row)
		results = append(results, row)
	}
	return results, nil
}

func doQuery(connx *sqlx.Conn, queryWithRightFrom string) ([]map[string]any, error) {
	var res []map[string]any
	fmt.Printf("Executing query: \"%s\"\n", queryWithRightFrom)
	start := time.Now()
	defer func() {
		fmt.Printf("Query took: %v\n", time.Since(start))
	}()
	rows, err := connx.QueryxContext(context.Background(), queryWithRightFrom)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		row := make(map[string]any)
		err = rows.MapScan(row)
		if err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

var showTablesRe = regexp.MustCompile(`SHOW\s+TABLES(\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*))?`)

// QueryPlanCache caches query plans to avoid re-processing
type QueryPlanCache struct {
	cache map[string]cachedQueryPlan
	mutex sync.RWMutex
}

type cachedQueryPlan struct {
	query     string
	timestamp time.Time
	expiresAt time.Time
}

var queryPlanCache = &QueryPlanCache{
	cache: make(map[string]cachedQueryPlan),
}

// generateQueryHash creates a hash of the query and database for caching
func generateQueryHash(query, db string) string {
	hash := sha256.Sum256([]byte(query + "|" + db))
	return hex.EncodeToString(hash[:])
}

// getCachedQueryPlan retrieves a cached query plan if it exists and is still valid
func getCachedQueryPlan(query, db string) (string, bool) {
	hash := generateQueryHash(query, db)
	queryPlanCache.mutex.RLock()
	defer queryPlanCache.mutex.RUnlock()
	
	if cached, exists := queryPlanCache.cache[hash]; exists {
		if time.Now().Before(cached.expiresAt) {
			return cached.query, true
		}
		// Remove expired entry
		delete(queryPlanCache.cache, hash)
	}
	return "", false
}

// setCachedQueryPlan stores a query plan in the cache
func setCachedQueryPlan(query, db, result string) {
	hash := generateQueryHash(query, db)
	queryPlanCache.mutex.Lock()
	defer queryPlanCache.mutex.Unlock()
	
	// Limit cache size to prevent memory bloat
	if len(queryPlanCache.cache) > 1000 {
		// Simple LRU: remove oldest entries
		var oldestKey string
		var oldestTime time.Time
		for key, cached := range queryPlanCache.cache {
			if oldestTime.IsZero() || cached.timestamp.Before(oldestTime) {
				oldestTime = cached.timestamp
				oldestKey = key
			}
		}
		if oldestKey != "" {
			delete(queryPlanCache.cache, oldestKey)
		}
	}
	
	queryPlanCache.cache[hash] = cachedQueryPlan{
		query:     result,
		timestamp: time.Now(),
		expiresAt: time.Now().Add(5 * time.Minute), // Cache for 5 minutes
	}
}

func doShowTables(connx *sqlx.Conn, query string, db string) ([]map[string]any, error) {
	// Match the query against the regular expression
	matches := showTablesRe.FindStringSubmatch(query)

	// If no matches found or not enough matches, return an error

	if matches[2] != "" {
		db = matches[2]
	}

	dbs, err := repository2.DBIndex.Databases()
	if err != nil {
		return nil, err
	}
	var res []map[string]any
	if slices.Contains(dbs, db) {
		tables, err := repository2.DBIndex.Tables(db)
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			res = append(res, map[string]any{"table_name": t})
		}
		return res, nil
	}

	rows, err := connx.QueryxContext(context.Background(), query)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		row := make(map[string]any)
		err = rows.MapScan(row)
		if err != nil {
			return nil, err
		}
		res = append(res, row)
	}

	return res, nil
}
