package reader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	repository2 "github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
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
		queryWithRightFrom, err := injectParquet(query.Query, db)
		if err != nil {
			return err
		}
		rows, err = doQuery(connx, queryWithRightFrom)
		if err != nil {
			return err
		}
	}

	switch r.URL.Query().Get("format") {
	case "ndjson":
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			enc.Encode(row)
			w.Write([]byte("\n"))
		}
		return nil
	case "json":
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

func injectParquet(query string, db string) (string, error) {
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

	entries, err := idx.GetAll()
	if err != nil {
		return "", err
	}

	queryWithRightFrom, err := py.Inject(string(query), entries)
	if err != nil {
		return "", err
	}
	return queryWithRightFrom, nil
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
