package parsers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"

	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/google/uuid"
)

var tableNameCheck = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// ParquetIngestionRequest defines the JSON payload for Parquet ingestion.
type ParquetIngestionRequest struct {
	Database     string       `json:"database"`
	Table        string       `json:"table"`
	Files        []FileSource `json:"files"`
	TimeColumn   string       `json:"time_column,omitempty"`
	TimeFormat   string       `json:"time_format,omitempty"`
	IsTimeseries bool         `json:"is_timeseries,omitempty"`
}

// FileSource specifies the location and type of a Parquet file.
type FileSource struct {
	URL  string `json:"url"`
	Type string `json:"type,omitempty"` // e.g., "https", "s3", "local"
}

// ParquetParser implements the IParser interface for Parquet files.
type ParquetParser struct{}

// Parse handles JSON payloads describing Parquet files.
func (p *ParquetParser) Parse(data []byte) (chan *ParserResponse, error) {
	// This parser works with readers, so this method is not implemented.
	return nil, fmt.Errorf("Parse method not implemented for ParquetParser")
}

// ParseReader handles streaming JSON payloads.
func (p *ParquetParser) ParseReader(ctx context.Context, r io.Reader) (chan *ParserResponse, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	var req ParquetIngestionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON request: %w", err)
	}

	if req.Database == "" || req.Table == "" || len(req.Files) == 0 {
		return nil, fmt.Errorf("database, table, and files are required")
	}

	if !tableNameCheck.MatchString(req.Database) || !tableNameCheck.MatchString(req.Table) {
		return nil, fmt.Errorf("database and table names must be alphanumeric")
	}

	out := make(chan *ParserResponse)
	go func() {
		defer close(out)

		tablePath := filepath.Join(config.Config.Gigapi.Root, req.Database, req.Table)

		// 1. Describe the schema from the first file to register the table.
		db := repository.GetDB()
		describeQuery := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", req.Files[0].URL)
		rows, err := db.Query(describeQuery)
		if err != nil {
			out <- &ParserResponse{Error: fmt.Errorf("failed to describe Parquet file schema: %w", err)}
			return
		}
		defer rows.Close()

		var columns []string
		var types []string
		for rows.Next() {
			var columnName, columnType, null, key, defaultValue, extra string
			if err := rows.Scan(&columnName, &columnType, &null, &key, &defaultValue, &extra); err != nil {
				out <- &ParserResponse{Error: fmt.Errorf("failed to scan schema row: %w", err)}
				return
			}
			columns = append(columns, columnName)
			types = append(types, columnType)
		}

		// 2. Copy each file to the table's directory.
		for _, file := range req.Files {
			newFileName := uuid.New().String() + ".parquet"
			destinationPath := filepath.Join(tablePath, newFileName)
			query := fmt.Sprintf("COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT 'parquet')", file.URL, destinationPath)
			if err := repository.ExecuteQuery(query); err != nil {
				out <- &ParserResponse{Error: fmt.Errorf("failed to copy Parquet file: %w", err)}
				return
			}
		}

		out <- &ParserResponse{
			Database:   req.Database,
			Table:      req.Table,
			IsExternal: true,
			Data: map[string]interface{}{
				"columns": columns,
				"types":   types,
			},
		}
	}()

	return out, nil
}

func init() {
	RegisterParser("parquet", func(fieldNames []string, fieldTypes []string) IParser {
		return &ParquetParser{}
	})
}