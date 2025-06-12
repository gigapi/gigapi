package service

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/metadata"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type fsMergeServicePerformer struct {
	tmpPath  string
	dataPath string
	table    *shared.Table
}

func newFsMergeService(layer config.LayersConfiguration, table *shared.Table) (mergeService, error) {
	tmpPath, err := buildPath(layer, table, "tmp")
	if err != nil {
		return nil, err
	}
	dataPath, err := buildPath(layer, table, "data")
	if err != nil {
		return nil, err
	}
	performer := &fsMergeServicePerformer{
		tmpPath:  tmpPath,
		dataPath: dataPath,
		table:    table,
	}
	manager := &mergeServiceManager{
		dataPath:              dataPath,
		tmpPath:               tmpPath,
		table:                 table,
		index:                 table.Index,
		mergeServicePerformer: performer,
	}
	return manager, nil
}

func downloadToTempFile(url string, fname string) (string, error) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", fname)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer tmpFile.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to GET from %s: %w", url, err)
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write the body to file
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write to temporary file: %w", err)
	}

	return tmpFile.Name(), nil
}

func installChSql(db *sql.DB) error {
	if CHSQL_EXT_URL == "community" {
		_, err := db.Exec("INSTALL chsql FROM community")
		if err != nil {
			return fmt.Errorf("failed to install chsql extension: %w", err)
		}

		_, err = db.Exec("LOAD chsql")
		return err
	}

	var (
		ver  string
		arch string
	)
	row := db.QueryRow("SELECT version()")
	if row == nil {
		return fmt.Errorf("failed to get version")
	}
	err := row.Scan(&ver)
	if err != nil {
		return fmt.Errorf("failed to scan version: %w", err)
	}

	row = db.QueryRow("PRAGMA platform")
	if row == nil {
		return fmt.Errorf("failed to get platform")
	}
	err = row.Scan(&arch)
	if err != nil {
		return fmt.Errorf("failed to scan platform: %w", err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]string{
		"VER":        CHSQL_VER,
		"DUCKDB_VER": ver,
		"ARCH":       arch,
	})
	if err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	chsqlURL := buf.String()

	fname, err := downloadToTempFile(chsqlURL, "chsql.duckdb_extension")

	_, err = db.Exec(fmt.Sprintf("INSTALL '%s'", fname))
	if err != nil {
		return fmt.Errorf("failed to install chsql extension: %w", err)
	}

	_, err = db.Exec("LOAD 'chsql'")
	return err
}

func (f *fsMergeServicePerformer) getAbsPaths(relPaths []string) []string {
	from := make([]string, len(relPaths))
	for i, p := range relPaths {
		from[i] = filepath.Join(f.dataPath, p)
	}
	return from
}

func (f *fsMergeServicePerformer) mergeFirstIteration(p metadata.MergePlan) error {
	firstIterationSemaphore.Acquire(context.Background(), 1)
	defer firstIterationSemaphore.Release(1)

	tmpFilePath := filepath.Join(f.tmpPath, filepath.Base(p.To))
	finalFilePath := filepath.Join(f.dataPath, p.To)
	conn, cancel, err := utils.ConnectDuckDB("?access_mode=READ_WRITE&allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	defer cancel()
	createTableSQL := fmt.Sprintf(
		`COPY(FROM read_parquet(ARRAY['%s'], hive_partitioning = false, union_by_name = true) ORDER BY %s)TO '%s' (FORMAT 'parquet')`,
		strings.Join(f.getAbsPaths(p.From), "','"),
		strings.Join(f.table.OrderBy, " ASC,")+" ASC", tmpFilePath)
	_, err = conn.Exec(createTableSQL)
	if err != nil {
		fmt.Println(createTableSQL)
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}

	err = os.Rename(tmpFilePath, finalFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (f *fsMergeServicePerformer) mergeMany(p metadata.MergePlan) error {
	conn, cancel, err := utils.ConnectDuckDB("?access_mode=READ_WRITE&allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	defer cancel()
	err = installChSql(conn)
	if err != nil {
		return err
	}

	tmpFilePath := filepath.Join(f.tmpPath, filepath.Base(p.To))

	from := make([]string, len(p.From))
	for i, p := range p.From {
		from[i] = filepath.Join(f.dataPath, p)
	}

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(from, "','"),
		strings.Join(f.table.OrderBy, ","),
		tmpFilePath,
	)
	_, err = conn.Exec(createTableSQL)

	if err != nil {
		fmt.Println(createTableSQL)
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}

	err = os.Rename(tmpFilePath, path.Join(f.dataPath, p.To))
	return err
}

func (f *fsMergeServicePerformer) mergeOne(p metadata.MergePlan) error {
	from := filepath.Join(f.dataPath, p.From[0])
	return os.Rename(from, filepath.Join(f.dataPath, p.To))
}
