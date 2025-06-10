package service

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/metadata"
	"golang.org/x/sync/semaphore"
	"html/template"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

var CHSQL_VER = "v1.0.10"

// const CHSQL_EXT_URL = "https://github.com/quackscience/duckdb-extension-clickhouse-sql/releases/download/{{.VER}}/chsql.{{.DUCKDB_VER}}.{{.ARCH}}.duckdb_extension"
const CHSQL_EXT_URL = "community"

type mergeService interface {
	DoMerge([]metadata.MergePlan) error
}

type fsMergeService struct {
	dataPath string
	tmpPath  string
	table    *shared.Table
	index    metadata.TableIndex
}

var tmpl = func() *template.Template {
	_tmpl, err := template.New("chsql_url").Parse(CHSQL_EXT_URL)
	if err != nil {
		panic(err)
	}
	return _tmpl
}()

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

// TODO: ADD configuration for this
var firstIterationSemaphore = semaphore.NewWeighted(1)

func (f *fsMergeService) getAbsPaths(relPaths []string) []string {
	from := make([]string, len(relPaths))
	for i, p := range relPaths {
		from[i] = filepath.Join(f.dataPath, p)
	}
	return from
}

func (f *fsMergeService) mergeFirstIteration(p metadata.MergePlan) error {
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

	if f.index != nil {
		err = f.updateIndex(p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *fsMergeService) mergeMany(p metadata.MergePlan) error {
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

func (f *fsMergeService) merge(p metadata.MergePlan) error {
	if p.Iteration == 1 {
		return f.mergeFirstIteration(p)
	}

	/*
		fmt.Printf("Merging files:\n  Base path: %s\n", f.path)
		for _, file := range p.From {
			fmt.Printf("  %s\n", file)
		}
		fmt.Printf("  Tmp path: %s\n", tmpFilePath)
		fmt.Printf("  Data path: %s\n", finalFilePath)
	*/

	var err error

	if len(p.From) == 1 {
		from := filepath.Join(f.dataPath, p.From[0])
		err = os.Rename(from, filepath.Join(f.dataPath, p.To))
	} else {
		err = f.mergeMany(p)
	}
	if err != nil {
		return err
	}

	if f.index != nil {
		err = f.updateIndex(p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *fsMergeService) updateIndex(merge metadata.MergePlan) error {
	_min := make(map[string]any)
	_max := make(map[string]any)
	var minTime int64
	var maxTime int64
	var rowCount int64
	toDelete := make([]*metadata.IndexEntry, len(merge.From))
	for i, file := range merge.From {
		toDelete[i] = &metadata.IndexEntry{
			Layer:    merge.Layer,
			Database: f.table.Database,
			Table:    f.table.Name,
			Path:     file,
			WriterID: merge.WriterID,
		}
		fromIdx := f.index.Get(merge.Layer, file)
		if i == 0 {
			minTime = fromIdx.MinTime
			maxTime = fromIdx.MaxTime
		} else {
			minTime = min(minTime, fromIdx.MinTime)
			maxTime = max(maxTime, fromIdx.MaxTime)
		}
		rowCount += fromIdx.RowCount
	}
	path, err := filepath.Abs(path.Join(f.dataPath, merge.To))
	if err != nil {
		return err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	newIdx := &metadata.IndexEntry{
		Layer:     merge.Layer,
		Database:  f.table.Database,
		Table:     f.table.Name,
		Path:      merge.To,
		SizeBytes: stat.Size(),
		RowCount:  rowCount,
		ChunkTime: time.Now().UnixNano(),
		Min:       _min,
		Max:       _max,
		MinTime:   minTime,
		MaxTime:   maxTime,
		WriterID:  "",
	}
	prom := f.index.Batch([]*metadata.IndexEntry{newIdx}, toDelete)
	_, err = prom.Get()
	if err != nil {
		return err
	}
	fmt.Printf("Finishing merge: %v\n", merge)
	_, err = f.index.GetMergePlanner().EndMerge(merge).Get()
	return err
}

func (f *fsMergeService) doMerge(merges []metadata.MergePlan, merge func(p metadata.MergePlan) error) error {
	//errGroup := errgroup.Group{}
	//sem := semaphore.NewWeighted(10)
	for _, m := range merges {
		_m := m
		/*sem.Acquire(context.Background(), 1)
		defer sem.Release(1)*/
		err := merge(_m)
		if err != nil {
			//errGroup.Cancel(err)
			return err
		}
	}
	return nil
}

func (f *fsMergeService) DoMerge(merges []metadata.MergePlan) error {
	_merges := make([]metadata.MergePlan, len(merges))
	copy(_merges, merges)
	return f.doMerge(_merges, f.merge)
}
