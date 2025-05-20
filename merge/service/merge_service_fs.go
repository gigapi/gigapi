package service

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"html/template"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var CHSQL_VER = "v1.0.10"

// const CHSQL_EXT_URL = "https://github.com/quackscience/duckdb-extension-clickhouse-sql/releases/download/{{.VER}}/chsql.{{.DUCKDB_VER}}.{{.ARCH}}.duckdb_extension"
const CHSQL_EXT_URL = "community"

type mergeService interface {
	GetFilesToMerge(iteration int) ([]FileDesc, error)
	PlanMerge([]FileDesc, int64, int) []PlanMerge
	DoMerge([]PlanMerge) error
}

type fsMergeService struct {
	dataPath string
	tmpPath  string
	table    *shared.Table
	index    shared.Index
}

func (f *fsMergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	dataDir := f.dataPath
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var parquetFiles []FileDesc
	suffix := fmt.Sprintf("%d.parquet", iteration)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), suffix) {
			continue
		}
		name := filepath.Join(dataDir, file.Name())
		stat, err := os.Stat(name)
		if err != nil {
			return nil, err
		}
		if f.index != nil {
			abs, err := filepath.Abs(name)
			if err != nil {
				return nil, err
			}
			entry := f.index.Get(abs)
			if entry == nil {
				continue
			}
		}

		parquetFiles = append(parquetFiles, struct {
			name string
			size int64
		}{name, stat.Size()})
	}
	sort.Slice(parquetFiles, func(a, b int) bool {
		return parquetFiles[a].size > parquetFiles[b].size
	})
	return parquetFiles, nil
}

func (f *fsMergeService) PlanMerge(files []FileDesc, maxResSize int64, iteration int) []PlanMerge {
	var res []PlanMerge
	mergeSize := int64(0)
	uid, _ := uuid.NewUUID()
	_res := PlanMerge{
		To:        path.Join(fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
		Iteration: iteration,
	}
	for _, file := range files {
		mergeSize += file.size
		_res.From = append(_res.From, file.name)
		if mergeSize > maxResSize {
			res = append(res, _res)
			uid, _ := uuid.NewUUID()
			_res = PlanMerge{
				To:        path.Join(fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
				Iteration: iteration,
			}
			mergeSize = 0
		}
	}
	if len(_res.From) > 0 {
		res = append(res, _res)
	}
	return res
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

func (f *fsMergeService) mergeFirstIteration(p PlanMerge) error {
	firstIterationSemaphore.Acquire(context.Background(), 1)
	defer firstIterationSemaphore.Release(1)
	tmpFilePath := filepath.Join(f.tmpPath, p.To)
	finalFilePath := filepath.Join(f.dataPath, p.To)
	conn, cancel, err := utils.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	defer cancel()
	createTableSQL := fmt.Sprintf(
		`COPY(FROM read_parquet(ARRAY['%s'], hive_partitioning = false, union_by_name = true) ORDER BY %s)TO '%s' (FORMAT 'parquet')`,
		strings.Join(p.From, "','"),
		strings.Join(f.table.OrderBy, " ASC,")+" ASC", tmpFilePath)
	_, err = conn.Exec(createTableSQL)
	if err != nil {
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

	f.cleanup(p)

	return nil
}

func (f *fsMergeService) cleanup(p PlanMerge) {
	for _, file := range p.From {
		_file := file
		go func() {
			<-time.After(time.Second * 30)
			os.Remove(_file)
			if f.index != nil {
				f.index.RmFromDropQueue([]string{_file})
			}
		}()
	}
}

func (f *fsMergeService) mergeMany(p PlanMerge, tmpFilePath, finalFilePath string) error {
	conn, cancel, err := utils.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	defer cancel()
	err = installChSql(conn)
	if err != nil {
		return err
	}

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(p.From, "','"),
		strings.Join(f.table.OrderBy, ","), tmpFilePath)
	_, err = conn.Exec(createTableSQL)

	if err != nil {
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}

	err = os.Rename(tmpFilePath, finalFilePath)
	return err
}

func (f *fsMergeService) merge(p PlanMerge) error {
	if p.Iteration == 1 {
		return f.mergeFirstIteration(p)
	}

	tmpFilePath := filepath.Join(f.tmpPath, p.To)
	finalFilePath := filepath.Join(f.dataPath, p.To)
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
		err = os.Rename(p.From[0], finalFilePath)
	} else {
		err = f.mergeMany(p, tmpFilePath, finalFilePath)
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

	f.cleanup(p)
	return nil
}

func (f *fsMergeService) updateIndex(merge PlanMerge) error {
	var minTime int64 = -1
	var maxTime int64 = -1
	var rowCount int64
	toDelete := make([]string, len(merge.From))
	for i, file := range merge.From {
		path, err := filepath.Abs(file)
		if err != nil {
			return err
		}
		toDelete[i] = path
		fromIdx := f.index.Get(path)
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
	newIdx := &shared.IndexEntry{
		Path:      path,
		SizeBytes: stat.Size(),
		RowCount:  rowCount,
		ChunkTime: time.Now().UnixNano(),
		MinTime:   minTime,
		MaxTime:   maxTime,
	}
	prom := f.index.Batch([]*shared.IndexEntry{newIdx}, toDelete)
	f.index.AddToDropQueue(merge.From)
	_, err = prom.Get()
	return err
}

func (f *fsMergeService) doMerge(merges []PlanMerge, merge func(p PlanMerge) error) error {
	errGroup := errgroup.Group{}
	sem := semaphore.NewWeighted(10)
	for _, m := range merges {

		_m := m
		errGroup.Go(func() error {
			sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			return merge(_m)
		})
	}
	return errGroup.Wait()
}

func (f *fsMergeService) DoMerge(merges []PlanMerge) error {
	_merges := make([]PlanMerge, len(merges))
	copy(_merges, merges)
	return f.doMerge(_merges, f.merge)
}
