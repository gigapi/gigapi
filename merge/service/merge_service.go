package service

import (
	"fmt"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/metadata"
	"golang.org/x/sync/semaphore"
	"html/template"
	"time"
)

var CHSQL_VER = "v1.0.10"

// const CHSQL_EXT_URL = "https://github.com/quackscience/duckdb-extension-clickhouse-sql/releases/download/{{.VER}}/chsql.{{.DUCKDB_VER}}.{{.ARCH}}.duckdb_extension"
const CHSQL_EXT_URL = "community"

type mergeService interface {
	DoMerge([]metadata.MergePlan) error
}

type mergeServicePerformer interface {
	mergeFirstIteration(p metadata.MergePlan) error
	mergeMany(p metadata.MergePlan) error
	mergeOne(p metadata.MergePlan) error
}

type mergeServiceManager struct {
	dataPath              string
	tmpPath               string
	table                 *shared.Table
	index                 metadata.TableIndex
	mergeServicePerformer mergeServicePerformer
	savePerformer         savePerformer
}

var tmpl = func() *template.Template {
	_tmpl, err := template.New("chsql_url").Parse(CHSQL_EXT_URL)
	if err != nil {
		panic(err)
	}
	return _tmpl
}()

// TODO: ADD configuration for this
var firstIterationSemaphore = semaphore.NewWeighted(1)

func (f *mergeServiceManager) merge(p metadata.MergePlan) error {
	var err error
	if p.Iteration == 1 {
		err = f.mergeServicePerformer.mergeFirstIteration(p)
	} else if len(p.From) == 1 {
		err = f.mergeServicePerformer.mergeOne(p)
	} else {
		err = f.mergeServicePerformer.mergeMany(p)
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

	/*
		fmt.Printf("Merging files:\n  Base path: %s\n", f.path)
		for _, file := range p.From {
			fmt.Printf("  %s\n", file)
		}
		fmt.Printf("  Tmp path: %s\n", tmpFilePath)
		fmt.Printf("  Data path: %s\n", finalFilePath)
	*/

}

func (f *mergeServiceManager) updateIndex(merge metadata.MergePlan) error {
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
	size, err := f.savePerformer.SizeB(merge.To)
	if err != nil {
		return err
	}
	newIdx := &metadata.IndexEntry{
		Layer:     merge.Layer,
		Database:  f.table.Database,
		Table:     f.table.Name,
		Path:      merge.To,
		SizeBytes: size,
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

func (f *mergeServiceManager) doMerge(merges []metadata.MergePlan, merge func(p metadata.MergePlan) error) error {
	for _, m := range merges {
		_m := m
		err := merge(_m)
		if err != nil {
			//errGroup.Cancel(err)
			return err
		}
	}
	return nil
}

func (f *mergeServiceManager) DoMerge(merges []metadata.MergePlan) error {
	_merges := make([]metadata.MergePlan, len(merges))
	copy(_merges, merges)
	return f.doMerge(_merges, f.merge)
}
