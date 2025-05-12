package main

import (
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi/v2/config"
	"github.com/gigapi/gigapi/v2/merge"
	"github.com/gigapi/gigapi/v2/merge/repository"
	utils2 "github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/gigapi/v2/utils"
	"io"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func startCPUProfile(t *testing.T) func() {
	cpuFile, err := os.Create("cpu.pprof")
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatal(err)
	}
	return func() {
		pprof.StopCPUProfile()
		cpuFile.Close()
	}
}

func writeMemProfile(t *testing.T) {
	memFile, err := os.Create("mem.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer memFile.Close()
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		t.Fatal(err)
	}
}

const N = 200
const S = 100000

func TestE2E(t *testing.T) {
	// Start CPU profiling
	stopCPUProfile := startCPUProfile(t)
	defer stopCPUProfile()

	config.Config = &config.Configuration{
		Gigapi: config.GigapiConfiguration{
			Root:          "_testdata",
			MergeTimeoutS: 10,
			Secret:        "XXXXXX",
		},
	}
	merge.Init(&api{})

	var data = map[string]any{
		"timestamp": []int64{},
		"value":     []float64{},
		"str":       []string{},
	}
	promises := make([]utils.Promise[int32], N)
	size := 0
	for i := 0; i < S; i++ {
		data["timestamp"] = append(data["timestamp"].([]int64), int64(time.Now().UnixNano()))
		data["value"] = append(data["value"].([]float64), float64(i)/100.0)
		str := fmt.Sprintf("str%d", i)
		data["str"] = append(data["str"].([]string), str)
		size += 8 + 8 + 8 + 1 + len(str)
	}
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		_i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			promises[_i] = repository.Store("", "test", data)
		}()

	}
	wg.Wait()
	fmt.Printf("Appending data %v\n", time.Since(start))
	for _, pp := range promises {
		_, err := pp.Get()
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("%d rows / %v MB written in %v\n", S*N, float64(size*N)/(1024*1024), time.Since(start))
	fmt.Println("Wating for merge...")
	time.Sleep(time.Second * 75)
}

type ParquetData struct {
	Type             string `json:"type"`
	ParquetSizeBytes int64  `json:"parquet_size_bytes"`
	RowCount         int64  `json:"row_count"`
	MinTime          int64  `json:"min_time"`
	MaxTime          int64  `json:"max_time"`
	WalSequence      int64  `json:"wal_sequence"`
	Files            []File `json:"files"`
}

type File struct {
	ID        int    `json:"id"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	RowCount  int64  `json:"row_count"`
	ChunkTime int64  `json:"chunk_time"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
	Range     string `json:"range"`
	Type      string `json:"type"`
}

func TestMetadataFiles(t *testing.T) {
	// Start CPU profiling
	stopCPUProfile := startCPUProfile(t)
	defer stopCPUProfile()

	config.Config = &config.Configuration{
		Gigapi: config.GigapiConfiguration{
			Root:          "_testdata",
			MergeTimeoutS: 10,
			Secret:        "XXXXXX",
		},
	}
	merge.Init(&api{})

	var data = map[string]any{
		"timestamp": []int64{},
		"value":     []float64{},
		"str":       []string{},
	}
	for i := 0; i < 5; i++ {
		data["timestamp"] = append(data["timestamp"].([]int64), int64(time.Now().UnixNano()))
		data["value"] = append(data["value"].([]float64), float64(i)/100.0)
		str := fmt.Sprintf("str%d", i)
		data["str"] = append(data["str"].([]string), str)
	}

	db, cancel, err := utils2.ConnectDuckDB("")
	if err != nil {
		panic(err)
	}
	defer cancel()

	var expectedSize int32 = 0

	checkMetadata := func() int64 {
		f, err := os.Open(
			fmt.Sprintf("_testdata/default/test/date=%s/hour=%02d/metadata.json",
				time.Now().UTC().Format("2006-01-02"), time.Now().UTC().Hour()))
		if err != nil {
			panic(err)
		}
		defer f.Close()
		data, err := io.ReadAll(f)
		if err != nil {
			panic(err)
		}
		pData := ParquetData{}
		json.Unmarshal(data, &pData)
		var pqts []string
		for _, f := range pData.Files {
			pqts = append(pqts, "'"+f.Path+"'")
		}
		rows, err := db.Query(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet([%s])", strings.Join(pqts, ",")))
		if err != nil {
			panic(err)
		}

		defer rows.Close()
		var count int64
		for rows.Next() {
			err := rows.Scan(&count)
			if err != nil {
				panic(err)
			}
		}
		return count
	}

	go func() {
		time.Sleep(time.Second * 5)
		for {
			mdSize := checkMetadata()
			fmt.Printf("Metadata count: %d - %d\n", mdSize, expectedSize)
			if mdSize != int64(expectedSize) {
				println("UNEXPECTED metadata count")
			}

			time.Sleep(time.Second * 1)
		}
	}()

	for i := 0; i < 100; i++ {
		promise := repository.Store("", "test", data)
		_, err := promise.Get()
		if err != nil {
			panic(err)
		}
		atomic.AddInt32(&expectedSize, 5)
		time.Sleep(time.Second)
	}
}
