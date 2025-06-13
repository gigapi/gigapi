package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge"
	"github.com/gigapi/gigapi/v2/merge/repository"
	utils2 "github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/gigapi/v2/router"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
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

const N = 200
const S = 10000

func runServer() {
	initModules()
	r := router.NewRouter()
	fmt.Printf("GigAPI Running: %s:%d\n", config.Config.HTTP.Host, config.Config.HTTP.Port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d",
		config.Config.HTTP.Host, config.Config.HTTP.Port), r); err != nil {
		panic(err)
	}
}

func testE2EWriting(t *testing.T) {
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		_i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var data strings.Builder
			for j := 0; j < S; j++ {
				data.WriteString(fmt.Sprintf("logs,instance=1,level=info msg=\"Hello world %d\"\n", _i))
			}
			_, err := http.Post("http://localhost:7988/gigapi/insert", "", bytes.NewBuffer(
				[]byte(data.String())))
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("%d rows MB written in %v\n", S*N, time.Since(start))
	parquets := 0
	filepath.Walk("./_testdata/l1", func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".parquet") {
			parquets++
		}
		return nil
	})
	fmt.Printf("Found %d parquets\n", parquets)
	fmt.Println("Wating for merge...")
	time.Sleep(time.Second * 75)
	parquets = 0
	filepath.Walk("./_testdata/l1", func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".parquet") {
			parquets++
		}
		return nil
	})
	fmt.Printf("Found %d parquets\n", parquets)
}

func testE2EReading(t *testing.T) {
	var requests = []string{
		`{"query": "SELECT count() as c FROM logs"}`,
		`{"query": "SHOW DATABASES"}`,
		`{"query": "SHOW TABLES"}`,
	}
	var responses = []string{
		`{"results":[{"c":"2000000"}]}`,
		`{"results":[{"database_name":"default"}]}`,
		`{"results":[{"table_name":"logs"}]}`,
	}
	for i, reqBody := range requests {
		res, err := http.Post("http://localhost:7988/query?db=default&format=json", "application/json",
			bytes.NewBuffer([]byte(reqBody)))
		if err != nil {
			panic(err)
		}
		if res.StatusCode/100 != 2 {
			body, _ := io.ReadAll(res.Body)
			panic(fmt.Sprintf("[%d]: %s", res.StatusCode, string(body)))
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			panic(err)
		}
		strBody := strings.TrimSpace(string(body))
		fmt.Println(strBody)
		if strBody != responses[i] {
			panic(fmt.Sprintf("Unexpected response: `%s`", strBody))
		}
	}

}

func TestE2E(t *testing.T) {
	// Start CPU profiling
	stopCPUProfile := startCPUProfile(t)
	defer stopCPUProfile()
	defer os.RemoveAll("_testdata/l1")

	config.Config = &config.Configuration{
		Gigapi: config.GigapiConfiguration{
			Root:          "_testdata",
			MergeTimeoutS: 10,
			Mode:          "aio",
			Metadata: config.MetadataConfiguration{
				Type: "json",
			},
			Layers: []config.LayersConfiguration{
				{
					Name:   "l1",
					Type:   "fs",
					Global: false,
					URL:    "file://./_testdata/l1",
					TTL:    0,
				},
			},
		},
		HTTP: config.HTTPConfiguration{
			Port:      7988,
			Host:      "localhost",
			BasicAuth: config.BasicAuthConfiguration{},
		},
		FlightSql: config.FlightSqlConfiguration{Port: 7989},
	}
	go runServer()
	time.Sleep(time.Second)
	testE2EWriting(t)
	testE2EReading(t)
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

func ConsistencyTestIter(t *testing.T, count *int64) error {
	url := "http://localhost:7971/gigapi/write"
	data := `weather,location=us-midwest,season=summer temperature=82 1748253664000000000
weather,location=us-midwest,season=summer temperature=83
weather,location=us-midwest,season2=summer2 temperature=84
weather,location=us-midwest,season2=summer2 temperature=84`

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(data))
	if err != nil {
		t.Fatal("error creating request: ", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("error sending request: ", err)
		return nil
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("error reading response: ", err)
		return nil
	}
	*count += 4
	fmt.Printf("Total rows: %d\n", *count)
	return nil
}

func TestConsistency(t *testing.T) {
	if os.Getenv("INTERNAL_TEST") != "1" {
		return
	}
	count := int64(0)
	nowNs := time.Now().UnixNano()
	tillNextSec := 1000000000 - nowNs%1000000000
	time.Sleep(time.Nanosecond * time.Duration(tillNextSec))
	tck := time.NewTicker(time.Second)
	for range tck.C {
		ConsistencyTestIter(t, &count)
	}

}

func TestMetadataFiles(t *testing.T) {
	if os.Getenv("INTERNAL_TEST") != "1" {
		return
	}
	// Start CPU profiling
	stopCPUProfile := startCPUProfile(t)
	defer stopCPUProfile()

	config.Config = &config.Configuration{
		Gigapi: config.GigapiConfiguration{
			Root:          "_testdata",
			MergeTimeoutS: 10,
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
