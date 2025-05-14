package main

import (
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge"
	"github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/utils"
	"os"
	"path"
	"time"
)

func main() {
	wd, _ := os.Getwd()
	cwd := path.Join(wd, "_data")
	config.Config = &config.Configuration{
		QuackPipe: config.QuackPipeConfiguration{
			Enabled:       true,
			Root:          cwd,
			MergeTimeoutS: 10,
			SaveTimeoutS:  1,
			Secret:        "XXXXXX",
		},
	}
	merge.Init()
	data := map[string]any{
		"str":   []string{}, // only []string, []int64, []uint64, []float64 are supported.
		"int":   []int64{},
		"float": []float64{},
	}
	fmt.Println("Start writing data...")
	t := time.Now()
	var promises []utils.Promise[int32]
	for i := 0; i < 1000000; i++ {
		data["str"] = append(data["str"].([]string), fmt.Sprintf("str%d", i))
		data["int"] = append(data["int"].([]int64), int64(i))
		data["float"] = append(data["float"].([]float64), float64(i)/100.0)
		if len(data["str"].([]string))%1000 == 0 { // don't flush more than 1000 rows due to an internal bug
			promises = append(promises, repository.Store("table1", data))
			data = map[string]any{
				"str":   []string{},
				"int":   []int64{},
				"float": []float64{},
			}
		}
	}
	for _, p := range promises {
		_, err := p.Get()
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("1M rows written in %v\n", time.Since(t))
	fmt.Println("Waiting 30s for merge...")
	time.Sleep(30 * time.Second)
	dirPath := path.Join(cwd, "table1", "data")
	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
	} else {
		fileCount := 0
		for _, file := range files {
			if !file.IsDir() {
				fileCount++
			}
		}
		fmt.Printf("Number of files merged: %d\n", fileCount)
	}
}
