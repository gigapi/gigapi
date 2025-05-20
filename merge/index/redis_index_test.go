package index

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/redis/go-redis/v9"
	"sync"
	"testing"
)

func TestRedisIndex(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	wg := &sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			arg, _ := json.Marshal(map[string]any{
				"path":       fmt.Sprintf("test/test3/b.%d.1.parquet", j),
				"size_bytes": 1000000,
			})

			err := c.Eval(context.Background(), SCRIPT_PATCH_INDEX, []string{"50000000"}, []any{string(arg)}).Err()
			if err != nil {
				panic(err)
			}
		}(i)
	}
	wg.Wait()
}

func TestEntry(t *testing.T) {
	e := &redisIndexEntry{
		IndexEntry: &shared.IndexEntry{
			Path:      "test/b.1.1.parquet",
			SizeBytes: 1000000,
		},
		Cmd: "patch",
	}

	data, _ := json.Marshal(e)
	fmt.Printf("Data: %s\n", string(data))

	e2 := shared.IndexEntry{}
	_ = json.Unmarshal(data, &e2)
	fmt.Println(e2)
}

func TestRedisIndexSHA(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Load the script and get the SHA
	sha, err := c.ScriptLoad(context.Background(), SCRIPT_PATCH_INDEX).Result()
	if err != nil {
		t.Fatalf("Failed to load script: %v", err)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			arg, _ := json.Marshal(map[string]any{
				"path":       fmt.Sprintf("test/b.%d.1.parquet", j),
				"size_bytes": 1000000,
			})

			err := c.EvalSha(context.Background(), sha, []string{}, string(arg)).Err()
			if err != nil {
				panic(fmt.Sprintf("Failed to execute script: %v", err))
			}
		}(i)
	}
	wg.Wait()
}

func TestRedisIndex2(t *testing.T) {
	config.InitConfig("")
	idx, err := NewRedisIndex("redis://localhost:6379/0")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	var ents []*shared.IndexEntry
	for i := 0; i < 10000; i++ {
		ents = append(ents, &shared.IndexEntry{
			Path:      fmt.Sprintf("test/b.%d.1.parquet", i),
			SizeBytes: 1000000,
		})
	}
	p := idx.Batch(ents, nil)
	_, err = p.Get()
	if err != nil {
		panic(err)
	}
}
