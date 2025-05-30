package utils

import (
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb/v2" // load duckdb driver
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var poolMap sync.Map

type dbWrapper struct {
	*sql.DB
	initedAt time.Time
}

var dbHeld int32
var poolSize int32

const (
	DEFAULT_MEM_LIMIT      = "1GB"
	DEFAULT_DB_THREAD_LIMIT = 1
)

func getDuckDBMemLimit() string {
	if v := os.Getenv("DUCKDB_MEM_LIMIT"); v != "" {
		return v
	}
	return DEFAULT_MEM_LIMIT
}

func getDuckDBThreadLimit() int {
	if v := os.Getenv("DUCKDB_THREAD_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_DB_THREAD_LIMIT
}

/*func init() {
	t := time.NewTicker(time.Second * 30)
	go func() {
	    for range t.C {
	        active := atomic.LoadInt32(&dbHeld)
	        idle := atomic.LoadInt32(&poolSize)
	        // Print when usage is high
	        if active >= idle-2 {
	            fmt.Printf("Duckdb pool stats: %d active / %d idle\n", active, idle)
	        }
	    }
	}()

}*/

// ConnectDuckDB opens and returns a connection to DuckDB.
func ConnectDuckDB(filePath string) (*sql.DB, func(), error) {
	// Open DuckDB connection (this will create a DuckDB instance in the specified file)
	pool, _ := poolMap.LoadOrStore(filePath, &sync.Pool{})
	db := pool.(*sync.Pool).Get()
	cancel := func() {
		atomic.AddInt32(&dbHeld, -1)
		if time.Now().Sub(db.(*dbWrapper).initedAt).Minutes() > 5 || atomic.LoadInt32(&poolSize) > 5 {
			db.(*dbWrapper).Close()
			return
		}
		atomic.AddInt32(&poolSize, 1)
		pool.(*sync.Pool).Put(db.(*dbWrapper))
	}
	if db != nil {
		atomic.AddInt32(&poolSize, -1)
		atomic.AddInt32(&dbHeld, 1)
		// Set baseline DuckDB settings
		_, _ = db.(*dbWrapper).Exec("SET memory_limit='" + getDuckDBMemLimit() + "'")
		_, _ = db.(*dbWrapper).Exec(fmt.Sprintf("SET threads TO %d", getDuckDBThreadLimit()))
		return db.(*dbWrapper).DB, cancel, nil
	}
	db, err := sql.Open("duckdb", filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	db = &dbWrapper{db.(*sql.DB), time.Now()}
	// Test the connection
	if err = db.(*dbWrapper).Ping(); err != nil {
		db.(*dbWrapper).Close()
		return nil, nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}
	// Set baseline DuckDB settings
	_, _ = db.(*dbWrapper).Exec("SET memory_limit='" + getDuckDBMemLimit() + "'")
	_, _ = db.(*dbWrapper).Exec(fmt.Sprintf("SET threads TO %d", getDuckDBThreadLimit()))
	atomic.AddInt32(&dbHeld, 1)
	return db.(*dbWrapper).DB, cancel, nil
}
