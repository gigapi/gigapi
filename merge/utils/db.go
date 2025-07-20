package utils

import (
	"database/sql"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/utils/tempconf"
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
	DEFAULT_MEM_LIMIT       = "1GB"
	DEFAULT_DB_THREAD_LIMIT = 1
	MEMDB_ACCESS_STRING     = "?allow_unsigned_extensions=1"
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

func CancelFunc(db *dbWrapper, pool *sync.Pool) func() {
	return func() {
		atomic.AddInt32(&dbHeld, -1)
		if time.Now().Sub(db.initedAt).Minutes() > 5 || atomic.LoadInt32(&poolSize) > 5 {
			db.Close()
			return
		}
		atomic.AddInt32(&poolSize, 1)
		pool.Put(db)
	}

}

func ConnectDucklake(database string) (*sql.DB, func(), error) {
	pool, _ := poolMap.LoadOrStore("ducklake", &sync.Pool{})
	db := pool.(*sync.Pool).Get()
	if db != nil {
		atomic.AddInt32(&poolSize, -1)
		atomic.AddInt32(&dbHeld, 1)

		return db.(*dbWrapper).DB, CancelFunc(db.(*dbWrapper), pool.(*sync.Pool)), nil
	}
	_db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	_, err = _db.Exec("INSTALL ducklake")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to install DuckDB extension: %w", err)
	}
	req := fmt.Sprintf(`ATTACH '%s' AS my_ducklake (DATA_PATH '%s/');`,
		tempconf.GetDuckDBAttachString(), config.Config.Gigapi.Root)
	_, err = _db.Exec(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to attach ducklake: %w", err)
	}
	if database != "" {
		_, err = _db.Exec(fmt.Sprintf("USE %s;", database))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to use database: %w", err)
		}
	}

	db = &dbWrapper{_db, time.Now()}
	return _db, CancelFunc(db.(*dbWrapper), pool.(*sync.Pool)), nil
}

// ConnectDuckDB opens and returns a connection to DuckDB.
func ConnectDuckDB(filePath string) (*sql.DB, func(), error) {
	// Open DuckDB connection (this will create a DuckDB instance in the specified file)
	pool, _ := poolMap.LoadOrStore(filePath, &sync.Pool{})
	db := pool.(*sync.Pool).Get()
	if db != nil {
		atomic.AddInt32(&poolSize, -1)
		atomic.AddInt32(&dbHeld, 1)
		// Set baseline DuckDB settings
		_, _ = db.(*dbWrapper).Exec("SET memory_limit='" + getDuckDBMemLimit() + "'")
		_, _ = db.(*dbWrapper).Exec(fmt.Sprintf("SET threads TO %d", getDuckDBThreadLimit()))
		return db.(*dbWrapper).DB, CancelFunc(db.(*dbWrapper), pool.(*sync.Pool)), nil
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
	return db.(*dbWrapper).DB, CancelFunc(db.(*dbWrapper), pool.(*sync.Pool)), nil
}
