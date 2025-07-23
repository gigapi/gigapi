package utils

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb/v2" // load duckdb driver
	"os"
	"strconv"
	"sync"
	"time"
)

var dbMap = make(map[string]*sqlx.DB)
var dbMtx sync.Mutex

func getDb(path string) (*sqlx.DB, error) {
	dbMtx.Lock()
	defer dbMtx.Unlock()
	db := dbMap[path]
	if db != nil {
		return db, nil
	}
	db, err := sqlx.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("SET memory_limit='" + getDuckDBMemLimit() + "'")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(fmt.Sprintf("SET threads TO %d", getDuckDBThreadLimit()))
	if err != nil {
		return nil, err
	}
	dbMap[path] = db
	return db, nil
}

var poolMap sync.Map

type connWrapper struct {
	*sqlx.Conn
	initedAt time.Time
}

var dbHeld int32
var poolSize int32

const (
	DEFAULT_MEM_LIMIT       = "1GB"
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

func getDefaultFilePath() string {
	return fmt.Sprintf("%s?access_mode=READ_WRITE&allow_unsigned_extensions=1",
		config.Config.Gigapi.DefaultDatabase)
}

// ConnectDuckDB opens and returns a connection to DuckDB.
func ConnectDuckDB(filePath string) (*sqlx.Conn, func(), error) {
	// Open DuckDB connection (this will create a DuckDB instance in the specified file)
	if filePath == "" {
		filePath = getDefaultFilePath()
	}
	db, err := getDb(filePath)
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.Connx(context.Background())
	if err != nil {
		return nil, nil, err
	}
	return conn, func() {
		conn.Close()
	}, nil
}
