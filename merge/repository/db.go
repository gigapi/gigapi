package repository

import (
	"database/sql"
	"sync"
)

var (
	db   *sql.DB
	dbMtx sync.Mutex
)

func SetDB(d *sql.DB) {
	dbMtx.Lock()
	defer dbMtx.Unlock()
	db = d
}

func GetDB() *sql.DB {
	dbMtx.Lock()
	defer dbMtx.Unlock()
	return db
}
