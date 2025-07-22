package reader

import (
	"encoding/json"
	"errors"
	"fmt"
	repository2 "github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"net/http"
	"strings"
)

func addCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func Query(w http.ResponseWriter, r *http.Request) error {
	addCORSHeaders(w)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return nil
	}

	query, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	db := r.URL.Query().Get("db")
	if db == "" {
		db = "default"
	}

	queryWithRightFrom, err := injectParquet(string(query), db)
	if err != nil {
		return err
	}
	fmt.Println("QUERY: " + queryWithRightFrom)

	conn, cancel, err := utils.ConnectDuckDB("?access_mode=READ_WRITE&allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	defer cancel()

	connx := sqlx.NewDb(conn, "duckdb")

	rows, err := connx.Queryx(queryWithRightFrom)
	if err != nil {
		return err
	}

	var _rows []map[string]any
	for rows.Next() {
		row := make(map[string]any)
		err = rows.MapScan(row)
		if err != nil {
			return err
		}
	}

	switch r.URL.Query().Get("format") {
	case "ndjson":
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		enc := json.NewEncoder(w)
		for _, row := range _rows {
			enc.Encode(row)
			w.Write([]byte("\n"))
		}
	case "json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(_rows)
	}
	return fmt.Errorf("unsupported format: %s", r.URL.Query().Get("format"))
}

func injectParquet(query string, db string) (string, error) {
	py := getPy()
	tables, err := py.Tables(string(query))
	if err != nil {
		return "", err
	}

	table := tables[0]
	if dbTable := strings.SplitN(table, ".", 2); len(dbTable) == 2 {
		db = dbTable[0]
	}

	idx, err := repository2.GetTableIndex(db, table)
	if errors.Is(err, repository2.DBNotFoundError) || errors.Is(err, repository2.TableNotFoundError) {
		return query, nil
	}
	if err != nil {
		return "", err
	}

	entries, err := idx.GetAll()
	if err != nil {
		return "", err
	}

	queryWithRightFrom, err := py.Inject(string(query), entries)
	if err != nil {
		return "", err
	}
	return queryWithRightFrom, nil
}
