package handlers

import (
	"compress/gzip"
	"context"
	"github.com/gigapi/gigapi/v2/merge/parsers"
	"github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/gigapi/gigapi/v2/utils"
	"io"
	"net/http"
	"strings"
)

var API modules.Api

func getDatabase(r *http.Request) (string, error) {
	db := r.URL.Query().Get("db")
	if db == "" {
		vars := API.GetPathParams(r)
		db = vars["db"]
	}
	if db == "" {
		db = "default"
	}
	if strings.Contains(db, "/") || strings.Contains(db, ".") {
		return "", utils.NewGigapiError("Invalid database name", http.StatusBadRequest)
	}

	return db, nil
}

func InsertIntoHandler(w http.ResponseWriter, r *http.Request) error {
	contentType := r.Header.Get("Content-Type")
	parser, err := parsers.GetParser(contentType, nil, nil)

	database, err := getDatabase(r)
	if err != nil {
		return err
	}

	ctx := r.Context()
	precision := r.URL.Query().Get("precision")
	if precision != "" {
		ctx = context.WithValue(ctx, "precision", precision)
	}

	if err != nil {
		return err
	}

	// Handle gzip compression
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(r.Body)
		if err != nil {
			return err
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	res, err := parser.ParseReader(ctx, reader)
	if err != nil {
		return err
	}
	var promises []utils.Promise[int32]
	for _res := range res {
		if _res.Error != nil {
			go func() {
				for range res {
				}
			}()
			return _res.Error
		}
		_database := database
		if _database == "" {
			database = _res.Database
		}
		promises = append(promises, repository.Store(_database, _res.Table, _res.Data))
	}
	for _, p := range promises {
		_, err = p.Get()
		if err != nil {
			return err
		}
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
