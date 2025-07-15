package handlers

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi/v2/merge/repository"
	utils2 "github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/jmoiron/sqlx"
	"io"
	"net/http"
	"strconv"
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
	if strings.Contains(db, "/") || strings.Contains(db, ".") || strings.Contains(db, "\\") {
		return "", utils.NewGigapiError("Invalid database name", http.StatusBadRequest)
	}

	return db, nil
}

func InsertIntoHandler(w http.ResponseWriter, r *http.Request) error {
	contentType := r.Header.Get("Content-Type")

	table := r.URL.Query().Get("table")
	if table == "" {
		return utils.NewGigapiError("Missing table parameter", http.StatusBadRequest)
	}

	if contentType != "application/json" {
		return utils.NewGigapiError("Invalid content type", http.StatusBadRequest)
	}

	//ctx := r.Context()

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

	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	p := repository.Store("", table, body)

	_, err = p.Get()
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
}

type QueryRequest struct {
	Query string `json:"query"`
	DB    string `json:"db,omitempty"`
}

func Query(w http.ResponseWriter, r *http.Request) error {
	dbName := r.URL.Query().Get("db")
	if dbName == "" {
		//TODO
		dbName = "my_ducklake"
	}

	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(r.Body)
		if err != nil {
			return err
		}
		defer gzipReader.Close()
		reader = gzipReader
	}
	query, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	format := r.URL.Query().Get("format")
	var req QueryRequest
	err = json.Unmarshal(query, &req)
	if err != nil {
		return err
	}
	db, cancel, err := utils2.ConnectDucklake(dbName)
	if err != nil {
		return err
	}
	defer cancel()

	dbx := sqlx.NewDb(db, "duckdb")

	rows, err := dbx.Queryx(req.Query)
	if err != nil {
		return err
	}
	res := QueryResponse{}
	for rows.Next() {
		row := make(map[string]any)
		err = rows.MapScan(row)
		if err != nil {
			return err
		}
		res.Results = append(res.Results, row)
	}
	switch format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		return json.NewEncoder(w).Encode(&res)
	case "ndjson":
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		for i := range res.Results {
			b, err := json.Marshal(res.Results[i])
			if err != nil {
				return err
			}
			w.Write(b)
			w.Write([]byte("\n"))
		}
	}
	return nil
}

func GetInternal(w http.ResponseWriter, r *http.Request) error {
	fmt.Println("GET internal")
	id := r.URL.Query().Get("id")
	if id == "" {
		return utils.NewGigapiError("Missing id parameter", http.StatusBadRequest)
	}
	fmt.Println("id:", id)
	w.Header().Set("Content-Type", "application/octet-stream")
	rdr := utils2.GetInternal(id)
	if rdr == nil {
		return utils.NewGigapiError("Internal data not found", http.StatusNotFound)
	}
	_rdr := rdr.(io.ReadSeeker)

	// Get the size of the data
	size, err := _rdr.Seek(0, io.SeekEnd)
	if err != nil {
		return utils.NewGigapiError("Error determining data size", http.StatusInternalServerError)
	}
	_, err = _rdr.Seek(0, io.SeekStart)
	if err != nil {
		return utils.NewGigapiError("Error resetting reader", http.StatusInternalServerError)
	}

	// Handle Range header
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		ranges, err := parseRange(rangeHeader, size)
		if err != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
			return utils.NewGigapiError("Invalid range", http.StatusRequestedRangeNotSatisfiable)
		}
		if len(ranges) > 1 {
			// We don't support multiple ranges
			return utils.NewGigapiError("Multiple ranges not supported", http.StatusRequestedRangeNotSatisfiable)
		}
		r := ranges[0]
		_, err = _rdr.Seek(r.start, io.SeekStart)
		if err != nil {
			return utils.NewGigapiError("Error seeking to range start", http.StatusInternalServerError)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", r.start, r.end, size))
		w.WriteHeader(http.StatusPartialContent)
		_, err = io.CopyN(w, _rdr, r.end-r.start+1)
		if err != nil {
			return utils.NewGigapiError("Error writing partial content", http.StatusInternalServerError)
		}
	} else {
		// No range header, return full content
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		_, err = io.Copy(w, _rdr)
		if err != nil {
			return utils.NewGigapiError("Error writing content", http.StatusInternalServerError)
		}
	}

	return nil
}

type httpRange struct {
	start, end int64
}

func parseRange(s string, size int64) ([]httpRange, error) {
	if !strings.HasPrefix(s, "bytes=") {
		return nil, fmt.Errorf("invalid range")
	}
	var ranges []httpRange
	noOverlap := false
	for _, ra := range strings.Split(s[6:], ",") {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, fmt.Errorf("invalid range")
		}
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r httpRange
		if start == "" {
			// If no start is specified, end specifies the
			// range start relative to the end of the file.
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid range")
			}
			if i > size {
				i = size
			}
			r.start = size - i
			r.end = size - 1
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, fmt.Errorf("invalid range")
			}
			if i >= size {
				// If the range begins after the size of the content,
				// then it does not overlap.
				noOverlap = true
				continue
			}
			r.start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.end = size - 1
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					return nil, fmt.Errorf("invalid range")
				}
				if i >= size {
					i = size - 1
				}
				r.end = i
			}
		}
		ranges = append(ranges, r)
	}
	if noOverlap && len(ranges) == 0 {
		// The specified ranges did not overlap with the content.
		return nil, fmt.Errorf("invalid range")
	}
	return ranges, nil
}
