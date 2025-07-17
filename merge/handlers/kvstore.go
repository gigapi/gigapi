package handlers

import (
	"github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/utils"
	"io"
	"net/http"
)

func addCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func KV(w http.ResponseWriter, r *http.Request) error {
	addCORSHeaders(w)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return nil
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		return utils.NewGigapiError("Key is required", http.StatusBadRequest)
	}

	switch r.Method {
	case http.MethodGet:
		val, err := repository.KV.Get(key)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "binary/octet-stream")
		w.Write(val)
		return nil
	case http.MethodPost:
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		err = repository.KV.Put(key, bodyBytes)
		if err != nil {
			return err
		}
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodDelete:
		err := repository.KV.Delete(key)
		if err != nil {
			return err
		}
		w.WriteHeader(http.StatusOK)
		return nil
	}
	return utils.NewGigapiError("Invalid method "+r.Method, http.StatusMethodNotAllowed)
}
