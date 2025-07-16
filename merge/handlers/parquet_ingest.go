package handlers

import (
	"net/http"

	"github.com/gigapi/gigapi/v2/merge/parsers"
	"github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/utils"
)

func ParquetIngestHandler(w http.ResponseWriter, r *http.Request) error {
	parser, err := parsers.GetParser("parquet", nil, nil)
	if err != nil {
		return err
	}

	res, err := parser.ParseReader(r.Context(), r.Body)
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
		promises = append(promises, repository.Store(_res.Database, _res.Table, _res.Data, _res.IsExternal))
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
