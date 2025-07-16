package handlers

import (
	"github.com/gigapi/gigapi/v2/modules"
	"net/http"
)

func InitRoutes(api modules.Api) {
	api.RegisterRoute(&modules.Route{
		Path:	"/gigapi/ingest/parquet",
		Methods: []string{http.MethodPost},
		Handler: ParquetIngestHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/api/v1/insert/{db}",
		Methods: []string{http.MethodPost},
		Handler: InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/api/v1/insert",
		Methods: []string{http.MethodPost},
		Handler: InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/gigapi/insert",
		Methods: []string{"POST"},
		Handler: InsertIntoHandler,
	})

	api.RegisterRoute(&modules.Route{
		Path:	"/gigapi/write/{db}",
		Methods: []string{"POST"},
		Handler: InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/gigapi/write",
		Methods: []string{"POST"},
		Handler: InsertIntoHandler,
	})

	// InfluxDB 2+3 compatibility endpoints
	api.RegisterRoute(&modules.Route{
		Path:	"/write",
		Methods: []string{"POST"},
		Handler: InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/api/v2/write",
		Methods: []string{"POST"},
		Handler: InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/api/v3/write_lp",
		Methods: []string{"POST"},
		Handler: InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/health",
		Methods: []string{"GET"},
		Handler: func(w http.ResponseWriter, r *http.Request) error {
			response := `{"checks": [], "commit": "null-commit", "message": "Service is healthy", "name": "GigAPI", "status": "pass", "version": "0.0.0"}`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(response + "\n"))
			return nil
		},
	})
	api.RegisterRoute(&modules.Route{
		Path:	"/ping",
		Methods: []string{"GET"},
		Handler: func(w http.ResponseWriter, r *http.Request) error {
			w.WriteHeader(http.StatusNoContent)
			return nil
		},
	})
}
