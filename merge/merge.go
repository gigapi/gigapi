package merge

import (
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/handlers"
	"github.com/gigapi/gigapi/v2/merge/repository"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/gigapi/metadata"
	"os"
	"path"
)

func Init(api modules.Api) {
	if config.Config.Gigapi.Mode != "writeonly" && config.Config.Gigapi.Mode != "aio" {
		return
	}
	metadata.MergeConfigurations = nil
	for _, mc := range shared.GetMergeConfigurations() {
		metadata.MergeConfigurations = append(metadata.MergeConfigurations, mc)
	}
	err := os.MkdirAll(path.Join(config.Config.Gigapi.Root, "_tmp"), 0750)
	if err != nil {
		panic(err)
	}
	conn, cancel, err := utils.ConnectDuckDB(utils.MEMDB_ACCESS_STRING)
	if err != nil {
		panic(err)
	}
	defer cancel()

	_, err = conn.Exec("INSTALL json; LOAD json;")
	if err != nil {
		panic(err)
	}

	err = repository.CreateDuckDBTablesTable(conn)
	if err != nil {
		panic(err)
	}

	err = repository.InitRegistry(conn)
	if err != nil {
		panic(err)
	}

	InitHandlers(api)
}

func InitHandlers(api modules.Api) {
	handlers.API = api
	api.RegisterRoute(&modules.Route{
		Path:    "/query",
		Methods: []string{"POST", "OPTIONS"},
		Handler: handlers.Query,
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/gigapi/insert",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/gigapi/writer/internal",
		Methods: []string{"GET"},
		Handler: handlers.GetInternal,
	})
	/*api.RegisterRoute(&modules.Route{
		Path:    "/gigapi/write/{db}",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/gigapi/write",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})

	// InfluxDB 2+3 compatibility endpoints
	api.RegisterRoute(&modules.Route{
		Path:    "/write",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/api/v2/write",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/api/v3/write_lp",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	api.RegisterRoute(&modules.Route{
		Path:    "/health",
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
		Path:    "/ping",
		Methods: []string{"GET"},
		Handler: func(w http.ResponseWriter, r *http.Request) error {
			w.WriteHeader(http.StatusNoContent)
			return nil
		},
	})*/
}
