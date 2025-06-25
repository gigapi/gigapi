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
)

func Init(api modules.Api) {
	if config.Config.Gigapi.Mode != "writeonly" && config.Config.Gigapi.Mode != "aio" {
		return
	}
	metadata.MergeConfigurations = nil
	for _, mc := range shared.GetMergeConfigurations() {
		metadata.MergeConfigurations = append(metadata.MergeConfigurations, mc)
	}
	err := os.MkdirAll(config.Config.Gigapi.Root, 0750)
	if err != nil {
		panic(err)
	}
	conn, cancel, err := utils.ConnectDuckDB("?access_mode=READ_WRITE&allow_unsigned_extensions=1")
	if err != nil {
		panic(err)
	}
	defer cancel()

	repository.SetDB(conn)

	_, err = conn.Exec("INSTALL json; LOAD json; INSTALL httpfs; LOAD httpfs;")
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
	handlers.InitRoutes(api)
}
