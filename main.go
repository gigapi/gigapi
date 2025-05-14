package main

import (
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi-querier/module"
	"github.com/gigapi/gigapi/v2/merge"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/gigapi/gigapi/v2/router"
	"github.com/gigapi/gigapi/v2/stdin"
	"net/http"
)

type api struct {
}

func (a api) RegisterRoute(r *modules.Route) {
	router.RegisterRoute(r)
}

func (a api) GetPathParams(r *http.Request) map[string]string {
	return router.GetPathParams(r)
}

func initModules() {
	stdin.Init()
	merge.Init(&api{})
	module.Init(&api{})
}

func main() {
	config.InitConfig("")
	initModules()
	r := router.NewRouter()
	fmt.Printf("GigAPI Running: %s:%d\n", config.Config.Host, config.Config.Port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", config.Config.Host, config.Config.Port), r); err != nil {
		panic(err)
	}
}
