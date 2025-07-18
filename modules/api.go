package modules

import "net/http"

type Api interface {
	RegisterRoute(r *Route)
	GetPathParams(r *http.Request) map[string]string
}

type Route struct {
	Path       string
	PathPrefix string
	Methods    []string
	Handler    func(w http.ResponseWriter, r *http.Request) error
}
