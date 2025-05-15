package router

import (
	"errors"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/gorilla/mux"
	"net/http"
)

type handlerFn func(w http.ResponseWriter, r *http.Request) error
type middlewareFn func(fn handlerFn) handlerFn

type MiddlewareApply struct {
	handler    handlerFn
	middleware []middlewareFn
}

func (m *MiddlewareApply) withErrorHandle() *MiddlewareApply {
	m.middleware = append(m.middleware, func(hndl handlerFn) handlerFn {
		return func(w http.ResponseWriter, r *http.Request) error {
			err := hndl(w, r)
			if err == nil {
				return nil
			}
			var gigapiErr utils.IGigapiError
			if errors.As(err, &gigapiErr) {
				w.WriteHeader(gigapiErr.Code())
				w.Write([]byte(gigapiErr.Error()))
				return nil
			}
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return nil
		}
	})
	return m
}

func (m *MiddlewareApply) withBasicAuth(username, password string) *MiddlewareApply {
	m.middleware = append(m.middleware, func(hndl handlerFn) handlerFn {
		return func(w http.ResponseWriter, r *http.Request) error {
			_username, _password, ok := r.BasicAuth()
			if !ok || _username != username || _password != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="gigapi"`)
				w.WriteHeader(401)
				return nil
			}
			return hndl(w, r)
		}
	})
	return m
}

func (m *MiddlewareApply) copy() *MiddlewareApply {
	return &MiddlewareApply{
		handler:    m.handler,
		middleware: append([]middlewareFn{}, m.middleware...),
	}
}

func (m *MiddlewareApply) Build() func(w http.ResponseWriter, r *http.Request) {
	hndl := m.handler
	for _, middleware := range m.middleware {
		hndl = middleware(hndl)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		hndl(w, r)
	}
}

var handlerRegistry []*modules.Route = nil

func RegisterRoute(r *modules.Route) {
	handlerRegistry = append(handlerRegistry, r)
}

func NewRouter() *mux.Router {
	router := mux.NewRouter()
	middleware := &MiddlewareApply{}
	if config.Config.HTTP.BasicAuth.Username != "" {
		middleware.withBasicAuth(config.Config.HTTP.BasicAuth.Username, config.Config.HTTP.BasicAuth.Password)
	}
	middleware.withErrorHandle()
	for _, r := range handlerRegistry {
		m := middleware.copy()
		m.handler = r.Handler
		router.HandleFunc(r.Path, m.Build()).Methods(r.Methods...)
	}
	return router
}

func GetPathParams(r *http.Request) map[string]string {
	return mux.Vars(r)
}
