package reader

import "C"
import (
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/metadata"
	"github.com/tliron/py4go"
	path2 "path"
	"slices"
	"sync"
)

var (
	initPy = sync.OnceFunc(func() {
		_py = &py{}
		_py.Init()
	})
	_py *py
)

func getPy() *py {
	initPy()
	return _py
}

type py struct {
	inject *python.Reference
	tables *python.Reference
	cancel []func()
}

func noErr(f func() error) func() {
	return func() {
		f()
	}
}

func (p *py) defr(f func()) {
	p.cancel = append([]func(){f}, p.cancel...)
}

func (p *py) Init() {
	python.Initialize()
	p.defr(noErr(python.Finalize))
	python.SetPythonPath("/src")
	sys, _ := python.Import("sys")
	p.defr(sys.Release)

	path, _ := sys.GetAttr("path")
	p.defr(path.Release)
	p.cancel = append(p.cancel, path.Release)

	apnd, _ := path.GetAttr("append")
	p.defr(apnd.Release)

	r, _ := apnd.Call(".")
	defer r.Release()

	foo, _ := python.Import("merge.reader")
	p.defr(foo.Release)

	inject, _ := foo.GetAttr("inject")
	p.defr(inject.Release)

	tables, _ := foo.GetAttr("tables")
	p.defr(tables.Release)
}

type bindMeta struct {
	Path string `json:"path"`
	Min  any    `json:"min"`
	Max  any    `json:"max"`
}

func (p *py) computePath(m *metadata.IndexEntry) (string, error) {
	idx := slices.IndexFunc(config.Config.Gigapi.Layers, func(l config.LayersConfiguration) bool {
		return l.Name == m.Layer
	})
	if idx == -1 {
		return "", fmt.Errorf("layer %q not found", m.Layer)
	}
	switch config.Config.Gigapi.Layers[idx].Type {
	case "s3":
		return "", fmt.Errorf("s3 layers are not supported")
	case "fs":
		return path2.Join(config.Config.Gigapi.Layers[idx].URL[7:], m.Database, m.Table, "data", m.Path), nil
	}
	return "", fmt.Errorf("unsupported layer type: %q", config.Config.Gigapi.Layers[idx].Type)
}

func (p *py) Inject(query string, metadata []*metadata.IndexEntry) (string, error) {
	binds := make([]bindMeta, len(metadata))
	for i, m := range metadata {
		pth, err := p.computePath(m)
		if err != nil {
			return "", err
		}
		binds[i] = bindMeta{
			Path: pth,
			Min:  m.MinTime,
			Max:  m.MaxTime,
		}
	}
	strMeta, err := json.Marshal(binds)
	if err != nil {
		return "", err
	}
	res, err := p.inject.Call("inject", query, string(strMeta))
	if err != nil {
		return "", err
	}
	defer res.Release()
	return res.String(), nil
}

func (p *py) Tables(query string) ([]string, error) {
	res, err := p.tables.Call(query)
	if err != nil {
		return nil, err
	}
	defer res.Release()

	var tables []string
	err = json.Unmarshal([]byte(res.String()), &tables)
	if err != nil {
		return nil, err
	}
	return tables, nil
}
