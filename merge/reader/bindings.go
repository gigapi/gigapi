package reader

import "C"
import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/metadata"
	"github.com/tliron/py4go"
	path2 "path"
	"runtime"
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
	m      sync.Mutex
	tasks  []func()
	next   context.Context
	doNext func()
	ctx    context.Context
	stop   func()
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

	p.inject, _ = foo.GetAttr("inject")
	p.defr(p.inject.Release)

	p.tables, _ = foo.GetAttr("tables")
	p.defr(p.tables.Release)

	p.next, p.doNext = context.WithCancel(context.Background())
	p.ctx, p.stop = context.WithCancel(context.Background())
	go p.Run()
}

func (p *py) Run() {
	runtime.LockOSThread()
	for true {
		select {
		case <-p.next.Done():
			p.m.Lock()
			_tasks := p.tasks
			p.tasks = nil
			p.next, p.doNext = context.WithCancel(context.Background())
			p.m.Unlock()
			for _, t := range _tasks {
				t()
			}
		case <-p.ctx.Done():
			break
		}
	}
	for _, c := range p.cancel {
		c()
	}
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

type resS[T any] struct {
	res T
	err error
}

type bndResponse[T any] struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Result T      `json:"result,omitempty"`
}

func bindResponse[T any](res string) (T, error) {
	var r bndResponse[T]
	err := json.Unmarshal([]byte(res), &r)
	var val T
	if err != nil {
		return val, fmt.Errorf("failed to parse JSON: %w", err)
	}
	if r.Error != "" {
		return val, fmt.Errorf(r.Error)
	}
	return r.Result, nil
}

func (p *py) Inject(query string, metadata []*metadata.IndexEntry) (string, error) {
	c := make(chan resS[string])
	defer close(c)
	p.m.Lock()
	p.tasks = append(p.tasks, func() {
		_res, err := p._inject(query, metadata)
		c <- resS[string]{_res, err}
	})
	p.doNext()
	p.m.Unlock()
	_res := <-c
	return bindResponse[string](_res.res)
}

func (p *py) Tables(query string) ([]string, error) {
	c := make(chan resS[string])
	defer close(c)
	p.m.Lock()
	p.tasks = append(p.tasks, func() {
		_res, err := p._tables(query)
		c <- resS[string]{_res, err}
	})
	p.doNext()
	p.m.Unlock()
	_res := <-c
	return bindResponse[[]string](_res.res)
}

func customMarshalBinds(binds []bindMeta) ([]byte, error) {
	var result []byte

	for _, bind := range binds {
		// Convert path to bytes
		pathBytes := []byte(bind.Path)

		// Path length (int16 little endian)
		pathLenBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(pathLenBytes, uint16(len(pathBytes)))
		result = append(result, pathLenBytes...)

		// Path string
		result = append(result, pathBytes...)

		// MinTime (int64 little endian)
		minTimeBytes := make([]byte, 8)
		minTime, ok := bind.Min.(int64)
		if !ok {
			return nil, fmt.Errorf("MinTime is not int64")
		}
		binary.LittleEndian.PutUint64(minTimeBytes, uint64(minTime))
		result = append(result, minTimeBytes...)

		// MaxTime (int64 little endian)
		maxTimeBytes := make([]byte, 8)
		maxTime, ok := bind.Max.(int64)
		if !ok {
			return nil, fmt.Errorf("MaxTime is not int64")
		}
		binary.LittleEndian.PutUint64(maxTimeBytes, uint64(maxTime))
		result = append(result, maxTimeBytes...)
	}

	return result, nil
}

func (p *py) _inject(query string, metadata []*metadata.IndexEntry) (string, error) {
	p.m.Lock()
	defer p.m.Unlock()

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
	strMeta, err := customMarshalBinds(binds) // json.Marshal(binds)
	if err != nil {
		return "", err
	}
	state := python.EnsureGilState()
	defer state.Release()
	res, err := p.inject.Call(query, strMeta)
	if err != nil {
		return "", err
	}
	defer res.Release()
	str := res.String()
	return str, nil
}

func (p *py) _tables(query string) (string, error) {
	p.m.Lock()
	defer p.m.Unlock()
	state := python.EnsureGilState()
	defer state.Release()
	res, err := p.tables.Call(query)
	if err != nil {
		return "", err
	}
	defer res.Release()

	return res.String(), nil
}

func (p *py) Destroy() {
	p.stop()
}
