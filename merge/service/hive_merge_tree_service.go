package service

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/gigapi/metadata"
	"github.com/go-faster/city"
	"golang.org/x/sync/errgroup"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func equals(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}

	switch va.Kind() {
	case reflect.Bool:
		return va.Bool() == vb.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return va.Int() == vb.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return va.Uint() == vb.Uint()
	case reflect.Float32, reflect.Float64:
		return va.Float() == vb.Float()
	case reflect.Complex64, reflect.Complex128:
		return va.Complex() == vb.Complex()
	case reflect.String:
		return va.String() == vb.String()
	case reflect.Ptr, reflect.Interface:
		return equals(va.Elem().Interface(), vb.Elem().Interface())
	}

	// Handle time.Time comparison
	if ta, ok := a.(time.Time); ok {
		if tb, ok := b.(time.Time); ok {
			return ta.Equal(tb)
		}
	}

	return reflect.DeepEqual(a, b)
}

func hash(v any) uint64 {
	if v == nil {
		return 0
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool:
		if rv.Bool() {
			return city.Hash64([]byte{1})
		}
		return city.Hash64([]byte{0})
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return city.Hash64(int64ToBytes(rv.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return city.Hash64(uint64ToBytes(rv.Uint()))
	case reflect.Float32:
		return city.Hash64(float32ToBytes(float32(rv.Float())))
	case reflect.Float64:
		return city.Hash64(float64ToBytes(rv.Float()))
	case reflect.Complex64:
		c := rv.Complex()
		return city.Hash64(append(float32ToBytes(float32(real(c))), float32ToBytes(float32(imag(c)))...))
	case reflect.Complex128:
		c := rv.Complex()
		return city.Hash64(append(float64ToBytes(real(c)), float64ToBytes(imag(c))...))
	case reflect.String:
		return city.Hash64([]byte(rv.String()))
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return 0
		}
		return hash(rv.Elem().Interface())
	}

	// Handle time.Time
	if t, ok := v.(time.Time); ok {
		return city.Hash64(int64ToBytes(t.UnixNano()))
	}

	// For unsupported types, use reflection to get a string representation
	return city.Hash64([]byte(fmt.Sprintf("%v", v)))
}

func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func float32ToBytes(f float32) []byte {
	return uint32ToBytes(math.Float32bits(f))
}

func float64ToBytes(f float64) []byte {
	return uint64ToBytes(math.Float64bits(f))
}

func uint32ToBytes(i uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, i)
	return b
}

type HiveMergeTreeService struct {
	*MergeTreeService

	partitions map[string]map[uint64]*Partition

	storeTicker *time.Ticker
	mergeTicker *time.Ticker

	flushCtx context.Context
	doFlush  context.CancelFunc

	mergeService map[string]mergeService
	moveService  map[string]*moveService
	dropService  map[string]*dropService
}

func buildPath(layer config.LayersConfiguration, t *shared.Table, suffix string) (string, error) {
	if strings.Contains("\\", t.Database) || strings.Contains("/", t.Database) ||
		strings.Contains("..", t.Database) {
		return "", fmt.Errorf("invalid database name: %q", t.Database)
	}
	if strings.Contains("\\", t.Name) || strings.Contains("/", t.Name) ||
		strings.Contains("..", t.Name) {
		return "", fmt.Errorf("invalid table name: %q", t.Name)
	}
	path := filepath.Join(strings.TrimPrefix(layer.URL, "file://"), t.Database, t.Name, suffix)
	return path, nil
}

func NewHiveMergeTreeService(t *shared.Table) (*HiveMergeTreeService, error) {
	res := &HiveMergeTreeService{
		MergeTreeService: &MergeTreeService{
			Table: t,
		},
		partitions:   make(map[string]map[uint64]*Partition),
		mergeService: make(map[string]mergeService),
		moveService:  make(map[string]*moveService),
		dropService:  make(map[string]*dropService),
	}
	for _, l := range config.Config.Gigapi.Layers {
		dataPath, err := buildPath(l, t, "data")
		if err != nil {
			return nil, err
		}
		tmpPath, err := buildPath(l, t, "tmp")
		if err != nil {
			return nil, err
		}

		os.MkdirAll(dataPath, 0o755)
		os.MkdirAll(tmpPath, 0o755)
		switch l.Type {
		case "fs":
			res.mergeService[l.Name], err = newFsMergeService(l, t)
		case "s3":
			res.mergeService[l.Name], err = newS3MergeService(l, t)
		default:
			return nil, fmt.Errorf("unsupported layer type: %q", l.Type)
		}
		if err != nil {
			return nil, err
		}

		res.moveService[l.Name] = &moveService{
			database: t.Database,
			table:    t.Name,
			layer:    l,
			t:        t,
			writerId: "",
		}
		res.dropService[l.Name], err = newDropService(l, t)
		if err != nil {
			return nil, err
		}
	}

	res.flushCtx, res.doFlush = context.WithTimeout(context.Background(), time.Second)
	/*err := res.discoverPartitions()
	if err != nil {
		return nil, err
	}*/
	//err := res.parsePartitionInfo()
	return res, nil
}

type ExprParserHelper struct {
	Identifiers []string
}

func (e *ExprParserHelper) Visit(node *ast.Node) {
	n, ok := (*node).(*ast.IdentifierNode)
	if !ok {
		return
	}
	ast.Patch(node, &ast.CallNode{
		Callee:    &ast.IdentifierNode{Value: "getValue"},
		Arguments: []ast.Node{&ast.StringNode{Value: n.String()}},
	})
	e.Identifiers = append(e.Identifiers, n.Value)
}

func (h *HiveMergeTreeService) parsePartitionExpression(expression [2]string) (*vm.Program, []string, error) {
	helper := ExprParserHelper{}
	prog, err := expr.Compile(expression[1], expr.Patch(&helper))
	if err != nil {
		return nil, nil, err
	}
	return prog, helper.Identifiers, nil
}

func (h *HiveMergeTreeService) Run() {
	for _, s := range h.moveService {
		s.Run()
	}
	for _, s := range h.dropService {
		s.Run()
	}
	go func() {
		for {
			select {
			case <-h.flushCtx.Done():
				h.flushCtx, h.doFlush = context.WithTimeout(context.Background(),
					time.Duration(config.Config.Gigapi.SaveTimeoutS)*time.Second)
				h.flush()
			}
		}
	}()
}

func (h *HiveMergeTreeService) flush() {
	wg := sync.WaitGroup{}
	for _, part := range h.partitions[config.Config.Gigapi.Layers[0].Name] {
		wg.Add(1)
		go func(part *Partition) {
			defer wg.Done()
			part.Save()
		}(part)
	}
	wg.Wait()
}

func (h *HiveMergeTreeService) Stop() {
	h.storeTicker.Stop()
}

func (h *HiveMergeTreeService) calculateSchema() map[string]string {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	schema := make(map[string]string)
	for _, part := range h.partitions[config.Config.Gigapi.Layers[0].Name] {
		for c, tp := range part.GetSchema() {
			schema[c] = tp
		}
	}
	return schema
}

func (h *HiveMergeTreeService) validateData(columns map[string]data_types.IColumn) error {
	err := h.validateColSizes(columns)
	if err != nil {
		return err
	}

	schema := h.calculateSchema()
	for name, col := range columns {
		if _, ok := schema[name]; !ok {
			continue
		}
		//TODO: check how merge operation in parquet works for column collision
		//TODO: log this failure well
		//TODO: if the merge operation fails because of this then consider the eralier type as "right" and later type as "wrong"
		//TODO: move the "wrong" batches elsewhere
		if col.GetTypeName() != schema[name] {
			return fmt.Errorf("column %s has different data type", name)
		}
	}
	return nil
}

func (h *HiveMergeTreeService) calculatePartitionHash(values [][2]string) uint64 {
	valuesHashes := make([]uint64, len(values))
	for i, v := range values {
		valuesHashes[i] = hash(v[1])
	}
	return city.CH64(unsafe.Slice((*byte)(unsafe.Pointer(&valuesHashes[0])), len(valuesHashes)*8))
}

func (h *HiveMergeTreeService) getPartPath(values [][2]string) string {
	var p []string
	for _, v := range values {
		p = append(p, fmt.Sprintf("%s=%v", v[0], v[1]))
	}
	return path.Join(p...)
}

func (h *HiveMergeTreeService) Store(columns map[string]any) utils.Promise[int32] {
	_columns, err := h.wrapColumns(columns)
	if err != nil {
		return utils.Fulfilled[int32](err, 0)
	}

	err = h.validateData(_columns)
	if err != nil {
		return utils.Fulfilled[int32](err, 0)
	}

	_columns, err = h.AutoTimestamp(_columns)
	if err != nil {
		return utils.Fulfilled[int32](err, 0)
	}

	//TODO: copy data to partitions right away
	partsDesc, err := h.Table.PartitionBy(_columns)
	if err != nil {
		return utils.Fulfilled[int32](err, 0)
	}

	var promises []utils.Promise[int32]
	h.mtx.Lock()
	defer h.mtx.Unlock()
	if h.partitions[config.Config.Gigapi.Layers[0].Name] == nil {
		h.partitions[config.Config.Gigapi.Layers[0].Name] = make(map[uint64]*Partition)
	}
	for _, part := range partsDesc {
		id := h.calculatePartitionHash(part.Values)
		if _, ok := h.partitions[config.Config.Gigapi.Layers[0].Name][id]; !ok {
			h.partitions[config.Config.Gigapi.Layers[0].Name][id], err = NewPartition(part.Values,
				config.Config.Gigapi.Layers[0],
				h.Table)
			if err != nil {
				h.mtx.Unlock()
				return utils.Fulfilled[int32](err, 0)
			}
		}
	}

	for _, part := range partsDesc {
		id := h.calculatePartitionHash(part.Values)
		promises = append(promises, h.partitions[config.Config.Gigapi.Layers[0].Name][id].
			StoreByMask(_columns, part.IndexMap))
	}

	s := int64(0)
	for _, p := range h.partitions[config.Config.Gigapi.Layers[0].Name] {
		s += p.Size()
	}
	//TODO: add the configuration for max row limit before flush
	if s > 1000000 {
		h.doFlush()
	}

	return utils.NewWaitForAll(promises)
}

func (h *HiveMergeTreeService) PlanMerge() (map[string][]metadata.MergePlan, error) {
	configurations := getMergeConfigurations()
	res := make(map[string][]metadata.MergePlan)
	for _, conf := range configurations {
		for _, l := range config.Config.Gigapi.Layers {
			_res, err := h.planMergeIteration(l, conf)
			if err != nil {
				return nil, err
			}
			res[l.Name] = append(res[l.Name], _res...)
		}
	}
	return res, nil
}

func (h *HiveMergeTreeService) planMergeIteration(layer config.LayersConfiguration,
	conf [3]int64) ([]metadata.MergePlan, error) {
	if time.Now().Sub(h.lastIterationTime[conf[2]-1]).Seconds() < float64(conf[0]) {
		return nil, nil
	}
	var res []metadata.MergePlan
	for i := 0; i < 5; i++ {
		plan, err := h.Table.Index.GetMergePlanner().GetMergePlan("", layer.Name, int(conf[2]))
		if err != nil {
			return nil, err
		}
		if len(plan.From) == 0 {
			break
		}
		res = append(res, plan)
	}
	return res, nil
}

func (h *HiveMergeTreeService) Merge(plan map[string][]metadata.MergePlan) error {
	fmt.Println("Starting merges...")
	start := time.Now()
	eg := &errgroup.Group{}
	for layer, plans := range plan {
		_layer := layer
		_plans := plans
		eg.Go(func() error {
			err := h.mergeService[_layer].DoMerge(_plans)
			return err
		})
	}
	err := eg.Wait()
	fmt.Printf("Merge time: %v\n", time.Since(start))
	return err
}

func (h *HiveMergeTreeService) DoMerge() error {
	plan, err := h.PlanMerge()
	if err != nil {
		return err
	}
	return h.Merge(plan)
}

type mtHiveStoreReq struct {
	data map[string]any
	res  chan utils.Promise[int32]
}

type MultithreadHiveMergeTreeService struct {
	svcs    []*HiveMergeTreeService
	channel chan *mtHiveStoreReq
}

func NewMultithreadHiveMergeTreeService(numThreads int, t *shared.Table) *MultithreadHiveMergeTreeService {
	if numThreads <= 0 {
		numThreads = runtime.NumCPU()
	}
	m := &MultithreadHiveMergeTreeService{
		channel: make(chan *mtHiveStoreReq, numThreads),
	}
	for i := 0; i < numThreads; i++ {
		h, _ := NewHiveMergeTreeService(t)
		m.svcs = append(m.svcs, h)

		go func() {
			for _c := range m.channel {
				_c.res <- h.Store(_c.data)
			}
		}()
	}
	return m
}

func (m *MultithreadHiveMergeTreeService) Run() {
	for _, _m := range m.svcs {
		_m.Run()
	}
}

func (m *MultithreadHiveMergeTreeService) Stop() {
	for _, _m := range m.svcs {
		_m.Stop()
	}
	close(m.channel)
}

func (m *MultithreadHiveMergeTreeService) Store(columns map[string]any) utils.Promise[int32] {
	req := &mtHiveStoreReq{
		data: columns,
		res:  make(chan utils.Promise[int32]),
	}
	defer close(req.res)
	m.channel <- req
	return <-req.res
}

func (m *MultithreadHiveMergeTreeService) DoMerge() error {
	return m.svcs[0].DoMerge()
}

func (m *MultithreadHiveMergeTreeService) GetTable() *shared.Table {
	return m.svcs[0].Table
}
