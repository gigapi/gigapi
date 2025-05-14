package service

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/go-faster/city"
	"golang.org/x/sync/errgroup"
	"io/fs"
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

	partitions map[uint64]*Partition

	storeTicker *time.Ticker
	mergeTicker *time.Ticker

	flushCtx context.Context
	doFlush  context.CancelFunc
}

func NewHiveMergeTreeService(t *shared.Table) (*HiveMergeTreeService, error) {
	res := &HiveMergeTreeService{
		MergeTreeService: &MergeTreeService{
			Table: t,
		},
		partitions: make(map[uint64]*Partition),
	}
	res.flushCtx, res.doFlush = context.WithTimeout(context.Background(), time.Second)
	err := res.discoverPartitions()
	if err != nil {
		return nil, err
	}
	//err := res.parsePartitionInfo()
	return res, nil
}

func (h *HiveMergeTreeService) discoverPartitions() error {
	lastSuffix := fmt.Sprintf(".%d.parquet", MERGE_ITERATIONS+1)
	err := filepath.Walk(h.Table.Path, func(p string, info fs.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}
		_, err = os.Stat(path.Join(p, "metadata.json"))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}

		isLivePartition := false
		entries, err := os.ReadDir(p)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".parquet") &&
				!strings.HasSuffix(entry.Name(), lastSuffix) {
				isLivePartition = true
			}
		}

		strPartitionPath := strings.TrimPrefix(p, h.Table.Path+string(filepath.Separator))
		if !isLivePartition {
			return filepath.SkipDir
		}
		arrPartitionPath := strings.Split(strPartitionPath, string(filepath.Separator))
		values := make([][2]string, 0, len(arrPartitionPath))
		for _, p := range arrPartitionPath {
			kv := strings.SplitN(p, "=", 2)
			if len(kv) < 2 {
				fmt.Println("Invalid partition path: " + strPartitionPath)
				return nil
			}
			values = append(values, [2]string{kv[0], kv[1]})
		}
		id := h.calculatePartitionHash(values)
		if _, ok := h.partitions[id]; !ok {
			h.partitions[id], err = NewPartition(values,
				path.Join(h.Table.Path, "tmp"),
				h.getDataPath(values),
				h.Table)
			if err != nil {
				return err
			}
		}
		return filepath.SkipDir
	})
	return err
}

/*func (h *HiveMergeTreeService) parsePartitionInfo() error {
	h.partitionExressions = make([]*vm.Program, len(h.Table.PartitionBy))
	idents := make(map[string]bool)

	for i, partition := range h.Table.PartitionBy {
		prog, identifiers, err := h.parsePartitionExpression(partition)
		if err != nil {
			return err
		}
		h.partitionExressions[i] = prog
		for _, id := range identifiers {
			idents[id] = true
		}
	}

	h.requiredColumns = make([]string, 0, len(idents))
	for id := range idents {
		h.requiredColumns = append(h.requiredColumns, id)
	}
	return nil
}*/

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
	for _, part := range h.partitions {
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
	for _, part := range h.partitions {
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

func (h *HiveMergeTreeService) getDataPath(values [][2]string) string {
	p := []string{h.Table.Path}
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
	for _, part := range partsDesc {
		id := h.calculatePartitionHash(part.Values)
		if _, ok := h.partitions[id]; !ok {
			h.partitions[id], err = NewPartition(part.Values,
				path.Join(h.Table.Path, "tmp"),
				h.getDataPath(part.Values),
				h.Table)
			if err != nil {
				h.mtx.Unlock()
				return utils.Fulfilled[int32](err, 0)
			}
		}
	}

	for _, part := range partsDesc {
		id := h.calculatePartitionHash(part.Values)
		promises = append(promises, h.partitions[id].StoreByMask(_columns, part.IndexMap))
	}

	s := int64(0)
	for _, p := range h.partitions {
		s += p.Size()
	}
	//TODO: add the configuration for max row limit before flush
	if s > 1000000 {
		h.doFlush()
	}
	h.mtx.Unlock()

	return utils.NewWaitForAll(promises)
}

func (h *HiveMergeTreeService) PlanMerge() (map[uint64][]PlanMerge, error) {
	mergeByPartition := make(map[uint64][]PlanMerge)
	for id, part := range h.partitions {
		plan, err := part.PlanMerge()
		if err != nil {
			return nil, err
		}
		mergeByPartition[id] = append(mergeByPartition[id], plan...)
	}
	return mergeByPartition, nil
}

func (h *HiveMergeTreeService) Merge(plan map[uint64][]PlanMerge) error {
	errGroup := errgroup.Group{}
	fmt.Println("Starting merges...")
	start := time.Now()
	for id, merges := range plan {
		_id := id
		_merges := merges
		errGroup.Go(func() error {
			if part, ok := h.partitions[_id]; ok {
				err := part.DoMerge(_merges)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err := errGroup.Wait()
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
	partitions := map[uint64]*Partition{}
	for _, _m := range m.svcs {
		for id, part := range _m.partitions {
			partitions[id] = part
		}
	}

	mergeByPartition := make(map[uint64][]PlanMerge)
	for id, part := range partitions {
		plan, err := part.PlanMerge()
		if err != nil {
			return err
		}
		mergeByPartition[id] = append(mergeByPartition[id], plan...)
	}
	wg := errgroup.Group{}
	for k, p := range mergeByPartition {
		_k := k
		_p := p
		wg.Go(func() error {
			return partitions[_k].DoMerge(_p)
		})
	}
	return wg.Wait()
}
