package service

import (
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/gigapi/metadata"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Partition struct {
	Values            [][2]string
	index             metadata.TableIndex
	unordered         *unorderedDataStore
	saveService       saveService
	promises          []utils.Promise[int32]
	m                 sync.Mutex
	table             *shared.Table
	lastStore         time.Time
	lastSave          time.Time
	lastIterationTime [MERGE_ITERATIONS]time.Time
	partPath          []string
	layer             config.LayersConfiguration
}

func NewPartition(values [][2]string, layer config.LayersConfiguration, t *shared.Table) (*Partition, error) {
	var partPath []string
	for _, v := range values {
		partPath = append(partPath, fmt.Sprintf("%s=%s", v[0], v[1]))
	}
	res := &Partition{
		Values:    values,
		unordered: newUnorderedDataStore(),
		table:     t,
		partPath:  partPath,
		layer:     layer,
	}
	for i := range res.lastIterationTime {
		res.lastIterationTime[i] = time.Now()
	}
	res.index = t.Index
	err := res.initServices(t, layer)
	return res, err
}

func (p *Partition) initServices(t *shared.Table, layer config.LayersConfiguration) error {
	var err error
	switch layer.Type {
	case "fs":
		p.saveService, err = newFsSaveService(layer, t)
	case "s3":
		p.saveService, err = newS3SaveService(layer, t)
	}
	return err
}

func (p *Partition) GetSchema() map[string]string {
	//TODO: create map[columnName]columnTypename
	return nil
}

func (p *Partition) StoreByMask(data map[string]data_types.IColumn, mask []byte) utils.Promise[int32] {
	p.m.Lock()
	defer p.m.Unlock()
	err := p.unordered.AppendByMask(data, mask)
	if err != nil {
		return utils.Fulfilled(err, int32(0))
	}
	res := utils.New[int32]()
	p.promises = append(p.promises, res)
	p.lastStore = time.Now()
	return res
}

func (p *Partition) Store(data map[string]data_types.IColumn) utils.Promise[int32] {
	p.m.Lock()
	defer p.m.Unlock()
	var err error
	err = p.unordered.AppendData(data)
	if err != nil {
		return utils.Fulfilled(err, int32(0))
	}
	res := utils.New[int32]()
	p.promises = append(p.promises, res)
	p.lastStore = time.Now()
	return res
}

func (p *Partition) Size() int64 {
	return p.unordered.GetSize()
}

func (p *Partition) Save() {
	p.m.Lock()
	promises := p.promises
	p.promises = nil
	unordered := p.unordered
	p.unordered = newUnorderedDataStore()
	p.lastSave = time.Now()
	p.m.Unlock()

	onErr := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}

	if len(promises) == 0 {
		return
	}
	fname := uuid.New().String() + ".1.parquet"
	relPath := p.saveService.Join(append(p.partPath, fname)...)

	//TODO: remove the logic of dynamic schema
	err := p.saveService.Save(mergeColumns(unordered), unordered, relPath)
	if err != nil {
		onErr(err)
		return
	}

	var minTime, maxTime any

	if col, ok := unordered.store[p.table.OrderBy[0]]; ok {
		minTime, maxTime = col.GetMinMax()
	}

	if p.index != nil {
		size, err := p.saveService.SizeB(relPath)
		if err != nil {
			onErr(err)
			return
		}

		rows := unordered.GetSize()

		prom := p.index.Batch([]*metadata.IndexEntry{{
			Layer:     config.Config.Gigapi.Layers[0].Name,
			Database:  p.table.Database,
			Table:     p.table.Name,
			Path:      relPath,
			SizeBytes: size,
			RowCount:  rows,
			ChunkTime: time.Now().UnixNano(),
			Min:       nil,
			Max:       nil,
			MinTime:   minTime.(int64),
			MaxTime:   maxTime.(int64),
			//TODO
			WriterID: "",
		}}, nil)
		_, err = prom.Get()
		if err != nil {
			onErr(err)
			return
		}
	}
	onErr(nil)
}
