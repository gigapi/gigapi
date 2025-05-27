package service

import (
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"github.com/gigapi/metadata"
	"os"
	"path/filepath"
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
	dataPath          string
	partPath          string
}

func NewPartition(values [][2]string, tmpPath, dataPath, partPath string, t *shared.Table) (*Partition, error) {
	res := &Partition{
		Values:    values,
		unordered: newUnorderedDataStore(),
		table:     t,
		dataPath:  dataPath,
		partPath:  partPath,
	}
	for i := range res.lastIterationTime {
		res.lastIterationTime[i] = time.Now()
	}
	res.index = t.Index
	err := res.initServices(tmpPath, dataPath, t)
	return res, err
}

func (p *Partition) initServices(tmpPath, dataPath string, t *shared.Table) error {
	err := os.MkdirAll(tmpPath, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Join(dataPath, p.partPath), 0755)
	if err != nil {
		return err
	}

	p.saveService = &fsSaveService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
		partPath: p.partPath,
	}
	return nil
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
	//TODO: remove the logic of dynamic schema
	fName, err := p.saveService.Save(mergeColumns(unordered), unordered)
	if err != nil {
		onErr(err)
		return
	}

	var minTime, maxTime any

	if col, ok := unordered.store[p.table.OrderBy[0]]; ok {
		minTime, maxTime = col.GetMinMax()
	}

	if p.index != nil {
		absDataPath := filepath.Join(p.dataPath, fName)
		stat, err := os.Stat(absDataPath)
		if err != nil {
			onErr(err)
			return
		}

		size := unordered.GetSize()

		prom := p.index.Batch([]*metadata.IndexEntry{{
			Database:  p.table.Database,
			Table:     p.table.Name,
			Path:      fName,
			SizeBytes: stat.Size(),
			RowCount:  size,
			ChunkTime: time.Now().UnixNano(),
			Min:       nil,
			Max:       nil,
			MinTime:   minTime.(int64),
			MaxTime:   maxTime.(int64),
		}}, nil)
		_, err = prom.Get()
		if err != nil {
			onErr(err)
			return
		}
	}
	onErr(nil)
}
