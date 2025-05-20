package service

import (
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Partition struct {
	Values            [][2]string
	index             shared.Index
	unordered         *unorderedDataStore
	saveService       saveService
	mergeService      mergeService
	promises          []utils.Promise[int32]
	m                 sync.Mutex
	table             *shared.Table
	lastStore         time.Time
	lastSave          time.Time
	lastIterationTime [MERGE_ITERATIONS]time.Time
	dataPath          string
}

func NewPartition(values [][2]string, tmpPath, dataPath string, t *shared.Table) (*Partition, error) {
	res := &Partition{
		Values:    values,
		unordered: newUnorderedDataStore(),
		table:     t,
		dataPath:  dataPath,
	}
	for i := range res.lastIterationTime {
		res.lastIterationTime[i] = time.Now()
	}
	if t.IndexCreator != nil {
		var err error
		res.index, err = t.IndexCreator(values)
		if err != nil {
			return nil, err
		}
		dropQueue := res.index.GetDropQueue()
		go func() {
			time.Sleep(time.Second * 10)
			for _, file := range dropQueue {
				os.Remove(filepath.Join(res.dataPath, file))
			}
			res.index.RmFromDropQueue(dropQueue)
		}()
	}
	err := res.initServices(tmpPath, dataPath, t)
	return res, err
}

func (p *Partition) initServices(tmpPath, dataPath string, t *shared.Table) error {
	err := os.MkdirAll(tmpPath, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(dataPath, 0755)
	if err != nil {
		return err
	}

	p.saveService = &fsSaveService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
	}
	p.mergeService = &fsMergeService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
		table:    t,
		index:    p.index,
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

	var minTime int64
	var maxTime int64

	if col, ok := unordered.store["__timestamp"]; ok {
		_minTime, _maxTime := col.GetMinMax()
		if _, ok := _minTime.(int64); ok {
			minTime, maxTime = _minTime.(int64), _maxTime.(int64)
		}
	}

	if p.index != nil {
		absDataPath, err := filepath.Abs(fName)
		if err != nil {
			onErr(err)
			return
		}
		stat, err := os.Stat(absDataPath)
		if err != nil {
			onErr(err)
			return
		}

		size := unordered.GetSize()

		prom := p.index.Batch([]*shared.IndexEntry{{
			Path:      absDataPath,
			SizeBytes: stat.Size(),
			RowCount:  size,
			ChunkTime: time.Now().UnixNano(),
			MinTime:   minTime,
			MaxTime:   maxTime,
		}}, nil)
		_, err = prom.Get()
		if err != nil {
			onErr(err)
			return
		}
	}
	onErr(nil)
}

func (p *Partition) PlanMerge() ([]PlanMerge, error) {
	var res []PlanMerge

	configurations := shared.GetMergeConfigurations()
	for _, conf := range configurations {
		if time.Now().Sub(p.lastIterationTime[conf[2]-1]).Seconds() > float64(conf[0]) {
			files, err := p.mergeService.GetFilesToMerge(int(conf[2]))
			if err != nil {
				return nil, err
			}
			plans := p.mergeService.PlanMerge(files, conf[1], int(conf[2]))
			res = append(res, plans...)
			p.lastIterationTime[conf[2]-1] = time.Now()
		}
	}
	return res, nil
}

func (p *Partition) DoMerge(plan []PlanMerge) error {
	return p.mergeService.DoMerge(plan)
}
