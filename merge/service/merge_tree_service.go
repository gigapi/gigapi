package service

import (
	"errors"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	_ "github.com/marcboeker/go-duckdb/v2"
	url2 "net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type IMergeTree interface {
	Store(columns map[string][]any) error
	Merge() error
	Run()
	Stop()
}

type MergeTreeService struct {
	Table              *shared.Table
	ticker             *time.Ticker
	working            uint32
	promises           []utils.Promise[int32]
	mtx                sync.Mutex
	save               saveService
	merge              mergeService
	lastIterationTime  [MERGE_ITERATIONS]time.Time
	unorderedDataStore *unorderedDataStore

	less func(store any, i int32, j int32) bool
}

func NewMergeTreeService(t *shared.Table) (*MergeTreeService, error) {
	res := &MergeTreeService{
		Table:    t,
		working:  0,
		promises: nil,
	}
	res.unorderedDataStore = newUnorderedDataStore()
	var err error
	tablePath := t.Path
	if tablePath == "" {
		tablePath = filepath.Join(config.Config.Gigapi.Root, t.Name)
	}
	res.save, err = res.newSaveService(path.Join(tablePath, "data"), path.Join(tablePath, "tmp"))
	if err != nil {
		return nil, err
	}
	res.merge, err = res.newMergeService()
	for i := range res.lastIterationTime {
		res.lastIterationTime[i] = time.Now()
	}
	return res, err
}

func (s *MergeTreeService) newMergeService() (mergeService, error) {
	if strings.HasPrefix(s.Table.Path, "s3://") {
		return s.newS3MergeService()
	}
	return s.newFileMergeService()
}

func (s *MergeTreeService) newFileMergeService() (mergeService, error) {
	return &fsMergeService{
		dataPath: path.Join(s.Table.Path, "data"),
		tmpPath:  path.Join(s.Table.Path, "tmp"),
		table:    s.Table,
	}, nil
}

func (s *MergeTreeService) newS3MergeService() (mergeService, error) {
	s3Conf, err := s.getS3Config(s.Table.Path)
	if err != nil {
		return nil, err
	}
	return &s3MergeService{
		fsMergeService: fsMergeService{
			tmpPath: path.Join(config.Config.Gigapi.Root, s.Table.Name, "tmp"),
			table:   s.Table,
		},
		s3Config: s3Conf,
	}, nil
}

func (s *MergeTreeService) newSaveService(dataPath, tmpPath string) (saveService, error) {
	if strings.HasPrefix(dataPath, "s3://") {
		return s.newS3SaveService(dataPath)
	}
	return s.newFileSaveService(dataPath, tmpPath)
}

func (s *MergeTreeService) newFileSaveService(dataPath, tmpPath string) (saveService, error) {
	return &fsSaveService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
	}, nil
}

func (s *MergeTreeService) newS3SaveService(dataPath string) (saveService, error) {
	s3Conf, err := s.getS3Config(dataPath)
	if err != nil {
		return nil, err
	}
	res := &s3SaveService{
		fsSaveService: fsSaveService{
			dataPath: "",
			tmpPath:  path.Join(config.Config.Gigapi.Root, s.Table.Name, "tmp"),
		},
		s3Config: s3Conf,
	}
	return res, nil
}

func (s *MergeTreeService) getS3Config(path string) (s3Config, error) {
	url, err := url2.Parse(path)
	if err != nil {
		return s3Config{}, err
	}
	if url.Scheme != "s3" {
		return s3Config{}, errors.New("invalid S3 URL")
	}
	pass, _ := url.User.Password()
	bucketPath := strings.SplitN(strings.TrimPrefix(url.Path, "/"), "/", 2)
	secure := !(url.Query().Get("secure") == "false")
	region := ""
	if url.Query().Get("region") != "" {
		region = url.Query().Get("region")
	}
	return s3Config{
		url:    url.Host,
		key:    url.User.Username(),
		secret: pass,
		bucket: bucketPath[0],
		region: region,
		path:   bucketPath[1],
		secure: secure,
	}, nil
}

func (s *MergeTreeService) size() int64 {
	return s.unorderedDataStore.GetSize()
}

func validateData(dataStore dataStore, data map[string]data_types.IColumn) error {
	var (
		size int64
		i    int
	)
	for k, field := range data {
		if i == 0 {
			size = field.GetLength()
		} else if size != field.GetLength() {
			return fmt.Errorf("column %s size mismatch: expected %d rows, got %d rows",
				k, size, field.GetLength())
		}
		i++
	}

	return dataStore.VerifyData(data)
}

func mergeColumns(unordered dataStore) []fieldDesc {
	nameTypes := unordered.GetSchema()
	var res []fieldDesc
	for name, tp := range nameTypes {
		res = append(res, fd(tp, name))
	}
	return res
}

func (s *MergeTreeService) flush() {
	s.mtx.Lock()
	unorderedDataStore := s.unorderedDataStore
	s.unorderedDataStore = newUnorderedDataStore()
	promises := s.promises
	s.promises = nil
	s.mtx.Unlock()
	onError := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}
	if unorderedDataStore.GetSize() == 0 {
		onError(nil)
		return
	}
	go func() {
		_, err := s.save.Save(mergeColumns(unorderedDataStore), unorderedDataStore)
		onError(err)
	}()
}

func (s *MergeTreeService) Run() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if !atomic.CompareAndSwapUint32(&s.working, 0, 1) {
		return
	}
	go func() {
		s.ticker = time.NewTicker(time.Millisecond * time.Duration(config.Config.Gigapi.SaveTimeoutS*1000))
		for range s.ticker.C {
			s.flush()
		}
	}()
}

func (s *MergeTreeService) Stop() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.ticker != nil {
		s.ticker.Stop()
	}
	atomic.StoreUint32(&s.working, 0)
}

func fastFillArray[T any](arr []T, data T) []T {
	return data_types.FastFillArray(arr, data)
}

func (s *MergeTreeService) validateColSizes(columns map[string]data_types.IColumn) error {
	var size int64
	i := -1
	for k, col := range columns {
		i++
		if i == 0 {
			size = col.GetLength()
			continue
		}
		if size != col.GetLength() {
			return fmt.Errorf("column %s size mismatch: expected %d rows, got %d rows", k, size, col.GetLength())
		}
	}
	return nil
}

func (s *MergeTreeService) validateData(columns map[string]data_types.IColumn) error {
	err := s.validateColSizes(columns)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	return validateData(s.unorderedDataStore, columns)
}

func (s *MergeTreeService) wrapColumns(columns map[string]any) (map[string]data_types.IColumn, error) {
	_columns := make(map[string]data_types.IColumn, len(columns)+1)
	var err error
	for k, v := range columns {
		_columns[k], err = data_types.WrapToColumn(k, v)
		if err != nil {
			return nil, err
		}

	}
	return _columns, nil
}

func (s *MergeTreeService) AutoTimestamp(columns map[string]data_types.IColumn) (map[string]data_types.IColumn, error) {
	if !s.Table.AutoTimestamp {
		return columns, nil
	}

	var sz int64
	for _, col := range columns {
		sz = col.GetLength()
		break
	}

	tsData := make([]int64, sz)
	for i := range tsData {
		tsData[i] = time.Now().UnixNano()
	}

	tsCol, err := data_types.WrapToColumn("__timestamp", tsData)
	if err != nil {
		return nil, err
	}
	columns["__timestamp"] = tsCol
	return columns, nil
}

func (s *MergeTreeService) Store(columns map[string]any) utils.Promise[int32] {
	_columns, err := s.wrapColumns(columns)
	if err != nil {
		return utils.Fulfilled(err, int32(0))
	}

	err = s.validateData(_columns)
	if err != nil {
		return utils.Fulfilled(err, int32(0))
	}

	_columns, err = s.AutoTimestamp(_columns)
	if err != nil {
		return utils.Fulfilled(err, int32(0))
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var ds dataStore = s.unorderedDataStore
	err = ds.AppendData(_columns)
	if err != nil {
		return utils.Fulfilled(err, int32(0))
	}
	p := utils.New[int32]()
	s.promises = append(s.promises, p)
	return p
}

type PlanMerge struct {
	From      []string
	To        string
	Iteration int
}

type FileDesc struct {
	name string
	size int64
}

const MERGE_ITERATIONS = 4

// get merge configurations from the overall configuration
// Each merge configuration is [3]int64 array {timeout in seconds, max result bytes, iteration id}
func getMergeConfigurations() [][3]int64 {
	timeoutS := int64(config.Config.Gigapi.MergeTimeoutS)
	return [][3]int64{
		{timeoutS, 100 * 1024 * 1024, 1},
		{timeoutS * 10, 400 * 1024 * 1024, 2},
		{timeoutS * 100, 4000 * 1024 * 1024, 3},
		{timeoutS * 420, 4000 * 1024 * 1024, 4},
	}
}

func (s *MergeTreeService) PlanMerge() ([]PlanMerge, error) {
	var res []PlanMerge
	// configuration - timeout_s - max_res_size_bytes - iteration_id
	configurations := getMergeConfigurations()
	for _, conf := range configurations {
		if time.Now().Sub(s.lastIterationTime[conf[2]-1]).Seconds() > float64(conf[0]) {
			files, err := s.merge.GetFilesToMerge(int(conf[2]))
			if err != nil {
				return nil, err
			}
			plans := s.merge.PlanMerge(files, conf[1], int(conf[2]))
			res = append(res, plans...)
			s.lastIterationTime[conf[2]-1] = time.Now()
		}
	}
	return res, nil
}

// Merge method implementation
func (s *MergeTreeService) Merge(plan []PlanMerge) error {
	return s.merge.DoMerge(plan)
}

func (s *MergeTreeService) DoMerge() error {
	plan, err := s.PlanMerge()
	if err != nil {
		return err
	}
	return s.Merge(plan)
}

type MergeService interface {
	Run()
	Stop()
	Store(columns map[string]any) utils.Promise[int32]
	DoMerge() error
	/*PlanMerge() ([]PlanMerge, error)
	Merge(plan []PlanMerge) error*/
}
