package service

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"io"
	"os"
	"path"
	"path/filepath"
)

type fieldDesc [2]string

func (f fieldDesc) GetType() string       { return f[0] }
func (f fieldDesc) GetName() string       { return f[1] }
func fd(tp string, name string) fieldDesc { return [2]string{tp, name} }

type saveService interface {
	Save(fields []fieldDesc, unorderedData dataStore, relPath string) error
	Join(part ...string) string
	SizeB(relPath string) (int64, error)
	MkDirAll(path ...string) error
}

type savePerformer interface {
	moveTmp(tmpPath string, filePath string) error
	Join(part ...string) string
	base(path string) string
	SizeB(path string) (int64, error)
	MkDirAll(path ...string) error
}

type saveServiceManager struct {
	table       *shared.Table
	layer       config.LayersConfiguration
	tmpPath     string
	recordBatch *array.RecordBuilder
	schema      *arrow.Schema
	savePerformer
}

func newFsSaveService(layer config.LayersConfiguration, table *shared.Table) (*saveServiceManager, error) {
	tmpPath := os.TempDir()

	perf, err := newFsSavePerformer(layer, table)
	if err != nil {
		return nil, err
	}
	res := &saveServiceManager{
		table:         table,
		layer:         layer,
		tmpPath:       tmpPath,
		savePerformer: perf,
	}
	return res, nil
}

func (fs *saveServiceManager) shouldRecreateSchema(fields []fieldDesc) bool {
	if fs.schema == nil {
		return true
	}
	for _, f := range fields {
		found := false
		for _, _f := range fs.schema.Fields() {
			if _f.Name == f.GetName() {
				found = true
			}
		}
		if !found {
			return true
		}
	}
	return false
}

// @param: filename []fieldDesc: [data type - fields name]
func (fs *saveServiceManager) maybeRecreateSchema(fields []fieldDesc) {
	if !fs.shouldRecreateSchema(fields) {
		return
	}
	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		var fieldType, _ = data_types.DataTypes[field.GetType()](field.GetName(), nil, 0, 0)
		arrowFields[i] = arrow.Field{Name: field.GetName(), Type: fieldType.ArrowDataType(), Nullable: true}
	}

	fs.schema = arrow.NewSchema(arrowFields, nil)
	fs.recordBatch = array.NewRecordBuilder(memory.DefaultAllocator, fs.schema)
}

func (fs *saveServiceManager) saveTmpFile(filename string, fields []fieldDesc, unorderedData dataStore) error {
	fs.maybeRecreateSchema(fields)
	err := unorderedData.StoreToArrow(fs.schema, fs.recordBatch)
	if err != nil {
		return err
	}
	record := fs.recordBatch.NewRecord()
	defer record.Release()
	if record.Column(0).Data().Len() == 0 {
		return nil
	}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	// Set up Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(8124),
	)
	arrprops := pqarrow.NewArrowWriterProperties()

	// Create Parquet file writer
	writer, err := pqarrow.NewFileWriter(fs.schema, file, writerProps, arrprops)
	if err != nil {
		return err
	}
	defer writer.Close()
	return writer.Write(record)
}

func (fs *saveServiceManager) Save(fields []fieldDesc, unorderedData dataStore, relPath string) error {
	fileName := fs.savePerformer.base(relPath)
	tmpPath := path.Join(fs.tmpPath, fileName)
	err := fs.saveTmpFile(tmpPath, fields, unorderedData)
	if err != nil {
		return err
	}
	return fs.savePerformer.moveTmp(tmpPath, relPath)
}

type fsSavePerformer struct {
	layer    config.LayersConfiguration
	table    *shared.Table
	dataPath string
}

func newFsSavePerformer(layer config.LayersConfiguration, table *shared.Table) (*fsSavePerformer, error) {
	dataPath, err := buildPath(layer, table, "data")
	if err != nil {
		return nil, err
	}
	os.MkdirAll(dataPath, 0755)
	return &fsSavePerformer{
		layer:    layer,
		table:    table,
		dataPath: dataPath,
	}, nil
}

func (f *fsSavePerformer) MkDirAll(part ...string) error {
	return os.MkdirAll(filepath.Join(f.dataPath, f.Join(part...)), 0755)
}

func (f *fsSavePerformer) Join(part ...string) string {
	return filepath.Join(part...)
}
func (f *fsSavePerformer) base(path string) string {
	return filepath.Base(path)
}

func (f *fsSavePerformer) moveTmp(tmpPath string, filePath string) error {
	to := filepath.Join(f.dataPath, filePath)
	err := os.Rename(tmpPath, to)
	if err != nil {
		err = f.moveTmpFallback(tmpPath, filePath)
	}
	return err
}

func (f *fsSavePerformer) moveTmpFallback(tmpPath string, filePath string) error {
	to := filepath.Join(f.dataPath, filePath)
	defer os.Remove(tmpPath)
	from, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	defer from.Close()
	fileTo, err := os.Create(to)
	if err != nil {
		return err
	}
	defer fileTo.Close()
	_, err = io.Copy(fileTo, from)
	return err
}

func (f *fsSavePerformer) SizeB(path string) (int64, error) {
	info, err := os.Stat(filepath.Join(f.dataPath, path))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
