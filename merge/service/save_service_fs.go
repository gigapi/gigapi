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
}

type savePerformer interface {
	moveTmp(tmpPath string, filePath string) error
	join(part ...string) string
	base(path string) string
	sizeB(path string) (int64, error)
}

type saveServiceManager struct {
	table         *shared.Table
	layer         config.LayersConfiguration
	tmpPath       string
	recordBatch   *array.RecordBuilder
	schema        *arrow.Schema
	savePerformer savePerformer
}

func newFsSaveService(layer config.LayersConfiguration, table *shared.Table) (*saveServiceManager, error) {
	tmpPath := os.TempDir()
	dataPath, err := buildPath(layer, table, "data")
	if err != nil {
		return nil, err
	}
	res := &saveServiceManager{
		table:   table,
		layer:   layer,
		tmpPath: tmpPath,
		savePerformer: &fsSavePerformer{
			layer:    layer,
			table:    table,
			dataPath: dataPath,
		},
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

func (fs *saveServiceManager) Join(part ...string) string {
	return fs.savePerformer.join(part...)
}

func (fs *saveServiceManager) SizeB(relPath string) (int64, error) {
	return fs.savePerformer.sizeB(relPath)
}

type fsSavePerformer struct {
	layer    config.LayersConfiguration
	table    *shared.Table
	dataPath string
}

func (f *fsSavePerformer) join(part ...string) string {
	return filepath.Join(part...)
}
func (f *fsSavePerformer) base(path string) string {
	return filepath.Base(path)
}

func (f *fsSavePerformer) moveTmp(tmpPath string, filePath string) error {
	to := filepath.Join(f.dataPath, filePath)
	err := os.Rename(tmpPath, to)
	return err
}

func (f *fsSavePerformer) sizeB(path string) (int64, error) {
	info, err := os.Stat(filepath.Join(f.dataPath, path))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
