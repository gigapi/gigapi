package service

import (
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/google/uuid"
	"os"
	"path"
	"path/filepath"
)

type fieldDesc [2]string

func (f fieldDesc) GetType() string       { return f[0] }
func (f fieldDesc) GetName() string       { return f[1] }
func fd(tp string, name string) fieldDesc { return [2]string{tp, name} }

type saveService interface {
	Save(fields []fieldDesc, unorderedData dataStore) (string, error)
}

type fsSaveService struct {
	dataPath    string
	tmpPath     string
	partPath    string
	recordBatch *array.RecordBuilder
	schema      *arrow.Schema
}

func (fs *fsSaveService) shouldRecreateSchema(fields []fieldDesc) bool {
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
func (fs *fsSaveService) maybeRecreateSchema(fields []fieldDesc) {
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

func (fs *fsSaveService) saveTmpFile(filename string, fields []fieldDesc, unorderedData dataStore) error {
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

func (fs *fsSaveService) Save(fields []fieldDesc, unorderedData dataStore) (string, error) {
	filename, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	var relPath []string
	if fs.partPath != "" {
		relPath = append(relPath, fs.partPath)
	}
	relPath = append(relPath, filename.String()+".1.parquet")
	tmpFileName := path.Join(fs.tmpPath, filename.String()+".1.parquet")
	absPath := append([]string{fs.dataPath}, relPath...)
	/*
		fmt.Printf("Saving file:\n  FileSave path: %s\n  tmp path: %s\n  data path:  %s\n",
			fs.path, tmpFileName, fileName)

	*/
	err = fs.saveTmpFile(tmpFileName, fields, unorderedData)
	if err != nil {
		return "", err
	}
	err = os.Rename(tmpFileName, filepath.Join(absPath...))
	if err != nil {
		err = fmt.Errorf("REN1: %v", err)
	}
	return filepath.Join(relPath...), err
}
