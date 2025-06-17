package service

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gigapi/gigapi/v2/merge/data_types"
)

type dataStore interface {
	VerifyData(data map[string]data_types.IColumn) error
	AppendData(data map[string]data_types.IColumn) error
	GetSize() int64
	StoreToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error
	AppendByMask(data map[string]data_types.IColumn, mask []byte) error
	GetSchema() map[string]string
}
type unorderedDataStore struct {
	store map[string]data_types.IColumn
	size  int64
	mtx   sync.Mutex
}

func newUnorderedDataStore() *unorderedDataStore {
	return &unorderedDataStore{
		store: make(map[string]data_types.IColumn),
		size:  0,
	}
}

func (uds *unorderedDataStore) VerifyData(data map[string]data_types.IColumn) error {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	for k, field := range data {
		dataCol, ok := data[k]
		if !ok {
			continue
		}
		if uds.store[k].GetTypeName() != field.GetTypeName() {
			return fmt.Errorf("column `%s` type mismatch: expected %s, got %s",
				k, field.GetTypeName(), dataCol.GetTypeName())
		}
	}
	return nil
}

func (uds *unorderedDataStore) AppendByMask(data map[string]data_types.IColumn, mask []byte) error {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	err := uds.normalizeSchema(data)
	if err != nil {
		return err
	}
	var nullFields []string
	sizeBefore := uds.getSize()
	var sizeAfter int64
	for k, field := range uds.store {
		dataCol, ok := data[k]
		if !ok {
			nullFields = append(nullFields, k)
			continue
		}
		err = field.AppendByMask(dataCol.GetData(), mask)
		if err != nil {
			return err
		}
		sizeAfter = field.GetLength()
	}

	dataSize := sizeAfter - sizeBefore
	for _, k := range nullFields {
		uds.store[k].AppendNulls(dataSize)
	}
	uds.size = sizeAfter
	return nil
}

func (uds *unorderedDataStore) MergeColumns(data map[string]data_types.IColumn) []string {
	c := map[string]bool{}
	for k := range data {
		c[k] = true
	}
	for k := range uds.store {
		c[k] = true
	}
	var res []string
	for k := range c {
		res = append(res, k)
	}
	return res
}

func (uds *unorderedDataStore) normalizeSchema(data map[string]data_types.IColumn) error {
	var err error
	for k, field := range data {
		_, ok := uds.store[k]
		if ok {
			continue
		}
		uds.store[k], err = data_types.DataTypes[field.GetTypeName()](k, nil, uds.getSize(), uds.getSize()*2+100000)
		if err != nil {
			return err
		}
	}
	return nil
}

func (uds *unorderedDataStore) AppendData(data map[string]data_types.IColumn) error {
	//TODO: remove the logic of dynamic schema and flush parquet immediately when schema changes
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	var sz int64
	for _, c := range data {
		sz = c.GetLength()
		break
	}
	storeSize := int64(uds.getSize())
	var err error
	cols := uds.MergeColumns(data)
	for _, k := range cols {
		_, ok := uds.store[k]
		if !ok {
			uds.store[k], err = data_types.DataTypes[data[k].GetTypeName()](k, nil,
				storeSize, storeSize+sz)
			if err != nil {
				return err
			}
		}
		_, ok = data[k]
		if !ok {
			uds.store[k].AppendNulls(sz)
			continue
		}
		if err := uds.store[k].Append(data[k].GetData()); err != nil {
			return err
		}
	}
	uds.size += sz
	return nil
}

func (uds *unorderedDataStore) getSize() int64 {
	return uds.size
}

func (uds *unorderedDataStore) GetSize() int64 {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	return uds.getSize()
}

func (uds *unorderedDataStore) GetSchema() map[string]string {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	res := make(map[string]string)
	for k, v := range uds.store {
		res[k] = v.GetTypeName()
	}
	return res
}

func (uds *unorderedDataStore) storeToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error {
	for i, field := range schema.Fields() {
		dataField, ok := uds.store[field.Name]
		arrowField := builder.Field(i)
		if !ok {
			arrowField.AppendNulls(int(uds.GetSize()))
			continue
		}
		err := dataField.WriteToBatch(arrowField)
		if err != nil {
			return err
		}
	}
	return nil
}

func (uds *unorderedDataStore) StoreToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error {
	return uds.storeToArrow(schema, builder)
}
