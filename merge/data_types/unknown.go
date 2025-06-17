package data_types

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
)

type UnknownType struct{}

func (u UnknownType) ParseFromStr(s string) (any, error) {
	return nil, nil
}

func (u UnknownType) GetMerger(data1 any, valid1 []bool, data2 any, valid2 []bool, s1 int64, s2 int64) any {
	return nil
}

func (u UnknownType) GetSorter(data any) sort.Interface {
	return nil
}

func (u UnknownType) AppendByMask(data any, toAppend any, mask []byte) (any, error) {
	return nil, nil
}

func (u UnknownType) AppendOne(val any, data any) any {
	return nil
}

func (u UnknownType) GetVal(i int64, store any) any {
	return nil
}
func (u UnknownType) GetValI32(i int32, store any) any {
	return nil
}

func (u UnknownType) GetName() string {
	return DATA_TYPE_NAME_UNKNOWN
}

func (u UnknownType) GetLength(store any) int64 {
	return 0
}

func (u UnknownType) MakeStore(sizeAndCap ...int) any {
	return nil
}

func (u UnknownType) AppendDefault(size int, store any) any {
	return nil
}

func (u UnknownType) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return nil, fmt.Errorf("unknown data type")
}

func (u UnknownType) Less(store any, i int32, j int32) bool {
	return false
}

func (f UnknownType) BLess(a any, b any) bool {
	return false
}

func (u UnknownType) ValidateData(data any) error {
	return fmt.Errorf("unknown data type")
}

func (u UnknownType) ArrowDataType() arrow.DataType {
	return nil
}

func (u UnknownType) AppendStore(store any, data any) (any, error) {
	return nil, fmt.Errorf("unknown data type")
}

func (u UnknownType) WriteToBatch(batch array.Builder, data any, indexes IndexType, valid []bool) error {
	return fmt.Errorf("unknown data type")
}
