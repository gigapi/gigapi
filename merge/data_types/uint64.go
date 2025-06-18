package data_types

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
)

func newUint64Column() *Column[uint64] {
	return &Column[uint64]{
		typeName:  DATA_TYPE_NAME_UINT64,
		arrowType: arrow.PrimitiveTypes.Int64,
		getBuilder: func(builder array.Builder) IArrowAppender[uint64] {
			return builder.(*array.Uint64Builder)
		},
		parseStr: func(s string) (uint64, error) {
			return strconv.ParseUint(s, 10, 64)
		},
		parseJson: func(d *jx.Decoder) (uint64, error) {
			return d.UInt64()
		},
	}
}

func uint64Builder(name string, data any, sizeAndCap ...int64) (IColumn, error) {
	return colBuilder[uint64](newUint64Column, name, data, sizeAndCap...)
}
