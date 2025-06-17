package data_types

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
)

func newInt64Column() *Column[int64] {
	return &Column[int64]{
		typeName:  DATA_TYPE_NAME_INT64,
		arrowType: arrow.PrimitiveTypes.Int64,
		getBuilder: func(builder array.Builder) IArrowAppender[int64] {
			return builder.(*array.Int64Builder)
		},
		parseStr: func(s string) (int64, error) {
			return strconv.ParseInt(s, 10, 64)
		},
		parseJson: func(d *jx.Decoder) (int64, error) {
			return d.Int64()
		},
	}
}

func int64Builder(name string, data any, sizeAndCap ...int64) (IColumn, error) {
	return colBuilder[int64](newInt64Column, name, data, sizeAndCap...)
}
