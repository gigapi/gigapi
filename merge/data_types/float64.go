package data_types

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
)

func newFloat64Column() *Column[float64] {
	return &Column[float64]{
		typeName:  DATA_TYPE_NAME_FLOAT64,
		arrowType: arrow.PrimitiveTypes.Float64,
		getBuilder: func(builder array.Builder) IArrowAppender[float64] {
			return builder.(*array.Float64Builder)
		},
		parseStr: func(s string) (float64, error) {
			return strconv.ParseFloat(s, 64)
		},
		parseJson: func(d *jx.Decoder) (float64, error) {
			return d.Float64()
		},
	}
}

func float64Builder(name string, data any, sizeAndCap ...int64) (IColumn, error) {
	return colBuilder[float64](newFloat64Column, name, data, sizeAndCap...)
}
