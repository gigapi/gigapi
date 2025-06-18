package data_types

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
)

func newStrColumn() *Column[string] {
	return &Column[string]{
		typeName:  DATA_TYPE_NAME_STRING,
		arrowType: arrow.BinaryTypes.String,
		getBuilder: func(builder array.Builder) IArrowAppender[string] {
			return builder.(*array.StringBuilder)
		},
		parseStr: func(s string) (string, error) {
			return s, nil
		},
		parseJson: func(d *jx.Decoder) (string, error) {
			return d.Str()
		},
	}
}

func strBuilder(name string, data any, sizeAndCap ...int64) (IColumn, error) {
	return colBuilder[string](newStrColumn, name, data, sizeAndCap...)
}
