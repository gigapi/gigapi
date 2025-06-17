package data_types

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
)

type IndexType []int32

func WrapToColumn(name string, data any) (IColumn, error) {
	switch data.(type) {
	case []int64:
		return int64Builder(name, data)
	case []uint64:
		return uint64Builder(name, data)
	case []float64:
		return float64Builder(name, data)
	case []string:
		return strBuilder(name, data)
	}
	return nil, fmt.Errorf("unsupported data type: %T", data)
}

const DATA_TYPE_NAME_INT64 = "INT8"
const DATA_TYPE_NAME_UINT64 = "UBIGINT"
const DATA_TYPE_NAME_FLOAT64 = "FLOAT8"
const DATA_TYPE_NAME_STRING = "VARCHAR"
const DATA_TYPE_NAME_UNKNOWN = "UNKNOWN"

var DataTypes = map[string]ColumnBuilder{
	"Int64":  int64Builder,
	"BIGINT": int64Builder,
	"INT8":   int64Builder,
	"LONG":   int64Builder,

	"UInt64":  uint64Builder,
	"UBIGINT": uint64Builder,

	"Float64": float64Builder,
	"DOUBLE":  float64Builder,
	"FLOAT8":  float64Builder,

	"String":  strBuilder,
	"STRING":  strBuilder,
	"VARCHAR": strBuilder,
	"CHAR":    strBuilder,
	"BPCHAR":  strBuilder,
	"TEXT":    strBuilder,

	/*"UHUGEINT":  UInt64{},
	"UINTEGER":  UInt64{},
	"USMALLINT": UInt64{},
	"UTINYINT":  UInt64{},


		"INTEGER":  Int64{},
		"INT4":     Int64{},
		"INT":      Int64{},
		"SIGNED":   Int64{},
		"SMALLINT": Int64{},
		"INT2":     Int64{},
		"SHORT":    Int64{},
		"TINYINT":  Int64{},
		"INT1":     Int64{},
		"HUGEINT":  Int64{},

	"FLOAT":  Float64{},
	"FLOAT4": Float64{},
	"REAL":   Float64{},

	"BIT":                      Bit{},
	"BITSTRING":                Bit{},
	"BLOB":                     Blob{},
	"BYTEA":                    Blob{},
	"BINARY":                   Blob{},
	"VARBINARY":                Blob{},
	"BOOLEAN":                  Boolean{},
	"BOOL":                     Boolean{},
	"LOGICAL":                  Boolean{},
	"DATE":                     Date{},
	"DECIMAL":                  Decimal{},
	"NUMERIC":                  Decimal{},
	"INTERVAL":                 Interval{},
	"JSON":                     Json{},
	"TIME":                     Time{},
	"TIMESTAMP WITH TIME ZONE": TimestampWithTimeZone{},
	"TIMESTAMPTZ":              TimestampWithTimeZone{},
	"TIMESTAMP":                Timestamp{},
	"DATETIME":                 Timestamp{},
	"UUID":                     Uuid{},*/
}

type IColumn interface {
	AppendNulls(size int64)
	GetLength() int64
	AppendFromJson(dec *jx.Decoder) error
	Less(i int32, j int32) bool
	ValidateData(data any) error
	ArrowDataType() arrow.DataType
	Append(data any) error
	AppendOne(val any) error
	AppendByMask(data any, mask []byte) error
	WriteToBatch(batch array.Builder) error
	GetName() string
	GetTypeName() string
	GetVal(i int64) any
	ParseFromStr(s string) error
	GetData() any
	GetMinMax() (any, any)
}

type ColumnBuilder func(name string, data any, sizeAndCap ...int64) (IColumn, error)
