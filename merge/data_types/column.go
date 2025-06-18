package data_types

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-faster/jx"
	"golang.org/x/exp/constraints"
)

type IArrowAppender[T constraints.Ordered] interface {
	AppendValues(values []T, valid []bool)
}

var _ IColumn = &Column[int64]{}

type Column[T constraints.Ordered] struct {
	data       []T
	valids     []bool
	name       string
	typeName   string
	arrowType  arrow.DataType
	getBuilder func(builder array.Builder) IArrowAppender[T]
	parseStr   func(s string) (T, error)
	parseJson  func(d *jx.Decoder) (T, error)
}

func colBuilder[T constraints.Ordered](createColumn func() *Column[T], name string, data any,
	sizeAndCap ...int64) (IColumn, error) {
	col := createColumn()
	col.name = name
	if data == nil {
		col.InitializeData(sizeAndCap...)
		return col, nil
	}
	err := col.ValidateData(data)
	if err != nil {
		return nil, err
	}
	col.data = data.([]T)
	col.valids = make([]bool, len(col.data))
	FastFillArray(col.valids, true)
	return col, nil
}

func (c *Column[T]) GetData() any {
	return c.data
}

func (c *Column[T]) InitializeData(sizeAndCap ...int64) {
	var size int64 = 1000
	if len(sizeAndCap) > 0 {
		size = sizeAndCap[0]
	}
	cap := size * 2
	if len(sizeAndCap) > 1 {
		cap = sizeAndCap[1]
	}
	if cap < size {
		cap = size
	}
	c.data = make([]T, size, cap)
	c.valids = make([]bool, size, cap)
}

func (c *Column[T]) GetMinMax() (any, any) {
	if c.GetLength() == 0 {
		return nil, nil
	}
	return slices.Min(c.data), slices.Max(c.data)
}

func (c *Column[T]) AppendNulls(size int64) {
	c.data = append(c.data, make([]T, size)...)
	c.valids = append(c.valids, make([]bool, size)...)
}

func (c *Column[T]) GetLength() int64 {
	return int64(len(c.data))
}

func (c *Column[T]) AppendFromJson(dec *jx.Decoder) error {
	return fmt.Errorf("not implemented")
}

func (c *Column[T]) Less(i int32, j int32) bool {
	return (!c.valids[i] && c.valids[j]) || (c.valids[i] && c.valids[j] && c.data[i] <= c.data[j])
}

func (c *Column[T]) ValidateData(data any) error {
	if _, ok := data.([]T); !ok {
		return fmt.Errorf("invalid data type")
	}
	return nil
}

func (c *Column[T]) ArrowDataType() arrow.DataType {
	return c.arrowType
}

func (c *Column[T]) Append(data any) error {
	err := c.ValidateData(data)
	if err != nil {
		return err
	}
	lenBefore := c.GetLength()
	_data := data.([]T)
	c.data = append(c.data, _data...)
	c.valids = append(c.valids, make([]bool, len(_data))...)
	FastFillArray(c.valids[lenBefore:], true)
	return nil
}

func (c *Column[T]) AppendOne(val any) error {
	if _, ok := val.(T); ok {
		c.data = append(c.data, val.(T))
		c.valids = append(c.valids, true)
		return nil
	}
	return fmt.Errorf("invalid data type")
}

func (c *Column[T]) AppendByMask(data any, mask []byte) error {
	err := c.ValidateData(data)
	if err != nil {
		return err
	}
	_data := data.([]T)
	if len(mask) != (len(_data)+7)/8 {
		return fmt.Errorf("invalid mask length")
	}

	startIdx := 0
	endIdx := 0
	for i := 0; i < len(mask)*8; i++ {
		if mask[i/8]&(1<<(i%8)) != 0 {
			endIdx = i + 1
			continue
		}
		if startIdx == endIdx {
			startIdx++
			endIdx++
			continue
		}
		c.data = append(c.data, _data[startIdx:endIdx]...)
		k := len(c.valids)
		c.valids = append(c.valids, make([]bool, endIdx-startIdx)...)
		FastFillArray(c.valids[k:], true)
		startIdx = endIdx
	}
	if startIdx != endIdx {
		c.data = append(c.data, _data[startIdx:]...)
		k := len(c.valids)
		c.valids = append(c.valids, make([]bool, len(_data[startIdx:]))...)
		FastFillArray(c.valids[k:], true)
	}
	return nil

	/*u64Mask := unsafe.Slice((*uint64)(unsafe.Pointer(&mask[0])), len(mask)/8)

	k := len(c.data)
	c.data = append(c.data, make([]T, len(_data))...)
	c.data = c.data[:k]


	for i, u := range u64Mask {
		if u == 0xFFFFFFFFFFFFFFFF {
			c.data = append(c.data, _data[i*64:i*64+64]...)
			continue
		}
		for j := 0; j < 64; j++ {
			if u&(1<<j) != 0 && i*64+j < len(_data) {
				c.data = append(c.data, _data[i*64+j])
			}
		}
	}

	for i := range mask[len(u64Mask)*8:] {
		k := i + len(u64Mask)*8
		if mask[i] == 0xFF {
			c.data = append(c.data, _data[k*8:k*8+8]...)
			continue
		}
		for j := 0; j < 8; j++ {
			if mask[i+len(u64Mask)*8]&(1<<j) != 0 {
				c.data = append(c.data, _data[k*8+j])
			}
		}
	}

	c.valids = append(c.valids, make([]bool, len(c.data)-len(c.valids))...)
	FastFillArray(c.valids[len(c.data)-len(_data):], true)
	return nil*/
}

/*func (c *Column[T]) Merge(data any) error {
	err := c.ValidateData(data)
	if err != nil {
		return err
	}

	_data := data.([]T)
	if len(_data) == 0 {
		return nil
	}

	if _data[0] >= c.data[len(c.data)-1] {
		c.data = append(c.data, _data...)
		c.valids = append(c.valids, make([]bool, len(_data))...)
		FastFillArray(c.valids[len(c.data)-len(_data):], true)
		return nil
	}

	if c.data[0] >= _data[len(_data)-1] {
		c.data = append(_data, c.data...)
		c.valids = append(c.valids, make([]bool, len(_data))...)
		FastFillArray(c.valids[len(_data):], true)
		return nil
	}

	first := c.data
	var firstValids []bool = c.valids
	second := _data
	var secondValids []bool = nil
	head, _ := slices.BinarySearch(c.data, _data[0])
	if head == 0 {
		head, _ = slices.BinarySearch(_data, c.data[0])
		first, second = second, first
		firstValids, secondValids = secondValids, firstValids
	}
	valids := make([]bool, 0, len(c.data)+len(_data))
	res := make([]T, 0, len(c.data)+len(_data))
	res = append(res, first[:head]...)
	i, j := head, 0
	for i < len(first) && j < len(second) {
		if c.Less(int32(i), int32(j)) {
			res = append(res, first[i])
			if firstValids != nil {
				valids = append(valids, firstValids[i])
			} else {
				valids = append(valids, true)
			}
			i++
		} else {
			res = append(res, second[j])
			if secondValids != nil {
				valids = append(valids, secondValids[i])
			} else {
				valids = append(valids, true)
			}
			j++
		}
	}
	if i < len(first) {
		res = append(res, first[i:]...)
	}
	if j < len(second) {
		res = append(res, second[j:]...)
	}
	c.data = res
	c.valids = valids
	return nil
}*/

func (c *Column[T]) WriteToBatch(batch array.Builder) error {
	c.getBuilder(batch).AppendValues(c.data, c.valids)
	return nil
}

func (c *Column[T]) GetName() string {
	return c.name
}

func (c *Column[T]) GetTypeName() string {
	return c.typeName
}

func (c *Column[T]) GetVal(i int64) any {
	return c.data[i]
}

func (c *Column[T]) ParseFromStr(s string) error {
	val, err := c.parseStr(s)
	if err != nil {
		return err
	}
	c.data = append(c.data, val)
	c.valids = append(c.valids, true)
	return nil
}
