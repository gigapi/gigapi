package parsers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/go-faster/city"
	_ "github.com/go-faster/city"
	"io"
	"regexp"
	"strings"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/models"
)

type LineProtoParser struct {
}

func (l *LineProtoParser) Parse(data []byte) (chan *ParserResponse, error) {
	return l.ParseReader(nil, bytes.NewReader(data))
}

func (l *LineProtoParser) ParseReader(ctx context.Context, r io.Reader) (chan *ParserResponse, error) {
	scanner := bufio.NewScanner(r)

	precision := "ns"
	if ctx != nil && ctx.Value("precision") != nil {
		precision = ctx.Value("precision").(string)
	}

	res := make(chan *ParserResponse)

	go l.parse(scanner, res, precision)
	return res, nil
}

func getSchemaId(fields models.Fields, tags models.Tags) uint64 {
	determs := []uint64{0, 0, 1}
	for _, t := range tags {
		hash := city.CH64(append(t.Key, 1))
		determs[0] = determs[0] + hash
		determs[1] = determs[1] ^ hash
		determs[2] = determs[2] * (1779033703 + 2*hash)
	}
	for k, v := range fields {
		var tp byte
		switch v.(type) {
		case string:
			tp = 1
		case int64:
			tp = 2
		case float64:
			tp = 3
		}
		hash := city.CH64(append([]byte(k), tp))
		determs[0] = determs[0] + hash
		determs[1] = determs[1] ^ hash
		determs[2] = determs[2] * (1779033703 + 2*hash)
	}
	return city.CH64(unsafe.Slice((*byte)(unsafe.Pointer(&determs[0])), 24))
}

func appendData(data *map[string]any, k string, v any) {
	_, ok := (*data)[k]
	if !ok {
		switch v.(type) {
		case string:
			(*data)[k] = []string{v.(string)}
		case int64:
			(*data)[k] = []int64{v.(int64)}
		case float64:
			(*data)[k] = []float64{v.(float64)}
		case bool:
			(*data)[k] = []bool{v.(bool)}
		}
		return
	}
	switch v.(type) {
	case string:
		(*data)[k] = append((*data)[k].([]string), v.(string))
	case int64:
		(*data)[k] = append((*data)[k].([]int64), v.(int64))
	case float64:
		(*data)[k] = append((*data)[k].([]float64), v.(float64))
	case bool:
		(*data)[k] = append((*data)[k].([]bool), v.(bool))
	}
}

var re = regexp.MustCompile(`^[^.,]+\.`)

func (l *LineProtoParser) parse(scanner *bufio.Scanner, res chan *ParserResponse, precision string) {
	defer close(res)

	var (
		table    string
		schemaId uint64
		data     map[string]any = make(map[string]any)
	)
	send := func() {
		database := ""
		if strings.Contains(table, ".") {
			databaseTable := strings.SplitN(table, ".", 2)
			database = databaseTable[0]
			table = databaseTable[1]
		}
		res <- &ParserResponse{Database: database, Table: table, Data: data}
		data = make(map[string]any)
		table = ""
		schemaId = 0
	}

	onErr := func(err error) {
		res <- &ParserResponse{Error: err}
	}

	for scanner.Scan() {
		line := scanner.Text()

		// Parse the line as InfluxDB line protocol
		point, err := models.ParsePointsWithPrecision([]byte(line), time.Now().UTC(), precision)
		if err != nil {
			onErr(fmt.Errorf("error parsing line: %w", err))
			return
		}

		for _, p := range point {
			_table := p.Name()
			__table := string(_table)
			if table != __table && table != "" {
				send()
			}
			table = __table
			fields, err := p.Fields()
			if err != nil {
				onErr(fmt.Errorf("error getting fields: %w", err))
				return
			}
			_schemaId := getSchemaId(fields, p.Tags())
			if _schemaId != schemaId && schemaId != 0 {
				send()
			}
			schemaId = _schemaId
			for k, v := range fields {
				appendData(&data, k, v)
			}
			for _, t := range p.Tags() {
				appendData(&data, string(t.Key), string(t.Value))
			}
			if _, ok := data["time"]; !ok {
				data["time"] = []int64{}
			}
			data["time"] = append(data["time"].([]int64), p.Time().UnixNano())
			if _, ok := data["__timestamp"]; !ok {
				data["__timestamp"] = []int64{}
			}
			data["__timestamp"] = append(data["__timestamp"].([]int64), p.Time().UnixNano())
		}
	}

	if err := scanner.Err(); err != nil {
		onErr(err)
		return
	}
	if table != "" {
		send()
	}
}

var _ = func() int {
	RegisterParser("", func(fieldNames []string, fieldTypes []string) IParser {
		return &LineProtoParser{}
	})
	return 0
}
