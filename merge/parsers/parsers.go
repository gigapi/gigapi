package parsers

import (
	"context"
	"fmt"
	"io"
	"strings"
)

type ParserFactory func(fieldNames []string, fieldTypes []string) IParser

var registry = make(map[string]ParserFactory)

type IParser interface {
	Parse(data []byte) (chan *ParserResponse, error)
	ParseReader(ctx context.Context, r io.Reader) (chan *ParserResponse, error)
}

type ParserResponse struct {
	Database   string
	Table      string
	Data       map[string]any
	IsExternal bool
	Error      error
}

func RegisterParser(name string, parser ParserFactory) {
	registry[name] = parser
}

func GetParser(name string, fieldNames []string, fieldTypes []string) (IParser, error) {
	for _name, parser := range registry {
		if strings.HasPrefix(name, _name) {
			return parser(fieldNames, fieldTypes), nil
		}
	}
	if parser, ok := registry[""]; ok {
		return parser(fieldNames, fieldTypes), nil
	}
	return nil, fmt.Errorf("parser %s not found", name)
}

func init() {
	RegisterParser("", func(fieldNames []string, fieldTypes []string) IParser {
		return &LineProtoParser{}
	})
}
