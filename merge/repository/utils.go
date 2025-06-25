package repository

import (
	"fmt"

	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/metadata"
)

func getTableIndex(table *shared.Table) (metadata.TableIndex, error) {
	layers := make([]metadata.Layer, len(config.Config.Gigapi.Layers))
	for i, l := range config.Config.Gigapi.Layers {
		layers[i] = metadata.Layer{
			URL:    l.URL,
			Name:   l.Name,
			Type:   l.Type,
			TTLSec: int32(l.TTL.Seconds()),
		}
	}
	switch config.Config.Gigapi.Metadata.Type {
	case "json":
		return metadata.NewJSONIndex(
			config.Config.Gigapi.Root,
			table.Database,
			table.Name,
			layers)
	case "redis":
		return metadata.NewRedisIndex(
			config.Config.Gigapi.Metadata.URL,
			table.Database,
			table.Name,
			layers)
	}
	return nil, fmt.Errorf("unknown metadata type: %q", config.Config.Gigapi.Metadata.Type)
}
