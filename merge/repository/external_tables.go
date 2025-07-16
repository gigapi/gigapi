package repository

import (
	"fmt"
	"path/filepath"

	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/shared"
)

func RegisterExternalTable(db, name string, columns []string, types []string) error {
	tablePath := filepath.Join(config.Config.Gigapi.Root, db, name)

	idx, err := getTableIndex(&shared.Table{
		Database: db,
		Name:     name,
	})
	if err != nil {
		return fmt.Errorf("failed to get table index: %w", err)
	}

	table := &shared.Table{
		Database:      db,
		Name:          name,
		Path:          tablePath,
		Engine:        "External",
		OrderBy:       []string{},
		AutoTimestamp: false, // This is key for non-timeseries tables
		PartitionBy: func(m map[string]data_types.IColumn) ([]shared.PartitionDesc, error) {
			// Non-timeseries tables have a single partition.
			return []shared.PartitionDesc{
				{
					Values:   [][2]string{},
					IndexMap: []byte{0xFF}, // A full bitmask
				},
			}, nil
		},
		Index: idx,
	}

	return RegisterNewTable(table)
}