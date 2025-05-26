package shared

import (
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/metadata"
)

type PartitionDesc struct {
	Values   [][2]string
	IndexMap []byte
}

type Table struct {
	Database      string
	Name          string
	Path          string
	Engine        string
	OrderBy       []string
	PartitionBy   func(map[string]data_types.IColumn) ([]PartitionDesc, error)
	AutoTimestamp bool
	Index         metadata.TableIndex
}

func GetMergeConfigurations() [][3]int64 {
	timeoutS := int64(config.Config.Gigapi.MergeTimeoutS)
	return [][3]int64{
		{timeoutS, 100 * 1024 * 1024, 1},
		{timeoutS * 10, 400 * 1024 * 1024, 2},
		{timeoutS * 100, 4000 * 1024 * 1024, 3},
		{timeoutS * 420, 4000 * 1024 * 1024, 4},
	}
}
