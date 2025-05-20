package shared

import (
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/utils"
)

type PartitionDesc struct {
	Values   [][2]string
	IndexMap []byte
}

type IndexEntry struct {
	Layer     string         `json:"layer"`
	Path      string         `json:"path"`
	SizeBytes int64          `json:"size_bytes"`
	RowCount  int64          `json:"row_count"`
	ChunkTime int64          `json:"chunk_time"`
	Min       map[string]any `json:"min"`
	Max       map[string]any `json:"max"`
	MinTime   int64          `json:"min_time"`
	MaxTime   int64          `json:"max_time"`
}

type Index interface {
	Batch(add []*IndexEntry, rm []string) utils.Promise[int32]
	Get(path string) *IndexEntry
	Run()
	Stop()
	AddToDropQueue(files []string) utils.Promise[int32]
	RmFromDropQueue(files []string) utils.Promise[int32]
	GetDropQueue() []string
}

type Table struct {
	Database      string
	Name          string
	Path          string
	Engine        string
	OrderBy       []string
	PartitionBy   func(map[string]data_types.IColumn) ([]PartitionDesc, error)
	AutoTimestamp bool
	IndexCreator  func(values [][2]string) (Index, error)
}

// get merge configurations from the overall configuration
// Each merge configuration is [3]int64 array {timeout in seconds, max result bytes, iteration id}
func GetMergeConfigurations() [][3]int64 {
	timeoutS := int64(config.Config.Gigapi.MergeTimeoutS)
	return [][3]int64{
		{timeoutS, 100 * 1024 * 1024, 1},
		{timeoutS * 10, 400 * 1024 * 1024, 2},
		{timeoutS * 100, 4000 * 1024 * 1024, 3},
		{timeoutS * 420, 4000 * 1024 * 1024, 4},
	}
}
