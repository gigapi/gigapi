package repository

import (
	"database/sql"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/data_types"
	"github.com/gigapi/gigapi/v2/merge/index"
	"github.com/gigapi/gigapi/v2/merge/service"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/utils"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var conn *sql.DB

var registry = make(map[[2]string]service.MergeService)
var mergeTicker *time.Ticker
var registryMtx sync.Mutex

func InitRegistry(_conn *sql.DB) error {
	if !config.Config.Gigapi.NoMerges {
		go RunMerge()
	}
	return nil
}

func GetTable(db string, name string) (service.MergeService, error) {
	table, ok := registry[[2]string{db, name}]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	return table, nil
}

func RunMerge() {
	mergeTicker = time.NewTicker(time.Second * 10)
	for range mergeTicker.C {
		_registry := make(map[[2]string]service.MergeService, len(registry))
		func() {
			registryMtx.Lock()
			defer registryMtx.Unlock()
			for k, v := range registry {
				_registry[k] = v
			}
		}()

		for _, table := range _registry {
			err := table.DoMerge()
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}
}

var tableNameCheck = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var m sync.Mutex

func Store(db string, name string, columns map[string]any) utils.Promise[int32] {
	if db == "" {
		db = "default"
	}
	//TODO: add the thread id to the table name
	//TODO: introduce Redis to synchronize several writers
	m.Lock()
	table := registry[[2]string{db, name}]
	if table == nil {
		err := RegisterSimpleTable(db, name)
		if err != nil {
			m.Unlock()
			return utils.Fulfilled(err, int32(0))
		}
		table = registry[[2]string{db, name}]
	}
	m.Unlock()
	return table.Store(columns)
}

func RegisterSimpleTable(db, name string) error {
	if db == "" {
		db = "default"
	}
	table := &shared.Table{
		Database: db,
		Name:     name,
		Engine:   "HiveMerge",
		OrderBy:  []string{"__timestamp"},
		Path:     path.Join(config.Config.Gigapi.Root, db, name),
		PartitionBy: func(m map[string]data_types.IColumn) ([]shared.PartitionDesc, error) {
			tsCol, ok := m["__timestamp"]
			if !ok {
				return nil, fmt.Errorf("table %q does not have a '__timestamp' column", name)
			}
			tsData, ok := tsCol.GetData().([]int64)
			if !ok {
				return nil, fmt.Errorf("column '__timestamp' has non-int64 data type")
			}

			parts := make(map[int64]*shared.PartitionDesc)
			lastPartId := int64(0)
			var lastPart *shared.PartitionDesc
			for i, ts := range tsData {
				id := int64(ts / 86400000000000)
				if lastPart == nil || lastPartId != id {
					lastPartId = id
					if _, ok := parts[id]; !ok {
						parts[id] = &shared.PartitionDesc{
							Values: [][2]string{
								{"date", time.Unix(0, ts).UTC().Format("2006-01-02")},
								{"hour", time.Unix(0, ts).UTC().Format("15")},
							},
							IndexMap: make([]byte, (len(tsData)+7)/8),
						}
					}
					lastPart = parts[id]
				}
				lastPart.IndexMap[i/8] |= 1 << (uint(i) % 8)
			}
			res := make([]shared.PartitionDesc, 0, len(parts))
			for _, desc := range parts {
				res = append(res, *desc)
			}
			return res, nil

		},
		AutoTimestamp: true,
	}
	m := sync.Mutex{}
	parts := make(map[string]shared.Index)
	table.IndexCreator = func(values [][2]string) (shared.Index, error) {
		m.Lock()
		defer m.Unlock()
		idxName := make([]string, len(values))
		for i, v := range values {
			idxName[i] = fmt.Sprintf("%s=%s", v[0], v[1])
		}
		idx, ok := parts[path.Join(idxName...)]
		if !ok {
			idx, err := index.NewJSONIndexForPartition(table, values)
			if err != nil {
				return nil, err
			}
			parts[path.Join(idxName...)] = idx
			idx.Run()
			return idx, nil
		}
		return idx, nil
	}
	return RegisterNewTable(table)
}

func RegisterNewTable(table *shared.Table) error {
	if !tableNameCheck.MatchString(table.Name) {
		return fmt.Errorf("invalid table name, only letters and _ are accepted: %q", table.Name)
	}
	if table.Path == "" {
		table.Path = filepath.Join(config.Config.Gigapi.Root, table.Database, table.Name)
	}
	if _, ok := registry[[2]string{table.Database, table.Name}]; ok {
		return nil
	}
	_table := *table
	if strings.HasPrefix(table.Path, "s3://") {
		_table.Path = path.Join(config.Config.Gigapi.Root, table.Database, table.Name)
	}
	err := createTableFolders(&_table)
	if err != nil {
		return err
	}
	/*err = InsertTableMetadata(conn, table)
	if err != nil {
		return err
	}*/
	registryMtx.Lock()
	defer registryMtx.Unlock()
	switch table.Engine {
	case "Merge":
		registry[[2]string{table.Database, table.Name}], err = service.NewMergeTreeService(table)
	case "HiveMerge":
		registry[[2]string{table.Database, table.Name}] =
			service.NewMultithreadHiveMergeTreeService(0, table)
	}
	if err != nil {
		return err
	}
	registry[[2]string{table.Database, table.Name}].Run()
	return nil
}

func PopulateRegistry() error {
	/*tables, err := GetAllTableMetadata(conn)
	if err != nil {
		return err
	}
	for _, table := range tables {
		registryMtx.Lock()
		registry[table.Name], err = service.NewMergeTreeService(table)
		if err != nil {
			registryMtx.Unlock()
			return err
		}
		registry[table.Name].Run()
		registryMtx.Unlock()
	}*/
	return nil

}

func createTableFolders(table *shared.Table) error {
	if !tableNameCheck.MatchString(table.Name) {
		return fmt.Errorf("invalid table name, only letters and _ are accepted: %q", table.Name)
	}
	err := os.MkdirAll(filepath.Join(table.Path, "tmp"), 0755)
	if err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(table.Path, "data"), 0755)
}
