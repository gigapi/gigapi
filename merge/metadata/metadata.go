package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi/v2/utils/tempconf"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"strconv"
)

var mdb *sql.DB

func getMetadataDB() (*sql.DB, error) {
	if mdb != nil {
		return mdb, nil
	}
	connStr := tempconf.GetPostgresConnectionString()
	if connStr == "" {
		log.Fatal("POSTGRES environment variables is not set")
	}

	var err error
	mdb, err = sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Test the connection
	err = mdb.Ping()
	if err != nil {
		return nil, err
	}

	return mdb, nil
}

type TableDesc struct {
	Id      int
	Name    string
	Path    string
	OrderBy []string
}

type ColumnDesc struct {
	Id           int64  `json:"column_id"`
	Name         string `json:"column_name"`
	Min          string `json:"min_value"`
	Max          string `json:"max_value"`
	Count        int64  `json:"value_count"`
	NullCount    int64  `json:"null_count"`
	ContainsNans bool   `json:"contains_nans"`
	SizeBytes    int64  `json:"column_size_bytes"`
}

type FilePartitionValue struct {
	PartitionKeyIndex int64  `json:"partition_key_index"`
	PartitionValue    string `json:"partition_value"`
}

type FileDesc struct {
	Id                  int64
	Table               TableDesc
	Path                string
	SizeBytes           int64
	FooterSizeBytes     int64
	RecordCount         int64
	ColumnStats         []ColumnDesc
	FilePartitionValues []FilePartitionValue
	PartitionId         int64
}

func GetFiles(table string, iteration int, withStats bool) ([]FileDesc, error) {
	db, err := getMetadataDB()
	if err != nil {
		return nil, err
	}
	req, args, err := (&fileSeeker{
		Iteration: iteration,
		Table:     table,
		WithStats: withStats,
	}).Build()
	rows, err := db.Query(req, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []FileDesc
	for rows.Next() {
		var (
			columnIds           []int64
			columnSizes         []int64
			valueCounts         []int64
			nullCounts          []int64
			minValues           []string
			maxValues           []string
			containsNans        []bool
			columnNames         []string
			partitionKeyIndices []int64
			partitionValues     []string
		)
		var file FileDesc
		err := rows.Scan(&file.Table.Id, &file.Table.Name, &file.Table.Path, &file.Id, &file.Path,
			&file.SizeBytes, &file.FooterSizeBytes, &file.RecordCount,
			pq.Array(&columnIds),
			pq.Array(&columnSizes),
			pq.Array(&valueCounts),
			pq.Array(&nullCounts),
			pq.Array(&minValues),
			pq.Array(&maxValues),
			pq.Array(&containsNans),
			pq.Array(&columnNames),
			pq.Array(&partitionKeyIndices),
			pq.Array(&partitionValues),
			pq.Array(&file.Table.OrderBy),
		)
		if err != nil {
			return nil, err
		}
		for i, id := range columnIds {
			file.ColumnStats = append(file.ColumnStats, ColumnDesc{
				Id:           id,
				Name:         columnNames[i],
				Min:          minValues[i],
				Max:          maxValues[i],
				Count:        valueCounts[i],
				NullCount:    nullCounts[i],
				ContainsNans: containsNans[i],
				SizeBytes:    columnSizes[i],
			})
		}
		for i, id := range partitionKeyIndices {
			file.FilePartitionValues = append(file.FilePartitionValues, FilePartitionValue{
				PartitionKeyIndex: id,
				PartitionValue:    partitionValues[i],
			})
		}
		files = append(files, file)
	}
	return files, nil
}

const (
	//TODO: configuration
	SCHEMA_NAME = "main"
)

func FinishMerge(ctx context.Context, delete []FileDesc, add []FileDesc, table *TableDesc) error {
	db, err := getMetadataDB()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var delFileIds []int64
	for _, file := range delete {
		delFileIds = append(delFileIds, file.Id)
	}

	requests := []string{
		`INSERT INTO ducklake_files_scheduled_for_deletion 
SELECT data_file_id, $1 || '/' || $2 || path as path, true as path_is_relative, NOW() as schedule_start
FROM ducklake_data_file
WHERE data_file_id = ANY($3::INT8[])`,
		`WITH files AS (
	select sum(record_count) as rc, sum(file_size_bytes) as fsb FROM ducklake_data_file WHERE data_file_id = ANY($1::INT8[])
) UPDATE ducklake_table_stats SET 
	record_count = record_count - (select rc FROM files), 
	file_size_bytes = file_size_bytes - (SELECT fsb FROM files)
WHERE table_id = $2`,
		`DELETE FROM ducklake_data_file WHERE data_file_id = ANY($1::INT8[])`,
	}
	args := [][]any{
		{SCHEMA_NAME, table.Name, pq.Array(delFileIds)},
		{pq.Array(delFileIds), table.Id},
		{pq.Array(delFileIds)},
	}

	for i := range add {
		columnDesc, _ := json.Marshal(&add[i].ColumnStats)
		fmt.Println(string(columnDesc))
		req := `
WITH a as (
    INSERT INTO ducklake_snapshot
    SELECT max(snapshot_id) + 1, NOW(), max(schema_version), max(next_catalog_id), max(next_file_id) + 1
    FROM (SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1) AS base
    RETURNING *),
b as (INSERT INTO ducklake_snapshot_changes SELECT max(snapshot_id), 'inserted_into_table:' || $1 FROM a RETURNING *),
c as (UPDATE ducklake_table_stats
    SET next_row_id = next_row_id + $2, record_count = record_count + $2, file_size_bytes = file_size_bytes + $3
    WHERE table_id = $4
    RETURNING *),
d as (INSERT INTO ducklake_data_file
    SELECT max(next_file_id) - 1,
           $4,
           max(snapshot_id),
           null,
           null,
           $5,
           true,
           'parquet',
           $2,
           $3,
           $6,
           (select max(next_row_id - $2) FROM c),
           $7 FROM a RETURNING *),
e as (INSERT INTO ducklake_file_column_statistics SELECT
        (select max(next_file_id) - 1 from a) as data_file_id,
        $4 as table_id,
        (elem->>'column_id')::int8 as column_id,
        (elem->>'column_size_bytes')::int8 as column_size_bytes,
        (elem->>'value_count')::int8 as value_count,
        (elem->>'null_count')::int8 as null_count,
        elem->>'min_value' AS min_value,
        elem->>'max_value' AS max_value,
        null as contains_nan
    FROM
        json_array_elements($8::json) AS elem
    RETURNING *)
select * from e;`
		requests = append(requests, req)
		args = append(args, []any{
			strconv.FormatInt(int64(table.Id), 10),
			add[i].RecordCount,
			add[i].SizeBytes,
			table.Id,
			add[i].Path,
			add[i].FooterSizeBytes,
			add[i].PartitionId,
			string(columnDesc),
		})
	}

	for i, r := range requests {
		_, err = tx.Exec(r, args[i]...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
