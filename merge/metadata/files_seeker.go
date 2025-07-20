package metadata

import (
	"fmt"
	"strings"
)

type fileSeeker struct {
	Iteration int
	Table     string
	WithStats bool

	with     []string
	columns  []string
	leftJoin []string
	where    []string
	args     []any
}

func (fs *fileSeeker) Build() (string, []any, error) {
	fs.init()
	fs.setTable()
	fs.setIteration()
	fs.setWithStats()
	fs.setWithPartitionValues()

	sql := fmt.Sprintf(
		`SELECT %s FROM %s`,
		strings.Join(fs.columns, ", "),
		strings.Join(fs.leftJoin, " LEFT JOIN "),
	)

	if len(fs.with) > 0 {
		sql = "WITH " + strings.Join(fs.with, ", ") + sql
	}

	if len(fs.where) > 0 {
		sql += " WHERE " + strings.Join(fs.where, " AND ")
	}

	return sql, fs.args, nil
}

func (fs *fileSeeker) init() {
	fs.with = []string{
		`table_sort AS (
SELECT array_agg(dc.column_name) as names, dct.table_id 
FROM ducklake_column_tag as dct 
LEFT JOIN ducklake_column as dc ON dct.column_id = dc.column_id and dct.table_id = dc.table_id  
WHERE dct.end_snapshot is null AND key='comment' AND value='orderby.1'
GROUP BY dct.table_id
)`,
	}
	fs.columns = []string{
		"tbl.table_id",
		"table_name",
		"tbl.path as table_path",
		"file.data_file_id as file_id",
		"file.path as file_path",
		"file.file_size_bytes",
		"file.footer_size",
		"file.record_count",
		"'{}'::bigint[] as column_ids",
		"ARRAY[]::BIGINT[] as column_sizes",
		"ARRAY[]::BIGINT[] as value_counts",
		"ARRAY[]::BIGINT[] as null_counts",
		"ARRAY[]::VARCHAR[] as min_values",
		"ARRAY[]::VARCHAR[] as max_values",
		"ARRAY[]::BOOLEAN[] as contains_nans",
		"ARRAY[]::VARCHAR[] as column_names",
		"ARRAY[]::BIGINT[] as partition_key_indices",
		"ARRAY[]::VARCHAR[] as partition_values",
		"coalesce(table_sort.names, ARRAY[]::VARCHAR[]) AS sort",
	}
	fs.leftJoin = []string{
		"ducklake_data_file as file",
		"ducklake_table as tbl ON file.table_id = tbl.table_id",
		"table_sort ON file.table_id = table_sort.table_id",
	}
	fs.where = nil
	fs.args = nil
}

func (fs *fileSeeker) setTable() {
	if fs.Table == "" {
		return
	}
	fs.where = append(fs.where, fmt.Sprintf("tbl.table_name = $%d", len(fs.args)+1))
	fs.args = append(fs.args, fs.Table)
}

func (fs *fileSeeker) setIteration() {
	switch fs.Iteration {
	case 0:
		return
	case 1:
		fs.where = append(fs.where, "file.path NOT SIMILAR TO '%.[0-9].parquet'")
	default:
		fs.where = append(fs.where, fmt.Sprintf("file.path SIMILAR TO '%%.%d.parquet'", fs.Iteration))
	}
}

func (fs *fileSeeker) setWithStats() {
	if !fs.WithStats {
		return
	}
	fs.with = append(fs.with, `col_stats AS (
SELECT 
    dfcs.data_file_id,
    array_agg(dfcs.column_id) AS column_ids,
    array_agg(dc.column_name) as column_names,
    array_agg(column_size_bytes) AS column_sizes,
    array_agg(value_count) AS value_counts,
    array_agg(null_count) AS null_counts,
    array_agg(min_value) AS min_values,
    array_agg(max_value) AS max_values,
    array_agg(coalesce(contains_nan, false)) AS contains_nans
FROM ducklake_file_column_statistics as dfcs LEFT JOIN ducklake_column as dc ON dfcs.column_id = dc.column_id 
GROUP BY data_file_id
)`)
	fs.leftJoin = append(fs.leftJoin, "col_stats ON file.data_file_id = col_stats.data_file_id")
	fs.columns[8] = "col_stats.column_ids as column_ids"
	fs.columns[9] = "col_stats.column_sizes as column_sizes"
	fs.columns[10] = "col_stats.value_counts as value_counts"
	fs.columns[11] = "col_stats.null_counts as null_counts"
	fs.columns[12] = "col_stats.min_values as min_values"
	fs.columns[13] = "col_stats.max_values as max_values"
	fs.columns[14] = "col_stats.contains_nans as contains_nans"
	fs.columns[15] = "col_stats.column_names as column_names"
}

func (fs *fileSeeker) setWithPartitionValues() {
	if !fs.WithStats {
		return
	}
	fs.with = append(fs.with, `partition_values AS (
SELECT 
	data_file_id, 
	array_agg(partition_key_index) AS partition_key_indices, 
	array_agg(partition_value) AS partition_values
FROM ducklake_file_partition_value
GROUP BY data_file_id
)`)
	fs.leftJoin = append(fs.leftJoin, "partition_values AS pv ON file.data_file_id = pv.data_file_id")
	fs.columns[16] = "pv.partition_key_indices as partition_key_indices"
	fs.columns[17] = "pv.partition_values as partition_values"
}
