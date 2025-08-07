from typing import List, Any, Tuple, Optional

class FileSeeker:
    def __init__(self):
        self.iteration: int = 0
        self.table: str = ""
        self.with_stats: bool = False
        self.with_: List[str] = []
        self.columns: List[str] = []
        self.left_join: List[str] = []
        self.where: List[str] = []
        self.args: List[Any] = []

    def build(self) -> Tuple[str, List[Any]]:
        self.init()
        self.set_table()
        self.set_iteration()
        self.set_with_stats()
        self.set_with_partition_values()

        sql = f"SELECT {', '.join(self.columns)} FROM {' LEFT JOIN '.join(self.left_join)}"

        if self.with_:
            sql = f"WITH {', '.join(self.with_)} {sql}"

        if self.where:
            sql += f" WHERE {' AND '.join(self.where)}"

        return sql, self.args

    def init(self):
        self.with_ = [
            """
            table_sort AS (
                SELECT array_agg(dc.column_name) as names, dct.table_id 
                FROM ducklake_column_tag as dct 
                LEFT JOIN ducklake_column as dc ON dct.column_id = dc.column_id and dct.table_id = dc.table_id  
                WHERE dct.end_snapshot is null AND key='comment' AND value='orderby.1'
                GROUP BY dct.table_id
            )
            """
        ]
        self.columns = [
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
        ]
        self.left_join = [
            "ducklake_data_file as file",
            "ducklake_table as tbl ON file.table_id = tbl.table_id",
            "table_sort ON file.table_id = table_sort.table_id",
        ]
        self.where = []
        self.args = []

    def set_table(self):
        if not self.table:
            return
        self.where.append(f"tbl.table_name = ${len(self.args) + 1}")
        self.args.append(self.table)

    def set_iteration(self):
        if self.iteration == 0:
            return
        elif self.iteration == 1:
            self.where.append("file.path NOT SIMILAR TO '%.[0-9].parquet'")
        else:
            self.where.append(f"file.path SIMILAR TO '%.{self.iteration}.parquet'")

    def set_with_stats(self):
        if not self.with_stats:
            return
        self.with_.append(
            """
            col_stats AS (
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
                FROM ducklake_file_column_statistics as dfcs 
                LEFT JOIN ducklake_column as dc ON dfcs.column_id = dc.column_id 
                GROUP BY data_file_id
            )
            """
        )
        self.left_join.append("col_stats ON file.data_file_id = col_stats.data_file_id")
        self.columns[8] = "col_stats.column_ids as column_ids"
        self.columns[9] = "col_stats.column_sizes as column_sizes"
        self.columns[10] = "col_stats.value_counts as value_counts"
        self.columns[11] = "col_stats.null_counts as null_counts"
        self.columns[12] = "col_stats.min_values as min_values"
        self.columns[13] = "col_stats.max_values as max_values"
        self.columns[14] = "col_stats.contains_nans as contains_nans"
        self.columns[15] = "col_stats.column_names as column_names"

    def set_with_partition_values(self):
        if not self.with_stats:
            return
        self.with_.append(
            """
            partition_values AS (
                SELECT 
                    data_file_id, 
                    array_agg(partition_key_index) AS partition_key_indices, 
                    array_agg(partition_value) AS partition_values
                FROM ducklake_file_partition_value
                GROUP BY data_file_id
            )
            """
        )
        self.left_join.append("partition_values AS pv ON file.data_file_id = pv.data_file_id")
        self.columns[16] = "pv.partition_key_indices as partition_key_indices"
        self.columns[17] = "pv.partition_values as partition_values"