import json
import logging
from typing import List, Optional
import asyncpg
from pydantic import BaseModel

from config import settings, postgres_connection_dict
from services.metadata.ducklake.file_seeker import FileSeeker

mdb = None

async def get_metadata_db():

    conn_dict = postgres_connection_dict()
    mdb = await asyncpg.connect(**conn_dict)
    return mdb

class TableDesc(BaseModel):
    id: int
    name: str
    path: str
    order_by: List[str]

class ColumnDesc(BaseModel):
    column_id: int = 0
    column_name: str = ""
    min_value: str = ""
    max_value: str = ""
    value_count: int
    null_count: int
    contains_nans: bool
    column_size_bytes: int

class FilePartitionValue(BaseModel):
    partition_key_index: int
    partition_value: str

class FileDesc(BaseModel):
    id: int
    table: TableDesc
    path: str
    size_bytes: int
    footer_size_bytes: int
    record_count: int
    column_stats: Optional[List[ColumnDesc]]
    file_partition_values: Optional[List[FilePartitionValue]]
    partition_id: int

async def get_files(table: str, iteration: int, with_stats: bool) -> List[FileDesc]:
    db = await get_metadata_db()
    seeker = FileSeeker()
    seeker.table = table
    seeker.iteration = iteration
    seeker.with_stats = with_stats
    query, args = seeker.build()

    rows = await db.fetch(query, *args)

    files = []
    for row in rows:
        ic(row)
        file = FileDesc(
            id=row['file_id'],
            table=TableDesc(
                id=row['table_id'],
                name=row['table_name'],
                path=row['table_path'],
                order_by=row['sort']
            ),
            path=row['file_path'],
            size_bytes=row['file_size_bytes'],
            footer_size_bytes=row['footer_size'],
            record_count=row['record_count'],
            column_stats=[
                ColumnDesc(
                    column_id=col_id,
                    column_name=col_name,
                    min_value=min_val,
                    max_value=max_val,
                    value_count=val_count,
                    null_count=null_count,
                    contains_nans=contains_nan,
                    column_size_bytes=col_size
                )
                for col_id, col_name, min_val, max_val, val_count, null_count, contains_nan, col_size in zip(
                    row['column_ids'], row['column_names'], row['min_values'], row['max_values'],
                    row['value_counts'], row['null_counts'], row['contains_nans'], row['column_sizes']
                )
            ],
            file_partition_values=[
                FilePartitionValue(
                    partition_key_index=key_index,
                    partition_value=value
                )
                for key_index, value in zip(row['partition_key_indices'], row['partition_values'])
            ],
            partition_id=0  # Assuming this is not provided in the query result
        )
        files.append(file)

    return files

SCHEMA_NAME = "main"  # TODO: configuration

async def finish_merge(delete: List[FileDesc], add: List[FileDesc], table: TableDesc):
    db = await get_metadata_db()

    async with db.transaction():
        del_file_ids = [file.id for file in delete]

        # Requests equivalent to the Go version
        await db.execute(
            """
            INSERT INTO ducklake_files_scheduled_for_deletion 
            SELECT data_file_id, $1 || '/' || $2 || path as path, true as path_is_relative, NOW() as schedule_start
            FROM ducklake_data_file
            WHERE data_file_id = ANY($3::INT8[])
            """,
            SCHEMA_NAME, table.name, del_file_ids
        )

        await db.execute(
            """
            WITH files AS (
                SELECT sum(record_count) as rc, sum(file_size_bytes) as fsb 
                FROM ducklake_data_file 
                WHERE data_file_id = ANY($1::INT8[])
            ) 
            UPDATE ducklake_table_stats SET 
                record_count = record_count - (SELECT rc FROM files), 
                file_size_bytes = file_size_bytes - (SELECT fsb FROM files)
            WHERE table_id = $2
            """,
            del_file_ids, table.id
        )

        await db.execute(
            "DELETE FROM ducklake_data_file WHERE data_file_id = ANY($1::INT8[])",
            del_file_ids
        )

        for file in add:
            column_desc = json.dumps([col.dict() for col in file.column_stats])
            ic(file)
            ic(file.file_partition_values)
            file_partition_values = json.dumps([partition_value.__dict__ for partition_value in file.file_partition_values])
            await db.execute(
                """
                WITH a AS (
                    INSERT INTO ducklake_snapshot
                    SELECT max(snapshot_id) + 1, NOW(), max(schema_version), max(next_catalog_id), max(next_file_id) + 1
                    FROM (SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1) AS base
                    RETURNING *
                ),
                b AS (
                    INSERT INTO ducklake_snapshot_changes 
                    SELECT max(snapshot_id), 'inserted_into_table:' || $1 FROM a 
                    RETURNING *
                ),
                c AS (
                    UPDATE ducklake_table_stats
                    SET next_row_id = next_row_id + $2, record_count = record_count + $2, file_size_bytes = file_size_bytes + $3
                    WHERE table_id = $4
                    RETURNING *
                ),
                d AS (
                    INSERT INTO ducklake_data_file
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
                           (SELECT max(next_row_id - $2) FROM c),
                           $7 FROM a 
                    RETURNING *
                ),
                e AS (
                    INSERT INTO ducklake_file_column_statistics 
                    SELECT
                        (SELECT max(next_file_id) - 1 FROM a) as data_file_id,
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
                    RETURNING *
                ),
                f AS (
                    INSERT INTO ducklake_file_partition_value
                    SELECT 
                        (SELECT max(next_file_id) - 1 FROM a) as data_file_id,
                        $4 as table_id,
                        (elem->>'partition_key_index')::int8 as partition_key_index,
                        (elem->>'partition_value')::text as partition_value
                    FROM json_array_elements($9::json) AS elem
                    RETURNING *
                )
                SELECT * FROM f;
                """,
                str(table.id),
                file.record_count,
                file.size_bytes,
                table.id,
                file.path,
                file.footer_size_bytes,
                file.partition_id,
                column_desc,
                file_partition_values #TODO
            )