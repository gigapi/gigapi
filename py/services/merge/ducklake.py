
import os
import time
import uuid
import struct
import asyncio
from typing import List, Dict, Any
from dataclasses import dataclass

from services.merge.fs import FsMergeServicePerformer
from services.metadata import MergePlan, Table
from services.metadata.ducklake.metadata import FileDesc, ColumnDesc, get_files, SCHEMA_NAME, finish_merge
from config import settings, get_merge_configurations
from utils.ddb import async_duckdb_connection, AsyncDuckDBConnection
from .copy_column_ids import copy_column_ids

@dataclass
class DucklakePlan:
    merge_plan: MergePlan
    from_files: List[FileDesc]

class DucklakeMergeService:
    def __init__(self):
        confs = get_merge_configurations()
        self.last_merge_time: List[float] = [time.time() for _ in confs]

    async def do_merge(self) -> None:
        confs = get_merge_configurations()
        if not self.last_merge_time:
            self.last_merge_time = [time.time() for _ in confs]

        tasks = []
        for conf in confs:
            if time.time() - self.last_merge_time[conf.iteration() - 1] < conf.timeout_s():
                continue
            tasks.append(self.merge_iteration(conf))

        await asyncio.gather(*tasks)

    async def merge_iteration(self, iteration: 'mergeConfiguration') -> None:
        files = await get_files("", iteration.iteration(), True)
        files_map: Dict[str, Dict[str, List[FileDesc]]] = {}
        unsorted_tables: Dict[str, bool] = {}

        for f in files:
            if not f.table.order_by:
                unsorted_tables[f.table.name] = True
                continue
            if f.table.name not in files_map:
                files_map[f.table.name] = {}
            dir_path = os.path.dirname(f.path)
            if dir_path not in files_map[f.table.name]:
                files_map[f.table.name][dir_path] = []
            files_map[f.table.name][dir_path].append(f)

        plans: Dict[str, Dict[str, List[DucklakePlan]]] = {}
        last_plan_size = 0

        def inc_plan(table: str, dir_path: str) -> None:
            nonlocal last_plan_size
            if table not in plans:
                plans[table] = {}
            if dir_path not in plans[table]:
                plans[table][dir_path] = []
            plans[table][dir_path].append(DucklakePlan(
                merge_plan=MergePlan(
                    table=table,
                    to=os.path.join(dir_path, f"{uuid.uuid4()}.{iteration.iteration() + 1}.parquet"),
                    iteration=iteration.iteration(),
                    from_paths=[]
                ),
                from_files=[]
            ))
            last_plan_size = 0

        for table, dir_files in files_map.items():
            for dir_path, files in dir_files.items():
                inc_plan(table, dir_path)
                for f in files:
                    if last_plan_size + f.size_bytes > iteration.max_result_bytes() or not plans:
                        inc_plan(table, dir_path)
                    current_plan = plans[table][dir_path][-1]
                    current_plan.merge_plan.from_paths.append(f.path)
                    current_plan.from_files.append(f)

        for table, dir_plans in plans.items():
            for dir_path, plans_list in dir_plans.items():
                performer = FsMergeServicePerformer(
                    data_path=os.path.join(settings.gigapi.root, SCHEMA_NAME, table),
                    table=Table(order_by=[plans_list[0].from_files[0].table.order_by[0]])
                )

                for p in plans_list:
                    if not p.from_files:
                        continue
                    if iteration.iteration() == 1:
                        await performer.merge_first_iteration(p.merge_plan)
                    elif len(p.from_files) == 1:
                        await performer.merge_one(p.merge_plan)
                    else:
                        await performer.merge_many(p.merge_plan)

                    to_file_desc = await self.to_file_desc(p)
                    print(p.merge_plan.from_paths)
                    print(p.merge_plan.to)
                    print(to_file_desc)

                    await finish_merge(p.from_files, [to_file_desc], p.from_files[0].table)

        self.last_merge_time[iteration.iteration() - 1] = time.time()

    async def to_file_desc(self, p: DucklakePlan) -> FileDesc:
        added = FileDesc(
            id=0,
            table=p.from_files[0].table,
            path=p.merge_plan.to,
            footer_size_bytes=0,
            record_count=0,
            column_stats=None,
            file_partition_values=None,
            partition_id=0,
            size_bytes=0
        )

        if p.merge_plan.iteration == 1 or len(p.from_files) > 1:
            copy_column_ids(
                os.path.join(settings.gigapi.root, SCHEMA_NAME, p.merge_plan.table, p.from_files[0].path),
                os.path.join(settings.gigapi.root, SCHEMA_NAME, p.merge_plan.table, p.merge_plan.to)
            )

        file_path = os.path.join(settings.gigapi.root, SCHEMA_NAME, added.table.name, p.merge_plan.to)
        added.size_bytes = os.path.getsize(file_path)

        with open(file_path, 'rb') as file:
            file.seek(-8, 2)
            footer_metadata = file.read(8)
            added.footer_size_bytes = struct.unpack('<I', footer_metadata[:4])[0]

        async with async_duckdb_connection() as conn:
            added.column_stats = p.from_files[0].column_stats.copy()

            for fn in [self.populate_min_max, self.populate_col_stats, self.populate_file_metadata]:
                await fn(file_path, conn, added)

        return added

    async def populate_min_max(self, path: str, conn: AsyncDuckDBConnection, m: FileDesc) -> None:
        col_names = await self.get_cols(path, conn)
        sel = []
        for col in col_names:
            sel.append(f"min({col})::VARCHAR as {col}_min")
            sel.append(f"max({col})::VARCHAR as {col}_max")

        min_max_query = f"SELECT {','.join(sel)} FROM read_parquet('{path}')"
        result = (await conn.aexecute(min_max_query)).fetchone()

        mins = {col: result[i*2] for i, col in enumerate(col_names)}
        maxs = {col: result[i*2+1] for i, col in enumerate(col_names)}

        for i, col_stat in enumerate(m.column_stats):
            col_stat.min_value = mins[col_stat.column_name]
            col_stat.max_value = maxs[col_stat.column_name]

    async def get_cols(self, path: str, conn: AsyncDuckDBConnection) -> List[str]:
        result = (await conn.aexecute(f"SELECT name from parquet_schema('{path}') where type is not null")).fetchall()
        return [row[0] for row in result]

    async def populate_col_stats(self, path: str, conn: AsyncDuckDBConnection, m: FileDesc) -> None:
        query = f"""
        SELECT 
            path_in_schema, 
            sum(total_compressed_size), 
            sum(num_values),
            sum(stats_null_count)
        FROM parquet_metadata('{path}')
        GROUP BY path_in_schema
        """
        result = (await conn.aexecute(query)).fetchall()

        col_stats_map = {
            row[0]: ColumnDesc(
                value_count=row[2],
                null_count=row[3],
                contains_nans=False,
                column_size_bytes=row[1]
            ) for row in result
        }

        for i, col_stat in enumerate(m.column_stats):
            if col_stat.column_name in col_stats_map:
                stats = col_stats_map[col_stat.column_name]
                col_stat.value_count = stats.value_count
                col_stat.null_count = stats.null_count
                col_stat.contains_nans = False
                col_stat.column_size_bytes = stats.column_size_bytes
            else:
                col_stat.value_count = 0
                col_stat.null_count = 0
                col_stat.contains_nans = False
                col_stat.column_size_bytes = 0

    async def populate_file_metadata(self, path: str, conn: AsyncDuckDBConnection, m: FileDesc) -> None:
        result = (await conn.aexecute(f"SELECT num_rows FROM parquet_file_metadata('{path}')")).fetchone()
        m.record_count = result[0]
