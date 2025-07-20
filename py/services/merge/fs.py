import os
import aiohttp
from typing import List
from config import settings
from utils.ddb import async_duckdb_connection

class FsMergeServicePerformer:
    def __init__(self, data_path: str, table):
        self.data_path = data_path
        self.table = table

    def get_abs_paths(self, rel_paths: List[str]) -> List[str]:
        return [os.path.join(self.data_path, p) for p in rel_paths]

    async def merge_first_iteration(self, merge_plan):
        tmp_file_path = os.path.join(self.data_path, merge_plan.to + ".tmp")
        final_file_path = os.path.join(self.data_path, merge_plan.to)

        async with async_duckdb_connection() as conn:
            create_table_sql = f"""
            COPY (
                FROM read_parquet(
                    ARRAY[{','.join(f"'{p}'" for p in self.get_abs_paths(merge_plan.from_paths))}],
                    hive_partitioning=false,
                    union_by_name=true
                )
                ORDER BY {', '.join(f'{col} ASC' for col in self.table.order_by)}
            ) TO '{tmp_file_path}' (FORMAT 'parquet')
            """
            await conn.aexecute(create_table_sql)

        os.rename(tmp_file_path, final_file_path)

    async def merge_many(self, merge_plan):
        async with async_duckdb_connection() as conn:
            await self.install_chsql(conn)

            tmp_file_path = os.path.join(self.data_path, merge_plan.to + ".tmp")
            from_paths = [os.path.join(self.data_path, p) for p in merge_plan.from_paths]

            create_table_sql = f"""
            COPY (
                SELECT * FROM read_parquet_mergetree(
                    ARRAY[{','.join(f"'{p}'" for p in from_paths)}],
                    '{','.join(self.table.order_by)}'
                )
            ) TO '{tmp_file_path}' (FORMAT 'parquet')
            """
            await conn.aexecute(create_table_sql)

        os.rename(tmp_file_path, os.path.join(self.data_path, merge_plan.to))

    async def merge_one(self, merge_plan):
        from_path = os.path.join(self.data_path, merge_plan.from_paths[0])
        to_path = os.path.join(self.data_path, merge_plan.to)
        os.rename(from_path, to_path)

    @staticmethod
    async def install_chsql(conn):
        if settings.CHSQL_EXT_URL == "community":
            await conn.aexecute("INSTALL chsql FROM community")
            await conn.aexecute("LOAD chsql")
        else:
            version = await conn.aquery("SELECT version()")
            platform = await conn.aquery("PRAGMA platform")

            chsql_url = settings.CHSQL_EXT_URL.format(
                VER=settings.CHSQL_VER,
                DUCKDB_VER=version[0][0],
                ARCH=platform[0][0]
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(chsql_url) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed to download CHSQL extension: {resp.status}")
                    content = await resp.read()

            with open("/tmp/chsql.duckdb_extension", "wb") as f:
                f.write(content)

            await conn.aexecute("INSTALL '/tmp/chsql.duckdb_extension'")
            await conn.aexecute("LOAD 'chsql'")

    @staticmethod
    async def download_to_temp_file(url: str, fname: str) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to download file: {resp.status}")
                content = await resp.read()

        with open(f"/tmp/{fname}", "wb") as f:
            f.write(content)

        return f"/tmp/{fname}"