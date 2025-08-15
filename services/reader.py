from duckdb.duckdb import DuckDBPyRelation
from utils.ddb import async_ducklake_connection, AsyncDuckDBConnection
from icecream import ic
import asyncio
lock = asyncio.Lock()
async def query(request: str, database: str = None):
    if database:
        if database == "memory":
            request = f"USE {database};" + request
        else:
            request = f"USE {database}.master;" + request

    async def get():
        global lock
        async with lock:
            async with (async_ducklake_connection() as conn):
                query_result: DuckDBPyRelation = await conn.aquery(request)
                column_names = []

                if query_result is not None:
                    column_names = [col[0] for col in query_result.description]

                if query_result is None:
                    return
                while True:
                    row = query_result.fetchone()
                    if row is None:
                        break
                    yield dict(zip(column_names, row))
    return get
