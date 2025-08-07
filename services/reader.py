from duckdb.duckdb import DuckDBPyRelation
from utils.ddb import async_ducklake_connection, AsyncDuckDBConnection
from icecream import ic

async def query(request: str, database: str = None):
    async with (async_ducklake_connection() as conn):
        if database:
            if database == "memory":
                await conn.aexecute(f"USE {database};")
            else:
                await conn.aexecute(f"USE {database}.master;")
        query_result: DuckDBPyRelation = await conn.aquery(request)
        column_names = []

        if query_result is not None:
            column_names = [col[0] for col in query_result.description]

        async def get():
            if query_result is None:
                return
            while True:
                row = query_result.fetchone()
                if row is None:
                    break
                yield dict(zip(column_names, row))
        return get
