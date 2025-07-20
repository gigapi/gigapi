from duckdb.duckdb import DuckDBPyRelation

from utils.ddb import async_duckdb_connection, AsyncDuckDBConnection




async def query(request: str, database: str = None):
    async with (async_duckdb_connection() as conn):
        if database:
            await conn.aexecute(f"USE {database};")
        query_result: DuckDBPyRelation = await conn.aquery(request)
        if query_result is None:
            return
        column_names = [col[0] for col in query_result.description]
        while True:
            row = query_result.fetchone()
            if row is None:
                break
            yield dict(zip(column_names, row))
