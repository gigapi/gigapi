from utils.ddb import async_duckdb_connection, memfs
import uuid

async def write_json(data: bytes, table: str):
    fname = f"{uuid.uuid4()}.json"
    async with async_duckdb_connection() as conn:
        with memfs.open(fname, 'wb') as f:
            f.write(data)
        try:
            await conn.aexecute(f"INSERT INTO {table} SELECT * FROM read_json('memory://{fname}')")
        finally:
            memfs.delete(fname)