import duckdb
from typing import Tuple, Callable, Optional, Any, AsyncGenerator
from contextlib import contextmanager, asynccontextmanager

from duckdb.duckdb import DuckDBPyRelation, DuckDBPyConnection

from config import settings
from fsspec.implementations import memory
import asyncio
import functools

memfs = memory.MemoryFileSystem()

class AsyncDuckDBConnection:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    async def aexecute(self, query: str, *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(self.conn.execute, query, *args, **kwargs))

    async def aquery(self, *args, **kwargs) -> DuckDBPyRelation:
        loop = asyncio.get_running_loop()
        return  await loop.run_in_executor(None, functools.partial(self.conn.query, *args, **kwargs))

    def close(self):
        self.conn.close()

def connect_duckdb(conn_str: str = None) -> DuckDBPyConnection:
    if not conn_str:
        conn_str = get_default_duckdb_conn_str()
    conn = duckdb.connect(conn_str)
    conn.register_filesystem(memfs)
    return conn

def connect_ducklake(conn_str: str = None) -> Tuple[AsyncDuckDBConnection, Callable[[], None]]:

    try:
        conn = connect_duckdb(conn_str)

        if settings.gigapi.metadata.type == "ducklake":
            # Install the ducklake extension
            conn.execute("INSTALL ducklake;")
            conn.execute("LOAD ducklake;")

            # Attach to the 'my_ducklake' database
            ducklake_url = settings.gigapi.metadata.url
            q = f"ATTACH 'ducklake:{ducklake_url}' AS my_ducklake (DATA_PATH '{settings.gigapi.root}');"
            conn.execute(q)
            conn.execute("USE my_ducklake;")

        if get_duckdb_mem_limit():
            conn.execute(f"SET memory_limit='{get_duckdb_mem_limit()}'")
        if get_duckdb_thread_limit():
            conn.execute(f"SET threads TO {get_duckdb_thread_limit()}")

        async_conn = AsyncDuckDBConnection(conn)

        # Define cancel function
        def cancel():
            async_conn.close()

        return async_conn, cancel

    except Exception as e:
        raise Exception(f"Failed to connect to DuckDB/Ducklake: {str(e)}")

@asynccontextmanager
async def async_ducklake_connection(conn_str: str = None):
    loop = asyncio.get_running_loop()
    async_conn, cancel = await loop.run_in_executor(None, functools.partial(connect_ducklake, conn_str))
    try:
        yield async_conn
    finally:
        await loop.run_in_executor(None, cancel)

@asynccontextmanager
async def async_duckdb_connection(conn_str: str = None):
    loop = asyncio.get_running_loop()
    async_conn, cancel = await loop.run_in_executor(None, functools.partial(connect_duckdb, conn_str))
    try:
        yield AsyncDuckDBConnection(async_conn)
    finally:
        await loop.run_in_executor(None, cancel)

def get_duckdb_mem_limit() -> Optional[str]:
    return None

def get_duckdb_thread_limit() -> Optional[int]:
    return None

def get_default_duckdb_conn_str() -> str:
    return ":memory:"