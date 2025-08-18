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
    def __init__(self, conn: duckdb.DuckDBPyConnection, temporary: bool = False):
        self.conn = conn
        self.temporary = temporary

    async def aexecute(self, query: str, *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(self.conn.execute, query, *args, **kwargs))

    async def aquery(self, *args, **kwargs) -> DuckDBPyRelation:
        loop = asyncio.get_running_loop()
        return  await loop.run_in_executor(None, functools.partial(self.conn.query, *args, **kwargs))

    def close(self):
        if self.temporary:
            self.conn.close()


conn: DuckDBPyRelation | None = None

def connect_duckdb(conn_str: str = None, temporary: bool = False) -> DuckDBPyConnection:
    global conn
    if not conn_str:
        conn_str = get_default_duckdb_conn_str()
    if conn and not temporary:
        return conn
    _conn = duckdb.connect(conn_str)
    _conn.register_filesystem(memfs)
    if not temporary:
        conn = _conn
    return _conn


def connect_airport(conn_str: str = None, temporary: bool = False) -> Tuple[AsyncDuckDBConnection, Callable[[], None]]:
    try:
        if conn is not None and not temporary:
            aconn = AsyncDuckDBConnection(conn)
            def cancel():
                aconn.close()
            return aconn, cancel
        _conn: DuckDBPyConnection = connect_duckdb(conn_str, temporary)
        if settings.gigapi.metadata.type == "ducklake":
            # Install the ducklake extension
            _conn.execute("INSTALL airport FROM community;")
            _conn.execute("LOAD airport")

            _conn.execute("""
CREATE SECRET airport_testing (type airport, auth_token 'example_token', scope 'grpc://localhost:60001/');
""")
            query_result: DuckDBPyRelation = _conn.query("SELECT name from airport_databases('grpc://localhost:60001/')")
            databases = query_result.fetchall()
            if len(databases) == 0:
                _conn.execute("CALL airport_action('grpc://localhost:60001/', 'create_database', 'my_airport');")
                _conn.execute("ATTACH 'my_airport' (TYPE  AIRPORT, location 'grpc://localhost:60001/')")
                _conn.execute(f"CREATE SCHEMA my_airport.master;")
            else:
                for d in databases:
                    _conn.execute(f"ATTACH '{d[0]}' (TYPE  AIRPORT, location 'grpc://localhost:60001/')")

        if get_duckdb_mem_limit():
            _conn.execute(f"SET memory_limit='{get_duckdb_mem_limit()}'")
        if get_duckdb_thread_limit():
            _conn.execute(f"SET threads TO {get_duckdb_thread_limit()}")

        async_conn = AsyncDuckDBConnection(_conn)

        # Define cancel function
        def cancel():
            async_conn.close()

        return async_conn, cancel

    except Exception as e:
        raise Exception(f"Failed to connect to DuckDB/Ducklake: {str(e)}")

@asynccontextmanager
async def async_ducklake_connection(conn_str: str = None, temporary: bool = False):
    loop = asyncio.get_running_loop()
    async_conn, cancel = await loop.run_in_executor(None, functools.partial(connect_airport, conn_str, temporary))
    try:
        yield async_conn
    finally:
        await loop.run_in_executor(None, cancel)

@asynccontextmanager
async def async_duckdb_connection(conn_str: str = None):
    loop = asyncio.get_running_loop()
    conn = connect_duckdb(conn_str)
    try:
        yield AsyncDuckDBConnection(conn)
    finally:
        conn.close()

def get_duckdb_mem_limit() -> Optional[str]:
    return None

def get_duckdb_thread_limit() -> Optional[int]:
    return None

def get_default_duckdb_conn_str() -> str:
    return ":memory:"