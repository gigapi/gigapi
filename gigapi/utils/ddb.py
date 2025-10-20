import duckdb
from typing import Tuple, Callable, Optional, Any, AsyncGenerator
from contextlib import contextmanager, asynccontextmanager

from duckdb.duckdb import DuckDBPyRelation, DuckDBPyConnection

from gigapi.config import settings
from fsspec.implementations import memory
import asyncio
import functools

from gigapi.utils.url_helper import LayerUrlHelper

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
    return _conn


def get_airport_host() -> str:
    host = settings.flightsql.host
    port = settings.flightsql.port
    return f"grpc://{host}:{port}"

def connect_airport(conn_str: str = None) -> Tuple[AsyncDuckDBConnection, Callable[[], None]]:
    global conn
    try:
        if conn is not None:
            aconn = AsyncDuckDBConnection(conn.cursor())
            def cancel():
                aconn.close()
            return aconn, cancel
        _conn: DuckDBPyConnection = connect_duckdb(conn_str)
        airport_host = get_airport_host()
        if settings.gigapi.metadata.type == "ducklake":
            # Install the ducklake extension
            _conn.execute("INSTALL airport FROM community;")
            _conn.execute("LOAD airport")
            _conn.execute("INSTALL httpfs")
            _conn.execute("LOAD httpfs")

            _conn.execute(f"""
CREATE SECRET airport_testing (type airport, auth_token 'example_token', scope '{airport_host}');
""")
            init_s3(_conn)
            query_result: DuckDBPyRelation = _conn.query(f"SELECT name from airport_databases('{airport_host}')")
            databases = query_result.fetchall()
            if len(databases) == 0:
                _conn.execute(f"CALL airport_action('{airport_host}', 'create_database', 'my_airport');")
                _conn.execute(f"ATTACH 'my_airport' (TYPE  AIRPORT, location '{airport_host}')")
                _conn.execute(f"CREATE SCHEMA my_airport.master;")
            else:
                for d in databases:
                    _conn.execute(f"ATTACH '{d[0]}' (TYPE  AIRPORT, location '{airport_host}')")

        if get_duckdb_mem_limit():
            _conn.execute(f"SET memory_limit='{get_duckdb_mem_limit()}'")
        if get_duckdb_thread_limit():
            _conn.execute(f"SET threads TO {get_duckdb_thread_limit()}")

        conn = _conn

        async_conn = AsyncDuckDBConnection(_conn.cursor())

        # Define cancel function
        def cancel():
            async_conn.close()

        return async_conn, cancel

    except Exception as e:
        raise Exception(f"Failed to connect to DuckDB/Ducklake: {str(e)}")

def init_s3(conn: DuckDBPyConnection):
    for l in settings.gigapi.layers:
        if l.type != "s3":
            continue
        print(f"Initializing S3 secrets for {settings.gigapi.metadata.name}")
        h = LayerUrlHelper(l.url)
        use_ssl = "true" if h.use_ssl else "false"
        print(f"""CREATE OR REPLACE SECRET {l.name}_secret (
            TYPE S3,
        KEY_ID '{h.username}',
        SECRET 'REDACTED',
        ENDPOINT '{h.hostname}:{h.port}',
        USE_SSL {use_ssl},
        URL_STYLE path,
        SCOPE 's3://{h.bucket_name}'
        );""")
        conn.execute(f"""
CREATE OR REPLACE SECRET {l.name}_secret (
    TYPE S3,
    KEY_ID '{h.username}',
    SECRET '{h.password}',
    ENDPOINT '{h.hostname}:{h.port}',
    USE_SSL {use_ssl},
    URL_STYLE path,
    SCOPE 's3://{h.bucket_name}'
);""")

@asynccontextmanager
async def async_ducklake_connection(conn_str: str = None):
    loop = asyncio.get_running_loop()
    async_conn, cancel = await loop.run_in_executor(None, functools.partial(connect_airport, conn_str))
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