import duckdb

def connect(conn: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(conn)