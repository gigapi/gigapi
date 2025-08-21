import duckdb
from duckdb import DuckDBPyConnection
from typing import Dict
import os
import json

schema = """
CREATE TABLE IF NOT EXISTS kv(
    key TEXT PRIMARY KEY,
    value TEXT
);"""

class FileStore:
    def __init__(self, base_path: str):
        self.base_path = base_path
        if os.path.exists(os.path.join(base_path, "kv.db")):
            self.conn = duckdb.connect(os.path.join(base_path, "kv.db"))
            self.kv = {}
            self.conn.execute(schema)
            rows = self.conn.execute("SELECT key, value FROM kv").fetchall()
            self.kv = {row[0]: row[1] for row in rows}
        elif os.path.exists(os.path.join(base_path, "kv.json")):
            with open(os.path.join(base_path, "kv.json"), "r") as f:
                self.kv = json.load(f)
            self.conn = duckdb.connect(os.path.join(base_path, "kv.db"))
            self.conn.execute(schema)
            for k, v in self.kv.items():
                self.set(k, v)
        else:
            self.conn = duckdb.connect(os.path.join(base_path, "kv.db"))
            self.conn.execute(schema)
            self.kv = {}

    def set(self, key: str, value: str):
        conn = self.conn.cursor()
        try:
            self.kv[key] = value
            conn.execute("""INSERT INTO kv (key, value) 
VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;""", (key, value))
        finally:
            conn.close()
    def delete(self, key: str):
        conn = self.conn.cursor()
        try:
            del self.kv[key]
            conn.execute("DELETE FROM kv WHERE key = $1", (key,))
        finally:
            conn.close()

    def get(self, key: str) -> str:
        return self.kv.get(key, "")