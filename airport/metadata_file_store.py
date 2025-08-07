import os.path
import shutil

import msgpack

from airport.model import TableInfo, encode_custom, decode_custom, MetaStore, TableFile
from icecream import ic
from duckdb import duckdb

create_metadata_schema_sql = """
CREATE TABLE IF NOT EXISTS files (
    filename TEXT PRIMARY KEY,
    file BLOB
);

CREATE TABLE IF NOT EXISTS schema (
    id INT2 PRIMARY KEY,
    schema BLOB
);"""

class MetadataFileStore(MetaStore):
    def __init__(self, base: str, database: str, schema: str, table: str, table_info: TableInfo = None):
        self.base = base
        self.database = database
        self.table = table
        self.table_info = table_info
        self.schema = schema
        self.conn = duckdb.connect(os.path.join(self.base, self.database, self.schema, self.table, "metadata.db" ))
        self.conn.execute(create_metadata_schema_sql)


    def on_schema_update(self):
        if self.table_info and self.table_info.table_schema:
            schema_blob = msgpack.packb(self.table_info.table_schema, default=encode_custom)
            self.conn.execute("""
INSERT INTO schema (id, schema)
VALUES (1, ?)
ON CONFLICT (id) DO UPDATE SET schema = excluded.schema
""", [schema_blob])
        else:
            print("Warning: Cannot update schema. TableInfo or table_schema is None.")

    def on_files_update(self, files_added: list[TableFile], files_removed: list[TableFile]):
        for file in files_added:
            fileb = msgpack.packb(file, default=encode_custom)
            self.conn.execute("INSERT INTO files (filename, file) VALUES ($1, $2)", (file.filename, fileb))
        for file in files_removed:
            self.conn.execute("DELETE FROM files WHERE filename = $1", (file.filename))

    def load(self):
        echema_exists = self.conn.execute("SELECT COUNT(*) FROM schema WHERE id = 1").fetchone()[0] == 1
        if not echema_exists:
            return None
        schema = self.conn.query("SELECT schema FROM schema where id = 1").fetchone()
        schema = msgpack.unpackb(schema[0], object_hook=decode_custom)
        files: list[TableFile] = []
        for file in self.conn.query("SELECT file FROM files").fetchall():
            f = msgpack.unpackb(file[0], object_hook=decode_custom)
            files.append(f)
        self.table_info = TableInfo(
            table_schema=schema,
            contents=files,
            table_versions=[],
            meta_store=self
        )
        return self.table_info
