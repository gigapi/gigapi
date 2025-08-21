import os.path
import shutil
import time
from contextlib import contextmanager
from string import Template

import msgpack

from .model import encode_custom, decode_custom, MetaStore, TableFile, MergePlansByFolder
from .delete_planner import DeletePlanner
from .table import TableInfo
from duckdb import duckdb
from .merge_planner import MergePlanner


create_metadata_schema_sql = """
CREATE TABLE IF NOT EXISTS $DB.files (
    filename TEXT PRIMARY KEY,
    file BLOB
);

CREATE TABLE IF NOT EXISTS $DB.schema (
    id INT2 PRIMARY KEY,
    schema BLOB
);

CREATE TABLE IF NOT EXISTS $DB.merge_plans
(
    id            TEXT PRIMARY KEY,
    plans         BLOB
);
"""

conn = duckdb.connect()
discovery_stats = {
    'db_load': 0,
    'data_load': 0,
    'merge_load': 0,
    'delete_load': 0,
}

@contextmanager
def cursor():
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

class MetadataFileStore(MetaStore):
    def __init__(self, base: str, database: str, schema: str, table: str, table_info: TableInfo = None,
                 merge_planner: MergePlanner = None, delete_planner: DeletePlanner = None):
        self.base = base
        self.database = database
        self.table = table
        self.table_info = table_info
        self.schema = schema
        mdb_path = os.path.join(self.base, database, schema, table, "metadata.db")
        self.mdbname = f"{self.database}_{self.schema}_{self.table}"
        s = time.time()
        with cursor() as conn:
            conn.execute(f"ATTACH DATABASE '{mdb_path}' AS {self.mdbname}")
            discovery_stats['db_load'] += time.time() - s
            t = Template(create_metadata_schema_sql)
            conn.execute(t.substitute({"DB": self.mdbname}))
        self.merge_planner = merge_planner
        if self.merge_planner:
            self.merge_planner.on_change = self.on_merge_planner_change
        self.delete_planner = delete_planner
        if self.delete_planner:
            self.delete_planner.on_change = self.on_delete_planner_change

    def on_schema_update(self):
        with cursor() as conn:
            self._on_schema_update(conn)
    def _on_schema_update(self, conn):
        if self.table_info and self.table_info.table_schema:
            schema_blob = msgpack.packb(self.table_info.table_schema, default=encode_custom)
            conn.execute(f"""
INSERT INTO {self.mdbname}.schema (id, schema)
VALUES (1, ?)
ON CONFLICT (id) DO UPDATE SET schema = excluded.schema
""", [schema_blob])
        else:
            print("Warning: Cannot update schema. TableInfo or table_schema is None.")

    def on_files_update(self, files_added: list[TableFile], files_removed: list[TableFile]):
        with cursor() as conn:
            self._on_files_update(files_added, files_removed, conn)

    def _on_files_update(self, files_added: list[TableFile], files_removed: list[TableFile], conn):
        for file in files_added:
            fileb = msgpack.packb(file, default=encode_custom)
            conn.execute(f"""INSERT INTO {self.mdbname}.files (filename, file) 
VALUES ($1, $2)
ON CONFLICT (filename) DO UPDATE SET file = excluded.file""", [file.filename, fileb])
        for file in files_removed:
            conn.execute(f"DELETE FROM {self.mdbname}.files WHERE filename = $1", [file.filename])

    def load(self):
        with cursor() as conn:
            return self._load(conn)

    def _load(self, conn):
        s = time.time()
        echema_exists = conn.execute(f"SELECT COUNT(*) FROM {self.mdbname}.schema WHERE id = 1").fetchone()[0] == 1
        if not echema_exists:
            self.detach()
            return None
        schema = conn.query(f"SELECT schema FROM {self.mdbname}.schema where id = 1").fetchone()
        schema = msgpack.unpackb(schema[0], object_hook=decode_custom)
        files: list[TableFile] = []
        for file in conn.query(f"SELECT file FROM {self.mdbname}.files").fetchall():
            f = msgpack.unpackb(file[0], object_hook=decode_custom)
            files.append(f)
        s1 = time.time()
        self.load_merge_planner(files, conn)
        discovery_stats['merge_load'] += time.time() - s1
        s1 = time.time()
        self.load_delete_planner(conn)
        discovery_stats['delete_load'] += time.time() - s1
        self.table_info = TableInfo(
            table_schema=schema,
            contents=files,
            table_versions=[],
            meta_store=self,
            merge_planner=self.merge_planner,
            delete_planner=self.delete_planner,
        )
        discovery_stats['data_load'] += time.time() - s
        return self.table_info

    def load_merge_planner(self, files: list[TableFile], conn):
        merge_plans = conn.query(f"SELECT plans FROM {self.mdbname}.merge_plans WHERE id = 1").fetchone()
        if merge_plans:
            merge_plans = msgpack.unpackb(merge_plans[0], object_hook=decode_custom)
            for mps in merge_plans.merge_plans.values():
                for mp in mps:
                    mp.updated_at = 0
            self.merge_planner = MergePlanner(self.base, self.database, self.schema, self.table, merge_plans)
            self.merge_planner.on_change = self.on_merge_planner_change
        else:
            self.merge_planner = MergePlanner(self.base, self.database, self.schema, self.table)
            for f in files:
                self.merge_planner.add_file(f)
            self.merge_planner.on_change = self.on_merge_planner_change
            self.on_merge_planner_change(self.merge_planner)

    def load_delete_planner(self, conn):
        delete_plans = conn.query(f"SELECT plans FROM {self.mdbname}.merge_plans WHERE id = 2").fetchone()
        if delete_plans:
            delete_plans = msgpack.unpackb(delete_plans[0], object_hook=decode_custom)
            self.delete_planner = DeletePlanner(self.base, self.database, self.schema, self.table, delete_plans)
            self.delete_planner.on_change = self.on_delete_planner_change
        else:
            self.delete_planner = DeletePlanner(self.base, self.database, self.schema, self.table)
            self.delete_planner.on_change = self.on_delete_planner_change


    def on_merge_planner_change(self, planner: MergePlanner):
        with cursor() as conn:
            self._on_merge_planner_change(planner, conn)

    def _on_merge_planner_change(self, planner: MergePlanner, conn):
        plan_blob = msgpack.packb(planner.merge_plans, default=encode_custom)
        conn.execute(f"""
INSERT INTO {self.mdbname}.merge_plans (id, plans)
VALUES (1, ?)
ON CONFLICT (id) DO UPDATE SET plans = excluded.plans
""", [plan_blob])

    def on_delete_planner_change(self, planner: DeletePlanner):
        with cursor() as conn:
            self._on_delete_planner_change(planner, conn)

    def _on_delete_planner_change(self, planner: DeletePlanner, conn):
        plan_blob = msgpack.packb(planner.delete_plans, default=encode_custom)
        conn.execute(f"""
INSERT INTO {self.mdbname}.merge_plans (id, plans)
VALUES (2,?)
ON CONFLICT (id) DO UPDATE SET plans = excluded.plans
""", [plan_blob])


    def detach(self):
        with cursor as conn:
            conn.execute(f"DETACH DATABASE {self.mdbname}")
