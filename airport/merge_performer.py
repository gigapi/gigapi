import uuid

from duckdb.duckdb import DuckDBPyConnection

from .configuraiton import LayerConfig, LayerType
from .fs_operator_s3 import FSOperatorS3
from .model import TableFile, MergePlan
import duckdb
import os
from dataclasses import dataclass
from .configuraiton import config
from .utils import LayerUrlHelper
import threading


@dataclass
class MergeConfiguration:
    timeout_s: int
    max_result_bytes: int
    iteration: int


configurations = [
    MergeConfiguration(timeout_s=10, max_result_bytes=40 * 1024 * 1024, iteration=1),
    MergeConfiguration(timeout_s=100, max_result_bytes=400 * 1024 * 1024, iteration=2),
    MergeConfiguration(timeout_s=1000, max_result_bytes=4000 * 1024 * 1024, iteration=3),
    MergeConfiguration(timeout_s=4000, max_result_bytes=4000 * 1024 * 1024, iteration=4),
]

class Merger:
    def __init__(self, database, schema, table, conn: DuckDBPyConnection):
        self.database = database
        self.schema = schema
        self.table = table
        self.conn = conn
    def do_merge(self, merge_plan: MergePlan):
        file = merge_plan.from_table_files[0]
        layer = [c for c in config().layer_configuration if c.name == file.layer_name]
        if len(layer) == 0:
            raise ValueError(f"Layer not found: {file.layer_name}")
        url = LayerUrlHelper(layer[0].url)
        if layer[0].type == LayerType.FILE:
            op = FSMerger(os.path.sep.join(url.prefix), self.database, self.schema, self.table, self.conn)
            return op.do_merge(merge_plan)
        if layer[0].type == LayerType.S3:
            op = S3Merger(layer[0], self.database, self.schema, self.table, self.conn)
            return op.do_merge(merge_plan)
        raise ValueError("Unsupported layer type")

    def get_file_size(self, layer: LayerConfig, file: str):
        url = LayerUrlHelper(layer.url)
        if layer.type == LayerType.FILE:
            op = FSMerger(os.path.sep.join(url.prefix), self.database, self.schema, self.table, self.conn)
            return op.get_file_size(file)
        if layer.type == LayerType.S3:
            op = S3Merger(layer, self.database, self.schema, self.table, self.conn)
            return op.get_file_size(file)
        raise ValueError("Unsupported layer type")


class FSMerger:
    def __init__(self, base, database, schema, table, conn: DuckDBPyConnection):
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table
        self.conn = conn
    def do_merge(self, merge_plan: MergePlan):
        conn = self.conn.cursor()
        try:
            from_files = ["'%s'" % os.path.join(self.base, self.database, self.schema, self.table, file)
                          for file in merge_plan.from_file_paths]
            to_file_path = "'%s'" % os.path.join(
                self.base, self.database, self.schema, self.table, merge_plan.to_file_path)
            q = f"COPY (SELECT * FROM read_parquet([{",".join(from_files)}], union_by_name=True)) TO {to_file_path}"
            conn.execute(q)
        finally:
            conn.close()

    def get_file_size(self, file):
        file_path = os.path.join(self.base, self.database, self.schema, self.table, file)
        return os.path.getsize(file_path)

lock = threading.Lock()

class S3Merger:
    def __init__(self, layer: LayerConfig, database, schema, table, conn: DuckDBPyConnection):
        self.layer = layer
        self.url = LayerUrlHelper(layer.url)
        self.database = database
        self.schema = schema
        self.table = table
        self.conn = conn

    def do_merge(self, merge_plan: MergePlan):
        conn = self.conn.cursor()
        use_ssl = "true" if self.url.use_ssl else "false"
        with lock:
            conn.execute(f"""
CREATE OR REPLACE SECRET {self.layer.name}_secret (
    TYPE S3,
    KEY_ID '{self.url.username}',
    SECRET '{self.url.password}',
    ENDPOINT '{self.url.hostname}:{self.url.port}',
    USE_SSL {use_ssl},
    URL_STYLE path,
    SCOPE 's3://{self.url.bucket_name}'
);""")
        try:
            prefix = "/".join(self.url.prefix).lstrip("/")
            if prefix != "":
                prefix += "/"
            from_files = [f"'s3://{self.url.bucket_name.rstrip("/")}/{prefix}{self.database}/{self.schema}/{self.table}/{file}'"
                          for file in merge_plan.from_file_paths]
            to_file_path = f"'s3://{self.url.bucket_name.rstrip("/")}/{prefix}{self.database}/{self.schema}/{self.table}/{merge_plan.to_file_path}'"
            q = f"COPY (SELECT * FROM read_parquet([{",".join(from_files)}], union_by_name=True)) TO {to_file_path}"
            conn.execute(q)
        finally:
            conn.close()

    def get_file_size(self, file_path):
        op = FSOperatorS3(
            self.url.hostname,
            self.url.port,
            self.url.username,
            self.url.password,
            self.url.bucket_name,
            "/".join(self.url.prefix),
            self.url.use_ssl)
        return op.get_size("/".join([self.database, self.schema, self.table, file_path.strip("/")]).strip("/"))
