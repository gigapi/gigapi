import uuid

from airport.model import TableFile, MergePlan
import duckdb
import os
from dataclasses import dataclass
from icecream import ic
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

class FSMerger:
    def __init__(self, base, database, schema, table):
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table
    def do_merge(self, merge_plan: MergePlan):
        conn = duckdb.connect()
        try:
            from_files = ["'%s'" % os.path.join(self.base, self.database, self.schema, self.table, file)
                          for file in merge_plan.from_file_paths]
            to_file_path = "'%s'" % os.path.join(
                self.base, self.database, self.schema, self.table, merge_plan.to_file_path)
            q = f"COPY (SELECT * FROM read_parquet([{",".join(from_files)}])) TO {to_file_path}"
            ic(q)
            conn.execute(q)
            conn.close()
        finally:
            conn.close()

    def get_file_size(self, file_path):
        return os.path.getsize(file_path)



