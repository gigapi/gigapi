import os.path
import time
import uuid
from .constants import max_parquet_size
from .model import MergePlan, TableFile, MergePlansByFolder, MergePlanState
from typing import Optional, Callable

class MergePlanWrapper:
    def __init__(self, merge_plan: MergePlan) -> None:
        self.merge_plan = merge_plan
        self.last_update = 0

class MergePlanner:
    def __init__(self, base: str, database: str, schema: str, table: str,
                 merge_plans: Optional[MergePlansByFolder] = None) -> None:
        self.merge_plans = merge_plans if merge_plans is not None else MergePlansByFolder()
        self.database = database
        self.schema = schema
        self.table = table
        self.base = base
        self.on_change: Optional[Callable[['MergePlanner'], None]] = None

    def normalize_filename(self, filename: str) -> str:
        for rm in [self.base, self.database, self.schema, self.table]:
            if filename.startswith(rm + "/"):
                filename = filename[len(rm)+1:]
        return filename


    def add_file(self, file: TableFile):
        if file.size_bytes > max_parquet_size * 0.8:
            return
        filename = self.normalize_filename(file.filename)
        folder = os.path.dirname(filename)
        if folder not in self.merge_plans.merge_plans:
            self.merge_plans.merge_plans[folder] = []
        if len(self.merge_plans.merge_plans[folder]) == 0 or \
            self.merge_plans.merge_plans[folder][-1].size_bytes + file.size_bytes > max_parquet_size or \
            self.merge_plans.merge_plans[folder][-1].state != MergePlanState.IDLE:
            to_file_path = os.path.join(folder, f"{uuid.uuid4()}.parquet")
            self.merge_plans.merge_plans[folder].append(MergePlan(
                database_name= self.database,
                schema_name=self.schema,
                table_name=self.table,
                from_table_files= [file],
                from_file_paths=[filename],
                size_bytes=file.size_bytes,
                to_file_path=to_file_path,
                iteration=0
            ))
        else:
            self.merge_plans.merge_plans[folder][-1].from_table_files.append(file)
            self.merge_plans.merge_plans[folder][-1].from_file_paths.append(filename)
            self.merge_plans.merge_plans[folder][-1].size_bytes += file.size_bytes
        if self.on_change is not None:
            self.on_change(self)

    def get_merge_plan(self):
        for folder, merge_plans in self.merge_plans.merge_plans.items():
            for merge_plan in merge_plans:
                if (merge_plan.state == MergePlanState.IDLE and
                        len(merge_plan.from_table_files) > 1 and
                        time.time() - merge_plan.created_at > 10) or (
                        merge_plan.state == MergePlanState.PROCESSING and
                        time.time() - merge_plan.updated_at > 1800
                ):
                    return merge_plan
        return None

    def cleanup(self):
        for folder in self.merge_plans.merge_plans.keys():
            self.merge_plans.merge_plans[folder] = [merge_plan for merge_plan in self.merge_plans.merge_plans[folder]
                                                    if merge_plan.state!= MergePlanState.DONE]
            if len(self.merge_plans.merge_plans[folder]) == 0:
                del self.merge_plans.merge_plans[folder]
