import os.path
import traceback
from concurrent.futures.thread import ThreadPoolExecutor

from .constants import max_merge_processes
from .merge_performer import FSMerger
from .merge_planner import MergePlanner
from .model import MergePlan, MergePlanState, TableFile
import pyarrow.compute as pc
import threading

from .table import TableInfo


class PlanWrapper:
    def __init__(self, base, database, schema, table, merge_plan: MergePlan, table_info: TableInfo):
        self.merge_plan = merge_plan
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table
        self.table_info = table_info

class MergeOrchestrator:
    def __init__(self):
        self.tables: list[TableInfo] = []
        self.timer = None
        self.start_timer()

    def start_timer(self):
        if self.timer is None:
            self.timer = threading.Timer(10.0, self.run_merge_iteration)
            self.timer.start()

    def run_merge_iteration(self):
        self.merge_iteration()
        self.timer = None
        self.start_timer()

    def merge_iteration(self):
        print("Running merge iteration...")
        while True:
            merge_plans = self.get_merge_plans()
            if len(merge_plans) == 0:
                break
            with ThreadPoolExecutor(max_workers=max_merge_processes) as executor:
                futures = [executor.submit(self.execute_merge, m) for m in merge_plans]
                for future in futures:
                    future.result()
                print(f"Finished {len(futures)} merges")


    def execute_merge(self, m: PlanWrapper):
        try:
            fsm = FSMerger(m.base, m.database, m.schema, m.table)
            fsm.do_merge(m.merge_plan)
            m.state = MergePlanState.DONE
            to_file_abs = os.path.join(m.base, m.database, m.schema, m.table, m.merge_plan.to_file_path)
            event_timestamp_min = pc.min([f.event_timestamp_min for f in m.merge_plan.from_table_files])
            event_timestamp_max = pc.max([f.event_timestamp_max for f in m.merge_plan.from_table_files])
            add_file = TableFile(
                # TODO: fix the absolute pahts
                filename=to_file_abs,
                event_timestamp_min=event_timestamp_min,
                event_timestamp_max=event_timestamp_max,
                size_bytes = fsm.get_file_size(to_file_abs)
            )
            m.table_info.alter_table_files([add_file], m.merge_plan.from_table_files)
        except Exception as e:
            print(f"Error executing merge: {str(e)}")
            print("Exception stack trace:")
            traceback.print_exc()
            m.state = MergePlanState.IDLE

    def get_merge_plans(self):
        merge_plans = []
        for table in self.tables:
            while True:
                planner = table.merge_planner
                p = planner.get_merge_plan()
                if p is None:
                    break
                p.state = MergePlanState.PROCESSING
                merge_plans.append(PlanWrapper(planner.base, planner.database, planner.schema, planner.table, p, table))
        return merge_plans

    def add_planner(self, table: TableInfo):
        self.tables.append(table)

    def stop(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None