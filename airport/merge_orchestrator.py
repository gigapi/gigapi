import os.path
import traceback
from concurrent.futures.thread import ThreadPoolExecutor

import duckdb
import structlog

from .constants import max_merge_processes
from .merge_performer import FSMerger
from .merge_planner import MergePlanner
from .model import MergePlan, MergePlanState, TableFile
import pyarrow.compute as pc
import threading
import time

from .table import TableInfo

log = structlog.get_logger()


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
        self.working = True
        self.start_timer()
        self.last_merge_cleanup = 0
        self.current_merge_plans = []
        self.conn = None

    def start_timer(self):
        if self.timer is None and self.working:
            self.timer = threading.Timer(10.0, self.run_merge_iteration)
            self.timer.start()

    def run_merge_iteration(self):
        if time.time() - self.last_merge_cleanup > 10:
            for t in self.tables:
                t.merge_planner.cleanup()
            self.last_merge_cleanup = time.time()
        self.merge_iteration()
        self.timer = None
        self.start_timer()

    def merge_iteration(self):
        self.conn = duckdb.connect()
        try:
            self._merge_iteration()
        finally:
            self.conn.close()
            self.conn = None

    def _merge_iteration(self):
        log.info("Running merge iteration")
        while True:
            self.current_merge_plans = self.get_merge_plans()
            log.info("Got merge plans to execute", count=len(self.current_merge_plans))
            if len(self.current_merge_plans) == 0:
                break
            with ThreadPoolExecutor(max_workers=max_merge_processes) as executor:
                futures = [executor.submit(self.execute_merge, m) for m in range(len(self.current_merge_plans))]
                start = time.time()
                for future in futures:
                    future.result()
                log.info("Finished merges", count=len(futures), duration=time.time() - start)

    def execute_merge(self, i: int):
        try:
            m = self.current_merge_plans[i]
            fsm = FSMerger(m.base, m.database, m.schema, m.table, self.conn)
            fsm.do_merge(m.merge_plan)
            m.merge_plan.state = MergePlanState.DONE
            to_file_abs = os.path.join(m.base, m.database, m.schema, m.table, m.merge_plan.to_file_path)
            event_timestamp_min = pc.min([f.event_timestamp_min for f in m.merge_plan.from_table_files])
            event_timestamp_max = pc.max([f.event_timestamp_max for f in m.merge_plan.from_table_files])
            add_file = TableFile(
                # TODO: fix the absolute paths
                filename=m.merge_plan.to_file_path,
                event_timestamp_min=event_timestamp_min,
                event_timestamp_max=event_timestamp_max,
                size_bytes = fsm.get_file_size(to_file_abs)
            )
            m.table_info.alter_table_files([add_file], m.merge_plan.from_table_files)
        except Exception as e:
            log.error("Error executing merge",
                      error=str(e),
                      traceback=traceback.format_exc(),
                      merge_plan=m.merge_plan)
            m.merge_plan.state = MergePlanState.IDLE

    def get_merge_plans(self):
        merge_plans = []
        for table in self.tables:
            if len(merge_plans) >= 50:
                break
            while True:
                if len(merge_plans) >= 50:
                    break
                planner = table.merge_planner
                p = planner.get_merge_plan()
                if p is None:
                    break
                p.state = MergePlanState.PROCESSING
                p.updated_at = time.time()
                merge_plans.append(PlanWrapper(planner.base, planner.database, planner.schema, planner.table, p, table))
        return merge_plans

    def add_planner(self, table: TableInfo):
        self.tables.append(table)

    def stop(self):
        self.working = False
        if self.timer:
            self.timer.cancel()
            self.timer = None
