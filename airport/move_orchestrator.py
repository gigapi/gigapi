import threading
import time

import structlog
import traceback

from .model import TableFile
from .move_performer import Mover
from .move_planner import MovePlanner
from .configuraiton import config
from .table import TableInfo
from typing import List, Optional

log = structlog.get_logger()

class MoveOrchestrator:
    def __init__(self, tables: Optional[List[TableInfo]]=None):
        if tables is None:
            tables = []
        self.tables = tables
        self.working = True
        self.timer = None
        self.start_timer()

    def add_planner(self, table: TableInfo):
        self.tables.append(table)

    def start_timer(self):
        if self.timer is None or not self.working:
            self.timer = threading.Timer(10.0, self.run_move_iteration)
            self.timer.start()

    def run_move_iteration(self):
        log.info("Running move iteration")
        self.timer = None
        moved = 0
        for table in self.tables:
            planner = table.move_planner
            move_plan = planner.get_move_plan()
            while move_plan is not None:
                removed_file = move_plan.file
                if not move_plan.layer_to_name:
                    table.alter_table_files([], removed_file)
                    continue
                layer_from = [x for x in config().layer_configuration if x.name == move_plan.layer_from_name]
                layer_to = [x for x in config().layer_configuration if x.name == move_plan.layer_to_name]
                if len(layer_from) == 0 or len(layer_to) == 0:
                    log.error("Layer not found", layer_from=move_plan.layer_from_name, layer_to=move_plan.layer_to_name)
                    move_plan = planner.get_move_plan()
                    continue
                performer = Mover(layer_from[0], layer_to[0], planner.database, planner.schema, planner.table)
                try:
                    performer.move(move_plan.file.filename)
                    added_file = TableFile(
                        filename=removed_file.filename,
                        event_timestamp_min=removed_file.event_timestamp_min,
                        event_timestamp_max=removed_file.event_timestamp_max,
                        event_timestamp_column=removed_file.event_timestamp_column,
                        size_bytes=removed_file.size_bytes,
                        file_created_at=int(time.time()),
                        layer_name=layer_to[0].name,
                    )
                    table.alter_table_files([added_file], [removed_file])
                    planner.remove_move_plan(move_plan.file.filename)
                    moved += 1
                except Exception as e:
                    log.error("Error executing move",
                              error=str(e),
                              traceback=traceback.format_exc(),
                              file_path=move_plan.file.filename)
                    break
                move_plan = planner.get_move_plan()
        log.info("Move iteration completed", moved_files=moved)
        self.start_timer()

    def stop(self):
        self.working = False
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None