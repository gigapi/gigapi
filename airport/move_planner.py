import time
from typing import Optional, Callable
from .model import MovePlan, MovePlans, TableFile
from .configuraiton import config


class MovePlanner:
    def __init__(self, base: str, database: str, schema: str, table: str, move_plans: Optional[MovePlans] = None):
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table
        self.on_change: Optional[Callable[['MovePlanner'], None]] = None
        mp = {}
        for p in move_plans.move_files if move_plans is not None else []:
            mp[p.layer_from_name + p.file.filename] = p
        self.move_plans = MovePlans(move_files=[x for x in mp.values()]) # move_plans if move_plans is not None else MovePlans()

    def add_move_plan(self, f: TableFile, layer_from_name: str, layer_to_name: str) -> None:
        if not f.filename.startswith("data"):
            raise ValueError("File must start with 'data'")
        if len([x for x in self.move_plans.move_files
                if x.file.filename == f.filename and x.layer_from_name == layer_from_name]) > 0:
            return
        self.move_plans.move_files.append(MovePlan(
            layer_from_name=layer_from_name,
            layer_to_name=layer_to_name,
            file=f,
        ))
        if self.on_change:
            self.on_change(self)

    def get_move_plan(self):
        layers = {}
        for p in config().layer_configuration:
            layers[p.name] = p
        for f in self.move_plans.move_files:
            if f.layer_from_name not in layers:
                continue
            if layers[f.layer_from_name].ttl_sec + f.created_at <= time.time():
                return f
        return None

    def remove_move_plan(self, file_path: str):
        self.move_plans.move_files = [p for p in self.move_plans.move_files if p.file.filename != file_path]
        if self.on_change:
            self.on_change(self)