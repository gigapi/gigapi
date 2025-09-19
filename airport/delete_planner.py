import time
from typing import Optional, Callable
from .model import DeletePlans, DeletePlan
import structlog

log = structlog.get_logger()


class DeletePlanner:
    def __init__(self, base: str, database: str, schema: str, table: str, delete_plans: Optional[DeletePlans] = None):
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table
        self.on_change: Optional[Callable[['DeletePlanner'], None]] = None
        self.delete_plans = delete_plans if delete_plans is not None else DeletePlans()

    def normalize_filename(self, filename: str) -> str:
        for rm in [self.base, self.database, self.schema, self.table]:
            if filename.startswith(rm + "/"):
                filename = filename[len(rm)+1:]
        return filename

    def add_delete_plan(self, plan: str, layer_name: str):
        log.info(f"Adding delete plan: {plan} (layer: {layer_name})")
        self.delete_plans.delete_files.append(DeletePlan(
            file_path=self.normalize_filename(plan),
            layer_name=layer_name))
        if self.on_change:
            self.on_change(self)

    def get_delete_plan(self):
        if len(self.delete_plans.delete_files) > 0 and time.time() - self.delete_plans.delete_files[0].created_at > 30:
            return self.delete_plans.delete_files[0]
        return None

    def remove_delete_plan(self, plan: DeletePlan):
        self.delete_plans.delete_files = [p for p in self.delete_plans.delete_files
                                          if p.file_path != plan.file_path or p.layer_name != plan.layer_name]
        if self.on_change:
            self.on_change(self)