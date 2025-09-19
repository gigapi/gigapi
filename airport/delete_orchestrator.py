import threading
import structlog
import traceback


from .delete_performer import DeletePerformer
from .delete_planner import DeletePlanner
from .configuraiton import config
from .model import DeletePlan

log = structlog.get_logger()

class DeleteOrchestrator:
    def __init__(self):
        self.planners: list[DeletePlanner] = []
        self.working = True
        self.timer = None
        self.start_timer()

    def add_planner(self, planner: DeletePlanner):
        self.planners.append(planner)

    def start_timer(self):
        if self.timer is None or not self.working:
            self.timer = threading.Timer(10.0, self.run_delete_iteration)
            self.timer.start()

    def get_layer(self, plan: DeletePlan):
        layer = [x for x in config().layer_configuration if x.name == plan.layer_name]
        if len(layer) == 0:
            raise ValueError(f"Layer not found: {plan.layer_name}")
        return layer[0]

    def run_delete_iteration(self):
        log.info("Running delete iteration")
        self.timer = None
        deleted = 0
        for planner in self.planners:
            delete_plan = planner.get_delete_plan()
            while delete_plan is not None:
                layer = self.get_layer(delete_plan)
                performer = DeletePerformer(layer, planner.database, planner.schema, planner.table)
                try:
                    log.info("Deleting file", delete_plan=delete_plan)
                    performer.do_delete(delete_plan.file_path)
                    planner.remove_delete_plan(delete_plan)
                    deleted += 1
                except Exception as e:
                    log.error("Error executing delete",
                              error=str(e),
                              traceback=traceback.format_exc(),
                              file_path=delete_plan.file_path)
                delete_plan = planner.get_delete_plan()
        log.info("Delete iteration completed", deleted_files=deleted)
        self.start_timer()

    def stop(self):
        self.working = False
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None