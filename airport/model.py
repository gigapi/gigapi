import time
from dataclasses import dataclass, field, asdict
import pyarrow as pa
import pyarrow.flight as flight
import query_farm_flight_server.flight_inventory as flight_inventory

from .constants import event_timestamp_column, default_schema_name
from .flight_descriptor import FlightDescriptorParts, ObjectTypeName
from .utils import CaseInsensitiveDict
from enum import Enum
from .configuraiton import config


def encode_custom(obj):
    if isinstance(obj, TableFile):
        state = obj.__getstate__()
        return {
            "__custom__": "TableFile",
            "data": state
        }
    elif isinstance(obj, CaseInsensitiveDict):
        return {
            "__custom__": "CaseInsensitiveDict",
            "data": dict(obj)
        }
    elif isinstance(obj, pa.Schema):
        return {
            "__custom__": "ArrowSchema",
            "data": obj.serialize().to_pybytes()
        }
    elif isinstance(obj, pa.Table):
        return {
            "__custom__": "ArrowTable",
            "data": obj.serialize().to_pybytes()
        }
    elif isinstance(obj, set):
        return {
            "__custom__": "set",
            "data": list(obj)
        }
    elif isinstance(obj, pa.TimestampScalar):
        return {
            "__custom__": "TimestampScalar",
            "data": obj.value if obj is not None else None
        }
    elif isinstance(obj, pa.Int64Scalar):
        return {
            "__custom__": "Int64Scalar",
            "data": obj.as_py()
        }
    elif isinstance(obj, MergePlan):
        return {
            "__custom__": "MergePlan",
            "data": obj.__getstate__()
        }
    elif isinstance(obj, MergePlansByFolder):
        state = obj.__getstate__()
        return {
            "__custom__": "MergePlansByFolder",
            "data": state
        }
    elif isinstance(obj, MergePlanState):
        return {
            "__custom__": "MergePlanState",
            "data": obj.value
        }
    elif isinstance(obj, DeletePlan):
        return {
            "__custom__": "DeletePlan",
            "data": asdict(obj)
        }
    elif isinstance(obj, DeletePlans):
        return {
            "__custom__": "DeletePlans",
            "data": obj.__getstate__()
        }
    elif isinstance(obj, MovePlans):
        return {
            "__custom__": "MovePlans",
            "data": obj.__getstate__()
        }
    elif isinstance(obj, MovePlan):
        return {
            "__custom__": "MovePlan",
            "data": obj.__getstate__()
        }
    return obj

def decode_custom(obj):
    if isinstance(obj, dict) and "__custom__" in obj:
        class_name = obj["__custom__"]
        if class_name == "TableFile":
            result = TableFile.__new__(TableFile)
            result.__setstate__(obj["data"])
            return result
        elif class_name == "CaseInsensitiveDict":
            return CaseInsensitiveDict(obj["data"])
        elif class_name == "ArrowSchema":
            return pa.ipc.read_schema(pa.py_buffer(obj["data"]))
        elif class_name == "ArrowTable":
            return pa.ipc.read_table(pa.py_buffer(obj["data"]))
        elif class_name == "set":
            return set(obj["data"])
        elif class_name == "TimestampScalar":
            if obj["data"] is not None:
                return pa.scalar(obj["data"], pa.timestamp('ns'))
            else:
                return None
        elif class_name == "Int64Scalar":
            return pa.scalar(obj["data"], type=pa.int64())
        elif class_name == "MergePlan":
            result = MergePlan.__new__(MergePlan)
            result.__setstate__(obj["data"])
            return result
        elif class_name == "MergePlansByFolder":
            return MergePlansByFolder(**obj["data"])
        elif class_name == "MergePlanState":
            return MergePlanState(obj["data"])
        elif class_name == "DeletePlan":
            return DeletePlan(**obj["data"])
        elif class_name == "DeletePlans":
            return DeletePlans(**obj["data"])
        elif class_name == "MovePlans":
            if "delete_files" in obj["data"]:
                obj["data"]["move_files"] = obj["data"]["delete_files"]
                del obj["data"]["delete_files"]
            return MovePlans(**obj["data"])
        elif class_name == "MovePlan":
            return MovePlan(**obj["data"])
    return obj


@dataclass
class TableFile:
    filename: str
    event_timestamp_min: int
    event_timestamp_max: int

    event_timestamp_column: str = event_timestamp_column
    size_bytes: int = field(default=0)
    file_created_at: int = field(default_factory=lambda: int(time.time()))
    layer_name: str = field(default_factory=lambda: config().layer_configuration[0].name)
    def __getstate__(self):
        return {
            "filename": self.filename,
            "event_timestamp_min": self.event_timestamp_min,
            "event_timestamp_max": self.event_timestamp_max,
            "event_timestamp_column": self.event_timestamp_column,
            "size_bytes": self.size_bytes,
            "file_created_at": self.file_created_at,
            "layer_name": self.layer_name
        }

    def __setstate__(self, state):
        self.filename = state["filename"]
        self.event_timestamp_min = state["event_timestamp_min"]
        self.event_timestamp_max = state["event_timestamp_max"]
        self.event_timestamp_column = state["event_timestamp_column"]
        self.size_bytes = state["size_bytes"]
        self.file_created_at = state["file_created_at"] if "file_created_at" in state else time.time()
        self.layer_name = state["layer_name"] if "layer_name" in state else config().layer_configuration[0].name


class MetaStore:
    def on_schema_update(self):
        pass
    def on_files_update(self, files_added: list[TableFile], files_removed: list[TableFile]):
        pass
    def load(self):
        pass

class MergePlanState(Enum):
    IDLE = 1
    PROCESSING = 2
    DONE = 3

@dataclass
class MergePlan:
    database_name: str
    schema_name: str
    table_name: str
    from_table_files: list[TableFile] = field(default_factory=list)
    from_file_paths: list[str] = field(default_factory=list)
    size_bytes: int = 0
    to_file_path: str = ""
    iteration: int = 0
    state: MergePlanState = field(default=MergePlanState.IDLE)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default=0)

    def __getstate__(self):
        d = asdict(self)
        d["from_table_files"] = self.from_table_files
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)

@dataclass
class MergePlansByFolder:
    merge_plans: dict[str, list[MergePlan]] = field(default_factory=dict)
    def __getstate__(self):
        return {
            "merge_plans": self.merge_plans
        }

@dataclass
class DeletePlan:
    file_path: str
    created_at: float = field(default_factory=time.time)

@dataclass
class DeletePlans:
    delete_files: list[DeletePlan] = field(default_factory=list)
    def __getstate__(self):
        return {
            "delete_files": self.delete_files
        }
    def __setstate__(self, state):
        self.__dict__.update(state)

@dataclass
class MovePlan:
    file: TableFile
    layer_from_name: str
    layer_to_name: str
    created_at: float = field(default_factory=time.time)

@dataclass
class MovePlans:
    move_files: list[MovePlan] = field(default_factory=list)
    def __getstate__(self):
        return {
            "move_files": self.move_files
        }
    def __setstate__(self, state):
        self.__dict__.update(state)
