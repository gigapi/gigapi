from dataclasses import dataclass, field, asdict
import pyarrow as pa
import pyarrow.flight as flight
import query_farm_flight_server.flight_inventory as flight_inventory

from .constants import event_timestamp_column, default_schema_name
from .flight_descriptor import FlightDescriptorParts, ObjectTypeName
from .utils import CaseInsensitiveDict
from icecream import ic


def encode_custom(obj):
    if isinstance(obj, TableFile):
        state = obj.__getstate__()
        return {
            "__custom__": "TableFile",
            "data": state
        }
    elif isinstance(obj, (DatabaseLibrary, DatabaseContents, SchemaCollection)):
        state = obj.__getstate__()

        return {
            "__custom__": obj.__class__.__name__,
            "data": state
        }
    elif isinstance(obj, TableInfo):
        data = obj.__getstate__()
        return {
            "__custom__": "TableInfo",
            "data": data
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
        obj = obj.as_py()
        return {
            "__custom__": "TimestampScalar",
            "data": obj.as_py().isoformat() if obj is not None else None
        }
    elif isinstance(obj, pa.Int64Scalar):
        return {
            "__custom__": "Int64Scalar",
            "data": obj.as_py()
        }
    return obj

def decode_custom(obj):
    if isinstance(obj, dict) and "__custom__" in obj:
        class_name = obj["__custom__"]
        if class_name == "DatabaseLibrary":
            result = DatabaseLibrary.__new__(DatabaseLibrary)
            result.__setstate__(obj["data"])
            return result
        elif class_name == "DatabaseContents":
            result = DatabaseContents.__new__(DatabaseContents)
            result.__setstate__(obj["data"])
            return result
        elif class_name == "SchemaCollection":
            result = SchemaCollection.__new__(SchemaCollection)
            result.__setstate__(obj["data"])
            return result
        elif class_name == "TableInfo":
            result = TableInfo.__new__(TableInfo)
            result.__setstate__(obj["data"])
            return result
        elif class_name == "TableFile":
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
                return pa.scalar(pa.timestamp(obj["data"]))
            else:
                return None
        elif class_name == "Int64Scalar":
            return pa.scalar(obj["data"], type=pa.int64())
    return obj



@dataclass
class TableFile:
    filename: str
    event_timestamp_min: int
    event_timestamp_max: int

    event_timestamp_column: str = event_timestamp_column
    size_bytes: int = field(default=0)
    def __getstate__(self):
        return {
            "filename": self.filename,
            "event_timestamp_min": self.event_timestamp_min,
            "event_timestamp_max": self.event_timestamp_max,
            "event_timestamp_column": self.event_timestamp_column,
            "size_bytes": self.size_bytes,
        }

    def __setstate__(self, state):
        self.filename = state["filename"]
        self.event_timestamp_min = state["event_timestamp_min"]
        self.event_timestamp_max = state["event_timestamp_max"]
        self.event_timestamp_column = state["event_timestamp_column"]
        self.size_bytes = state["size_bytes"]


class MetaStore:
    def on_schema_update(self):
        pass
    def on_files_update(self, files_added: list[TableFile], files_removed: list[TableFile]):
        pass
    def load(self):
        pass

@dataclass
class TableInfo:
    # This is a list of parquet files.
    table_schema: pa.Schema

    contents: list[TableFile] = field(default_factory=list)

    table_versions: list[pa.Table] = field(default_factory=list)

    meta_store: MetaStore | None = field(default=None, repr=False, compare=False)

    def update_table(self, table: pa.Table) -> None:
        assert table is not None
        assert isinstance(table, pa.Table)
        self.table_versions.append(table)

    def update_table_schema(self, schema: pa.Schema) -> None:
        assert isinstance(schema, pa.Schema)
        self.table_schema = schema
        self.meta_store.on_schema_update()

    def alter_table_files(self, added: list[TableFile], removed: list[TableFile]) -> None:
        self.meta_store.on_files_update(added, removed)
        self.contents.extend(added)
        for file in removed:
            self.contents = [f for f in self.contents if f.filename!= file.filename]

    def version(self, version: int | None = None) -> pa.Table:
        """
        Get the version of the table.
        """
        assert len(self.table_versions) > 0
        if version is None:
            return self.table_versions[-1]

        assert version < len(self.table_versions)
        return self.table_versions[version]

    def flight_info(
            self,
            *,
            name: str,
            catalog_name: str,
            schema_name: str,
    ) -> tuple[flight.FlightInfo, flight_inventory.FlightSchemaMetadata]:
        """
        Often its necessary to create a FlightInfo object for the table,
        standardize doing that here.
        """
        metadata = flight_inventory.FlightSchemaMetadata(
            type="table",
            catalog=catalog_name,
            schema=schema_name,
            name=name,
            comment=None,
        )
        flight_info = flight.FlightInfo(
            self.table_schema,
            FlightDescriptorParts.pack(FlightDescriptorParts(catalog_name, schema_name, "table", name)),
            [],
            -1,
            -1,
            app_metadata=metadata.serialize(),
        )
        return (flight_info, metadata)

    def __getstate__(self):
        return {
            "table_schema": self.table_schema,
            "contents": self.contents,
        }

    def __setstate__(self, state):
        self.__dict__.update(state)


@dataclass
class SchemaCollection:
    tables_by_name: CaseInsensitiveDict[TableInfo] = field(default_factory=CaseInsensitiveDict[TableInfo])

    def containers(
            self,
    ) -> list[CaseInsensitiveDict[TableInfo]]:
        return [
            self.tables_by_name,
        ]

    def by_name(self, type: ObjectTypeName, name: str) -> TableInfo:
        assert name is not None
        assert name != ""
        if type == "table":
            table = self.tables_by_name.get(name)
            if not table:
                raise flight.FlightServerError(f"Table {name} does not exist.")
            return table

    def __getstate__(self):
            return asdict(self)

    def __setstate__(self, state):
        self.__dict__.update(state)


@dataclass
class DatabaseContents:
    # Collection of schemas by name.
    schemas_by_name: CaseInsensitiveDict[SchemaCollection] = field(
        default_factory=CaseInsensitiveDict[SchemaCollection]
    )

    # The version of the database, updated on each schema change.
    version: int = 1

    def by_name(self, name: str | None) -> SchemaCollection:
        if name is None or name == "":
            name = default_schema_name
        if name not in self.schemas_by_name:
            raise flight.FlightServerError(f"Schema {name} does not exist.")
        return self.schemas_by_name[name]

    def __getstate__(self):
        return asdict(self)

    def __setstate__(self, state):
        self.__dict__.update(state)


@dataclass
class DatabaseLibrary:
    """
    The database library, which contains all of the databases, organized by token.
    """

    # Collection of databases by token.
    databases_by_name: CaseInsensitiveDict[DatabaseContents] = field(
        default_factory=CaseInsensitiveDict[DatabaseContents]
    )

    def by_name(self, name: str) -> DatabaseContents:
        if name not in self.databases_by_name:
            raise flight.FlightServerError(f"Database {name} does not exist.")
        return self.databases_by_name[name]

    def __getstate__(self):
        return {"databases_by_name": dict(self.databases_by_name)}

    def __setstate__(self, state):
        self.databases_by_name = CaseInsensitiveDict(state["databases_by_name"])

@dataclass
class MergePlan:
    from_table_files: list[TableFile] = field(default_factory=list)
    from_file_paths: list[str] = field(default_factory=list)
    size_bytes: int = 0
    to_file_path: str = ""
    iteration: int = 0

    def __getstate__(self):
        return asdict(self)

    def __setstate__(self, state):
        self.__dict__.update(state)
