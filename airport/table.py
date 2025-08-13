import time
from dataclasses import dataclass, field, asdict
import pyarrow as pa
import pyarrow.flight as flight
import query_farm_flight_server.flight_inventory as flight_inventory

from .constants import default_schema_name
from .delete_planner import DeletePlanner
from .flight_descriptor import FlightDescriptorParts, ObjectTypeName
from .merge_planner import MergePlanner
from .model import TableFile, MetaStore
from .utils import CaseInsensitiveDict


@dataclass
class TableInfo:
    # This is a list of parquet files.
    table_schema: pa.Schema
    contents: list[TableFile] = field(default_factory=list)
    table_versions: list[pa.Table] = field(default_factory=list)
    meta_store: MetaStore | None = field(default=None, repr=False, compare=False)
    merge_planner: MergePlanner = field(default=None, repr=False, compare=False)
    delete_planner: DeletePlanner = field(default=None, repr=False, compare=False)

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
        for file in added:
            self.merge_planner.add_file(file)
        for file in removed:
            self.contents = [f for f in self.contents if f.filename!= file.filename]
            self.delete_planner.add_delete_plan(file.filename)

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
