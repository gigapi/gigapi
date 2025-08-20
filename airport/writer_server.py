import hashlib
import os
import shutil
import uuid
from collections.abc import Generator, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar, Optional

import click
import msgpack
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight
import pyarrow.parquet as pq
import pytz
import query_farm_duckdb_json_serialization.expression
import query_farm_flight_server.auth as auth
import query_farm_flight_server.auth_manager as auth_manager
import query_farm_flight_server.auth_manager_naive as auth_manager_naive
import query_farm_flight_server.flight_handling as flight_handling
import query_farm_flight_server.flight_inventory as flight_inventory
import query_farm_flight_server.middleware as base_middleware
import query_farm_flight_server.parameter_types as parameter_types
import query_farm_flight_server.schema_uploader as schema_uploader
import query_farm_flight_server.server as base_server
import query_farm_sql_manipulation.transforms as sql_transforms
import query_farm_sql_scan_planning.planner as scan_planner
import sqlglot.expressions
import structlog
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator
from query_farm_flight_server.parameter_types import unpack_with_model
from query_farm_flight_server.server import ActionHandlerSpec, CallContext

from .constants import event_timestamp_column, ingest_timestamp_column, default_schema_name
from .delete_orchestrator import DeleteOrchestrator
from .delete_planner import DeletePlanner
from .flight_descriptor import FlightDescriptorParts, ObjectTypeName
from .merge_orchestrator import MergeOrchestrator
from .merge_planner import MergePlanner
from .metadata_file_store import MetadataFileStore
from .utils import CaseInsensitiveDict
from .model import TableFile
from .table import TableInfo, SchemaCollection, DatabaseContents, DatabaseLibrary
from .database_discovery import discover_databases
from .bunch_of_parquets import BunchOfParquets

log = structlog.get_logger()


def check_schema_is_subset_of_schema(existing_schema: pa.Schema, new_schema: pa.Schema) -> None:
    """
    Check that the new schema is a subset of the existing schema.
    """
    existing_contents = set([(field.name, field.type) for field in existing_schema])
    new_contents = set([(field.name, field.type) for field in new_schema])

    unknown_fields = new_contents - existing_contents
    if unknown_fields:
        raise flight.FlightServerError(f"Unknown fields in insert: {unknown_fields}")
    return


def conform_nullable(schema: pa.Schema, table: pa.Table) -> pa.Table:
    """
    Conform the table to the nullable flags as defined in the schema.

    There shouldn't be null values in the columns.

    This is needed because DuckDB doesn't send the nullable flag in the schema
    it sends via the DoExchange call.
    """
    for idx, table_field in enumerate(schema):
        if not table_field.nullable:
            # Only update the column if the new schema allows nulls where the original did not
            new_field = table_field.with_nullable(False)

            # Check for null values.
            if table.column(idx).null_count > 0:
                raise flight.FlightServerError(
                    f"Column {table_field.name} has null values, but the schema does not allow nulls."
                )

            table = table.set_column(idx, new_field, table.column(idx))
    return table




class FlightTicketData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)  # for Pydantic v2
    descriptor: flight.FlightDescriptor
    #    json_filters: str
    #    column_ids: list[int]

    _validate_flight_descriptor = field_validator("descriptor", mode="before")(
        parameter_types.deserialize_flight_descriptor
    )

    @field_serializer("descriptor")
    def serialize_flight_descriptor(self, value: flight.FlightDescriptor, info: Any) -> bytes:
        return parameter_types.serialize_flight_descriptor(value, info)


T = TypeVar("T", bound=BaseModel)

class GigapipeWriterArrowFlightServer(base_server.BasicFlightServer[auth.Account, auth.AccountToken]):
    def __init__(
        self,
        *,
        location: str | None,
        auth_manager: auth_manager.AuthManager[auth.Account, auth.AccountToken],
        **kwargs: dict[str, Any],
    ) -> None:
        self.service_name = "gigapipe_writer"
        self._auth_manager = auth_manager
        if "base_path" in kwargs:
            self.base_path = kwargs["base_path"]
            kwargs.pop("base_path")
        else:
            self.base_path = "data"

        # token, database name, schema, table_name
        if os.path.exists(self.base_path):
            log.info("Discovering existing data...")
            self.contents: DatabaseLibrary = discover_databases(self.base_path)
            log.info("Existing data discovered.")
        else:
            self.contents: DatabaseLibrary = DatabaseLibrary()

        self.merge_orchestrator = MergeOrchestrator()
        self.delete_orchestrator = DeleteOrchestrator()
        for schemas in self.contents.databases_by_name.values():
            for schema in schemas.schemas_by_name.values():
                for table_info in schema.tables_by_name.values():
                    if table_info.merge_planner is not None:
                        self.merge_orchestrator.add_planner(table_info)
                        self.delete_orchestrator.add_planner(table_info.delete_planner)

        super().__init__(location=location, **kwargs)




    def action_list_schemas(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.ListSchemas,
    ) -> base_server.AirportSerializedCatalogRoot:
        assert context.caller is not None

        library = self.contents
        database = library.by_name(parameters.catalog_name)

        dynamic_inventory: dict[str, dict[str, list[flight_inventory.FlightInventoryWithMetadata]]] = {}

        catalog_contents = dynamic_inventory.setdefault(parameters.catalog_name, {})

        for schema_name, schema in database.schemas_by_name.items():
            schema_contents = catalog_contents.setdefault(schema_name, [])
            for coll in schema.containers():
                for name, obj in coll.items():
                    schema_contents.append(
                        obj.flight_info(
                            name=name,
                            catalog_name=parameters.catalog_name,
                            schema_name=schema_name
                        )
                    )

        return flight_inventory.upload_and_generate_schema_list(
            flight_service_name=self.service_name,
            flight_inventory=dynamic_inventory,
            schema_details={},
            skip_upload=True,
            serialize_inline=True,
            catalog_version=1,
            catalog_version_fixed=False,
            upload_parameters=flight_inventory.UploadParameters(
                s3_client=None,
                base_url="http://localhost",
                bucket_name="test_bucket",
                bucket_prefix="test_prefix",
            ),
        )

    def impl_list_flights(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        criteria: bytes,
    ) -> Iterator[flight.FlightInfo]:
        assert context.caller is not None
        library = self.contents

        def yield_flight_infos() -> Generator[flight.FlightInfo, None, None]:
            for db_name, db in library.databases_by_name.items():
                for schema_name, schema in db.schemas_by_name.items():
                    for coll in schema.containers():
                        for name, obj in coll.items():
                            yield obj.flight_info(name=name, catalog_name=db_name, schema_name=schema_name)[0]

        return yield_flight_infos()

    def impl_get_flight_info(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        assert context.caller is not None

        if descriptor.path[0].decode("utf-8") == "__databases":
            return flight.FlightInfo(
                pa.schema([('name', pa.string())]),
                flight.FlightDescriptor.for_path("__databases"),
                [],
                -1,
                -1,
                "",
            )

        descriptor_parts = FlightDescriptorParts.unpack(descriptor)
        library = self.contents
        database = library.by_name(descriptor_parts.catalog_name)
        schema = database.by_name(descriptor_parts.schema_name)

        obj = schema.by_name(descriptor_parts.type, descriptor_parts.name)
        return obj.flight_info(
            name=descriptor_parts.name,
            catalog_name=descriptor_parts.catalog_name,
            schema_name=descriptor_parts.schema_name,
        )[0]

    def action_catalog_version(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.CatalogVersion,
    ) -> base_server.GetCatalogVersionResult:
        assert context.caller is not None

        library = self.contents
        database = library.by_name(parameters.catalog_name)

        context.logger.debug(
            "catalog_version_result",
            catalog_name=parameters.catalog_name,
            version=database.version,
        )
        return base_server.GetCatalogVersionResult(catalog_version=database.version, is_fixed=False)

    def action_create_transaction(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.CreateTransaction,
    ) -> base_server.CreateTransactionResult:
        try:
            return base_server.CreateTransactionResult(identifier=None)
        except Exception as e:
            context.logger.exception("Error creating transaction", exc_info=True)
            raise flight.FlightServerError(str(e))

    def action_create_schema(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.CreateSchema,
    ) -> base_server.AirportSerializedContentsWithSHA256Hash:
        assert context.caller is not None

        library = self.contents
        database = library.by_name(parameters.catalog_name)

        if database.schemas_by_name.get(parameters.schema_name) is not None:
            raise flight.FlightServerError(f"Schema {parameters.schema_name} already exists")

        database.schemas_by_name[parameters.schema_name] = SchemaCollection()
        database.version += 1

        # FIXME: this needs to be handled better on the server side...
        # rather than calling into internal methods.
        packed_data = msgpack.packb([])
        assert packed_data
        compressed_data = schema_uploader._compress_and_prefix_with_length(packed_data, compression_level=3)

        empty_hash = hashlib.sha256(compressed_data).hexdigest()
        return base_server.AirportSerializedContentsWithSHA256Hash(
            url=None, sha256=empty_hash, serialized=compressed_data
        )

    def action_drop_table(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.DropObject,
    ) -> None:
        assert context.caller is not None


        library = self.contents
        database = library.by_name(parameters.catalog_name)
        schema = database.by_name(parameters.schema_name)

        schema.by_name("table", parameters.name)

        table_path = os.path.join(self.base_path, parameters.catalog_name, parameters.schema_name, parameters.name)
        if os.path.exists(table_path):
            shutil.rmtree(table_path)

        schema.tables_by_name[parameters.name].meta_store.detach()
        del schema.tables_by_name[parameters.name]
        database.version += 1

    def action_drop_schema(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.DropObject,
    ) -> None:
        assert context.caller is not None


        library = self.contents
        database = library.by_name(parameters.catalog_name)

        if database.schemas_by_name.get(parameters.name) is None:
            raise flight.FlightServerError(f"Schema '{parameters.name}' does not exist")

        del database.schemas_by_name[parameters.name]
        database.version += 1

        if os.path.exists(os.path.join(self.base_path, parameters.catalog_name, parameters.name)):
            shutil.rmtree(os.path.join(self.base_path, parameters.catalog_name, parameters.name))

    def action_create_table(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.CreateTable,
    ) -> flight.FlightInfo:
        assert context.caller is not None


        library = self.contents
        database = library.by_name(parameters.catalog_name)
        schema = database.by_name(parameters.schema_name)

        if parameters.table_name in schema.tables_by_name:
            raise flight.FlightServerError(
                f"Table {parameters.table_name} already exists for token {context.caller.token}"
            )

        actual_schema = parameters.arrow_schema
        assert ingest_timestamp_column not in actual_schema.names
        actual_schema = actual_schema.append(pa.field(ingest_timestamp_column, pa.timestamp("ns")))

        assert event_timestamp_column not in actual_schema.names
        actual_schema = actual_schema.append(pa.field(event_timestamp_column, pa.timestamp("ns")))

        table_info = TableInfo(
            table_schema=actual_schema,
            merge_planner=MergePlanner(
                self.base_path, parameters.catalog_name, parameters.schema_name, parameters.table_name
            ),
            delete_planner=DeletePlanner(
                self.base_path, parameters.catalog_name, parameters.schema_name, parameters.table_name
            ))
        os.makedirs(os.path.join(self.base_path, parameters.catalog_name, parameters.schema_name,
                                 parameters.table_name), exist_ok=True)
        table_info.meta_store = MetadataFileStore(self.base_path,
              parameters.catalog_name, parameters.schema_name, parameters.table_name, table_info)
        table_info.meta_store.on_schema_update()

        schema.tables_by_name[parameters.table_name] = table_info

        database.version += 1

        self.merge_orchestrator.add_planner(table_info)
        self.delete_orchestrator.add_planner(table_info.delete_planner)

        return table_info.flight_info(
            name=parameters.table_name,
            catalog_name=parameters.catalog_name,
            schema_name=parameters.schema_name,
        )[0]

    def impl_do_action(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        action: flight.Action,
    ) -> Iterator[bytes]:
        assert context.caller is not None

        if action.type == "reset":
            context.logger.debug("Resetting server state")
            self.contents = DatabaseLibrary()
            return iter([])
        elif action.type == "create_database":
            database_name = action.body.to_pybytes().decode("utf-8")
            context.logger.debug("Creating database", database_name=database_name)

            library = self.contents
            if database_name in library.databases_by_name:
                raise flight.FlightServerError(f"Database {database_name} already exists")
            library.databases_by_name[database_name] = DatabaseContents()
            return iter([])
        elif action.type == "drop_database":
            database_name = action.body.to_pybytes().decode("utf-8")
            context.logger.debug("Dropping database", database_name=database_name)


            library = self.contents
            if action.body.decode("utf-8") not in library.databases_by_name:
                raise flight.FlightServerError(f"Database {action.body.decode('utf-8')} does not exist")
            del library.databases_by_name[action.body.decode("utf-8")]
            return iter([])

        raise flight.FlightServerError(f"Unsupported action type: {action.type}")
    def exchange_insert(
            self,
            *,
            context: base_server.CallContext[auth.Account, auth.AccountToken],
            descriptor: flight.FlightDescriptor,
            reader: flight.MetadataRecordBatchReader,
            writer: flight.MetadataRecordBatchWriter,
            return_chunks: bool,
    ) -> int:
        assert context.caller is not None

        descriptor_parts = FlightDescriptorParts.unpack(descriptor)

        if descriptor_parts.type != "table":
            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")
        library = self.contents
        database = library.by_name(descriptor_parts.catalog_name)
        schema = database.by_name(descriptor_parts.schema_name)
        table_info = schema.by_name("table", descriptor_parts.name)

        writer.begin(table_info.table_schema)
        change_count = 0

        check_schema_is_subset_of_schema(table_info.table_schema, reader.schema)

        ingest_time = datetime.now(pytz.UTC)
        ingest_time_scalar = pa.scalar(ingest_time, type=pa.timestamp("ns"))

        table_files = []
        event_timestamp_min = None
        event_timestamp_max = None
        file_name = f"{uuid.uuid4()}.parquet"
        with BunchOfParquets(self.base_path, descriptor_parts.catalog_name, descriptor_parts.schema_name,
                             descriptor_parts.name, file_name) as jbop:
            for chunk in reader:
                if chunk.data is None:
                    continue
                new_rows = pa.Table.from_batches([chunk.data])
                assert new_rows.num_rows > 0
                change_count += new_rows.num_rows
                new_rows = conform_nullable(table_info.table_schema, new_rows)
                timestamp_array = pa.repeat(ingest_time_scalar, new_rows.num_rows)
                if ingest_timestamp_column in new_rows.column_names:
                    new_rows = new_rows.drop(ingest_timestamp_column)
                new_rows = new_rows.append_column(ingest_timestamp_column, timestamp_array)
                new_rows = new_rows.select(table_info.table_schema.names)
                assert event_timestamp_column in new_rows.column_names
                min_max = pc.min_max(new_rows[event_timestamp_column])
                event_timestamp_min = pc.min(pa.array([event_timestamp_min, min_max["min"]])) \
                    if event_timestamp_min is not None else min_max["min"]
                event_timestamp_max = pc.max(pa.array([event_timestamp_max, min_max["max"]])) \
                    if event_timestamp_max is not None else min_max["max"]
                # Split the data by hour
                if pa.types.is_integer(new_rows[event_timestamp_column].type):
                    timestamps = new_rows[event_timestamp_column].cast(pa.timestamp('ns'))
                else:
                    timestamps = new_rows[event_timestamp_column]
                hours = pc.floor_temporal(timestamps, unit="hour")
                unique_hours = pc.unique(hours)
                for hour in unique_hours:
                    if isinstance(hour, pa.TimestampScalar):
                        hour_dt = hour.as_py()
                    else:
                        # If it's not a TimestampScalar, assume it's a nanosecond timestamp
                        hour_dt = datetime.fromtimestamp(hour.as_py() / 1e9, pytz.UTC)
                    mask = pc.equal(hours, hour)
                    hour_chunk = new_rows.filter(mask)
                    jbop.write_chunk(hour_dt, table_info.table_schema, hour_chunk)
                if return_chunks:
                    writer.write_table(new_rows)
            table_files = [f.table_file for f in jbop.parquet_files.values()]
        # Update the metadata
        table_info.alter_table_files(table_files, [])
        return change_count
#    def exchange_insert(
#        self,
#        *,
#        context: base_server.CallContext[auth.Account, auth.AccountToken],
#        descriptor: flight.FlightDescriptor,
#        reader: flight.MetadataRecordBatchReader,
#        writer: flight.MetadataRecordBatchWriter,
#        return_chunks: bool,
#    ) -> int:
#        assert context.caller is not None
#
#        descriptor_parts = FlightDescriptorParts.unpack(descriptor)
#
#        if descriptor_parts.type != "table":
#            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")
#        library = self.contents
#        database = library.by_name(descriptor_parts.catalog_name)
#        schema = database.by_name(descriptor_parts.schema_name)
#        table_info = schema.by_name("table", descriptor_parts.name)
#
#        writer.begin(table_info.table_schema)
#        change_count = 0
#
#        # DuckDB won't send field metadata when it sends us the schema that it uses
#        # to perform an insert, so we need some way to adapt the schema we
#        check_schema_is_subset_of_schema(table_info.table_schema, reader.schema)
#
#        # Open up a new parquet writer u
#
#        output_path = os.path.join(self.base_path, descriptor_parts.catalog_name,
#              descriptor_parts.schema_name, descriptor_parts.name, f"{uuid.uuid4()}.parquet")
#        directory_base = os.path.dirname(output_path)
#
#        os.makedirs(directory_base, exist_ok=True)
#
#        ingest_time = datetime.now(pytz.UTC)
#        ingest_time_scalar = pa.scalar(ingest_time, type=pa.timestamp("ns"))
#
#        with pq.ParquetWriter(output_path, schema=table_info.table_schema) as parquet_writer:
#            event_timestamp_min = None #pa.scalar(None, type=pa.timestamp("ns"))
#            event_timestamp_max = None #pa.scalar(None, type=pa.timestamp("ns"))
#
#            for chunk in reader:
#                if chunk.data is not None:
#                    new_rows = pa.Table.from_batches([chunk.data])
#                    assert new_rows.num_rows > 0
#
#                    # append the row id column to the new rows.
#                    chunk_length = new_rows.num_rows
#
#                    change_count += chunk_length
#
#                    new_rows = conform_nullable(table_info.table_schema, new_rows)
#
#                    timestamp_array = pa.repeat(ingest_time_scalar, new_rows.num_rows)
#
#                    # Add the ingest timestamp column to the new rows.
#                    if ingest_timestamp_column in new_rows.column_names:
#                        new_rows = new_rows.drop(ingest_timestamp_column)
#                    new_rows = new_rows.append_column(ingest_timestamp_column, timestamp_array)
#
#                    new_rows = new_rows.select(table_info.table_schema.names)
#
#                    assert event_timestamp_column in new_rows.column_names
#                    min_max = pc.min_max(new_rows[event_timestamp_column])
#
#                    # So we want to get the min and max event time for the event_timestamp
#
#                    event_timestamp_min = pc.min(pa.array([event_timestamp_min, min_max["min"]])) \
#                        if event_timestamp_min is not None else min_max["min"]
#                    event_timestamp_max = pc.max(pa.array([event_timestamp_max, min_max["max"]])) \
#                        if event_timestamp_max is not None else min_max["max"]
#                    # TimestampScalar
#                    parquet_writer.write_table(new_rows)
#
#                    if return_chunks:
#                        writer.write_table(new_rows)
#
#        # Along with adding the file we should update the metadata.
#        table_info.alter_table_files([
#            TableFile(
#                filename=output_path,
#                event_timestamp_min=event_timestamp_min,
#                event_timestamp_max=event_timestamp_max,
#            )
#        ], [])
#        return change_count

    def get_schema_name(self, parameter: str | None):
        return default_schema_name if parameter is None or parameter == "" else parameter

    def action_add_column(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.AddColumn,
    ) -> flight.FlightInfo:
        assert context.caller is not None

        library = self.contents
        database = library.by_name(parameters.catalog)
        schema = database.by_name(self.get_schema_name(parameters.schema_name))

        table_info = schema.by_name("table", parameters.name)

        assert len(parameters.column_schema.names) == 1

        # Don't allow duplicate column names.
        assert parameters.column_schema.field(0).name not in table_info.table_schema.names

        table_info.update_table_schema(table_info.table_schema.append(parameters.column_schema.field(0)))
        database.version += 1

        return table_info.flight_info(
            name=parameters.name,
            catalog_name=parameters.catalog,
            schema_name=parameters.schema_name,
        )[0]

    def action_remove_column(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.RemoveColumn,
    ) -> flight.FlightInfo:
        assert context.caller is not None


        library = self.contents
        database = library.by_name(parameters.catalog)
        schema = database.by_name(parameters.schema_name)

        table_info = schema.by_name("table", parameters.name)

        assert parameters.removed_column in table_info.table_schema.names

        assert parameters.removed_column not in (ingest_timestamp_column, event_timestamp_column)

        # Just drop the column in the schema for now.
        table_info.update_table_schema(table_info.table_schema.remove(
            table_info.table_schema.get_field_index(parameters.removed_column)
        ))
        database.version += 1

        return table_info.flight_info(
            name=parameters.name,
            catalog_name=parameters.catalog,
            schema_name=parameters.schema_name,
        )[0]

    # This isn't necessary because all of the parquet files are
    # scanned directly by duckdb.

    def impl_do_get(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        ticket: flight.Ticket,
    ) -> flight.RecordBatchStream:
        assert context.caller is not None

        ticket_data = flight_handling.decode_ticket_model(ticket, FlightTicketData)
        if ticket_data.descriptor.path[0].decode("utf-8") == "__databases":

            library = self.contents
            databases = [k for k in library.databases_by_name.keys()]
            # Create a Table from the schema and data
            schema = pa.schema([("name", pa.string())])
            table = pa.Table.from_arrays([pa.array(databases)], schema=schema)
            return flight.RecordBatchStream(table)
        raise flight.FlightServerError(f"Unsupported descriptor path: {ticket_data.descriptor.path}")

    def action_endpoints(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.Endpoints,
    ) -> list[flight.FlightEndpoint]:
        assert context.caller is not None
        if parameters.descriptor.path[0].decode("utf-8") == "__databases":
            desc = flight.FlightDescriptor.for_path(b"__databases")
            ticket_data = FlightTicketData(
                descriptor=desc
            )
            return [flight_handling.endpoint(ticket_data=ticket_data)]

        descriptor_parts = FlightDescriptorParts.unpack(parameters.descriptor)
        library = self.contents
        database = library.by_name(descriptor_parts.catalog_name)
        schema = database.by_name(descriptor_parts.schema_name)
        filename = lambda x: x if x.startswith("/") else os.path.join(self.base_path,
                                                                      descriptor_parts.catalog_name,
                                                                      descriptor_parts.schema_name,
                                                                      descriptor_parts.name,
                                                                      x)

        if descriptor_parts.type == "table":
            table_info = schema.by_name("table", descriptor_parts.name)

            if parameters.parameters.json_filters is not None:
                planner = scan_planner.Planner(
                    [
                        (
                            filename(i.filename),
                            {
                                f"{i.event_timestamp_column}": scan_planner.RangeFieldInfo(
                                    min_value=i.event_timestamp_min,
                                    max_value=i.event_timestamp_max,
                                    has_nulls=False,
                                    has_non_nulls=True,
                                ),
                            },
                        )
                        for i in table_info.contents
                    ]
                )

                filter_sql_where_clause, filter_sql_field_type_info = (
                    query_farm_duckdb_json_serialization.expression.convert_to_sql(
                        source=parameters.parameters.json_filters.filters,
                        bound_column_names=parameters.parameters.json_filters.column_binding_names_by_index,
                    )
                )
                if filter_sql_where_clause == "" or filter_sql_where_clause is None:
                    files_to_scan = [filename(i.filename) for i in table_info.contents]
                else:
                    filter_expression = sqlglot.parse_one(
                        f"select * from data where {filter_sql_where_clause}", dialect="duckdb"
                    )
                    filter_expression = sql_transforms.filter_column_references(
                        statement=filter_expression,
                        selector=lambda x: x.name in {event_timestamp_column},
                    )
                    filter_expression = sql_transforms.filter_predicates_with_right_side_column_references(
                        filter_expression
                    )

                    # So we should get the where clause.
                    where_clause = filter_expression.find(sqlglot.expressions.Where)
                    if where_clause is None:
                        files_to_scan = [filename(i.filename) for i in table_info.contents]
                    else:
                        files_to_scan = list(planner.files(where_clause.this))
                        context.logger.debug("Files to scan", files_to_scan=files_to_scan)
            else:
                # If there are no filters, we can just return all of the files.
                files_to_scan = [filename(i.filename) for i in table_info.contents]

            # Lets just return end the parquet files for now.
            ticket_data = FlightTicketData(
                descriptor=parameters.descriptor,
            )

            return (
                [
                    flight_handling.endpoint(
                        ticket_data=ticket_data,
                        locations=[
                            flight_handling.dict_to_msgpack_duckdb_call_data_uri(
                                {
                                    "function_name": "read_parquet",
                                    # So arguments could be a record batch.
                                    "data": flight_handling.serialize_arrow_ipc_table(
                                        pa.Table.from_pylist(
                                            [
                                                {
                                                    "arg_0": files_to_scan,
                                                    "hive_partitioning": False,
                                                    "union_by_name": True,
                                                }
                                            ],
                                        )
                                    ),
                                }
                            )
                        ],
                    )
                ]
                if files_to_scan
                else []
            )
        else:
            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")

    def action_flight_info(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.FlightInfo,
    ) -> flight.FlightInfo:
        assert context.caller is not None

        library = self.contents
        descriptor_parts = FlightDescriptorParts.unpack(parameters.descriptor)

        database = library.by_name(descriptor_parts.catalog_name)
        schema = database.by_name(descriptor_parts.schema_name)

        if descriptor_parts.type == "table":
            table = schema.by_name("table", descriptor_parts.name)

            return table.flight_info(
                name=descriptor_parts.name,
                catalog_name=descriptor_parts.catalog_name,
                schema_name=descriptor_parts.schema_name,
            )[0]
        else:
            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")
    def shutdown(self) -> None:
        super().shutdown()
        self.merge_orchestrator.stop()
        self.delete_orchestrator.stop()

server: Optional[GigapipeWriterArrowFlightServer] = None

# @click.command()
# @click.option(
#     "--location",
#     type=str,
#     default="grpc://127.0.0.1:60001",
#     help="The location where the server should listen.",
# )
def run(location: str, base_path: str) -> None:
    global server
    log.info("Starting server", location=location)

    auth_manager = auth_manager_naive.AuthManagerNaive[auth.Account, auth.AccountToken](
        account_type=auth.Account,
        token_type=auth.AccountToken,
        allow_anonymous_access=False,
    )

    server = GigapipeWriterArrowFlightServer(
        middleware={
            "headers": base_middleware.SaveHeadersMiddlewareFactory(),
            "auth": base_middleware.AuthManagerMiddlewareFactory(auth_manager=auth_manager),
        },
        location=location,
        auth_manager=auth_manager,
        base_path=base_path
    )
    server.serve()

def shutdown() -> None:
    global server
    if server is not None:
        server.shutdown()