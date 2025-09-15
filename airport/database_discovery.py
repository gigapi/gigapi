## TODO: we have a folder structure:
## <root>
##  <database_name>
##    <table_name>
##      "data"
##          date=<date>
##              hour=<hour>
##                  metadata.json
##                  <uuid>.parquet
## We have to walk through the root folder to get all the databases.
## then find all the tables for each database.
## then we have to parse metadata and find the information about each file.
## at last we have to create DatabaseLibrary containing all the discovered data

import os
import json
import time

from .delete_planner import DeletePlanner
from .merge_planner import MergePlanner
from .metadata_file_store import MetadataFileStore, discovery_stats
from .move_planner import MovePlanner
from .table import DatabaseLibrary, DatabaseContents, TableInfo, TableFile, SchemaCollection
import pyarrow.parquet as pq
import pyarrow as pa
import structlog
import shutil
import traceback

log = structlog.get_logger()

class DatabaseDiscovery:
    def __init__(self, root):
        self.root = root
        self.root = root
        self.current_database_name = None
        self.current_database = None
        self.library = DatabaseLibrary()
        self.current_schema_name = None
        self.current_schema = None
        self.current_table_name = None
        self.current_table_schema = None
        self.current_table_files = []

    def discover(self):
        for database_name in os.listdir(self.root):
            if not os.path.isdir(os.path.join(self.root, database_name)):
                continue
            self.current_database_name = database_name
            if self.check_if_old():
                self.migrate_old()
            self.current_database = DatabaseContents()
            self.discover_schemas()
            self.library.databases_by_name[self.current_database_name] = self.current_database
        log.info("Discovery stats", **discovery_stats)

    def check_if_old(self):
        base_path = os.path.join(self.root, self.current_database_name)

        if not os.path.isdir(base_path):
            return False

        for table_name in os.listdir(base_path):
            table_path = os.path.join(base_path, table_name)

            data_path = os.path.join(table_path, "data")
            if not os.path.isdir(data_path):
                return False

            for item in os.listdir(data_path):
                date_path = os.path.join(data_path, item)
                if os.path.isdir(date_path) and not item.startswith("date="):
                    return False

        return True

    def migrate_old(self):
        base_path = os.path.join(self.root, self.current_database_name)
        master_path = os.path.join(base_path, "master")

        # Create the master directory if it doesn't exist
        os.makedirs(master_path, exist_ok=True)

        # Get all items in the base directory
        items = os.listdir(base_path)

        for item in items:
            item_path = os.path.join(base_path, item)

            # Skip if it's not a directory or if it's the master directory
            if not os.path.isdir(item_path) or item == "master":
                continue

            # Move the directory to the master folder
            destination_path = os.path.join(master_path, item)
            shutil.move(item_path, destination_path)

        log.info(f"Migrated old structure for database: {self.current_database_name}")


    def discover_schemas(self):
        database_path = os.path.join(self.root, self.current_database_name)
        for schema in os.listdir(database_path):
            self.current_schema_name = schema
            self.current_schema = SchemaCollection()
            self.current_database.schemas_by_name[self.current_schema_name] = self.current_schema
            self.discover_tables()

    def discover_tables(self):
        schema_path = os.path.join(self.root, self.current_database_name, self.current_schema_name)
        for table_name in os.listdir(schema_path):
            self.current_table_name = table_name
            self.current_table_schema = None
            self.current_table_files = []
            self.discover_table()

    def discover_table(self):
        log.info("Discovered table", table_name=self.current_table_name)
        table_path = os.path.join(self.root, self.current_database_name, self.current_schema_name, self.current_table_name)
        mdb_path = os.path.join(table_path, "metadata.db")
        if os.path.exists(mdb_path):
            meta_store = MetadataFileStore(self.root,
                                           self.current_database_name,
                                           self.current_schema_name,
                                           self.current_table_name)
            if meta_store.load() is not None:
                self.current_schema.tables_by_name[self.current_table_name] = meta_store.table_info
                return
        start = time.time()
        self.discover_legacy_table()
        if self.current_table_schema is None:
            return
        delete_planner = DeletePlanner(self.root,
                                       self.current_database_name,
                                       self.current_schema_name,
                                       self.current_table_name)
        merge_planner = MergePlanner(self.root,
                                     self.current_database_name,
                                     self.current_schema_name,
                                     self.current_table_name)
        move_planner = MovePlanner(self.root,
                                   self.current_database_name,
                                   self.current_schema_name,
                                   self.current_table_name)
        table_info = TableInfo(
            table_schema=None,
            contents=[],
            table_versions=[],
            delete_planner=delete_planner,
            merge_planner=merge_planner,
            move_planner=move_planner)
        meta_store=MetadataFileStore(
            self.root,
            self.current_database_name,
            self.current_schema_name,
            self.current_table_name,
            table_info,
            merge_planner,
            delete_planner,
            move_planner)
        table_info.meta_store = meta_store
        table_info.update_table_schema(self.current_table_schema)
        table_info.alter_table_files(self.current_table_files, [])
        self.current_schema.tables_by_name[self.current_table_name] = table_info
        log.info("Discovered legacy table",
                 database=self.current_database_name,
                 schema=self.current_schema_name,
                 table=self.current_table_name,
                 discovery_time=time.time() - start)

    def discover_legacy_table(self):
        table_path = os.path.join(self.root, self.current_database_name, self.current_schema_name, self.current_table_name)
        self.iterate_directories(table_path)
        if len(self.current_table_files) == 0:
            return
        schema = self.parse_schema(os.path.join(table_path, self.current_table_files[0].filename))
        for f in self.current_table_files[1:]:
            _schema = self.parse_schema(os.path.join(table_path, f.filename))
            schema = self.merge_schema(schema, _schema)
        self.current_table_schema = schema



    def iterate_directories(self, path):
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):
                self.iterate_directories(item_path)
            if item == "metadata.json":
                with open(item_path, 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                    self.current_table_files.extend(self.parse_metadata(metadata))

    def parse_schema(self, file_path):
        try:
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema.to_arrow_schema()

            return schema
        except Exception as e:
            log.error("Error parsing schema",
                      file_path=file_path,
                      error=str(e),
                      traceback=traceback.format_exc())
            return None

    def parse_metadata(self, metadata):
        res = []
        for f in metadata["files"]:
            if os.path.exists(os.path.join(self.root,
                                           self.current_database_name,
                                           self.current_schema_name,
                                           self.current_table_name,
                                           "data",
                                           f["path"])):
                res.append(TableFile(
                    filename=str(os.path.join("data", f["path"])),
                    event_timestamp_column="__timestamp",
                    event_timestamp_min=pa.scalar(f["min_time"], pa.int64()),
                    event_timestamp_max=pa.scalar(f["max_time"], pa.int64()),
                    size_bytes=f["size_bytes"]
                ))
        return res

    def merge_schema(self, schema1, schema2):
        if schema1 is None:
            return schema2
        if schema2 is None:
            return schema1

        # 1. Find common fields and check if they have the same data type
        common_fields = {}
        for field in schema1:
            if field.name in schema2.names:
                schema2_field = schema2.field(field.name)
                if field.type == schema2_field.type:
                    common_fields[field.name] = field
                else:
                    raise ValueError(f"Field '{field.name}' has different types in schemas: {field.type} vs {schema2_field.type}")

        # 2. Find fields present in schema2 and not in schema1
        new_fields = [field for field in schema2 if field.name not in schema1.names]

        # 3. Add these fields to schema1
        merged_fields = list(common_fields.values()) + new_fields

        return pa.schema(merged_fields)


def discover_databases(root):
    d = DatabaseDiscovery(root)
    d.discover()
    return d.library