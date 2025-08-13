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

from .metadata_file_store import MetadataFileStore
from .table import DatabaseLibrary, DatabaseContents, TableInfo, TableFile, SchemaCollection
import pyarrow.parquet as pq
from icecream import ic
import pyarrow as pa


def discover_databases(root):
    library = DatabaseLibrary()

    def process_table_data(root, database, schema, table):
        table_data_path = os.path.join(root, database, schema, table, "data")
        if not os.path.isdir(table_data_path):
            table_data_path = os.path.join(root, database, schema, table)
        if os.path.exists(os.path.join(root, database, schema, table, "metadata.db")):
            meta_store = MetadataFileStore(root, database, schema, table)
            if meta_store.load() is not None:
                return meta_store.table_info

        def find_metadata_and_files(path):
            files = []
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path):
                    sub_files = find_metadata_and_files(item_path)
                    files.extend(sub_files)
                elif item == "metadata.json":
                    with open(item_path, 'r') as metadata_file:
                        metadata = json.load(metadata_file)
                        files.extend(parse_metadata(table_data_path, metadata))

            return files


        files = find_metadata_and_files(table_data_path)
        if len(files) == 0:
            return None

        db_schema = schema
        filename = lambda x: x if x.startswith("/") else os.path.join(root, database, db_schema, table_name, x)
        schema = parse_schema(filename(files[0].filename))
        for file in files[1:]:
            schema = merge_schema(schema, parse_schema(filename(file.filename)))



        table_info = TableInfo(
            table_schema=schema,
            contents=files,
            table_versions=[],
        )
        meta_store = MetadataFileStore(root, database, db_schema, table, table_info)
        table_info.meta_store = meta_store
        meta_store.on_schema_update()
        meta_store.on_files_update(files, [])
        return table_info


    for database_name in os.listdir(root):
        database_path = os.path.join(root, database_name)
        if os.path.isdir(database_path):
            database = DatabaseContents()
            for schema in os.listdir(database_path):
                schema_path = os.path.join(database_path, schema)
                for table_name in os.listdir(schema_path):
                    table_path = os.path.join(schema_path, table_name)
                    if os.path.isdir(table_path):
                        data_path = os.path.join(table_path)
                        data_path = data_path if os.path.exists(data_path) else table_path
                        if not os.path.isdir(data_path):
                            continue
                        table_info = process_table_data(root, database_name, schema, table_name)
                        if schema not in database.schemas_by_name:
                            database.schemas_by_name[schema] = SchemaCollection()
                        if table_info is not None:
                            database.schemas_by_name[schema].tables_by_name[table_name] = table_info
            library.databases_by_name[database_name] = database

    return library

def parse_metadata(table_path, metadata):
    return [TableFile(
        filename=str(os.path.join("data", f["path"])),
        event_timestamp_column="__timestamp",
        event_timestamp_min=pa.scalar(f["min_time"], pa.int64()),
        event_timestamp_max=pa.scalar(f["max_time"], pa.int64()),
        size_bytes=f["size_bytes"]
    ) for f in metadata["files"]]

def parse_schema(file_path):
    try:
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema.to_arrow_schema()

        return schema
    except Exception as e:
        print(f"Error parsing schema for file {file_path}: {str(e)}")
        return None
    
def merge_schema(schema1, schema2):
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
