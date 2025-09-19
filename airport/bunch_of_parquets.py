import datetime

import pyarrow.parquet as pq
import pyarrow as pa
import os
import pyarrow.compute as pc

from .constants import event_timestamp_column
from .model import TableFile
import structlog
import sys
import traceback
from .configuraiton import config

log = structlog.get_logger()


class ParquetWrapper:
    def __init__(self, path: str, table_file: TableFile, writer: pq.ParquetWriter):
        self.path = path
        self.table_file = table_file
        self.writer = writer

class BunchOfParquets:
    def __init__(self, root: str, database: str, schema_name: str, table: str, filename: str):
        self.root = root
        self.database = database
        self.schema_name = schema_name
        self.table = table
        self.parquet_files = {}
        self.filename = filename

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_parquet_writer(self, path: str, schema: pa.Schema) -> ParquetWrapper:
        abs_path = os.path.join(self.root, self.database, self.schema_name, self.table, path)
        if path in self.parquet_files:
            return self.parquet_files[path]

        os.makedirs(os.path.dirname(abs_path), exist_ok=True)

        self.parquet_files[path] = ParquetWrapper(
            abs_path,
            TableFile(
                filename=path,
                event_timestamp_min=None,
                event_timestamp_max=None,
                layer_name=config().layer_configuration[0].name
            ),
            pq.ParquetWriter(
                abs_path,
                schema
            )
        )
        return self.parquet_files[path]

    def write_chunk(self, hour: datetime.datetime, schema: pa.Schema, chunk: any):
        if schema is None:
            raise ValueError("Schema must be provided when creating a new ParquetWriter")
        path = os.path.join("data", f"date={hour.strftime('%Y-%m-%d')}/hour={hour.strftime('%H')}", self.filename)
        w = self.get_parquet_writer(path, schema)
        w.writer.write_table(chunk)
        min_max = pc.min_max(chunk[event_timestamp_column])
        if w.table_file.event_timestamp_max is None:
            w.table_file.event_timestamp_max = min_max["max"]
            w.table_file.event_timestamp_min = min_max["min"]
        else:
            w.table_file.event_timestamp_max = pc.max(pa.array([w.table_file.event_timestamp_max, min_max["max"]]))
            w.table_file.event_timestamp_min = pc.min(pa.array([w.table_file.event_timestamp_min, min_max["min"]]))

    def close(self):
        for path, writer in self.parquet_files.items():
            try:
                writer.writer.close()
            except Exception as e:
                exc_info = sys.exc_info()
                log.info(
                    "Error closing ParquetWriter",
                    path=path,
                    error=str(e),
                    exc_info=exc_info,
                    traceback=traceback.format_exception(*exc_info)
                )

        self.parquet_files.clear()
