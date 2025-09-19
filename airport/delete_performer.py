import os

from .configuraiton import LayerConfig, LayerType
from .fs_operator_s3 import FSOperatorS3
from .utils import LayerUrlHelper

class DeletePerformer:
    def __init__(self, layer: LayerConfig, database: str, schema: str, table: str):
        self.layer = layer
        self.url = LayerUrlHelper(layer.url)
        self.database = database
        self.schema = schema
        self.table = table
    def do_delete(self, delete_plan: str):
        if self.layer.type == LayerType.FILE:
            op = FSDeletePerformer(os.path.sep.join(self.url.prefix), self.database, self.schema, self.table)
            op.do_delete(delete_plan)
        elif self.layer.type == LayerType.S3:
            op = S3DeletePerformer(self.layer, self.database, self.schema, self.table)
            op.do_delete(delete_plan)
        else:
            raise ValueError("Unsupported layer type")

class FSDeletePerformer:
    def __init__(self, base: str, database: str, schema: str, table: str):
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table

    def do_delete(self, delete_plan: str):
        p = os.path.join(self.base, self.database, self.schema, self.table, delete_plan)
        if os.path.exists(p):
            os.remove(p)

class S3DeletePerformer:
    def __init__(self, layer: LayerConfig, database: str, schema: str, table: str):
        self.layer = layer
        self.url = LayerUrlHelper(layer.url)
        self.database = database
        self.schema = schema
        self.table = table


    def do_delete(self, delete_plan: str):
        (FSOperatorS3(
            self.url.hostname,
            self.url.port,
            self.url.username,
            self.url.password,
            self.url.bucket_name,
            "/".join(self.url.prefix), self.url.use_ssl)
        ).rmrf(
            "/".join([self.database, self.schema, self.table, delete_plan])
        )