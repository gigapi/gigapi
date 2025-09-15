from .configuraiton import LayerConfig, LayerType
from .fs_operator_smart import FsOperatorSmart
from typing import List
import os
import structlog

log = structlog.get_logger()


class Mover:
    def __init__(self, layer_from: LayerConfig, layer_to: LayerConfig, database: str, schema: str, table: str):
        self.layer_from = layer_from
        self.layer_to = layer_to
        self.database = database
        self.schema = schema
        self.table = table

    def move(self, path: str) -> None:
        operator = FsOperatorSmart(self.layer_from.url)
        path = [self.database, self.schema, self.table, path]
        destination_url = self.join(self.layer_to.type, self.layer_to.url, path)
        frm = self.join(self.layer_from.type,  "", path).lstrip("/").lstrip(os.path.sep)
        if not operator.is_file(frm):
            log.error("!!!!!!!!!! FUCKFUCKFUCK")
            return
        operator.copy_external(
            self.join(self.layer_from.type,  "", path).lstrip("/").lstrip(os.path.sep),
            destination_url
        )

    def join(self, layer_type: LayerType, url: str, path: List[str]) -> str:
        if layer_type == LayerType.FILE:
            return url + os.path.sep + os.path.sep.join(path)
        if layer_type == LayerType.S3:
            return url + "/" + "/".join(path)
        raise ValueError("Unsupported file system")