import os.path
from typing import BinaryIO, List

from .configuraiton import LayerType
from .fs_operator_local import FSOperatorLocal
from .fs_operator_s3 import FSOperatorS3
from airport.fs_operator import FsOperator
from urllib.parse import urlparse, parse_qsl
from .utils import LayerUrlHelper


class FsOperatorSmart(FsOperator):
    def __init__(self, url: str):
        self.fs_operator = self._init(url)
        self.url = url

    def _init(self, url: str):
        if url.startswith("file://"):
            return self._init_local(url)
        if url.startswith("s3://"):
            return self._init_s3(url)
        raise ValueError(f"Unsupported file system: {url}")

    def _init_local(self, url: str):
        if not url.startswith("file://"):
            raise ValueError(f"Invalid file URL: {url}")
        return FSOperatorLocal(os.path.sep.join(LayerUrlHelper(url).prefix))



    def _init_s3(self, url: str):
        h = LayerUrlHelper(url)
        return FSOperatorS3(
            hostname=h.hostname,
            port=h.port,
            username=h.username,
            password=h.password,
            bucket_name=h.bucket_name,
            prefix="/".join(h.prefix),
            use_ssl=h.use_ssl,
        )

    def rmrf(self, path: str):
        return self.fs_operator.rmrf(path)

    def mkdir(self, path: str):
        return self.fs_operator.mkdir(path)

    def copy_internal(self, src_path: str, dst_path: str) -> None:
        return self.fs_operator.copy_internal(src_path, dst_path)

    def copy_external(self, src_path: str, dst_url: str) -> None:
        h = LayerUrlHelper(dst_url)
        dst_filename = h.prefix[-1]
        h.set_prefix(h.prefix[:-1])
        to_op = self._init(h.string())
        with self.fs_operator.open_file(src_path) as f:
            to_op.create_file(dst_filename, f)

    def create_file(self, path: str, content: BinaryIO) -> None:
        return self.fs_operator.create_file(path, content)

    def ls(self, path: str) -> List[str]:
        return self.fs_operator.ls(path)

    def find(self, path: str, pattern: str) -> List[str]:
        return self.fs_operator.find(path, pattern)

    def get_size(self, path: str) -> int:
        return self.fs_operator.get_size(path)

    def is_file(self, path: str) -> bool:
        return self.fs_operator.is_file(path)