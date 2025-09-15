from typing import BinaryIO, List

from .configuraiton import LayerType
from .fs_operator_local import FSOperatorLocal
from .fs_operator_s3 import FSOperatorS3
from airport.fs_operator import FsOperator
from urllib.parse import urlparse

class FsOperatorSmart(FsOperator):
    def __init__(self, url: str):
        self.fs_operator = self._init(url)

    def _init(self, url: str):
        if url.startswith("file://"):
            return self._init_local(url)
        if url.startswith("s3://"):
            return self._init_s3(url)
        raise ValueError(f"Unsupported file system: {url}")

    def _init_local(self, url: str):
        if not url.startswith("file://"):
            raise ValueError(f"Invalid file URL: {url}")
        return FSOperatorLocal(url[7:])



    def _init_s3(self, url: str):
        if not url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {url}")

        parsed_url = urlparse(url)

        # Extract username and password
        if '@' in parsed_url.netloc:
            auth, host_port = parsed_url.netloc.split('@', 1)
            username, password = auth.split(':', 1)
        else:
            host_port = parsed_url.netloc
            username = password = None

        # Extract hostname and port
        if ':' in host_port:
            hostname, port = host_port.split(':', 1)
            port = int(port)
        else:
            hostname = host_port
            port = 443  # Default HTTPS port

        # Extract bucket name and prefix
        path_parts = parsed_url.path.strip('/').split('/', 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        return FSOperatorS3(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            bucket_name=bucket_name,
            prefix=prefix
        )


    def rmrf(self, path: str):
        return self.fs_operator.rmrf(path)

    def mkdir(self, path: str):
        return self.fs_operator.mkdir(path)

    def copy_internal(self, src_path: str, dst_path: str) -> None:
        return self.fs_operator.copy_internal(src_path, dst_path)

    def copy_external(self, src_path: str, dst_url: str) -> None:
        path_parts = dst_url.rstrip('/').split('/')
        _dst_url = '/'.join(path_parts[:-1])
        dst_filename = path_parts[-1]
        to_op = self._init(_dst_url)
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