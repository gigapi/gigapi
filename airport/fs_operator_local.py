import os
import shutil
from typing import BinaryIO, List
import fnmatch
from .fs_operator import FsOperator

class FSOperatorLocal(FsOperator):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def rmrf(self, path: str) -> None:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)
        shutil.rmtree(path)

    def mkdir(self, path: str) -> None:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)
        os.makedirs(path, exist_ok=True)

    def copy_internal(self, src_path: str, dst_path: str) -> None:
        if not src_path.startswith("/"):
            src_path = os.path.join(self.base_path, src_path)
        if not dst_path.startswith("/"):
            dst_path = os.path.join(self.base_path, dst_path)
        shutil.copy2(src_path, dst_path)

    def copy_external(self, src_path: str, dst_url: str) -> None:
        pass

    def create_file(self, path: str, content: BinaryIO) -> None:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Write the content from the stream to the file
        with open(path, 'wb') as f:
            shutil.copyfileobj(content, f)

    def ls(self, path: str) -> List[str]:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)

        try:
            return os.listdir(path)
        except OSError as e:
            raise RuntimeError(f"Error listing directory {path}: {str(e)}")

    def find(self, path: str, pattern: str) -> List[str]:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)

        result = []
        try:
            for root, dirs, files in os.walk(path):
                for name in files:
                    if fnmatch.fnmatch(name, pattern):
                        full_path = os.path.join(root, name)
                        relative_path = os.path.relpath(full_path, self.base_path)
                        result.append(relative_path)
            return result
        except OSError as e:
            raise RuntimeError(f"Error searching in {path}: {str(e)}")

    def open_file(self, path: str) -> BinaryIO:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)

        try:
            # Open the file in binary mode
            return open(path, 'rb')
        except IOError as e:
            raise RuntimeError(f"Error opening file {path}: {str(e)}")

    def get_size(self, path: str) -> int:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)

        try:
            return os.path.getsize(path)
        except OSError as e:
            raise RuntimeError(f"Error getting size of file {path}: {str(e)}")


    def is_file(self, path: str) -> bool:
        if not path.startswith("/"):
            path = os.path.join(self.base_path, path)

        try:
            return os.path.isfile(path)
        except OSError as e:
            raise RuntimeError(f"Error checking if path is a file {path}: {str(e)}")
