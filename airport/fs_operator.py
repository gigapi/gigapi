from typing import BinaryIO, List


class FsOperator:
    def rmrf(self, path: str):
        pass

    def mkdir(self, path: str):
        pass

    def copy_internal(self, src_path: str, dst_path: str) -> None:
        pass

    def copy_external(self, src_path: str, dst_url: str) -> None:
        pass

    def create_file(self, path: str, content: BinaryIO) -> None:
        pass

    def open_file(self, path: str) -> BinaryIO:
        pass

    def ls(self, path: str) -> List[str]:
        pass

    def find(self, path: str, pattern: str) -> List[str]:
        pass

    def get_size(self, path: str) -> int:
        pass

    def is_file(self, path: str) -> bool:
        pass

