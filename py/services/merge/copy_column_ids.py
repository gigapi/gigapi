import fastparquet
import struct
from fastparquet import ParquetFile
from fastparquet.parquet_thrift import FileMetaData
from icecream import ic

def custom_thrift_copy(obj):
    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    elif isinstance(obj, list):
        return [custom_thrift_copy(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: custom_thrift_copy(v) for k, v in obj.items()}
    else:
        # For Thrift objects
        new_obj = obj.__class__()
        for key in obj.__dict__:
            if not key.startswith('_'):
                setattr(new_obj, key, custom_thrift_copy(getattr(obj, key)))
        return new_obj

async def copy_column_ids(src: str, dest: str) -> None:
    # Read the source file's metadata
    src_pf = ParquetFile(src)
    src_metadata = src_pf.metadata
    ic(src_metadata.schema)
    return

    # Read the destination file's metadata
    dest_pf = ParquetFile(dest)
    dest_metadata = dest_pf.metadata

    # Copy the schema from source to destination
    dest_metadata.schema = custom_thrift_copy(src_metadata.schema)

    # Serialize the updated metadata
    footer_data = FileMetaData().write(dest_metadata)

    # Write the updated metadata back to the destination file
    with open(dest, 'r+b') as f:
        # Go to the end of the file
        f.seek(0, 2)
        file_length = f.tell()

        # Write the new footer
        f.write(footer_data)

        # Write the footer length (4 bytes) and "PAR1" magic bytes
        footer_length = len(footer_data)
        f.write(struct.pack('<I', footer_length))
        f.write(b'PAR1')

        # Update file size
        new_file_length = f.tell()

    # Update the Parquet file metadata
    dest_pf.metadata.footer_size = footer_length
    dest_pf._file_size = new_file_length

    print(f"Updated footer for {dest}. New file size: {new_file_length}, Footer size: {footer_length}")

import asyncio

async def main():
    await copy_column_ids(
        "/home/hromozeka/QXIP/quackpipe/_testdata/main/test/ducklake-01982819-8f99-7079-9748-5fcb9e492a49.parquet",
        "/home/hromozeka/QXIP/quackpipe/_testdata/main/test/ducklake-01982819-8f99-7079-9748-5fcb9e492a49_copy.parquet"
    )

if __name__ == "__main__":
    asyncio.run(main())