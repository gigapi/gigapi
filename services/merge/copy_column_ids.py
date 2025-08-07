import fastparquet
import struct
from fastparquet import ParquetFile
from fastparquet.parquet_thrift import FileMetaData
from icecream import ic
import copy

def copy_column_ids(src: str, dest: str) -> None:
    # Read the source file's metadata
    src_pf = ParquetFile(src)
    src_metadata = src_pf.fmd
    # Read the destination file's metadata
    dest_pf = ParquetFile(dest)
    dest_metadata = dest_pf.fmd

    # Copy the schema from source to destination
    dest_metadata.schema = copy.copy(src_metadata.schema)
    ic(dest_metadata)

    # Serialize the updated metadata
    ic(dest_metadata.to_bytes())

    # Write the updated metadata back to the destination file
    with open(dest, 'r+b') as f:
        # Go to the end of the file
        f.seek(0, 2)
        file_length = f.tell()
        f.seek(-8, 2)
        footer_metadata = f.read(8)
        footer_size_bytes = struct.unpack('<I', footer_metadata[:4])[0]
        f.seek(-1 * footer_size_bytes, 2)

        # Write the new footer
        footer_data = dest_metadata.to_bytes()
        f.write(footer_data)

        # Write the footer length (4 bytes) and "PAR1" magic bytes
        footer_length = len(footer_data)
        f.write(struct.pack('<I', footer_length))
        f.write(b'PAR1')
