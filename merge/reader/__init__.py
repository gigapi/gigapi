import traceback
from query_farm_sql_scan_planning.planner import RangeFieldInfo, Planner
import pyarrow as pa
import sqlglot
import json
import re
import struct
from icecream import ic

class CustomDuckDBDialect(sqlglot.dialects.DuckDB):
    class Parser(sqlglot.dialects.DuckDB.Parser):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            for f in ["EPOCH", "EPOCH_MS", "ARRAY", "TIME_BUCKET", "TO_TIMESTAMP"]:
                if f in self.FUNCTIONS:
                    del self.FUNCTIONS[f]

def tables(query) -> str:
    try:
        table_names = []
        res = sqlglot.parse_one(query, dialect="duckdb")
        for node in res.walk():
            if isinstance(node, sqlglot.expressions.From):
                table_names.append(str(node.this.this))
        return json.dumps({"status": "ok", "result": table_names})
    except Exception as e:
        print(f"Error in tables function: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)})

def decode_binds(encoded_binds):
    result = []
    offset = 0
    while offset < len(encoded_binds):
        # Read path length (2 bytes, little-endian)
        path_length = struct.unpack_from('<H', encoded_binds, offset)[0]
        offset += 2

        # Read path
        path = encoded_binds[offset:offset+path_length].decode('utf-8')
        offset += path_length

        # Read MinTime (8 bytes, little-endian)
        min_time = struct.unpack_from('<q', encoded_binds, offset)[0]
        offset += 8

        # Read MaxTime (8 bytes, little-endian)
        max_time = struct.unpack_from('<q', encoded_binds, offset)[0]
        offset += 8

        result.append({
            'path': path,
            'min': min_time,
            'max': max_time
        })

    return result


def inject(query, metadata_json):
    try:
        # OPTIMIZATION: Early return if no metadata
        if not metadata_json:
            return json.dumps({"status": "ok", "result": query})
        
        # OPTIMIZATION: Process metadata more efficiently
        metadata = []
        for f in decode_binds(metadata_json):
            metadata.append((
                f['path'], 
                { "__timestamp": RangeFieldInfo(
                    min_value=pa.scalar(f['min'], pa.int64()),
                    max_value=pa.scalar(f['max'], pa.int64()),
                    has_nulls=False,
                    has_non_nulls=True
                )}
            ))
        
        res = sqlglot.parse_one(query)
        frm = None
        whr = None
        
        # OPTIMIZATION: Single pass through AST
        for node in res.walk():
            if isinstance(node, sqlglot.expressions.From):
                frm = node
            elif isinstance(node, sqlglot.expressions.Where):
                whr = node.this
                break  # Found WHERE clause, no need to continue
        
        # OPTIMIZATION: Early filtering based on WHERE clause
        if whr:
            planner = Planner(metadata)
            matching_files = set(planner.files(whr))
        else:
            # OPTIMIZATION: Use set comprehension for better performance
            matching_files = {x[0] for x in metadata}
        
        # OPTIMIZATION: Early return if no matching files
        if not matching_files:
            return json.dumps({"status": "ok", "result": query})
        
        # OPTIMIZATION: More efficient string building
        file_list = ",".join(f"'{f}'" for f in matching_files)
        q = f"FROM read_parquet(ARRAY[{file_list}])"
        
        frm2 = sqlglot.parse_one(q)
        for node in frm2.walk():
            if isinstance(node, sqlglot.expressions.From):
                frm.this.this.replace(node.this.this)
                break  # Found FROM clause, no need to continue
        
        _res = re.sub("ARRAY\(([^)]+)\)", "ARRAY[\\1]", str(res))
        return json.dumps({"status": "ok", "result": _res})
    except Exception as e:
        print(f"Error in inject function: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)})
