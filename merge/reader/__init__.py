import traceback
from query_farm_sql_scan_planning.planner import RangeFieldInfo, Planner
import pyarrow as pa
import sqlglot
import json
import re

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

def inject(query, metadata_json):
    try:
        metadata = [(f['path'], { "__timestamp": RangeFieldInfo(
                min_value=pa.scalar(f['min']),
                max_value=pa.scalar(f['max']),
                has_nulls=False,
                has_non_nulls=True
        )}) for f in json.loads(metadata_json)]
        res = sqlglot.parse_one(query, dialect="duckdb")
        frm = None
        whr = None
        for node in res.walk():
            if isinstance(node, sqlglot.expressions.From):
                frm = node
            if isinstance(node, sqlglot.expressions.Where):
                whr = node.this
        if whr:
            planner = Planner(metadata)
            matching_files = set(planner.files(whr))
        else:
            matching_files = set([x[0] for x in metadata])
        q = "FROM read_parquet(ARRAY[%s])" % ",".join([f"'{f}'" for f in matching_files])
        frm2 = sqlglot.parse_one(q, dialect="duckdb")
        for node in frm2.walk():
            if isinstance(node, sqlglot.expressions.From):
                frm.this.this.replace(node.this.this)
        _res = re.sub("ARRAY\(([^)]+)\)", "ARRAY[\\1]", str(res))
        return json.dumps({"status": "ok", "result": _res})
    except Exception as e:
        print(f"Error in inject function: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)})
