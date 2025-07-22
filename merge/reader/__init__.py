import sqlglot
from query_farm_sql_scan_planning.planner import RangeFieldInfo, SetFieldInfo, Planner
import pyarrow as pa
from icecream import ic
import json

# metadata = None
#
# def set_metadata(provider):
#     global metadata
#     metadata = provider
#
# def load_files(table):
#     for f in json.loads(metadata.load_files(table)):
#         yield (f['path'], { "__timestamp": RangeFieldInfo(
#             min_value=pa.scalar(f['min']),
#             max_value=pa.scalar(f['max']),
#             has_nulls=False,
#             has_non_nulls=True
#         )})

def tables(query) -> str:
    table_names = []
    res = sqlglot.parse_one(query, dialect="duckdb")
    for node in res.walk():
        if isinstance(node, sqlglot.expressions.From):
            table_names.append(str(node.this))
    return json.dumps(table_names)

def inject(query, metadata_json):
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
    planner = Planner(metadata)
    matching_files = set(planner.files(whr))
    frm2 = sqlglot.parse_one("FROM read_parquet([%s])" % ",".join([f"'{f}'" for f in matching_files]), dialect="duckdb")
    for node in frm2.walk():
        if isinstance(node, sqlglot.expressions.From):
            frm.replace(node)
