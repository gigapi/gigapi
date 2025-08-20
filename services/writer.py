import json
import asyncio
from typing import List, Dict
from utils.ddb import async_ducklake_connection, memfs, AsyncDuckDBConnection
import uuid
from .points import parse_points
import structlog

log = structlog.get_logger()



class Buffer:
    def __init__(self):
        self.data_by_table = {}
        self.futures: List[asyncio.Future] = []
        self.timer_task = asyncio.create_task(self._timer())
        self.working = True

    def add(self, data_by_table: Dict[str, Dict[str, List[Dict]]]):
        future = asyncio.get_running_loop().create_future()
        for database, tables in data_by_table.items():
            if database not in self.data_by_table:
                self.data_by_table[database] = {}
            for table, points in tables.items():
                if table not in self.data_by_table[database]:
                    self.data_by_table[database][table] = []
                self.data_by_table[database][table].extend(points)
                self.futures.append(future)
        return future

    async def _timer(self):
        while self.working:
            try:
                await asyncio.sleep(0.5)
                await self._flush()
            except Exception as e:
                log.error("Failed to flush buffer", err=e, exc_info=True)

    async def _flush(self):
        data = self.data_by_table
        self.data_by_table = {}
        futures = self.futures
        self.futures = []
        try:
            await self._write_to_airport(data)
            for future in futures:
                if not future.done():
                    future.set_result(None)
        except Exception as e:
            for future in futures:
                if not future.done():
                    future.set_exception(e)

    async def _write_to_airport(self, data_by_table: Dict[str, Dict[str, List[Dict]]]):
        if len(data_by_table) == 0:
            return
        async with async_ducklake_connection() as conn:
            databases = (await conn.aquery(f"SHOW DATABASES")).fetchall()
            for database, tables in data_by_table.items():
                if database not in [row[0] for row in databases]:
                    await conn.aexecute(f"CALL airport_action('grpc://localhost:60001/', 'create_database', '{database}');")
                    await conn.aexecute(f"ATTACH '{database}' (TYPE  AIRPORT, location 'grpc://localhost:60001/')")
                    await conn.aexecute(f"CREATE SCHEMA {database}.master;")
                await conn.aexecute(f"USE {database}.master;")
                for table, points in tables.items():
                    filename = f"{uuid.uuid4()}.json"
                    try:
                        memfs.write_text(filename, "\n".join([json.dumps(p) for p in points]))
                        fields = (await conn.aquery(f"describe SELECT * FROM read_json('memory://{filename}')")).fetchall()
                        table_fields = await prepare_table(conn, table, fields)
                        timestamp_field = [field for field in table_fields if field[0] == "__timestamp"][0]
                        fields_part = ", ".join([f"\"{field[0]}\"" for field in fields])
                        select_part = []
                        for field in fields:
                            if field[0] == "__timestamp" and timestamp_field[1] == 'TIMESTAMP_NS':
                                select_part.append(f"make_timestamp_ns({field[0]})")
                                continue
                            select_part.append(f"\"{field[0]}\"")
                        q = f"INSERT INTO {table} ({fields_part}) SELECT {','.join(select_part)} FROM read_json('memory://{filename}')"
                        await conn.aexecute(q)
                    finally:
                        memfs.delete(filename)

    async def close(self):
        self.working = False
        await self._flush()

buf = None

def init():
    global buf
    buf = Buffer()

async def write_lineproto(data: bytes, database: str):
    await _write_lineproto(data, database)
async def _write_lineproto(data: bytes, database: str):
    global buf
    data = parse_points(data)
    json_by_database = {}
    for p in data:
        k = p.Fields()
        k.update(p.Tags())
        k["__timestamp"] = p.time
        if "." in p.Name():
            db_name, table_name = p.Name().split(".")
        else:
            db_name = database
            table_name = p.Name()
        if db_name not in json_by_database:
            json_by_database[db_name] = {}
        json_by_table = json_by_database[db_name]
        json_by_table[table_name] = json_by_table[table_name] if p.Name() in json_by_table else []
        json_by_table[table_name].append(k)
        json_by_database[db_name] = json_by_table
    f = buf.add(json_by_database)
    await f

async def prepare_table(conn: AsyncDuckDBConnection, table: str, fields):
    tables = (await conn.aquery("SHOW TABLES;")).fetchall()

    if tables is None or table not in [row[0] for row in tables]:
        try:
            await conn.aexecute(f"CREATE TABLE {table} ({', '.join([f'\"{field[0]}\" {field[1]}' 
                for field in fields if not field[0].startswith("__")])});")
        except Exception as e:
            if "Table" in str(e) and "already exists" in str(e):
                pass
            else:
                raise e
    existing_fields = (await conn.aquery(f"DESCRIBE {table};")).fetchall()
    absent_fields = [field for field in fields if field[0] not in [f[0] for f in existing_fields]]
    if [f for f in absent_fields if f[0].startswith("__")]:
        raise ValueError(f"Cannot create table {table} because it contains fields starting with '__'")
    if len(absent_fields) > 0:
        for field in absent_fields:
            q = f"ALTER TABLE {table} ADD COLUMN \"{field[0]}\" {field[1]};"
            await conn.aexecute(q)
    return existing_fields

