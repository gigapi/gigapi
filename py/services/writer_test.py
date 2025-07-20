import time

from icecream import ic
from dotenv import load_dotenv
load_dotenv()

import asyncio
from utils.ddb import async_duckdb_connection
from services.writer import write_json
from datetime import datetime
import json


async def main():
    async with async_duckdb_connection() as conn:
        await conn.aexecute("CREATE TABLE IF NOT EXISTS test (time TIMESTAMP, str VARCHAR)")
        await write_json("\n".join([
            json.dumps({
                "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "str": "abc"}) for _ in range(1000)]).encode("utf-8"),
            "test")

if __name__ == "__main__":
    asyncio.run(main())