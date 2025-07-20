import asyncio
from utils.ddb import async_duckdb_connection

async def main():
    async with async_duckdb_connection() as conn:
        # Example query
        pass

if __name__ == "__main__":
    asyncio.run(main())