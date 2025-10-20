import asyncio
import time

import aiohttp
import json
from assertpy import assert_that
import random

BASE_URL = "http://localhost:7971"

async def query(session, db, query):
    url = f"{BASE_URL}/query?db={db}"
    payload = {"query": query}
    async with session.post(url, json=payload) as response:
        print(f"Response status: {response.status}")
        print(f"Response headers: {response.headers}")
        content_type = response.headers.get('Content-Type', '')
        print(f"Content-Type: {content_type}")

        if 'application/json' in content_type:
            res = await response.json()
            return res['results'] if 'results' in res else res
        else:
            return await response.text()

async def write(session, db, data):
    url = f"{BASE_URL}/gigapi/write?db={db}"
    async with session.post(url, data=data) as response:
        print(f"Write response status: {response.status}")
        return await response.text()

async def memory_database_should_persist():
    async with aiohttp.ClientSession() as session:
        # Drop the table if it exists
        drop_table_query = "DROP TABLE IF EXISTS test_table"
        drop_result = await query(session, "memory", drop_table_query)
        print("Drop table result:", drop_result)

        # Create a table in the 'memory' database
        create_table_query = "CREATE TABLE test_table (id INTEGER, name VARCHAR)"
        create_result = await query(session, "memory", create_table_query)
        print("Create table result:", create_result)

        # Insert data into the table
        insert_query = "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
        insert_result = await query(session, "memory", insert_query)
        print("Insert data result:", insert_result)

        # Select count of rows inserted
        count_query = "SELECT COUNT(*) FROM test_table"
        count_result = await query(session, "memory", count_query)
        print("Count result:", count_result)

        # Try to parse the count result
        try:
            if isinstance(count_result, str):
                count = json.loads(count_result)
            else:
                count = count_result

            if isinstance(count, list) and len(count) > 0:
                count = count[0]['count_star()']
            elif isinstance(count, dict) and 'data' in count:
                count = count['data'][0][0]

            assert_that(count).is_equal_to(3)
            print("Test passed: Correct number of rows inserted.")
        except Exception as e:
            print(f"Test failed: Couldn't parse count result. Error: {e}")
            print(f"Raw count result: {count_result}")
            raise e

async def airport_should_work():
    async with aiohttp.ClientSession() as session:
        t = await query(session, "my_new_db", "SHOW TABLES")
        print(t)
        # Drop the table if it exists
        drop_table_query = "DROP TABLE weather"
        await query(session, "my_new_db", drop_table_query)
        drop_table_query = "DROP TABLE weather2"
        await query(session, "my_new_db", drop_table_query)

        # Write data
        write_data = (
            "weather2,location=us-midwest,season2=summer2 temperature=84,season3=\"summer3\",season4=\"summer4\"\n"
            "weather,location=us-midwest,season2=summer2 temperature=84"
        )
        write_result = await write(session, "my_new_db", write_data)
        print("Write result:", write_result)

        # Query to check the data
        count_query = "SELECT COUNT(*) FROM weather"
        count_result = await query(session, "my_new_db", count_query)
        print("Count result:", count_result)

        try:
            if isinstance(count_result, str):
                count_result = json.loads(count_result)

            if isinstance(count_result, list) and len(count_result) > 0:
                count = count_result[0]['count_star()']
            elif isinstance(count_result, dict) and 'data' in count_result:
                count = count_result['data'][0][0]
            else:
                raise ValueError(f"Unexpected count_result format: {count_result}")

            assert_that(count).is_equal_to(1)
            print("Test passed: Correct number of rows in 'weather' table.")
        except Exception as e:
            print(f"Test failed: Couldn't verify row count. Error: {e}")
            print(f"Raw count result: {count_result}")
            raise e

        # Query to check the data content
        select_query = "SELECT * FROM weather"
        select_result = await query(session, "my_new_db", select_query)
        print("Select result:", select_result)

        try:
            if isinstance(select_result, str):
                select_result = json.loads(select_result)

            assert_that(select_result).is_not_empty()
            row = select_result[0]
            assert_that(row).contains_key('location').contains_key('season2').contains_key('temperature')
            assert_that(row['location']).is_equal_to('us-midwest')
            assert_that(row['season2']).is_equal_to('summer2')
            assert_that(row['temperature']).is_equal_to(84)
            print("Test passed: Data content is correct.")
        except Exception as e:
            print(f"Test failed: Data content is incorrect. Error: {e}")
            print(f"Raw select result: {select_result}")
            raise e

async def heavy_multithreaded_insert():
    async with aiohttp.ClientSession() as session:
        # Drop the tables if they exist
        await query(session, "my_new_db", "DROP TABLE IF EXISTS weather")
        await query(session, "my_new_db", "DROP TABLE IF EXISTS weather2")

        # Prepare 150 write requests
        write_tasks = []
        for _ in range(150):
            temperature = random.randint(60, 100)
            write_data = f"weather,location=us-midwest,season2=summer2 temperature={temperature}"
            write_tasks.append(write(session, "my_new_db", write_data))

        # Execute all write requests concurrently
        start = time.time()
        write_results = await asyncio.gather(*write_tasks)
        print(f"Completed {len(write_results)} write operations in {time.time() - start} seconds.")

        # Query to check the data
        count_query = "SELECT COUNT(*) FROM weather"
        count_result = await query(session, "my_new_db", count_query)
        print("Count result:", count_result)

        try:
            if isinstance(count_result, str):
                count_result = json.loads(count_result)

            if isinstance(count_result, list) and len(count_result) > 0:
                count = count_result[0]['count_star()']
            elif isinstance(count_result, dict) and 'data' in count_result:
                count = count_result['data'][0][0]
            else:
                raise ValueError(f"Unexpected count_result format: {count_result}")

            assert_that(count).is_equal_to(150)
            print("Test passed: Correct number of rows (150) in 'weather' table after multithreaded insert.")
        except Exception as e:
            print(f"Test failed: Couldn't verify row count. Error: {e}")
            print(f"Raw count result: {count_result}")
            raise e

async def main():
    await memory_database_should_persist()
    await airport_should_work()
    await heavy_multithreaded_insert()

if __name__ == "__main__":
    asyncio.run(main())