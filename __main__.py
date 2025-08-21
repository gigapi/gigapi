import os
from contextlib import asynccontextmanager

import duckdb
from dotenv import load_dotenv

import services.writer

load_dotenv()

import uvicorn
from fastapi import FastAPI
from views import reader, writer, middlewares, ui, kvstore
import asyncio
from config import settings
from threading import Thread
from airport import writer_server
from airport import database_discovery
from icecream import ic
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from airport.writer_server import GigapipeWriterArrowFlightServer
import signal
import sys
from airport.metadata_file_store import MetadataFileStore
import objgraph
import time
from services.kvstore import FileStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    asyncio.create_task(start_background_tasks())
    yield

app = FastAPI(lifespan=lifespan)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": exc.body}
    )

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred."}
    )

async def monitor_object_growth():
    while True:
        print("Object growth in the last minute:")
        objgraph.show_growth(limit=10)
        await asyncio.sleep(60)  # Wait for 60 seconds

app.add_middleware(middlewares.ErrorHandlerMiddleware)

app.include_router(reader.router)
app.include_router(writer.router)
app.include_router(ui.router)
app.include_router(kvstore.router)

shutdown_event = asyncio.Event()

def signal_handler(signum, frame):
    print("Received shutdown signal")
    writer_server.shutdown()

async def start_background_tasks():
    # Start the run function as a background task
    services.writer.init()
    asyncio.create_task(monitor_object_growth())
    f = FileStore(settings.gigapi.root)
    kvstore.kv_store = f


def run_airport_server():
    print("START")
    writer_server.run("grpc://127.0.0.1:60001", settings.gigapi.root)
    print("END")

async def run_server():
    config = uvicorn.Config("__main__:app", host=settings.http.host, port=settings.http.port, loop="asyncio")
    server = uvicorn.Server(config)
    await server.serve()

def main():
    t = Thread(target=run_airport_server)
    t.start()
    asyncio.run(run_server())

if __name__ == "__main__":
    if os.getenv("CMD") == "show_stats":
        table_path = os.getenv("TABLE_PATH")
        if not table_path:
            print("Error: TABLE_PATH environment variable is not set.")
            sys.exit(1)

        # Split the table_path into its components
        path_parts = table_path.strip('/').split('/')
        if len(path_parts) < 4:
            print("Error: TABLE_PATH should be in the format '/base/path/database/schema/table'")
            sys.exit(1)

        base_path = '/'.join(path_parts[:-3])
        database_name = path_parts[-3]
        schema_name = path_parts[-2]
        table_name = path_parts[-1]

        # Initialize MetadataFileStore
        m = MetadataFileStore(
            base=base_path,
            database=database_name,
            schema=schema_name,
            table=table_name
        )
        m.load()
        print("Table statistics:")
        print(f"Total files: {len(m.table_info.contents)}")
        print(f"Delete plans: {len(m.delete_planner.delete_plans.delete_files)}")
        print("Merge plans: ")
        for folder, merge_plans in m.merge_planner.merge_plans.merge_plans.items():
            for merge_plan in merge_plans:
                print(f"  Folder: {folder}")
                print(f"  State: {merge_plan.state}")
                print(f"  From files: {len(merge_plan.from_table_files)}")
                print(f"  To file: {merge_plan.to_file_path}")
                print(f"  Created at: {merge_plan.created_at}")
                print(f"  Updated at: {merge_plan.updated_at}")
                print("  ---")
    elif os.getenv("CMD") == "setup":
        ddb = duckdb.connect()
        ddb.execute("INSTALL airport FROM community;")
    else:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        main()
    #test_merge_schema()
