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
from icecream import ic
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import signal
import sys
import objgraph
import time
from services.kvstore import FileStore
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import Depends, HTTPException, status
from gigapi.config import settings
import logging

def get_log_level():
    loglevels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARN,
        "error": logging.ERROR,
        "fatal": logging.FATAL
    }
    if settings.loglevel.lower() in loglevels:
        return loglevels[settings.loglevel.lower()]
    logging.error(f"Invalid log level: {settings.loglevel}. Using default log level: info")
    return logging.INFO

logging.basicConfig(level=get_log_level())
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    asyncio.create_task(start_background_tasks())
    yield

security = HTTPBasic(auto_error=False)

class UnauthorizedException(HTTPException):
    pass

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = settings.http.basic_auth.username.strip()
    correct_password = settings.http.basic_auth.password.strip()
    if credentials is None or not (credentials.username == correct_username and credentials.password == correct_password):
        raise UnauthorizedException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic realm=\"Access to the API\""},
        )
    return credentials.username

def get_app_dependencies():
    deps = []
    if settings.http.basic_auth is not None and \
            settings.http.basic_auth.username is not None and \
            settings.http.basic_auth.username != "":
        deps.append(Depends(verify_credentials))
    return deps

app = FastAPI(lifespan=lifespan, dependencies=get_app_dependencies())

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": exc.body}
    )

@app.exception_handler(UnauthorizedException)
async def http_exception_handler(request, exc):
    return Response(
        status_code=exc.status_code,
        headers={"WWW-Authenticate": "Basic realm=\"Access to the API\""},
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
    if get_log_level() > logging.DEBUG:
        return
    while True:
        logger.debug("Object growth in the last minute:")
        objgraph.show_growth(limit=10)
        await asyncio.sleep(60)  # Wait for 60 seconds

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],  # Allows all methods
    allow_headers=["Content-Type"],  # Allows all headers
)

app.add_middleware(middlewares.ErrorHandlerMiddleware)

app.include_router(reader.router)
app.include_router(writer.router)
app.include_router(ui.router)
app.include_router(kvstore.router)

shutdown_event = asyncio.Event()

def signal_handler(signum, frame):
    logger.info("Received shutdown signal")

async def start_background_tasks():
    # Start the run function as a background task
    services.writer.init()
    asyncio.create_task(monitor_object_growth())
    f = FileStore(settings.gigapi.root)
    kvstore.kv_store = f

async def run_server():
    config = uvicorn.Config("__main__:app", host=settings.http.host, port=settings.http.port, loop="asyncio")
    server = uvicorn.Server(config)
    await server.serve()

def main():
    asyncio.run(run_server())

if __name__ == "__main__":
    if os.getenv("CMD") == "show_stats":
        table_path = os.getenv("TABLE_PATH")
        if not table_path:
            logger.error("Error: TABLE_PATH environment variable is not set.")
            sys.exit(1)

        # Split the table_path into its components
        path_parts = table_path.strip('/').split('/')
        if len(path_parts) < 4:
            logger.error("Error: TABLE_PATH should be in the format '/base/path/database/schema/table'")
            sys.exit(1)

        base_path = '/'.join(path_parts[:-3])
        database_name = path_parts[-3]
        schema_name = path_parts[-2]
        table_name = path_parts[-1]

    elif os.getenv("CMD") == "setup":
        ddb = duckdb.connect()
        ddb.execute("INSTALL airport FROM community;")
        ddb.execute("INSTALL httpfs")
    else:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        main()
    #test_merge_schema()
