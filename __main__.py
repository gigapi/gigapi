from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()

import uvicorn
from fastapi import FastAPI
from views import reader, writer, middlewares, ui
from services.merge import run
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



app.add_middleware(middlewares.ErrorHandlerMiddleware)

app.include_router(reader.router)
app.include_router(writer.router)
app.include_router(ui.router)

shutdown_event = asyncio.Event()

def signal_handler(signum, frame):
    print("Received shutdown signal")
    writer_server.shutdown()

async def start_background_tasks():
    # Start the run function as a background task
    asyncio.create_task(run())

def run_airport_server():
    print("START")
    writer_server.run("grpc://127.0.0.1:60001", settings.gigapi.root)
    print("END")

def main():
    t = Thread(target=run_airport_server)
    t.start()
    loop = asyncio.get_event_loop()
    config = uvicorn.Config("__main__:app", host=settings.http.host, port=settings.http.port, loop=loop, reload=True)
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
    #test_merge_schema()
