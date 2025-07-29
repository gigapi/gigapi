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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    asyncio.create_task(start_background_tasks())
    yield

app = FastAPI(lifespan=lifespan)


app.add_middleware(middlewares.ErrorHandlerMiddleware)

app.include_router(reader.router)
app.include_router(writer.router)
app.include_router(ui.router)

async def start_background_tasks():
    # Start the run function as a background task
    asyncio.create_task(run())

def run_airport_server():
    print("START")
    writer_server.run()
    print("END")

def main():
    t = Thread(target=run_airport_server)
    t.start()
    loop = asyncio.get_event_loop()
    config = uvicorn.Config("__main__:app", host=settings.http.host, port=settings.http.port, loop=loop, reload=True)
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())

if __name__ == "__main__":
    main()
