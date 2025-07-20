from dotenv import load_dotenv
load_dotenv()

import uvicorn
from fastapi import FastAPI
from views import reader, writer, middlewares, ui

app = FastAPI()

app.add_middleware(middlewares.ErrorHandlerMiddleware)

app.include_router(reader.router)
app.include_router(writer.router)
app.include_router(ui.router)

if __name__ == "__main__":
    uvicorn.run(
        "__main__:app",
        host="0.0.0.0",
        port=8000,
        reload=True  # Enable auto-reload during development
    )