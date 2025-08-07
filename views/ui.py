import zipfile
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from starlette.staticfiles import StaticFiles
from pathlib import Path
from fsspec.implementations.memory import MemoryFileSystem
from io import BytesIO
import re
from icecream import ic
import mimetypes

router = APIRouter()

memfs = MemoryFileSystem()

def remDist(file_path: str) -> str:
    return re.sub(r"^/?dist/", "", file_path)

def get_media_type(file_path: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or 'application/octet-stream'

# Extract the zip file to memory and create routes
zip_path = Path(__file__).parent / "ui.zip"
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    for zip_info in zip_ref.infolist():
        serve_filename = remDist(zip_info.filename)
        if not serve_filename:
            continue
        if zip_info.filename[-1] == '/':
            memfs.mkdir(remDist(zip_info.filename))
        else:
            with zip_ref.open(zip_info.filename) as file:
                content = file.read()
                memfs.pipe(serve_filename, content)

                # Create a route for this file
                file_path = "/" + serve_filename.lstrip("/")

                @router.get(file_path, response_class=Response)
                async def read_file(file_path=file_path):
                    content = memfs.cat(file_path.lstrip("/"))
                    media_type = get_media_type(file_path)
                    return Response(content=content, media_type=media_type)

# Special case for the root path
@router.get("/", response_class=Response)
@router.get("/ui", response_class=Response)
@router.get("/ui/{path:path}", response_class=Response)
async def read_root():
    if memfs.exists("index.html"):
        content = memfs.cat("index.html")
        return Response(content=content, media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail="index.html not found")
