from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()

@router.post("/write")
async def write_to_db(request: Request):
    # Read the raw bytes from the request body
    body = await request.body()

    # Here you would process the byte array (body) as needed
    # For example, you might want to decode it, parse it, or store it directly

    # For demonstration, let's just return the length of the received data
    return JSONResponse(content={"received_bytes": len(body)})