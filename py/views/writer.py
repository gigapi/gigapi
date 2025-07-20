from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse
from services.writer import write_json

router = APIRouter()

@router.post("/write")
async def write_to_db(request: Request, table: str = Query(..., description="The table to write to")):
    body = await request.body()
    await write_json(body, table)
    return JSONResponse(content={
        "status": "success",
        "message": f"ok"
    })
