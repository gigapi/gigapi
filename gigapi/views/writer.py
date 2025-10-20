from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse
from gigapi.services.writer import write_lineproto

router = APIRouter()
# TODO: support
# Path: "/gigapi/write/{db}",

@router.post("/gigapi/write")
@router.post("/write")
@router.post("/api/v2/write")
@router.post("/api/v3/write_lp")
async def write_to_db(request: Request, db: str = Query("default", description="The table to write to")):
    body = await request.body()
    await write_lineproto(body, db)
    return JSONResponse(content={
        "status": "success",
        "message": f"ok"
    })
