from typing import Optional

from gigapi.services.kvstore import FileStore
from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse, Response

kv_store: Optional[FileStore] = None
router = APIRouter()

@router.post("/kv")
async def set_key_value(
        request: Request,
        key: str = Query(None, description="Key")
):
    global kv_store
    if kv_store is None:
        raise Exception("Not initialized")
    body = await request.body()
    kv_store.set(key, body)
    return JSONResponse(content={
        "status": "success",
        "message": f"ok"
    })

@router.get("/kv")
async def get_key_value(
        key: str = Query(None, description="Key")
):
    global kv_store
    if kv_store is None:
        raise Exception("Not initialized")
    value = kv_store.get(key)
    return Response(content=value)

@router.delete("/kv")
async def delete_key_value(
        key: str = Query(None, description="Key")
):
    global kv_store
    if kv_store is None:
        raise Exception("Not initialized")
    kv_store.delete(key)
    return JSONResponse(content={
        "status": "success",
        "message": f"ok"
    })