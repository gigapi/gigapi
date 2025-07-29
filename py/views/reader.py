import json
from typing import Optional

from fastapi import APIRouter, Request, Query, Body
from fastapi.responses import JSONResponse, Response, StreamingResponse
from services.reader import query
from pydantic import BaseModel, Field
from datetime import datetime, date

router = APIRouter()

class QueryRequest(BaseModel):
    query: str = Field(..., description="The SQL query to execute")
    db: Optional[str] = Field(None, description="The database to query (optional)")

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

def custom_json_dumps(obj):
    return json.dumps(obj, cls=CustomJSONEncoder)

@router.post("/query")
async def query_data(database: Optional[str] = Query(None, description="database to query"),
                     format: Optional[str] = Query("json", description="response format (json or ndjson"),
                     query_request: QueryRequest = Body(...)):
    if not database:
        database = query_request.db
    if not database:
        database = ""

    if format == "json":
        data = [row async for row in query(query_request.query, database)]
        return Response(content=json.dumps(data, cls=CustomJSONEncoder), media_type="application/json")
    elif format == "ndjson":
        async def stream_ndjson():
            async for row in query(query_request.query, database):
                yield custom_json_dumps(row) + "\n"

        return StreamingResponse(stream_ndjson(), media_type="application/x-ndjson")
