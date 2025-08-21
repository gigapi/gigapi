import json
import csv
import io
from typing import Optional

from fastapi import APIRouter, Request, Query, Body
from fastapi.responses import JSONResponse, Response, StreamingResponse
from services.reader import query
from pydantic import BaseModel, Field
from datetime import datetime, date
from asyncio import Lock

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

def format_csv_data(data, delimiter=','):
    """Format data as CSV with proper header and row handling"""
    if not data:
        return ""
    
    # Get column names from the first row
    columns = list(data[0].keys())
    
    # Create CSV string
    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimiter)
    
    # Write header
    writer.writerow(columns)
    
    # Write data rows
    for row in data:
        writer.writerow([row.get(col, '') for col in columns])
    
    return output.getvalue()

async def stream_csv_data(data_generator, delimiter=','):
    """Stream CSV data with proper header and row handling"""
    first_row = True
    columns = None
    
    async for row in data_generator():
        if first_row:
            # Get column names from the first row
            columns = list(row.keys())
            # Yield header
            output = io.StringIO()
            writer = csv.writer(output, delimiter=delimiter)
            writer.writerow(columns)
            yield output.getvalue()
            first_row = False
        
        # Yield data row
        output = io.StringIO()
        writer = csv.writer(output, delimiter=delimiter)
        writer.writerow([row.get(col, '') for col in columns])
        yield output.getvalue()

@router.post("/query")
@router.post("/api/v3/query_sql")
async def query_data(db: Optional[str] = Query(None, description="database to query"),
                     format: Optional[str] = Query("json", description="response format (json, ndjson, jsonl, csv, or tsv)"),
                     query_request: QueryRequest = Body(...)):
    database = db
    if not database:
        database = query_request.db
    if not database:
        database = ""
    # Convert format to lowercase for case-insensitive comparison
    format_lower = format.lower() if format else "json"
    if format_lower == "json":
        res = await query(query_request.query, database)
        data = {"results": [row async for row in res()]}
        return Response(headers={"Content-Type": "application/json"}, content=json.dumps(data, cls=CustomJSONEncoder))
    elif format_lower in ["ndjson", "jsonl"]:
        res = await query(query_request.query, database)
        async def stream_ndjson():
            async for row in res():
                yield custom_json_dumps(row) + "\n"
        return StreamingResponse(stream_ndjson(), media_type="application/x-ndjson")
    elif format_lower == "csv":
        res = await query(query_request.query, database)
        data = [row async for row in res()]
        csv_content = format_csv_data(data, delimiter=',')
        return Response(content=csv_content, media_type="text/csv")
    elif format_lower == "tsv":
        res = await query(query_request.query, database)
        data = [row async for row in res()]
        tsv_content = format_csv_data(data, delimiter='\t')
        return Response(content=tsv_content, media_type="text/tab-separated-values")
    else:
        # Default to JSON for unsupported formats
        res = await query(query_request.query, database)
        data = [row async for row in res()]
        return Response(content=json.dumps(data, cls=CustomJSONEncoder))

