from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from gigapi.utils.errors import GigapiException

class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
            return response
        except GigapiException as e:
            return JSONResponse(
                status_code=e.code,
                content={
                    "status": "error",
                    "message": e.message
                }
            )
        except Exception as e:
            # Log the error here if needed
            print(f"An error occurred: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": str(e)
                }
            )