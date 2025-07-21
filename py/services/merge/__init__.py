from config import settings
from services.merge.ducklake import DucklakeMergeService
import asyncio

import logging
import traceback

# Set up logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

merge_service = None

async def run():
    global merge_service
    if settings.gigapi.metadata.type == "ducklake":
        print("STARTING MERGE")
        merge_service = DucklakeMergeService()
        while True:
            print("MERGE")
            try:
                await merge_service.do_merge()
            except Exception as e:
                logger.error(e, exc_info=True)
            finally:
                await asyncio.sleep(10)
