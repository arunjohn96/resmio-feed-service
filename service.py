import asyncio
import logging
import json
import os

import aiofiles

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self):
        pass
    
    async def write_batch_to_file(self, batch, batch_num):
        file_path = f"batch_{batch_num}.txt"
        try:
            async with aiofiles.open(file_path, 'w') as f:
               async for row in batch:
                    await f.write("DUMMY")
            return file_path
        except Exception as e:
            logger.error(f"error {str(e)}")
            raise