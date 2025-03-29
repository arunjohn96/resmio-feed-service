import asyncio
from db import PostgresRepository
from adapters import S3Adapter
from service import BatchProcessor
from config import Config
import logging

logger = logging.getLogger(__name__)


async def main():
    # Process data
    processor = BatchProcessor()
    await processor.process_batch()

if __name__ == "__main__":
    # run asyncio multithreaded
    asyncio.run(main())
    print("Hello world!")