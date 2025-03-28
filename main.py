import asyncio
from db import PostgresRepository
from adapters import S3Adapter
from service import BatchProcessor
from config import Config
import logging

logger = logging.getLogger(__name__)


async def x():
    # Initial setup
    db_repo = PostgresRepository(Config.DB_HOST,
                                 Config.DB_PORT,
                                 Config.DB_USER,
                                 Config.DB_PASSWORD,
                                 Config.DB_NAME)
    

    for batch_num in range(1,10):
        data = db_repo.fetch_data()
        # Process data
        logger.info(f"Processing batch {batch_num}")
        processor = BatchProcessor()
        x_upload_file = await processor.write_batch_to_file(data, batch_num)

        # Upload to S3
        logger.info(f"Uploading batch {batch_num}")
        x_upload = S3Adapter(Config.S3_BUCKET_NAME)
        await x_upload.upload_file(x_upload_file, x_upload_file)
    # # Process data
    # processor = BatchProcessor()
    # x_upload_file = await processor.write_batch_to_file(data, 1)

    # # Upload to S3
    # x_upload = S3Adapter(Config.S3_BUCKET_NAME)
    # await x_upload.upload_file(x_upload_file, x_upload_file)
        

if __name__ == "__main__":
    asyncio.run(x())
    print("Hello world!")