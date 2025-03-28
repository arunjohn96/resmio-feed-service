import asyncio
from db import PostgresRepository
from adapters import S3Adapter
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
    
    data = db_repo.fetch_data()
    async for row in data:
        print(row)
    
    x_upload = S3Adapter(Config.S3_BUCKET_NAME)
    # Upload to S3
    await x_upload.upload_file("/home/arun/Projects/resmio-feed-service/requirements.txt", "requirements.txt")
        

if __name__ == "__main__":
    asyncio.run(x())
    print("Hello world!")