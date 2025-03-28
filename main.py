import asyncio
from db import PostgresRepository
from config import Config
import logging
import asyncpg

logger = logging.getLogger(__name__)


async def x():
    # Initial setup
    db_repo = PostgresRepository(Config.DB_HOST,
                                 Config.DB_PORT,
                                 Config.DB_USER,
                                 Config.DB_PASSWORD,
                                 Config.DB_NAME)
    
    data = await db_repo.fetch_data()
    for row in data:
        print(row)
        

if __name__ == "__main__":
    asyncio.run(x())
    print("Hello world!")