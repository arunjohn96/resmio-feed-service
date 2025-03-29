import asyncpg
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class PostgresRepository:
    def __init__(self,db_host,db_port,db_user,db_password,db_name):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name

    @asynccontextmanager
    async def get_connection(self):
        try:
            conn = await asyncpg.connect(host=self.db_host,
                                         port=self.db_port,
                                         user=self.db_user,
                                         password=self.db_password,
                                         database=self.db_name)
            yield conn
        except Exception as e:
            raise
        finally:
            await conn.close()
            

    async def fetch_data(self, query, batch_size):
        async with self.get_connection() as conn:
            async with conn.transaction():
                try:
                    cursor = await conn.cursor(f"{query}")
                    while True:
                        # Fetch the next batch of rows
                        rows = await cursor.fetch(batch_size)                          
                        if not rows:
                            break
                        yield rows
                except Exception as e:
                    raise