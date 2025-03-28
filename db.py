import asyncpg
import logging

from config import Config

logger = logging.getLogger(__name__)

class PostgresRepository:
    def __init__(self,db_host,db_port,db_user,db_password,db_name):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name


    async def get_connection(self):
        try:
            conn = await asyncpg.connect(host=self.db_host,
                                         port=self.db_port,
                                         user=self.db_user,
                                         password=self.db_password,
                                         database=self.db_name)
            return conn
        except Exception as e:
            raise
        finally:
            # await conn.close()
            pass

    async def fetch_data(self):
        conn = await self.get_connection()
        async with conn.transaction():
            try:
                cursor = await conn.cursor(f"SELECT * FROM dummy")
                rows = await cursor.fetch(100)                          
                return rows
            except Exception as e:
                raise