import asyncio
import gzip
import logging
import json
import os
import time

import aiofiles

from adapters import S3Adapter
from config import Config
from db import PostgresRepository

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self):
        self.db_repo = PostgresRepository(Config.DB_HOST,
                                 Config.DB_PORT,
                                 Config.DB_USER,
                                 Config.DB_PASSWORD,
                                 Config.DB_NAME)
        self.batch_size = Config.BATCH_SIZE
        self.s3_adapter = S3Adapter(Config.S3_BUCKET_NAME)
        self.query = Config.FEED_QUERY
        self.timestamp = int(time.time())
        self.file_list = []
    
    async def write_batch_to_file(self, batch, batch_num, timestamp=0):
        file_path = f"facility_feed_{timestamp + batch_num}.json.gz"
        try:
            data = {"data": []}
            for record in batch:
                data["data"].append({
                        "entity_id": f"dining-{record['id']}",
                        "name": record["name"],
                        "telephone": record["phone"],
                        "url": record["url"],
                        "location": {
                            "latitude": record["latitude"],
                            "longitude": record["longitude"],
                            "address": {
                                "country": record["country"],
                                "locality": record["locality"],
                                "region": record["region"],
                                "postal_code": record["postal_code"],
                                "street_address": record["street_address"]
                            }
                        }
                    })
            # Convert to JSON bytes
            json_data = json.dumps(data).encode('utf-8')
            async with aiofiles.open(file_path, 'wb') as f:
                with gzip.GzipFile(fileobj=f, mode="wb") as gz_file:
                    await f.write(json_data)
            return file_path
        except Exception as e:
            logger.error(f"error {str(e)}")
            raise
    
    async def generate_metadata_file(self):
        metadata = {
            "generation_timestamp": self.timestamp,
            "name": "reservewithgoogle.entity",
            "data_file": self.file_list
        }
        with open("metadata.json", "w") as f:
            json.dump(metadata, f)

    async def process_batch(self):
        batch_num = 0
        tasks = []
        # Fetch data in batches
        async for data in self.db_repo.fetch_data(self.query,self.batch_size):
            logger.info(f"Processing batch {batch_num}")
            
            # Create feed file
            batch_upload_file = await self.write_batch_to_file(data, batch_num,timestamp=self.timestamp)
            
            # Upload feed file to S3
            tasks.append(self.s3_adapter.upload_file(batch_upload_file, batch_upload_file))

            # Update metadata file list
            self.file_list.append(batch_upload_file)
            
            batch_num += 1
        
        await asyncio.gather(*tasks)
        
        # Generate metadata file
        await self.generate_metadata_file()
        
        # Upload metadata file to S3
        await self.s3_adapter.upload_file("metadata.json", "metadata.json")


