import asyncio
import os
import aioboto3
import logging

from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Adapter:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.session = aioboto3.Session(aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                                        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
                                        region_name=Config.AWS_REGION)

    async def upload_file(self, file_path, key):
        try:
            async with self.session.client('s3') as s3_client:
                with open(file_path, 'rb') as f:
                    # await s3_client.upload_fileobj(f,self.bucket_name,key)

                    # async sleep for 1 seconds
                    await asyncio.sleep(1)
            # Remove the local file after upload
            os.remove(file_path)
            logger.info(f"Uploaded {key} to S3")
            return key
        except Exception as e:
            logger.error(f"Failed to upload {key}: {str(e)}")
            raise