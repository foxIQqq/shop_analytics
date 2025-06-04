import os
from minio import Minio
from api.config import settings
import logging

logger = logging.getLogger(__name__)

def get_minio_client():
    client = Minio(
        settings.MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=False
    )
    return client

def upload_to_s3(file_data, bucket_name, object_name=None):

    try:
        client = get_minio_client()
        
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created new bucket: {bucket_name}")
        
        if hasattr(file_data, 'read'):
            size = file_data.getbuffer().nbytes if hasattr(file_data, 'getbuffer') else None
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_data,
                length=size
            )
        else:
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_data,
                length=len(file_data)
            )
            
        url = f"{settings.MINIO_ENDPOINT}/{bucket_name}/{object_name or 'default'}"
        logger.info(f"Generated URL for object: {url}")
        return url
    except Exception as e:
        logger.error(f"Error working with S3: {str(e)}")
        return None 