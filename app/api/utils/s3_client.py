import os
from minio import Minio
from api.config import settings
import logging

logger = logging.getLogger(__name__)

def get_minio_client():
    """
    Создает и возвращает клиент MinIO
    """
    client = Minio(
        settings.MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=False
    )
    return client

def upload_to_s3(file_data, bucket_name, object_name=None):
    """
    Загрузка файла в S3-совместимое хранилище (MinIO)
    
    :param file_data: Данные файла
    :param bucket_name: Имя бакета
    :param object_name: Имя объекта (если None, будет сгенерировано)
    :return: URL загруженного объекта
    """
    try:
        client = get_minio_client()
        
        # Проверяем существование бакета, создаем если нет
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created new bucket: {bucket_name}")
        
        # Загружаем файл
        if hasattr(file_data, 'read'):
            # Если file_data - это файлоподобный объект
            size = file_data.getbuffer().nbytes if hasattr(file_data, 'getbuffer') else None
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_data,
                length=size
            )
        else:
            # Если file_data - это байты
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_data,
                length=len(file_data)
            )
            
        # Возвращаем URL загруженного объекта
        url = f"{settings.MINIO_ENDPOINT}/{bucket_name}/{object_name or 'default'}"
        logger.info(f"Generated URL for object: {url}")
        return url
    except Exception as e:
        logger.error(f"Error working with S3: {str(e)}")
        return None 