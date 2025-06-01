import os
import logging
from minio import Minio

logger = logging.getLogger(__name__)

def get_s3_client():
    """
    Создает и возвращает клиент MinIO с настройками из переменных окружения
    
    :return: Экземпляр Minio клиента
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    logger.info(f"Initialized S3 client for endpoint: {endpoint}")
    return client

def upload_to_s3(file_data, bucket_name, object_name=None):
    """
    Загрузка файла в S3-совместимое хранилище (MinIO)
    
    :param file_data: Данные файла (байты или BytesIO)
    :param bucket_name: Имя бакета
    :param object_name: Имя объекта (если None, будет сгенерировано)
    :return: URL загруженного объекта
    """
    try:
        client = get_s3_client()
        
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
        url = f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}/{bucket_name}/{object_name}"
        logger.info(f"Uploaded object to: {url}")
        return url
    except Exception as e:
        logger.error(f"Error working with S3: {str(e)}")
        return None