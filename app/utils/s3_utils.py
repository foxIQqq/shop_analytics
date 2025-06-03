import pandas as pd
import io
from utils.s3_client import get_s3_client
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def json_to_parquet_s3(data: list, bucket_name: str, object_name: str = None) -> tuple:
    """
    Преобразует массив JSON данных в формат Parquet и сохраняет в S3.
    
    Args:
        data: Список словарей (JSON данные)
        bucket_name: Имя bucket в S3
        object_name: Имя объекта в S3 (если None, будет сгенерировано автоматически)
        
    Returns:
        tuple: (успех (bool), сообщение или путь к файлу (str))
    """
    try:
        # Создаем DataFrame из JSON данных
        df = pd.DataFrame(data)
        
        # Если имя объекта не указано, генерируем его
        if object_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_name = f"bulk_upload_{timestamp}.parquet"
        
        # Преобразуем DataFrame в Parquet в памяти
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        
        # Получаем S3 клиент
        s3 = get_s3_client()
        
        # Проверяем существование bucket
        if not s3.bucket_exists(bucket_name):
            s3.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
            
        # Загружаем файл в S3
        s3.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=parquet_buffer,
            length=len(parquet_buffer.getvalue())
        )
        
        logger.info(f"Successfully uploaded {len(data)} records as Parquet to s3://{bucket_name}/{object_name}")
        return True, f"s3://{bucket_name}/{object_name}"
        
    except Exception as e:
        error_msg = f"Failed to upload data as Parquet: {str(e)}"
        logger.error(error_msg)
        return False, error_msg 