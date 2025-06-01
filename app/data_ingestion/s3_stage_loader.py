# data_ingestion/s3_stage_loader.py

import os
import io
import pyarrow.parquet as pq
from minio.error import S3Error
from utils.s3_client import get_s3_client

# Попытка импортировать CopySource из того места, где он есть в текущей версии minio-py:
try:
    # В большинстве версий minio-py CopySource находится именно здесь:
    from minio.commonconfig import CopySource
except ImportError:
    # Если в вашей версии CopySource лежит в minio.api, то этот импорт сработает:
    try:
        from minio.api import CopySource
    except ImportError:
        # Если класс CopySource вообще отсутствует, придётся использовать «посредник» —
        # сначала скачивать объект, а затем загружать заново.
        CopySource = None

# 1) Настройки: имена raw- и stage-бакетов
RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "shop-raw-data")
STAGE_BUCKET = os.getenv("S3_BUCKET_STAGE", "shop-stage-data")

# 2) Инициализируем MinIO-клиент
client = get_s3_client()

def bucket_exists_or_create(bucket_name: str):
    """
    Проверяем: если бакет не существует, создаём его.
    """
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")

def list_parquet_objects(bucket_name: str, prefix: str):
    """
    Возвращает список object_name в bucket_name с данным prefix и суффиксом '.parquet'.
    """
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]

def copy_with_copysource(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str):
    """
    Копируем объект, используя CopySource (когда он доступен в библиотеке).
    """
    copy_src = CopySource(src_bucket, src_key)
    try:
        client.copy_object(
            dst_bucket,   # бакет-назначение
            dst_key,      # ключ в бакете-назначении
            copy_src      # экземпляр CopySource
        )
        print(f"Copied {src_bucket}/{src_key} → {dst_bucket}/{dst_key}")
    except S3Error as err:
        print(f"Failed to copy {src_bucket}/{src_key} via CopySource: {err}")

def copy_by_download_upload(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str):
    """
    Если CopySource недоступен (CopySource == None), скачиваем объект локально в память
    и сразу же грузим в назначение. Не самый оптимальный путь, но работает всегда.
    """
    try:
        response = client.get_object(src_bucket, src_key)
        data = response.read()
        response.close()
        response.release_conn()

        client.put_object(
            bucket_name=dst_bucket,
            object_name=dst_key,
            data=bytes(data),
            length=len(data),
            content_type="application/octet-stream"
        )
        print(f"Downloaded and re-uploaded {src_bucket}/{src_key} → {dst_bucket}/{dst_key}")
    except S3Error as err:
        print(f"Failed to download-and-upload {src_bucket}/{src_key}: {err}")

def copy_object(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str):
    """
    Основная обёртка для копирования:
    — если CopySource доступен, вызываем copy_with_copysource;
    — иначе — скачиваем и загружаем заново.
    """
    if CopySource is not None:
        copy_with_copysource(src_bucket, src_key, dst_bucket, dst_key)
    else:
        copy_by_download_upload(src_bucket, src_key, dst_bucket, dst_key)

def validate_parquet(bucket_name: str, object_name: str) -> bool:
    """
    Простая проверка: объект должен существовать и быть непустым.
    """
    try:
        stat = client.stat_object(bucket_name, object_name)
        if stat.size > 0:
            return True
        else:
            print(f"Parquet file is empty: {bucket_name}/{object_name}")
            return False
    except S3Error as err:
        print(f"Error accessing {bucket_name}/{object_name}: {err}")
        return False

# Ожидаемые колонки для каждой категории данных
EXPECTED_COLUMNS = {
    "customers/": {"customer_id", "first_name", "last_name", "email", "created_at"},
    "purchases/": {"customer_id", "product_id", "seller_id", "quantity","price_at_time", "purchased_at"},
    "products/":  {"product_id", "product_name", "category", "price", "stock_quantity"},
}

def validate_parquet_schema(bucket_name: str, object_name: str, prefix: str) -> bool:
    """
    Скачивает Parquet-файл в память, проверяет наличие ожидаемых колонок.
    Если файл нельзя прочитать или пропущены колонки — возвращает False.
    """
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        parquet_bytes = io.BytesIO(data)

        # Читаем схему через PyArrow
        parquet_file = pq.ParquetFile(parquet_bytes)
        schema_cols = set(parquet_file.schema.names)

        expected = EXPECTED_COLUMNS.get(prefix)
        if expected is None:
            print(f"No expected schema defined for prefix '{prefix}', skipping schema check.")
            return True

        missing = expected - schema_cols
        if missing:
            print(f"Schema validation failed for {bucket_name}/{object_name}: missing columns {missing}")
            return False

        return True

    except Exception as e:
        print(f"Error reading Parquet schema for {bucket_name}/{object_name}: {e}")
        return False

def main():
    # 1) Убедимся, что бакет Stage существует
    bucket_exists_or_create(STAGE_BUCKET)

    # 2) Набор «директорий» (префиксов) в raw-слое, которые надо скопировать
    prefixes = ["customers/", "purchases/", "products/"]

    for prefix in prefixes:
        parquet_keys = list_parquet_objects(RAW_BUCKET, prefix)
        if not parquet_keys:
            print(f"No parquet files found under raw/{prefix}")
        for key in parquet_keys:
            # 3) Базовая проверка Parquet (непустой файл)
            if not validate_parquet(RAW_BUCKET, key):
                print(f"Skipping invalid parquet: {RAW_BUCKET}/{key}")
                continue

            # 4) Проверка схемы
            if not validate_parquet_schema(RAW_BUCKET, key, prefix):
                print(f"Skipping schema-mismatched parquet: {RAW_BUCKET}/{key}")
                continue

            # 5) Если всё хорошо — копируем
            dst_key = key  # сохраняем ту же структуру папок
            copy_object(RAW_BUCKET, key, STAGE_BUCKET, dst_key)

if __name__ == "__main__":
    main()