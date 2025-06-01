# file: data_ingestion/postgres_to_s3.py

import os
import io
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.s3_client import get_s3_client

# 1) Загружаем переменные окружения из .env (если требуется).
#    Обычно Docker передаёт .env в окружение автоматически,
#    но если вы используете python-dotenv, можно явно:
load_dotenv()

# 2) Настройки PostgreSQL (проверяем, что эти переменные заданы в .env):
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB", "shop")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "password")

# 3) Имя S3-бакета для raw-данных (MinIO)
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "shop-raw-data")

# 4) Инициируем MinIO-клиент (вернёт объект класса Minio)
s3 = get_s3_client()


def export_customers():
    """
    Экспортирует все записи из PostgreSQL таблицы customers в Parquet
    и загружает полученный файл в MinIO под ключом:
    customers/year=YYYY/month=MM/customers_YYYY_MM.parquet
    """
    # 5) Формируем строку подключения к PostgreSQL
    conn_info = (
        f"host={PG_HOST} "
        f"port={PG_PORT} "
        f"dbname={PG_DB} "
        f"user={PG_USER} "
        f"password={PG_PASS}"
    )
    try:
        conn = psycopg2.connect(conn_info)
    except Exception as e:
        print("Failed to connect to Postgres:", e)
        return

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers;")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        # 6) Преобразуем в Pandas DataFrame
        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            print("No customers found in Postgres, skipping upload.")
            return

        # 7) Определяем текущий год и месяц (можно заменить на анализ created_at, но сейчас — просто текущая дата)
        now = datetime.now()
        year = now.year
        month = now.month

        # 8) Сериализуем DF в Parquet в памяти
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps="ms")
        buffer.seek(0)

        buckets = [b.name for b in s3.list_buckets()]
        if S3_BUCKET_RAW not in buckets:
            try:
                s3.make_bucket(bucket_name=S3_BUCKET_RAW)
                print(f"Bucket '{S3_BUCKET_RAW}' created.")
            except Exception as e:
                print(f"Error creating bucket {S3_BUCKET_RAW}:", e)
                return

        # 9) Собираем ключ (path) в бакете
        key = f"customers/year={year}/month={month}/customers_{year}_{month}.parquet"

        # 10) Загружаем в MinIO
        try:
            s3.put_object(
                bucket_name=S3_BUCKET_RAW,
                object_name=key,
                data=buffer,
                length=len(buffer.getvalue()),
                content_type="application/octet-stream"
            )
            print(f"Uploaded customers to s3://{S3_BUCKET_RAW}/{key}")
        except Exception as err:
            print("Failed to upload to S3:", err)

    except Exception as e:
        print("Error querying Postgres:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    export_customers()