import os
import io
import pandas as pd
from minio import Minio
from minio.error import S3Error

endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "shop-raw-data")

def upload_products_from_parquet(local_parquet_path: str):
    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)

    df = pd.read_parquet(local_parquet_path)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    object_key = f"products/products_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.parquet"

    client.put_object(
        bucket_name=RAW_BUCKET,
        object_name=object_key,
        data=buffer,
        length=len(buffer.getvalue()),
        content_type="application/octet-stream"
    )
    print(f"Uploaded products to s3://{RAW_BUCKET}/{object_key}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python upload_products.py <local_parquet_path>")
    else:
        upload_products_from_parquet(sys.argv[1])