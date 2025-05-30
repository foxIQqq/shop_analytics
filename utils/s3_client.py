from minio import Minio
import os

def get_s3_client():
    return Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False
    )