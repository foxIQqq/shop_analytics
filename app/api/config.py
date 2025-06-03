import os
from pydantic import BaseModel
from typing import Optional

class Settings(BaseModel):
    # Database
    POSTGRES_URL: str = os.getenv("POSTGRES_URL", "postgresql://admin:password@postgres:5432/shop")
    
    # Auth
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_PRODUCTS_TOPIC: str = "products-topic"
    KAFKA_PURCHASES_TOPIC: str = "purchases"
    KAFKA_SELLERS_TOPIC: str = "sellers-topic"
    KAFKA_CUSTOMERS_TOPIC: str = "customers-topic"
    
    # MinIO
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    # S3 Buckets
    S3_BUCKET_RAW: str = os.getenv("S3_BUCKET_RAW", "shop-raw-data")
    S3_BUCKET_STAGE: str = os.getenv("S3_BUCKET_STAGE", "shop-stage-data")
    S3_BUCKET_CHECKPOINT: str = os.getenv("S3_BUCKET_CHECKPOINT", "shop-checkpoints")
    
    # API
    API_V1_PREFIX: str = "/api/v1"
    RATE_LIMIT_DEFAULT: str = "100/minute"
    CORS_ORIGINS: list = ["*"]

# Создаем глобальный экземпляр настроек
settings = Settings() 