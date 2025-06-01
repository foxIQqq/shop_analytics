from pyspark.sql import SparkSession
from product_etl import process_products
from customer_etl import process_customers
from purchases_etl import process_purchases
from aggregations import run_aggregations
from utils.s3_client import get_s3_client  # Импортируем MinIO-клиент
import os

def ensure_analytics_bucket():
    s3 = get_s3_client()
    bucket_analytics = "analytics"

    if not s3.bucket_exists(bucket_analytics):
        try:
            s3.make_bucket(bucket_analytics)
            print(f"Bucket '{bucket_analytics}' created.")
        except Exception as e:
            print(f"Failed to create bucket '{bucket_analytics}': {e}")
            exit(1)  # Завершаем, если не удалось создать бакет

def main():
    ensure_analytics_bucket()

    spark = SparkSession.builder \
        .appName("DailyShopJob") \
        .config("spark.sql.catalog.analytics", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.analytics.type", "hadoop") \
        .config("spark.sql.catalog.analytics.warehouse", "s3a://analytics/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    process_products(spark)
    process_customers(spark)
    process_purchases(spark)
    run_aggregations(spark)

    spark.stop()
    print("✅ DAILY ETL JOB COMPLETE")

if __name__ == "__main__":
    main()