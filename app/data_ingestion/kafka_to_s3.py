import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

from utils.s3_client import get_s3_client

def start_streaming():
    s3 = get_s3_client()
    bucket_raw = os.getenv("S3_BUCKET_RAW", "shop-raw-data")
    bucket_checkpoint = os.getenv("S3_BUCKET_CHECKPOINT", "shop-checkpoints")

    if not s3.bucket_exists(bucket_raw):
        try:
            s3.make_bucket(bucket_raw)
            print(f"Bucket '{bucket_raw}' created.")
        except Exception as e:
            print(f"Failed to create bucket '{bucket_raw}': {e}")
            return

    if not s3.bucket_exists(bucket_checkpoint):
        try:
            s3.make_bucket(bucket_checkpoint)
            print(f"Bucket '{bucket_checkpoint}' created.")
        except Exception as e:
            print(f"Failed to create bucket '{bucket_checkpoint}': {e}")
            return

    spark = SparkSession.builder \
        .appName("KafkaToS3") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    purchase_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("seller_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_at_time", DoubleType(), True),
        StructField("purchased_at", TimestampType(), True)
    ])


    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC_PURCHASES", "purchases")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), purchase_schema).alias("data")) \
        .select("data.*")
    
    df_with_date = df_parsed.withColumn("purchase_date", to_date(col("purchased_at")))

    raw_path = f"s3a://{bucket_raw}/purchases/"
    checkpoint_path = f"s3a://{bucket_checkpoint}/purchases/"

    query = df_with_date.writeStream \
        .format("parquet") \
        .option("path", raw_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("purchase_date") \
        .trigger(once=True) \
        .outputMode("append") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming gracefully...")
        query.stop()
    finally:
        spark.stop()

def main():
    try:
        required_env_vars = [
            "MINIO_ENDPOINT", 
            "MINIO_ROOT_USER", 
            "MINIO_ROOT_PASSWORD",
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC_PURCHASES",
            "S3_BUCKET_RAW",
            "S3_BUCKET_CHECKPOINT"
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            print(f"Warning: Missing environment variables: {', '.join(missing_vars)}")
            print("Using default values for missing variables")
        
        start_streaming()
        return True
    except Exception as e:
        print(f"Error in Kafka to S3 processing: {e}")
        return False

if __name__ == "__main__":
    main()