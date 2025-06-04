import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from utils.s3_client import get_s3_client
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_kafka_with_spark():
    logger.info("Starting Kafka processing with PySpark")
    
    try:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        kafka_topic = os.getenv("KAFKA_TOPIC_PURCHASES", "purchases")
        bucket_raw = os.getenv("S3_BUCKET_RAW", "shop-raw-data")
        bucket_checkpoint = os.getenv("S3_BUCKET_CHECKPOINT", "shop-checkpoints")
        
        s3 = get_s3_client()
        
        if not s3.bucket_exists(bucket_raw):
            try:
                s3.make_bucket(bucket_raw)
                logger.info(f"Bucket '{bucket_raw}' created.")
            except Exception as e:
                logger.error(f"Failed to create bucket '{bucket_raw}': {e}")
                return False
                
        if not s3.bucket_exists(bucket_checkpoint):
            try:
                s3.make_bucket(bucket_checkpoint)
                logger.info(f"Bucket '{bucket_checkpoint}' created.")
            except Exception as e:
                logger.error(f"Failed to create bucket '{bucket_checkpoint}': {e}")
                return False
        
        try:
            import findspark
            findspark.init()
            import pyspark
            spark_home = pyspark.__path__[0]
            logger.info(f"Found PySpark at: {spark_home}")
        except Exception as e:
            spark_home = os.getenv("SPARK_HOME", "/home/airflow/.local/lib/python3.8/site-packages/pyspark")
            logger.warning(f"Could not automatically detect SPARK_HOME, using: {spark_home}. Error: {e}")
        
        jar_paths = []
        possible_jar_locations = [
            "/opt/airflow/jars",
            "/app/jars", 
            os.path.join(spark_home, "jars")
        ]
        
        for jar_loc in possible_jar_locations:
            if os.path.exists(jar_loc):
                for jar_file in ["hadoop-aws-3.3.4.jar", "aws-java-sdk-bundle-1.12.262.jar", 
                                "spark-sql-kafka-0-10_2.12-3.4.1.jar", "kafka-clients-3.3.1.jar"]:
                    jar_path = os.path.join(jar_loc, jar_file)
                    if os.path.exists(jar_path):
                        jar_paths.append(jar_path)
                        logger.info(f"Found JAR file: {jar_path}")
        
        if jar_paths:
            jars_config = ",".join(jar_paths)
            logger.info(f"Using JAR files: {jars_config}")
        else:
            jars_config = ""
            logger.warning("No JAR files found, PySpark may not work correctly with S3 and Kafka")
        
        spark = SparkSession.builder \
            .appName("KafkaToS3Airflow") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
        if jars_config:
            spark = spark.config("spark.jars", jars_config)
            
        spark = spark.getOrCreate()
            
        logger.info("Created Spark session successfully")
        
        purchase_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("seller_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price_at_time", DoubleType(), True),
            StructField("purchased_at", TimestampType(), True)
        ])
        
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
        
        logger.info(f"Writing stream data to {raw_path}")
        
        query = df_with_date.writeStream \
            .format("parquet") \
            .option("path", raw_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("purchase_date") \
            .trigger(once=True) \
            .outputMode("append") \
            .start()
            
        query.awaitTermination()
        logger.info("Stream processing completed successfully")
        
        spark.stop()
        logger.info("Spark session stopped")
        
        return True
    except Exception as e:
        logger.error(f"Error in Spark Kafka processing: {e}")
        return False
        
if __name__ == "__main__":
    process_kafka_with_spark() 