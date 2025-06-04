import os
import logging
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, sum, avg, to_date, year, month, dayofmonth
from utils.s3_client import get_s3_client

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_daily_batch_job():
    logger.info("Starting daily batch job with PySpark")
    
    try:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        bucket_raw = os.getenv("S3_BUCKET_RAW", "shop-raw-data")
        bucket_stage = os.getenv("S3_BUCKET_STAGE", "shop-stage-data")
        clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        clickhouse_port = os.getenv("CLICKHOUSE_PORT", "9000")
        clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
        
        s3 = get_s3_client()
        
        if not s3.bucket_exists("analytics"):
            try:
                s3.make_bucket("analytics")
                logger.info("Bucket 'analytics' created.")
            except Exception as e:
                logger.error(f"Failed to create bucket 'analytics': {e}")
                return False
        
        try:
            findspark.init()
        except Exception as e:
            logger.warning(f"Error initializing findspark: {e}")
        
        try:
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
        
        jar_files = [
            "hadoop-aws-3.3.4.jar", 
            "aws-java-sdk-bundle-1.12.262.jar", 
            "iceberg-spark-runtime-3.4_2.12-1.4.3.jar",
            "clickhouse-jdbc-0.4.6.jar"
        ]
        
        for jar_loc in possible_jar_locations:
            if os.path.exists(jar_loc):
                for jar_file in jar_files:
                    jar_path = os.path.join(jar_loc, jar_file)
                    if os.path.exists(jar_path):
                        jar_paths.append(jar_path)
                        logger.info(f"Found JAR file: {jar_path}")
        
        if jar_paths:
            jars_config = ",".join(jar_paths)
            logger.info(f"Using JAR files: {jars_config}")
        else:
            jars_config = ""
            logger.warning("No JAR files found, PySpark may not work correctly with S3 and ClickHouse")
        
        spark = SparkSession.builder \
            .appName("DailyBatchJobAirflow") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", f"s3a://analytics/") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        if jars_config:
            spark = spark.config("spark.jars", jars_config)
            
        spark = spark.getOrCreate()
        
        logger.info("Created Spark session successfully")
        
        logger.info("Loading data from STAGE layer")
        
        purchases_path = f"s3a://{bucket_stage}/purchases/"
        try:
            purchases_df = spark.read.parquet(purchases_path)
            logger.info(f"Loaded purchases data, count: {purchases_df.count()}")
        except Exception as e:
            logger.error(f"Error loading purchases data: {e}")
            purchases_df = None
        
        products_path = f"s3a://{bucket_stage}/products/"
        try:
            products_df = spark.read.parquet(products_path)
            logger.info(f"Loaded products data, count: {products_df.count()}")
        except Exception as e:
            logger.error(f"Error loading products data: {e}")
            products_df = None
            
        customers_path = f"s3a://{bucket_stage}/customers/"
        try:
            customers_df = spark.read.parquet(customers_path)
            logger.info(f"Loaded customers data, count: {customers_df.count()}")
        except Exception as e:
            logger.error(f"Error loading customers data: {e}")
            customers_df = None
        
        if purchases_df is None and products_df is None and customers_df is None:
            logger.error("No data was loaded from STAGE layer, aborting batch job")
            return False
        
        if purchases_df is not None:
            logger.info("Creating purchase aggregations")
            
            purchases_df = purchases_df.withColumn(
                "purchase_date", to_date(col("purchased_at"))
            )
            
            daily_purchase_agg = purchases_df.groupBy(
                col("purchase_date")
            ).agg(
                count("*").alias("total_purchases"),
                sum("quantity").alias("total_items"),
                sum(col("price_at_time") * col("quantity")).alias("total_revenue"),
                avg("price_at_time").alias("avg_price")
            )
            
            monthly_purchase_agg = purchases_df.withColumn(
                "purchase_month", date_format(col("purchase_date"), "yyyy-MM")
            ).groupBy(
                col("purchase_month")
            ).agg(
                count("*").alias("total_purchases"),
                sum("quantity").alias("total_items"),
                sum(col("price_at_time") * col("quantity")).alias("total_revenue"),
                avg("price_at_time").alias("avg_price")
            )
            
            logger.info("Writing aggregations to ClickHouse")
            
            jdbc_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/analytics"
            
            try:
                daily_purchase_agg.write \
                    .format("jdbc") \
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                    .option("url", jdbc_url) \
                    .option("user", clickhouse_user) \
                    .option("password", clickhouse_password) \
                    .option("dbtable", "daily_purchase_stats") \
                    .mode("append") \
                    .save()
                logger.info("Successfully wrote daily aggregations to ClickHouse")
            except Exception as e:
                logger.error(f"Error writing daily aggregations to ClickHouse: {e}")
            
            try:
                monthly_purchase_agg.write \
                    .format("jdbc") \
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                    .option("url", jdbc_url) \
                    .option("user", clickhouse_user) \
                    .option("password", clickhouse_password) \
                    .option("dbtable", "monthly_purchase_stats") \
                    .mode("append") \
                    .save()
                logger.info("Successfully wrote monthly aggregations to ClickHouse")
            except Exception as e:
                logger.error(f"Error writing monthly aggregations to ClickHouse: {e}")
        
        if purchases_df is not None and products_df is not None:
            logger.info("Saving data to Iceberg tables")
            
            try:
                purchases_with_products = purchases_df.join(
                    products_df,
                    purchases_df.product_id == products_df.id,
                    "left"
                )
                
                purchases_with_products.writeTo("iceberg.analytics.purchase_product_facts") \
                    .partitionBy(
                        year(col("purchase_date")),
                        month(col("purchase_date")),
                        dayofmonth(col("purchase_date"))
                    ) \
                    .createOrReplace()
                
                logger.info("Successfully saved purchase_product_facts to Iceberg")
            except Exception as e:
                logger.error(f"Error saving to Iceberg: {e}")
        
        spark.stop()
        logger.info("Spark session stopped")
        
        return True
    except Exception as e:
        logger.error(f"Error in batch job processing: {e}")
        return False
        
if __name__ == "__main__":
    run_daily_batch_job() 