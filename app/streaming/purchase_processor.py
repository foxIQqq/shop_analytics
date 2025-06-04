import os
import json
import time
import logging
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, expr, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "purchases")

ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG_NAME")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "s3a://analytics/checkpoints/purchase_processor")


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PurchaseProcessor")
        .config("spark.sql.catalog.iceberg", ICEBERG_CATALOG)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "iceberg")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.streaming.kafka.consumer.cache.enabled", "true")
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "300s")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_kafka_stream(spark: SparkSession) -> DataFrame:

    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    
    kafka_raw_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .option("kafka.max.poll.records", "500")
        .option("maxOffsetsPerTrigger", "10000")
        .option("failOnDataLoss", "false")
        .load()
    )

    purchase_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("seller_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_at_time", DoubleType(), True),
        StructField("purchased_at", TimestampType(), True)
    ])

    purchases_df = (
        kafka_raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), purchase_schema).alias("data"),
            col("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
        .withColumn(
            "purchased_at", 
            expr("CASE WHEN purchased_at IS NULL THEN kafka_timestamp ELSE purchased_at END")
        )
        .drop("kafka_timestamp")
    )

    return purchases_df


def read_static_dictionaries(spark: SparkSession) -> tuple[DataFrame, DataFrame]:

    logger.info("Reading static dictionaries from Iceberg catalog")
    
    products_table = f"{ICEBERG_CATALOG}.default.products"
    customers_table = f"{ICEBERG_CATALOG}.default.customers"

    try:
        products_df = spark.read.format("iceberg").load(products_table)
        logger.info(f"Successfully loaded products table with {products_df.count()} rows")
        
        customers_df = spark.read.format("iceberg").load(customers_table)
        logger.info(f"Successfully loaded customers table with {customers_df.count()} rows")
        
        products_df = products_df.cache()
        customers_df = customers_df.cache()
        
        return products_df, customers_df
    
    except Exception as e:
        logger.error(f"Error reading static dictionaries: {str(e)}")
        raise


def enrich_stream(
    purchases_df: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame
) -> DataFrame:

    logger.info("Enriching purchase stream with product and customer data")
    
    try:
        enriched = (
            purchases_df
            .join(
                products_df, 
                on=purchases_df["product_id"] == products_df["product_id"], 
                how="left"
            )
            .join(
                customers_df, 
                on=purchases_df["customer_id"] == customers_df["customer_id"], 
                how="left"
            )
            .withColumn(
                "product_category", 
                expr("CASE WHEN product_category IS NULL THEN 'Unknown' ELSE product_category END")
            )
        )
        
        return enriched
    except Exception as e:
        logger.error(f"Error enriching stream: {str(e)}")
        return purchases_df.withColumn("product_category", lit("Unknown"))


def build_windowed_aggregates(enriched_df: DataFrame) -> DataFrame:

    logger.info("Building windowed aggregates")
    
    agg_df = (
        enriched_df
        .withWatermark("purchased_at", "10 minutes")
        .groupBy(
            window(col("purchased_at"), "1 hour", "10 minutes").alias("time_window"),
            col("product_category")
        )
        .agg(
            {"*": "count", "price_at_time": "sum", "quantity": "sum"}
        )
        .withColumnRenamed("count(1)", "cnt")
        .withColumnRenamed("sum(price_at_time)", "total_amount")
        .withColumnRenamed("sum(quantity)", "total_quantity")
        .select(
            col("product_category"),
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("cnt"),
            col("total_amount"),
            col("total_quantity"),
            current_timestamp().alias("processed_at")
        )
    )
    return agg_df


def write_to_clickhouse(batch_df: DataFrame, batch_id: int) -> None:

    start_time = time.time()
    
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id}: No data to process")
        return

    try:
        rows = batch_df.collect()
        data_tuples = [
            (
                row["product_category"],
                row["window_start"],
                row["window_end"],
                row["cnt"],
                row["total_amount"],
            ) for row in rows
        ]

        from utils.ch_client import get_clickhouse_client, bulk_insert

        client = get_clickhouse_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database="analytics"
        )

        columns = ["product_category", "window_start", "window_end", "cnt", "total_amount"]
        inserted_rows = bulk_insert(
            client=client,
            table="analytics.purchases_hourly",
            columns=columns,
            data=data_tuples,
            batch_size=5000
        )
        
        elapsed_time = time.time() - start_time
        logger.info(f"Batch {batch_id}: Successfully inserted {inserted_rows} out of {len(data_tuples)} rows "
                   f"in {elapsed_time:.2f} seconds")
    
    except Exception as e:
        logger.error(f"Batch {batch_id}: Unexpected error in write_to_clickhouse: {str(e)}")


def main():

    logger.info("Starting Purchase Processor")
    
    try:
        spark = create_spark_session()

        products_df, customers_df = read_static_dictionaries(spark)

        purchases_df = get_kafka_stream(spark)

        enriched_df = enrich_stream(purchases_df, products_df, customers_df)

        agg_df = build_windowed_aggregates(enriched_df)

        query = (
            agg_df.writeStream
            .outputMode("update")
            .foreachBatch(write_to_clickhouse)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime="10 minutes")
            .start()
        )

        logger.info("Purchase Processor started successfully")
        
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()