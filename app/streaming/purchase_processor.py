# ----------------------------------------------------------------------
# Основной Spark Structured Streaming job, который:
# 1) Читает события покупок из Kafka (топик: purchases),
# 2) Десериализует JSON,
# 3) Обогащает справочниками из Iceberg (продукты, клиенты),
# 4) Делает оконную агрегацию (количество покупок и сумма по каждой
#    категории товаров за последние 1 час каждые 10 минут),
# 5) Записывает результаты в ClickHouse (таблица analytics.purchases_hourly).
# ----------------------------------------------------------------------

import os
import json
import time
import logging
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, expr, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# Параметры из окружения
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
    """
    Создаёт SparkSession с настройками для работы с Iceberg (через S3) и 
    Spark Structured Streaming + Kafka.
    """
    spark = (
        SparkSession.builder
        .appName("PurchaseProcessor")
        # Подключаем Iceberg Catalog (в .env должен быть ICEBERG_CATALOG_NAME)
        .config("spark.sql.catalog.iceberg", ICEBERG_CATALOG)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "iceberg")
        # Настройки для S3 (MinIO/AWS). 
        # Пример: endpoint = http://localhost:9000 для локального MinIO.
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Оптимизации для Spark Streaming
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.streaming.kafka.consumer.cache.enabled", "true")
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "300s")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true")
        # Настройки памяти и параллелизма
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    
    # Установка уровня логирования
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Читает поток из Kafka, десериализует значение (JSON) и отдаёт DataFrame со 
    следующими колонками:
      customer_id, product_id, seller_id, quantity, price_at_time, purchased_at
    """
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    
    # 1) Читаем «сырая» структура c колонками: key, value (байты), topic, partition, offset, timestamp, timestampType
    kafka_raw_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # Настраиваем десериализацию ключей/значений в String:
        .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        # Настройки для оптимизации чтения из Kafka
        .option("kafka.max.poll.records", "500")
        .option("maxOffsetsPerTrigger", "10000")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Определяем схему JSON для поля value - адаптировано для формата данных в проекте
    purchase_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("seller_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_at_time", DoubleType(), True),
        StructField("purchased_at", TimestampType(), True)
    ])

    # 3) Конвертируем value (binary) → строка → JSON → колонки
    purchases_df = (
        kafka_raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), purchase_schema).alias("data"),
            col("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
        # Добавляем обработку возможных проблем с временем покупки
        .withColumn(
            "purchased_at", 
            expr("CASE WHEN purchased_at IS NULL THEN kafka_timestamp ELSE purchased_at END")
        )
        .drop("kafka_timestamp")
    )

    return purchases_df


def read_static_dictionaries(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """
    Читает справочники (статические данные) из Iceberg:
      - Таблица товаров: iceberg_catalog.products
      - Таблица клиентов: iceberg_catalog.customers
    Возвращает два DataFrame: products_df, customers_df.
    """
    logger.info("Reading static dictionaries from Iceberg catalog")
    
    # Для Iceberg путь вида: "iceberg_catalog.db_name.table_name"
    # Предположим, у нас нет разделения на БД, и каталоге только одна БД: default.
    # Тогда:
    products_table = f"{ICEBERG_CATALOG}.default.products"
    customers_table = f"{ICEBERG_CATALOG}.default.customers"

    try:
        products_df = spark.read.format("iceberg").load(products_table)
        logger.info(f"Successfully loaded products table with {products_df.count()} rows")
        
        customers_df = spark.read.format("iceberg").load(customers_table)
        logger.info(f"Successfully loaded customers table with {customers_df.count()} rows")
        
        # Кэшируем справочники для оптимизации производительности
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
    """
    Обогащает стрим purchases_df статическими данными из products_df и customers_df,
    выполняя Stream–Static Join:
      1) По product_id присоединяет name, category, price (и др. атрибуты товара),
      2) По customer_id присоединяет имя, регион, сегмент (и др. атрибуты клиента).
    Возвращает DataFrame enriched_df со всеми колонками.
    """
    logger.info("Enriching purchase stream with product and customer data")
    
    # Предполагаем, что в products_df колонки: product_id, product_name, product_category, product_price, ...
    # И в customers_df колонки: customer_id, customer_name, customer_region, customer_segment, ...
    try:
        # Добавляем обработку случаев, когда продукт или клиент не найдены
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
            # Обработка случаев, когда категория товара отсутствует
            .withColumn(
                "product_category", 
                expr("CASE WHEN product_category IS NULL THEN 'Unknown' ELSE product_category END")
            )
        )
        
        return enriched
    except Exception as e:
        logger.error(f"Error enriching stream: {str(e)}")
        # В случае ошибки возвращаем исходный DataFrame с добавленной колонкой product_category
        return purchases_df.withColumn("product_category", lit("Unknown"))


def build_windowed_aggregates(enriched_df: DataFrame) -> DataFrame:
    """
    Строит оконную агрегацию по последнему часу, «каждые 10 минут»:
      - Считаем количество покупок (cnt) и сумму amount (total_amount)
        по каждой комбинации (window_start, window_end, product_category).
    Возвращает DataFrame agg_df с колонками:
      product_category, window_start, window_end, cnt, total_amount
    """
    logger.info("Building windowed aggregates")
    
    agg_df = (
        enriched_df
        # Указываем, что события могут приходить «опозданно» до 10 минут
        .withWatermark("purchased_at", "10 minutes")
        .groupBy(
            window(col("purchased_at"), "1 hour", "10 minutes").alias("time_window"),
            col("product_category")
        )
        .agg(
            {"*": "count", "price_at_time": "sum", "quantity": "sum"}
        )
        # Переименовываем колонки, полученные после агрегации
        .withColumnRenamed("count(1)", "cnt")
        .withColumnRenamed("sum(price_at_time)", "total_amount")
        .withColumnRenamed("sum(quantity)", "total_quantity")
        # Разворачиваем структуру window в две колонки: window_start, window_end
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
    """
    foreachBatch-функция. На каждый микробатч (batch DataFrame) Spark вызывает
    эту функцию, передавая:
      - batch_df: DataFrame для данного "микробатча" (с уже подсчитанными)
      - batch_id: уникальный идентификатор пакета
    Мы преобразуем batch_df → список кортежей и делаем bulk INSERT в ClickHouse.
    """
    start_time = time.time()
    
    # Нулевая длина (нет данных) — выходим
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id}: No data to process")
        return

    try:
        # Собираем все строки в память (помним, что batch ≈ «раз в 10 минут данные за последний час»).
        rows = batch_df.collect()  # list of Row(product_category, window_start, window_end, cnt, total_amount)
        # Преобразуем в list of tuples
        data_tuples = [
            (
                row["product_category"],
                row["window_start"],
                row["window_end"],
                row["cnt"],
                row["total_amount"],
            ) for row in rows
        ]

        # Импортируем и создаём клиент ClickHouse
        from utils.ch_client import get_clickhouse_client, bulk_insert

        client = get_clickhouse_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database="analytics"
        )

        # Используем улучшенную функцию bulk_insert с обработкой ошибок и разбивкой на батчи
        columns = ["product_category", "window_start", "window_end", "cnt", "total_amount"]
        inserted_rows = bulk_insert(
            client=client,
            table="analytics.purchases_hourly",
            columns=columns,
            data=data_tuples,
            batch_size=5000  # Оптимальный размер батча для вставки
        )
        
        elapsed_time = time.time() - start_time
        logger.info(f"Batch {batch_id}: Successfully inserted {inserted_rows} out of {len(data_tuples)} rows "
                   f"in {elapsed_time:.2f} seconds")
    
    except Exception as e:
        logger.error(f"Batch {batch_id}: Unexpected error in write_to_clickhouse: {str(e)}")


def main():
    """
    Основная функция:
      - Создаём SparkSession
      - Читаем справочники
      - Собираем стрим, обогащаем, делаем агрегацию
      - Запускаем writeStream с foreachBatch
    """
    logger.info("Starting Purchase Processor")
    
    try:
        spark = create_spark_session()

        # 1) Читаем справочники (batch-режим, static DataFrame)
        products_df, customers_df = read_static_dictionaries(spark)

        # 2) Читаем стрим (когда spark.readStream не блокирует, а создаёт StreamingQuery)
        purchases_df = get_kafka_stream(spark)

        # 3) Обогащаем
        enriched_df = enrich_stream(purchases_df, products_df, customers_df)

        # 4) Оконная агрегация
        agg_df = build_windowed_aggregates(enriched_df)

        # 5) Записываем результат в ClickHouse (foreachBatch)
        query = (
            agg_df.writeStream
            .outputMode("update")  # Для оконных агрегаций используем "update"
            .foreachBatch(write_to_clickhouse)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime="10 minutes")  # Запускаем каждые 10 минут
            .start()
        )

        logger.info("Purchase Processor started successfully")
        
        # Ожидаем завершения (или прерывания)
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()