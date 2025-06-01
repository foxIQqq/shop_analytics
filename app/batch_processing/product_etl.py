from pyspark.sql import SparkSession

def process_products(spark: SparkSession):
    # 1. Чтение из stage-слоя
    df = spark.read.parquet("s3a://shop-stage-data/products/")

    # 2. Очистка и дедупликация
    df_clean = df.dropDuplicates(["product_id"]).dropna(subset=["product_id", "product_name", "category"])

    # 3. Явное создание таблицы Iceberg, если её нет
    spark.sql("""
        CREATE TABLE IF NOT EXISTS analytics.products (
            product_id INT,
            product_name STRING,
            category STRING,
            price DOUBLE,
            stock_quantity INT
        )
        USING iceberg
        PARTITIONED BY (category)
    """)

    # 4. Запись в таблицу Iceberg (overwritePartitions = безопасный режим для обновления)
    df_clean.writeTo("analytics.products").overwritePartitions()
