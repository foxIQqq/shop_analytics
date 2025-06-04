from pyspark.sql import SparkSession

def process_products(spark: SparkSession):
    df = spark.read.parquet("s3a://shop-stage-data/products/")

    df_clean = df.dropDuplicates(["product_id"]).dropna(subset=["product_id", "product_name", "category"])

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

    df_clean.writeTo("analytics.products").overwritePartitions()
