from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col


def process_purchases(spark: SparkSession):
    df = spark.read.parquet("s3a://shop-stage-data/purchases/")

    df_clean = (
        df.dropDuplicates()
        .dropna(subset=["customer_id", "product_id", "purchased_at"])
        .withColumn("purchase_date", to_date(col("purchased_at")))
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS analytics.purchases (
            customer_id INT,
            product_id INT,
            seller_id INT,
            quantity INT,
            price_at_time DOUBLE,
            purchased_at TIMESTAMP,
            purchase_date DATE
        )
        USING iceberg
        PARTITIONED BY (purchase_date)
    """)

    df_clean.writeTo("analytics.purchases").overwritePartitions()

    print("Purchases ETL complete")