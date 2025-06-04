from pyspark.sql import SparkSession

def process_customers(spark: SparkSession):
    df = spark.read.parquet("s3a://shop-stage-data/customers/")

    df_clean = df.dropDuplicates(["email"])
    
    df_clean = df_clean.select( "customer_id","first_name", "last_name", "email", "phone", "created_at"
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS analytics.customers (
            customer_id INT,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
    """)

    df_clean.writeTo("analytics.customers").overwritePartitions()
