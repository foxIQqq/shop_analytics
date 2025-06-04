from pyspark.sql.functions import sum, count, max, col, datediff, lit, to_date, col
from datetime import datetime

def run_aggregations(spark):
    purchases = spark.table("analytics.purchases")
    products = spark.table("analytics.products")

    df = purchases.join(products, on="product_id")

    category_sales = df.groupBy("category").agg(
        sum("price_at_time").alias("total_revenue"),
        count("*").alias("total_sales")
    )

    category_sales.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "category_sales") \
        .option("user", "default") \
        .option("password", "") \
        .option("truncate", "true") \
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY category") \
        .mode("overwrite") \
        .save()

    current_date = datetime.today().strftime("%Y-%m-%d")
    rfm = df.groupBy("customer_id").agg(
        max("purchased_at").alias("last_purchase"),
        count("*").alias("frequency"),
        sum("price_at_time").alias("monetary")
    ).withColumn("recency", datediff(to_date(lit(current_date)), col("last_purchase")))

    rfm.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "customer_rfm") \
        .option("user", "default") \
        .option("password", "") \
        .option("truncate", "true") \
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY customer_id") \
        .mode("overwrite") \
        .save()

    top_products = df.groupBy("product_id").agg(
        count("*").alias("purchase_count"),
        sum("price_at_time").alias("revenue")
    ).orderBy(col("revenue").desc())

    top_products.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "top_products") \
        .option("user", "default") \
        .option("password", "") \
        .option("truncate", "true") \
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY product_id") \
        .mode("overwrite") \
        .save()