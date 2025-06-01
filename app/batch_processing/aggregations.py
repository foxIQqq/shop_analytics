from pyspark.sql.functions import sum, count, max, min, col, datediff, lit
from datetime import datetime

def run_aggregations(spark):
    # Чтение из Iceberg таблиц вместо прямого доступа к файлам
    purchases = spark.table("analytics.purchases")
    products = spark.table("analytics.products")

    # Присоединение для получения категорий
    df = purchases.join(products, on="product_id")

    # 💡 1. Продажи по категориям
    category_sales = df.groupBy("category").agg(
        sum("price_at_time").alias("total_revenue"),
        count("*").alias("total_sales")
    )

    category_sales.write.format("jdbc").options(
        url="jdbc:clickhouse://clickhouse:8123/default",
        driver="com.clickhouse.jdbc.ClickHouseDriver",
        dbtable="category_sales",
        user="default",
        password=""
    ).mode("overwrite").save()

    # 💡 2. RFM-анализ
    current_date = datetime.today().strftime('%Y-%m-%d')
    rfm = df.groupBy("customer_id").agg(
        max("purchased_at").alias("last_purchase"),
        count("*").alias("frequency"),
        sum("price_at_time").alias("monetary")
    ).withColumn("recency", datediff(lit(current_date), col("last_purchase")))

    rfm.write.format("jdbc").options(
        url="jdbc:clickhouse://clickhouse:8123/default",
        driver="com.clickhouse.jdbc.ClickHouseDriver",
        dbtable="customer_rfm",
        user="default",
        password=""
    ).mode("overwrite").save()

    # 💡 3. Топ товаров
    top_products = df.groupBy("product_id").agg(
        count("*").alias("purchase_count"),
        sum("price_at_time").alias("revenue")
    ).orderBy(col("revenue").desc())

    top_products.write.format("jdbc").options(
        url="jdbc:clickhouse://clickhouse:8123/default",
        driver="com.clickhouse.jdbc.ClickHouseDriver",
        dbtable="top_products",
        user="default",
        password=""
    ).mode("overwrite").save()