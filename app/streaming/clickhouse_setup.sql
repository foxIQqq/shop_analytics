-- --------------------------------------------------------------------
-- streaming/clickhouse_setup.sql
--
-- Содержит DDL для ClickHouse: создание базы analytics и необходимых таблиц.
-- --------------------------------------------------------------------

-- 1) Создадим Базу
CREATE DATABASE IF NOT EXISTS analytics;

-- 2) Таблица-факт: агрегаты покупок по категории за каждый час
CREATE TABLE IF NOT EXISTS analytics.purchases_hourly
(
    product_category String,
    window_start     DateTime,
    window_end       DateTime,
    cnt              UInt64,
    total_amount     Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)  -- Партиционирование по дням для более эффективного удаления старых данных
ORDER BY (product_category, window_start)
TTL window_end + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;


-- 3) Материализованное представление: топ-5 категорий по сумме count
CREATE TABLE IF NOT EXISTS analytics.top_categories
(
    product_category String,
    total_cnt        UInt64,
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (total_cnt, product_category)
TTL updated_at + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.top_categories_mv
TO analytics.top_categories
AS
SELECT
    product_category,
    sum(cnt) AS total_cnt,
    now() as updated_at
FROM analytics.purchases_hourly
GROUP BY product_category
ORDER BY total_cnt DESC
LIMIT 5;

-- 4) Таблица для анализа покупок по часам дня
CREATE TABLE IF NOT EXISTS analytics.hourly_sales_patterns
(
    hour_of_day      UInt8,
    product_category String,
    avg_sales        Float64,
    total_cnt        UInt64,
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (product_category, hour_of_day)
TTL updated_at + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.hourly_sales_patterns_mv
TO analytics.hourly_sales_patterns
AS
SELECT
    toHour(window_start) AS hour_of_day,
    product_category,
    avg(total_amount) AS avg_sales,
    sum(cnt) AS total_cnt,
    now() as updated_at
FROM analytics.purchases_hourly
GROUP BY hour_of_day, product_category;

-- 5) Таблица для анализа продаж по дням недели
CREATE TABLE IF NOT EXISTS analytics.weekly_sales_patterns
(
    day_of_week      UInt8,
    product_category String,
    avg_sales        Float64,
    total_cnt        UInt64,
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (product_category, day_of_week)
TTL updated_at + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.weekly_sales_patterns_mv
TO analytics.weekly_sales_patterns
AS
SELECT
    toDayOfWeek(window_start) AS day_of_week,
    product_category,
    avg(total_amount) AS avg_sales,
    sum(cnt) AS total_cnt,
    now() as updated_at
FROM analytics.purchases_hourly
GROUP BY day_of_week, product_category;

-- 6) Таблица для анализа трендов продаж по месяцам
CREATE TABLE IF NOT EXISTS analytics.monthly_sales_trends
(
    year_month       String,
    product_category String,
    total_amount     Float64,
    total_cnt        UInt64,
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (product_category, year_month)
TTL updated_at + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.monthly_sales_trends_mv
TO analytics.monthly_sales_trends
AS
SELECT
    formatDateTime(window_start, '%Y-%m') AS year_month,
    product_category,
    sum(total_amount) AS total_amount,
    sum(cnt) AS total_cnt,
    now() as updated_at
FROM analytics.purchases_hourly
GROUP BY year_month, product_category;