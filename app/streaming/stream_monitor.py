#!/usr/bin/env python3
# ----------------------------------------------------------------------
# streaming/stream_monitor.py
#
# Скрипт для мониторинга состояния Spark Streaming job и метрик ClickHouse.
# Выводит статистику по количеству обработанных записей, задержкам и т.д.
# ----------------------------------------------------------------------

import sys
import os
# Добавляем корневую директорию проекта в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
import argparse
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from utils.ch_client import get_clickhouse_client, execute_with_retry

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры из окружения
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

SPARK_UI_URL = os.getenv("SPARK_UI_URL", "http://localhost:4040")


def check_spark_job_status() -> Dict[str, Any]:
    """
    Проверяет статус Spark Streaming job через Spark UI REST API.
    
    Returns:
        Dict с информацией о статусе job.
    """
    try:
        # Получаем информацию о активных приложениях
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=5)
        if response.status_code != 200:
            logger.error(f"Failed to get Spark applications: {response.status_code}")
            return {"status": "error", "message": f"Failed to get Spark applications: {response.status_code}"}
        
        apps = response.json()
        if not apps:
            return {"status": "not_running", "message": "No active Spark applications found"}
        
        # Находим наше приложение PurchaseProcessor
        purchase_app = None
        for app in apps:
            if app.get("name") == "PurchaseProcessor":
                purchase_app = app
                break
        
        if not purchase_app:
            return {"status": "not_running", "message": "PurchaseProcessor application not found"}
        
        app_id = purchase_app.get("id")
        
        # Получаем информацию о streaming jobs
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications/{app_id}/streaming/statistics", timeout=5)
        if response.status_code != 200:
            logger.error(f"Failed to get streaming statistics: {response.status_code}")
            return {
                "status": "running", 
                "app_id": app_id,
                "name": purchase_app.get("name"),
                "streaming_stats_available": False
            }
        
        streaming_stats = response.json()
        
        # Получаем информацию о batch jobs
        response = requests.get(f"{SPARK_UI_URL}/api/v1/applications/{app_id}/streaming/batches", timeout=5)
        if response.status_code != 200:
            logger.error(f"Failed to get streaming batches: {response.status_code}")
            batch_stats = {"available": False}
        else:
            batch_stats = response.json()
            batch_stats["available"] = True
        
        return {
            "status": "running",
            "app_id": app_id,
            "name": purchase_app.get("name"),
            "streaming_stats": streaming_stats,
            "batch_stats": batch_stats
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Spark UI: {str(e)}")
        return {"status": "unknown", "message": f"Error connecting to Spark UI: {str(e)}"}


def get_clickhouse_metrics() -> Dict[str, Any]:
    """
    Получает метрики из ClickHouse:
    - Количество записей в основной таблице
    - Распределение по категориям товаров
    - Объем данных и использование диска
    
    Returns:
        Dict с метриками.
    """
    try:
        client = get_clickhouse_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database="analytics"
        )
        
        # Общее количество записей
        total_records_query = "SELECT count() FROM analytics.purchases_hourly"
        total_records = execute_with_retry(client, total_records_query)[0][0]
        
        # Статистика по категориям товаров
        categories_query = """
            SELECT 
                product_category,
                count() as records,
                sum(cnt) as total_purchases,
                sum(total_amount) as total_amount
            FROM analytics.purchases_hourly
            GROUP BY product_category
            ORDER BY total_amount DESC
        """
        categories_stats = execute_with_retry(client, categories_query)
        
        # Распределение по времени (последние 24 часа)
        time_query = """
            SELECT 
                toStartOfHour(window_start) as hour,
                sum(cnt) as purchases,
                sum(total_amount) as amount
            FROM analytics.purchases_hourly
            WHERE window_start >= now() - INTERVAL 24 HOUR
            GROUP BY hour
            ORDER BY hour
        """
        time_stats = execute_with_retry(client, time_query)
        
        # Информация о таблицах и их размере
        tables_query = """
            SELECT
                table,
                formatReadableSize(sum(bytes)) as size,
                sum(rows) as rows,
                max(modification_time) as last_modified
            FROM system.parts
            WHERE active AND database = 'analytics'
            GROUP BY table
        """
        tables_stats = execute_with_retry(client, tables_query)
        
        return {
            "status": "ok",
            "total_records": total_records,
            "categories_stats": [
                {
                    "category": row[0],
                    "records": row[1],
                    "total_purchases": row[2],
                    "total_amount": row[3]
                } for row in categories_stats
            ],
            "time_stats": [
                {
                    "hour": row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    "purchases": row[1],
                    "amount": row[2]
                } for row in time_stats
            ],
            "tables_stats": [
                {
                    "table": row[0],
                    "size": row[1],
                    "rows": row[2],
                    "last_modified": row[3].strftime("%Y-%m-%d %H:%M:%S") if row[3] else None
                } for row in tables_stats
            ]
        }
    except Exception as e:
        logger.error(f"Error getting ClickHouse metrics: {str(e)}")
        return {"status": "error", "message": f"Error getting ClickHouse metrics: {str(e)}"}


def print_metrics(spark_status: Dict[str, Any], clickhouse_metrics: Dict[str, Any]) -> None:
    """
    Выводит метрики в консоль в удобочитаемом формате.
    """
    print("\n" + "=" * 80)
    print(f"STREAMING JOB MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Статус Spark Job
    print("\nSPARK JOB STATUS:")
    print(f"Status: {spark_status.get('status', 'unknown')}")
    if spark_status.get('message'):
        print(f"Message: {spark_status.get('message')}")
    if spark_status.get('app_id'):
        print(f"Application ID: {spark_status.get('app_id')}")
    
    # Статистика Streaming (если доступна)
    if spark_status.get('status') == 'running' and spark_status.get('streaming_stats'):
        stats = spark_status['streaming_stats']
        print("\nSTREAMING STATISTICS:")
        print(f"Receivers: {stats.get('receivers', 0)}")
        print(f"Active batches: {stats.get('activeBatches', 0)}")
        print(f"Completed batches: {stats.get('completedBatches', 0)}")
        if stats.get('avgInputRate') is not None:
            print(f"Average input rate: {stats.get('avgInputRate', 0):.2f} records/sec")
        if stats.get('avgProcessingTime') is not None:
            print(f"Average processing time: {stats.get('avgProcessingTime', 0):.2f} ms")
        if stats.get('avgSchedulingDelay') is not None:
            print(f"Average scheduling delay: {stats.get('avgSchedulingDelay', 0):.2f} ms")
    
    # ClickHouse метрики
    print("\nCLICKHOUSE METRICS:")
    if clickhouse_metrics.get('status') == 'ok':
        print(f"Total records in purchases_hourly: {clickhouse_metrics.get('total_records', 0):,}")
        
        # Таблицы и их размеры
        if clickhouse_metrics.get('tables_stats'):
            print("\nTABLES:")
            for table in clickhouse_metrics['tables_stats']:
                print(f"  {table['table']}: {table['size']} ({table['rows']:,} rows), "
                      f"Last modified: {table['last_modified']}")
        
        # Топ категорий
        if clickhouse_metrics.get('categories_stats'):
            print("\nTOP CATEGORIES:")
            for i, cat in enumerate(clickhouse_metrics['categories_stats'][:5], 1):
                print(f"  {i}. {cat['category']}: {cat['total_purchases']:,} purchases, "
                      f"${cat['total_amount']:,.2f} total")
        
        # Статистика по времени (последние записи)
        if clickhouse_metrics.get('time_stats'):
            print("\nRECENT ACTIVITY (last 24h):")
            recent_stats = clickhouse_metrics['time_stats'][-5:] if len(clickhouse_metrics['time_stats']) > 5 else clickhouse_metrics['time_stats']
            for stat in recent_stats:
                print(f"  {stat['hour']}: {stat['purchases']:,} purchases, ${stat['amount']:,.2f}")
    else:
        print(f"Error: {clickhouse_metrics.get('message', 'Unknown error')}")
    
    print("\n" + "=" * 80)


def main():
    """
    Основная функция: собирает метрики и выводит их.
    """
    parser = argparse.ArgumentParser(description='Monitor Spark Streaming job and ClickHouse metrics')
    parser.add_argument('--watch', '-w', action='store_true', help='Watch mode: continuously monitor')
    parser.add_argument('--interval', '-i', type=int, default=60, help='Interval in seconds (for watch mode)')
    parser.add_argument('--json', '-j', action='store_true', help='Output in JSON format')
    args = parser.parse_args()
    
    def collect_and_print_metrics():
        spark_status = check_spark_job_status()
        clickhouse_metrics = get_clickhouse_metrics()
        
        if args.json:
            # Вывод в формате JSON
            output = {
                "timestamp": datetime.now().isoformat(),
                "spark_status": spark_status,
                "clickhouse_metrics": clickhouse_metrics
            }
            print(json.dumps(output, indent=2))
        else:
            # Вывод в человекочитаемом формате
            print_metrics(spark_status, clickhouse_metrics)
    
    if args.watch:
        try:
            while True:
                collect_and_print_metrics()
                print(f"\nNext update in {args.interval} seconds. Press Ctrl+C to exit.")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
    else:
        collect_and_print_metrics()


if __name__ == "__main__":
    main() 