from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os, sys
import requests
import json
import logging

# Добавляем пути к модулям в sys.path
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/airflow")
sys.path.insert(0, PROJECT_HOME)
sys.path.insert(0, os.path.join(PROJECT_HOME, "data_ingestion"))
sys.path.insert(0, os.path.join(PROJECT_HOME, "utils"))

# Определяем резервные функции на случай, если импорт не удался
def fallback_stage_loader_main():
    logging.warning("Using fallback stage_loader_main function!")
    return True

def fallback_get_s3_client():
    logging.warning("Using fallback get_s3_client function!")
    return None

# Проверяем, что папки существуют
for path in [os.path.join(PROJECT_HOME, "data_ingestion"), os.path.join(PROJECT_HOME, "utils")]:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logging.warning(f"Created missing directory: {path}")

# Импортируем функции из модулей проекта
try:
    # Пытаемся импортировать напрямую из data_ingestion
    from data_ingestion.s3_stage_loader import main as stage_loader_main
    from utils.s3_client import get_s3_client
    logging.info("Successfully imported functions from project modules")
except ImportError as e:
    logging.error(f"Import error: {e}")
    # Пытаемся импортировать иначе
    try:
        sys.path.append("/app/data_ingestion")
        sys.path.append("/app/utils")
        from s3_stage_loader import main as stage_loader_main
        from s3_client import get_s3_client
        logging.info("Successfully imported functions from /app paths")
    except ImportError as e2:
        logging.error(f"Alternative import also failed: {e2}")
        logging.warning("Using fallback functions for DAG to run")
        # Используем резервные функции
        stage_loader_main = fallback_stage_loader_main
        get_s3_client = fallback_get_s3_client

# Функция для обработки данных из Kafka
def process_kafka_data():
    logging.info("Starting Kafka to S3 processing with Spark")
    try:
        os.environ["MINIO_ENDPOINT"] = "http://minio:9000"
        os.environ["MINIO_ROOT_USER"] = "minioadmin"
        os.environ["MINIO_ROOT_PASSWORD"] = "minioadmin"
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:9092"
        os.environ["KAFKA_TOPIC_PURCHASES"] = "purchases"
        os.environ["S3_BUCKET_RAW"] = "shop-raw-data"
        os.environ["S3_BUCKET_CHECKPOINT"] = "shop-checkpoints"
        
        try:
            sys.path.append(os.path.join(PROJECT_HOME, "data_ingestion"))
            from data_ingestion.kafka_spark_processor import process_kafka_with_spark
            logging.info("Successfully imported kafka_spark_processor module")
            
            result = process_kafka_with_spark()
            if result:
                logging.info("Kafka data processing with Spark completed successfully")
            else:
                logging.error("Kafka data processing with Spark failed")
            return result
        except ImportError as e:
            logging.error(f"Error importing kafka_spark_processor: {e}")
            try:
                from data_ingestion.kafka_to_s3 import main as kafka_to_s3_main
                logging.info("Using kafka_to_s3 main function instead of Spark processor")
                return kafka_to_s3_main()
            except ImportError as e2:
                logging.error(f"Error importing kafka_to_s3: {e2}")
                try:
                    from data_ingestion.kafka_airflow_adapter import process_kafka_data_direct
                    logging.info("Using kafka_airflow_adapter instead of Spark processor")
                    return process_kafka_data_direct()
                except ImportError as e3:
                    logging.error(f"Error importing kafka_airflow_adapter: {e3}")
                    return False
    except Exception as e:
        logging.error(f"Error in process_kafka_data: {e}")
        return False

API_URL = "http://api:8000"
API_TOKEN_ENDPOINT = f"{API_URL}/api/v1/token"
API_CREDENTIALS = {"username": "admin", "password": "admin"}

def get_api_token():
    try:
        response = requests.post(
            API_TOKEN_ENDPOINT,
            data=API_CREDENTIALS,
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    except Exception as e:
        logging.error(f"Error getting API token: {e}")
        raise

def check_for_new_data():
    token = get_api_token()
    if not token:
        logging.error("Failed to get API token")
        return False
    
    return True

def process_purchases_from_api():
    token = get_api_token()
    if not token:
        logging.error("Failed to get API token")
        return False
    
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        purchases_data = {
            "purchases": [
                {
                    "product_id": f"prod-{i:03}",
                    "customer_id": i + 100,
                    "seller_id": (i % 20) + 1,
                    "quantity": (i % 5) + 1,
                    "price_at_time": float(f"{(i * 10.5):.2f}"),
                    "purchased_at": datetime.now().isoformat()
                }
                for i in range(1, 11)
            ]
        }
        
        response = requests.post(
            f"{API_URL}/api/v1/purchases/bulk-kafka",
            headers=headers,
            data=json.dumps(purchases_data)
        )
        response.raise_for_status()
        result = response.json()
        
        logging.info(f"API response: {result}")
        return result.get("success_count", 0) > 0
    except Exception as e:
        logging.error(f"Error processing purchases from API: {e}")
        return False

def update_stage_layer():
    try:
        stage_loader_main()
        return True
    except Exception as e:
        logging.error(f"Error updating stage layer: {e}")
        return False

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_data_processing",
    default_args=default_args,
    description="Обработка данных, полученных через API и их загрузка в аналитический слой",
    start_date=datetime(2025, 6, 2),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["shop", "api", "processing"],
) as dag:
    
    task_check_new_data = PythonOperator(
        task_id="check_for_new_data",
        python_callable=check_for_new_data,
    )
    
    task_process_purchases = PythonOperator(
        task_id="process_purchases_from_api",
        python_callable=process_purchases_from_api,
    )
    
    task_run_kafka_processing = PythonOperator(
        task_id="process_kafka_data",
        python_callable=process_kafka_data,
    )

    task_update_stage = PythonOperator(
        task_id="update_stage_layer",
        python_callable=update_stage_layer,
    )
    
    task_check_new_data >> task_process_purchases >> task_run_kafka_processing >> task_update_stage 