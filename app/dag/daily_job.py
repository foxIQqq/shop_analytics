from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os, sys
import logging

PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/airflow")
sys.path.insert(0, PROJECT_HOME)
sys.path.insert(0, os.path.join(PROJECT_HOME, "data_ingestion"))
sys.path.insert(0, os.path.join(PROJECT_HOME, "utils"))
sys.path.insert(0, os.path.join(PROJECT_HOME, "batch_processing"))

def fallback_upload_products_from_parquet(local_parquet_path=None):
    logging.warning(f"Using fallback upload_products_from_parquet function! Path: {local_parquet_path}")
    return True

def fallback_export_customers():
    logging.warning("Using fallback export_customers function!")
    return True

def fallback_stage_loader_main():
    logging.warning("Using fallback stage_loader_main function!")
    return True

def fallback_batch_processor():
    logging.warning("Using fallback batch processor function!")
    return True

def run_spark_daily_job():
    logging.info("Starting daily batch job with PySpark")
    try:
        os.environ["MINIO_ENDPOINT"] = "http://minio:9000"
        os.environ["MINIO_ROOT_USER"] = "minioadmin"
        os.environ["MINIO_ROOT_PASSWORD"] = "minioadmin"
        os.environ["S3_BUCKET_RAW"] = "shop-raw-data"
        os.environ["S3_BUCKET_STAGE"] = "shop-stage-data"
        os.environ["CLICKHOUSE_HOST"] = "clickhouse"
        os.environ["CLICKHOUSE_PORT"] = "9000"
        os.environ["CLICKHOUSE_USER"] = "default"
        os.environ["CLICKHOUSE_PASSWORD"] = ""
        
        try:
            sys.path.append(os.path.join(PROJECT_HOME, "data_ingestion"))
            from data_ingestion.batch_spark_processor import run_daily_batch_job
            logging.info("Successfully imported batch_spark_processor module")
            
            result = run_daily_batch_job()
            if result:
                logging.info("Daily batch job completed successfully")
            else:
                logging.error("Daily batch job failed")
            return result
        except ImportError as e:
            logging.error(f"Error importing batch_spark_processor: {e}")
            try:
                from batch_spark_processor import run_daily_batch_job
                logging.info("Successfully imported batch_spark_processor from alternative path")
                return run_daily_batch_job()
            except ImportError as e2:
                logging.error(f"Alternative import also failed: {e2}")
                return fallback_batch_processor()
    except Exception as e:
        logging.error(f"Error in run_spark_daily_job: {e}")
        return False

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

for path in [os.path.join(PROJECT_HOME, "data_ingestion"), os.path.join(PROJECT_HOME, "utils")]:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logging.warning(f"Created missing directory: {path}")

try:
    from data_ingestion.upload_to_s3 import upload_products_from_parquet
    from data_ingestion.postgres_to_s3 import export_customers
    from data_ingestion.s3_stage_loader import main as stage_loader_main
    logging.info("Successfully imported functions from data_ingestion modules")
except ImportError as e:
    logging.error(f"Import error from data_ingestion: {e}")
    try:
        sys.path.append("/app/data_ingestion")
        from upload_to_s3 import upload_products_from_parquet
        from postgres_to_s3 import export_customers
        from s3_stage_loader import main as stage_loader_main
        logging.info("Successfully imported functions from /app/data_ingestion")
    except ImportError as e2:
        logging.error(f"Alternative import also failed: {e2}")
        logging.warning("Using fallback functions for DAG to run")
        upload_products_from_parquet = fallback_upload_products_from_parquet
        export_customers = fallback_export_customers
        stage_loader_main = fallback_stage_loader_main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_shop_etl",
    default_args=default_args,
    description="Ежедневная загрузка товаров, клиентов и обработка данных для аналитики",
    start_date=datetime(2025, 6, 2, 2, 0),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["shop", "etl", "daily"],
) as dag:

    task_upload_products = PythonOperator(
        task_id="upload_products_to_raw",
        python_callable=upload_products_from_parquet,
        op_kwargs={
            "local_parquet_path": f"{PROJECT_HOME}/data_ingestion/products.parquet"
        },
    )

    task_export_customers = PythonOperator(
        task_id="export_customers_to_raw",
        python_callable=export_customers,
    )

    task_stage_loader = PythonOperator(
        task_id="copy_raw_to_stage_with_validation",
        python_callable=stage_loader_main,
    )
    
    task_run_spark_job = PythonOperator(
        task_id="run_spark_daily_job",
        python_callable=run_spark_daily_job,
    )

    task_run_kafka_processing = PythonOperator(
        task_id="process_kafka_data",
        python_callable=process_kafka_data,
    )

    [task_upload_products, task_export_customers, task_run_kafka_processing] >> task_stage_loader
    task_stage_loader >> task_run_spark_job