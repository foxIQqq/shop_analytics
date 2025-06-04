#!/bin/bash

show_status() {
  echo -e "\n\033[1;34m===> $1\033[0m"
}

check_airflow() {
  echo "Проверка доступности Airflow..."
  for i in {1..10}; do
    if curl -s http://localhost:8090/health > /dev/null; then
      echo "Airflow webserver доступен!"
      return 0
    fi
    echo "Ожидание запуска Airflow webserver... $i/10"
    sleep 5
  done
  echo "Airflow webserver не запустился. Проверьте логи."
  return 1
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
COMPOSE_FILE="/home/foxiq/shop_analytics/app/infrastructure/docker-compose.yml"

show_status "Шаг 1/5: Установка зависимостей Airflow"

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "pip install minio psycopg2-binary kafka-python pyarrow pandas pyspark==3.4.1 py4j==0.10.9.7 requests findspark pytz urllib3 numpy && pip install 'pyspark[sql, pandas_on_spark]'"

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "mkdir -p /opt/airflow/jars"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/airflow/jars/hadoop-aws-3.3.4.jar"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar -o /opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar -o /opt/airflow/jars/kafka-clients-3.3.1.jar"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar -o /opt/airflow/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "curl -L https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6.jar -o /opt/airflow/jars/clickhouse-jdbc-0.4.6.jar"

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "echo 'export PYSPARK_SUBMIT_ARGS=\"--jars /opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,/opt/airflow/jars/kafka-clients-3.3.1.jar,/opt/airflow/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar,/opt/airflow/jars/clickhouse-jdbc-0.4.6.jar pyspark-shell\"' >> /opt/airflow/.bashrc"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "echo 'export PYSPARK_PYTHON=/usr/local/bin/python' >> /opt/airflow/.bashrc"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "echo 'export SPARK_HOME=/opt/airflow/.local/lib/python3.8/site-packages/pyspark' >> /opt/airflow/.bashrc"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "echo 'export PYTHONPATH=\$PYTHONPATH:\$SPARK_HOME/python:\$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip' >> /opt/airflow/.bashrc"

show_status "Шаг 2/5: Проверка статуса Airflow"

if ! check_airflow; then
  show_status "Запуск Airflow"
  docker-compose -f "$COMPOSE_FILE" up -d airflow-init
  docker-compose -f "$COMPOSE_FILE" up -d airflow-webserver airflow-scheduler
  sleep 10
  
  if ! check_airflow; then
    echo "Не удалось запустить Airflow. Проверьте логи."
    exit 1
  fi
fi

show_status "Шаг 3/5: Настройка путей и создание структуры модулей"

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver mkdir -p /opt/airflow/data_ingestion
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver mkdir -p /opt/airflow/utils
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver mkdir -p /opt/airflow/batch_processing

show_status "Шаг 4/5: Копирование необходимых модулей в Airflow"
echo "Копирование data_ingestion..."
docker cp /home/foxiq/shop_analytics/app/data_ingestion/. $(docker-compose -f "$COMPOSE_FILE" ps -q airflow-webserver):/opt/airflow/data_ingestion/
docker cp /home/foxiq/shop_analytics/app/utils/. $(docker-compose -f "$COMPOSE_FILE" ps -q airflow-webserver):/opt/airflow/utils/
docker cp /home/foxiq/shop_analytics/app/batch_processing/. $(docker-compose -f "$COMPOSE_FILE" ps -q airflow-webserver):/opt/airflow/batch_processing/

if [ -f /home/foxiq/shop_analytics/app/data_ingestion/batch_spark_processor.py ]; then
  echo "Копирование batch_spark_processor.py..."
  docker cp /home/foxiq/shop_analytics/app/data_ingestion/batch_spark_processor.py $(docker-compose -f "$COMPOSE_FILE" ps -q airflow-webserver):/opt/airflow/data_ingestion/batch_spark_processor.py
else
  echo "ВНИМАНИЕ: Файл batch_spark_processor.py не найден!"
fi

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver bash -c "
chown -R airflow:root /opt/airflow/data_ingestion
chown -R airflow:root /opt/airflow/utils
chown -R airflow:root /opt/airflow/batch_processing
chown -R airflow:root /opt/airflow/jars

if [ ! -f /opt/airflow/data_ingestion/__init__.py ]; then
  touch /opt/airflow/data_ingestion/__init__.py
fi
if [ ! -f /opt/airflow/utils/__init__.py ]; then
  touch /opt/airflow/utils/__init__.py
fi
if [ ! -f /opt/airflow/batch_processing/__init__.py ]; then
  touch /opt/airflow/batch_processing/__init__.py
fi
"

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver ls -la /opt/airflow/data_ingestion/
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver ls -la /opt/airflow/utils/
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver ls -la /opt/airflow/jars/

show_status "Шаг 5/5: Активация DAGs"
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags unpause daily_shop_etl
docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags unpause api_data_processing

docker-compose -f "$COMPOSE_FILE" exec -T airflow-webserver airflow dags list

echo ""
echo "Инициализация Airflow завершена. Доступ к веб-интерфейсу:"
echo "URL: http://localhost:8090"
echo "Логин: admin"
echo "Пароль: admin"
echo "" 