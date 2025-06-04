#!/bin/bash

show_status() {
  echo -e "\n\033[1;34m===> $1\033[0m"
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

show_status "Шаг 1/10: Запуск контейнеров..."
docker-compose -f infrastructure/docker-compose.yml up -d
echo "Ожидание инициализации базовых сервисов..."
sleep 15

echo "Проверка доступности ClickHouse..."
for i in {1..10}; do
  if curl -s http://localhost:8123/ping > /dev/null; then
    echo "ClickHouse доступен!"
    break
  fi
  echo "Ожидание ClickHouse... $i/10"
  sleep 3
done

show_status "Шаг 2/10: Создание топиков Kafka..."
docker-compose -f infrastructure/docker-compose.yml exec -T kafka kafka-topics \
  --create --topic purchases --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 --if-not-exists

show_status "Шаг 2.5/10: Создание бакетов в MinIO..."
echo "Создание бакетов в MinIO..."
docker-compose -f infrastructure/docker-compose.yml exec -T minio bash -c "mkdir -p /tmp/mc && cd /tmp/mc && wget -q https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && ./mc alias set myminio http://localhost:9000 minioadmin minioadmin && ./mc mb --ignore-existing myminio/shop-raw-data myminio/shop-stage-data myminio/shop-checkpoints myminio/analytics myminio/analytics/checkpoints"

show_status "Шаг 3/10: Отправка тестовых сообщений в Kafka..."
docker-compose -f infrastructure/docker-compose.yml exec -T kafka bash -c "echo '{\"customer_id\": 123, \"product_id\": 456, \"seller_id\": 789, \"quantity\": 3, \"price_at_time\": 29.99, \"purchased_at\": \"2025-05-31T15:30:00\"}' | kafka-console-producer --topic purchases --bootstrap-server localhost:9092" > /dev/null 2>&1
docker-compose -f infrastructure/docker-compose.yml exec -T kafka bash -c "echo '{\"customer_id\": 42, \"product_id\": 101, \"seller_id\": 55, \"quantity\": 1, \"price_at_time\": 149.99, \"purchased_at\": \"2025-05-31T18:45:22\"}' | kafka-console-producer --topic purchases --bootstrap-server localhost:9092" > /dev/null 2>&1
docker-compose -f infrastructure/docker-compose.yml exec -T kafka bash -c "echo '{\"customer_id\": 7, \"product_id\": 2048, \"seller_id\": 16, \"quantity\": 5, \"price_at_time\": 9.99, \"purchased_at\": \"2025-05-31T22:10:37\"}' | kafka-console-producer --topic purchases --bootstrap-server localhost:9092" > /dev/null 2>&1

show_status "Шаг 4/10: Выгрузка данных из PostgreSQL в S3..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python /app/data_ingestion/postgres_to_s3.py"

show_status "Шаг 5/10: Запуск Spark-стриминга для загрузки данных из Kafka в S3..."
docker-compose -f infrastructure/docker-compose.yml exec -T spark-master bash -c "pip install minio > /dev/null 2>&1 && export PYTHONPATH=/app && spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 /app/data_ingestion/kafka_to_s3.py" > /dev/null 2>&1
echo "Spark-стриминг для Kafka успешно завершен"

show_status "Шаг 6/10: Загрузка products.parquet в S3..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python /app/data_ingestion/upload_to_s3.py /app/data_ingestion/products.parquet"

show_status "Шаг 7/10: Загрузка данных в STAGE слой..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python /app/data_ingestion/s3_stage_loader.py"

show_status "Шаг 8/10: Создание аналитического слоя (первичный батч)..."
echo "Запуск одноразового батча для создания аналитической инфраструктуры..."
docker-compose -f infrastructure/docker-compose.yml exec -T spark-master bash -c "export PYTHONPATH=/app && spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,com.clickhouse:clickhouse-jdbc:0.4.6 /app/batch_processing/daily_job.py" > /dev/null 2>&1

docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python -c \"from utils.s3_client import get_s3_client; s3 = get_s3_client(); s3.make_bucket('analytics') if not s3.bucket_exists('analytics') else None; print('Проверка создания бакета analytics завершена.')\""

show_status "Шаг 9/10: Инициализация и запуск Airflow..."
echo "Обновление исходного кода в контейнерах..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "mkdir -p /app/data_ingestion /app/utils /app/batch_processing"
docker cp "$SCRIPT_DIR/data_ingestion/." $(docker-compose -f infrastructure/docker-compose.yml ps -q api):/app/data_ingestion/
docker cp "$SCRIPT_DIR/utils/." $(docker-compose -f infrastructure/docker-compose.yml ps -q api):/app/utils/
docker cp "$SCRIPT_DIR/batch_processing/." $(docker-compose -f infrastructure/docker-compose.yml ps -q api):/app/batch_processing/

bash "$SCRIPT_DIR/infrastructure/init_airflow.sh"

show_status "Шаг 10/10: Все шаги выполнены успешно!"
echo ""
echo "Доступ к сервисам:"
echo "API: http://localhost:8000"
echo "MinIO Console: http://localhost:9001 (логин: minioadmin / пароль: minioadmin)"
echo "Spark UI: http://localhost:8080"
echo "ClickHouse HTTP: http://localhost:8123"
echo "ClickHouse UI: http://localhost:8124 (Tabix interface - if login required, use login: default / no password)"
echo "Airflow: http://localhost:8090 (логин: admin / пароль: admin)"
echo ""
echo "Потоковая обработка данных запущена в фоновом режиме через сервис streaming-processor."
echo ""

show_status "Информация о сервисах"
echo ""
echo "Доступ к сервисам:"
echo "- API: http://localhost:8000"
echo "- MinIO Console: http://localhost:9001 (login: minioadmin / password: minioadmin)"
echo "- Spark Master UI: http://localhost:8080"
echo "- ClickHouse HTTP: http://localhost:8123 (raw HTTP interface)"
echo "- ClickHouse UI: http://localhost:8124 (Tabix interface - if login required, use login: default / no password)"
echo "- Airflow: http://localhost:8090 (login: admin / password: admin)"
echo ""
echo "Для просмотра логов сервисов:"
echo "docker-compose -f infrastructure/docker-compose.yml logs -f [service_name]"
echo ""
echo "Для остановки всех сервисов:"
echo "docker-compose -f infrastructure/docker-compose.yml down"

create_kafka_topics() {
  echo "Создание топиков Kafka..."
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T kafka \
    kafka-topics --create --topic purchases --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T kafka \
    kafka-topics --create --topic products-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
    
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T kafka \
    kafka-topics --create --topic sellers-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
    
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T kafka \
    kafka-topics --create --topic customers-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
}

init_minio() {
  echo "Инициализация MinIO..."
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
  
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T minio mc mb --ignore-existing myminio/shop-raw-data
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T minio mc mb --ignore-existing myminio/shop-stage-data
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T minio mc mb --ignore-existing myminio/shop-checkpoints
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T minio mc mb --ignore-existing myminio/analytics
  docker-compose -f $INFRA_DIR/docker-compose.yml exec -T minio mc mb --ignore-existing myminio/analytics/checkpoints
  



if [ "$START_SERVICES" = true ]; then
  echo "Запуск основных сервисов..."
  docker-compose -f $INFRA_DIR/docker-compose.yml up -d
  
  echo "Ожидание запуска сервисов..."
  sleep 15
  
  create_kafka_topics
  
  init_minio
  
  echo "Запуск стримингового процессора..."
  docker-compose -f $INFRA_DIR/docker-compose.yml up -d streaming-processor
  
  echo "Инициализация Airflow..."
  bash $INFRA_DIR/init_airflow.sh
fi 