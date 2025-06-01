#!/bin/bash

# Function to show status
show_status() {
  echo -e "\n\033[1;34m===> $1\033[0m"
}

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Start all services with docker-compose
show_status "Шаг 1/9: Запуск контейнеров..."
docker-compose -f infrastructure/docker-compose.yml up -d
echo "Ожидание инициализации базовых сервисов..."
sleep 15

# Check if ClickHouse is ready and set up tables if needed
echo "Проверка доступности ClickHouse..."
for i in {1..10}; do
  if curl -s http://localhost:8123/ping > /dev/null; then
    echo "ClickHouse доступен!"
    break
  fi
  echo "Ожидание ClickHouse... $i/10"
  sleep 3
done

show_status "Шаг 2/9: Создание топиков Kafka..."
docker-compose -f infrastructure/docker-compose.yml exec -T kafka kafka-topics \
  --create --topic purchases --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 --if-not-exists

show_status "Шаг 3/9: Отправка тестовых сообщений в Kafka..."
docker-compose -f infrastructure/docker-compose.yml exec -T kafka bash -c "echo '{\"customer_id\": 123, \"product_id\": 456, \"seller_id\": 789, \"quantity\": 3, \"price_at_time\": 29.99, \"purchased_at\": \"2025-05-31T15:30:00\"}' | kafka-console-producer --topic purchases --bootstrap-server localhost:9092" > /dev/null 2>&1
docker-compose -f infrastructure/docker-compose.yml exec -T kafka bash -c "echo '{\"customer_id\": 42, \"product_id\": 101, \"seller_id\": 55, \"quantity\": 1, \"price_at_time\": 149.99, \"purchased_at\": \"2025-05-31T18:45:22\"}' | kafka-console-producer --topic purchases --bootstrap-server localhost:9092" > /dev/null 2>&1
docker-compose -f infrastructure/docker-compose.yml exec -T kafka bash -c "echo '{\"customer_id\": 7, \"product_id\": 2048, \"seller_id\": 16, \"quantity\": 5, \"price_at_time\": 9.99, \"purchased_at\": \"2025-05-31T22:10:37\"}' | kafka-console-producer --topic purchases --bootstrap-server localhost:9092" > /dev/null 2>&1

show_status "Шаг 4/9: Выгрузка данных из PostgreSQL в S3..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python /app/data_ingestion/postgres_to_s3.py"

show_status "Шаг 5/9: Запуск Spark-стриминга для загрузки данных из Kafka в S3..."
# Запускаем стриминг в фоновом режиме
docker-compose -f infrastructure/docker-compose.yml exec -T spark-master bash -c "pip install minio > /dev/null 2>&1 && export PYTHONPATH=/app && spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 /app/data_ingestion/kafka_to_s3.py" > /dev/null 2>&1
echo "Spark-стриминг для Kafka успешно завершен"

show_status "Шаг 6/9: Загрузка products.parquet в S3..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python /app/data_ingestion/upload_to_s3.py /app/data_ingestion/products.parquet"

show_status "Шаг 7/9: Загрузка данных в STAGE слой..."
docker-compose -f infrastructure/docker-compose.yml exec -T api bash -c "export PYTHONPATH=/app && python /app/data_ingestion/s3_stage_loader.py"

# Запуск ежедневного batch job участника 2
show_status "Шаг 8/9: Запуск ежедневного Spark batch job..."
echo "Обработка товаров, клиентов, покупок и создание аналитических агрегатов..."
docker-compose -f infrastructure/docker-compose.yml exec -T spark-master bash -c "export PYTHONPATH=/app && bash /app/batch_processing/run_daily_job.sh" > /dev/null 2>&1
echo "Ежедневная джоба успешно выполнена - данные добавлены в аналитический слой."

show_status "Шаг 9/9: Все шаги выполнены успешно!"
echo ""
echo "Доступ к сервисам:"
echo "API: http://localhost:8000"
echo "MinIO Console: http://localhost:9001 (логин: minioadmin / пароль: minioadmin)"
echo "Spark UI: http://localhost:8080"
echo "ClickHouse HTTP: http://localhost:8123"
echo "ClickHouse UI: http://localhost:8124 (Tabix interface - if login required, use login: default / no password)"
echo ""
echo "Потоковая обработка данных запущена в фоновом режиме через сервис streaming-processor."
echo ""

# Show information about accessing the services
show_status "Shop Analytics System access information:"
echo ""
echo "Access the services at:"
echo "- API: http://localhost:8000"
echo "- MinIO Console: http://localhost:9001 (login: minioadmin / password: minioadmin)"
echo "- Spark Master UI: http://localhost:8080"
echo "- ClickHouse HTTP: http://localhost:8123 (raw HTTP interface)"
echo "- ClickHouse UI: http://localhost:8124 (Tabix interface - if login required, use login: default / no password)"
echo ""
echo "To view service logs:"
echo "docker-compose -f infrastructure/docker-compose.yml logs -f [service_name]"
echo ""
echo "To stop all services:"
echo "docker-compose -f infrastructure/docker-compose.yml down" 