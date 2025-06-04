# Shop Analytics

Система аналитики данных магазина с потоковой и батч-обработкой.



### Основные возможности

- Сбор данных о покупках через API и Kafka
- Обработка данных в режиме реального времени с помощью Spark Streaming
- Пакетная обработка данных с помощью Spark для агрегации и анализа
- Многослойная архитектура данных (RAW, STAGE, ANALYTICS)
- Визуализация данных через ClickHouse и Tabix UI
- Оркестрация рабочих потоков через Apache Airflow
- API для интеграции с внешними системами


### Ключевые компоненты:

1. **API** - FastAPI-сервис для приема данных и управления системой
2. **Kafka** - обеспечивает потоковую передачу данных о покупках
3. **PostgreSQL** - хранение мастер-данных (товары, клиенты)
4. **MinIO** - S3-совместимое хранилище для сырых и обработанных данных
5. **Spark** - обработка данных (batch и streaming)
6. **ClickHouse** - OLAP-база данных для аналитических запросов
7. **Airflow** - оркестрация и планирование задач
8. **Tabix UI** - веб-интерфейс для ClickHouse


## Процесс обработки данных

Система реализует многослойную архитектуру хранения данных:

1. **RAW Layer** - сырые данные в формате Parquet в MinIO (S3)
2. **STAGE Layer** - проверенные и структурированные данные
3. **ANALYTICS Layer** - агрегированные данные, готовые для аналитики (Iceberg таблицы в S3 и ClickHouse)

### Потоки данных:

1. **API → Kafka → Spark Streaming → MinIO (RAW)**
   - Данные о покупках поступают через API, отправляются в Kafka
   - Spark Streaming обрабатывает поток и сохраняет данные в MinIO

2. **API → Postgres → S3 ETL → MinIO (RAW)**
   - Данные о клиентах поступаютиз API, загружаются в PostgreSQL, а из PostgreSQL выгружаются в MinIO

3. **API → S3 ETL → MinIO (RAW)**
   - Данные о товарах из поступают через API выгружаются в MinIO в виде .parquet файлов

4. **RAW → STAGE → ANALYTICS**
   - Данные из RAW слоя проходят валидацию и перемещаются в STAGE
   - Аналитические преобразования создают агрегаты в ClickHouse и Iceberg таблицы

## Установка и запуск

### Запуск системы

1. Клонируйте репозиторий:
   ```bash
   git clone <repository-url>
   cd shop_analytics
   ```

2. Запустите систему одной командой:
   ```bash
   cd app
   ./start.sh
   ```

3. Дождитесь запуска всех сервисов

### Доступ к сервисам

- **API**: http://localhost:8000
- **MinIO Console**: http://localhost:9001 (логин: minioadmin / пароль: minioadmin)
- **Spark UI**: http://localhost:8080
- **ClickHouse UI**: http://localhost:8124 
- **Airflow**: http://localhost:8090 (логин: admin / пароль: admin)

## API сервисы

API предоставляет следующие эндпоинты:

### Аутентификация
- `POST /api/v1/token` - получение JWT токена (username: admin, password: admin)

#### Процесс аутентификации
Все защищенные эндпоинты требуют JWT токен для доступа. Чтобы получить токен:

1. Отправьте POST запрос на `/api/v1/token` с данными:
   ```json
   {
     "username": "admin",
     "password": "admin"
   }
   ```

2. В ответе вы получите токен доступа:
   ```json
   {
     "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
     "token_type": "bearer"
   }
   ```

3. Используйте этот токен в заголовке `Authorization` для всех последующих запросов:
   ```
   Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
   ```

Срок действия токена - 30 минут. После истечения срока необходимо получить новый токен.

### Данные о покупках
- `POST /api/v1/purchases` - добавление одной покупки
- `GET /api/v1/purchases` - получение данных о покупках
- `GET /api/v1/purchases/{purchase_id}` - получение информации о конкретной покупке
- `DELETE /api/v1/purchases/{purchase_id}` - удаление покупки
- `POST /api/v1/purchases/bulk-kafka` - пакетная загрузка покупок в Kafka

### Товары
- `POST /api/v1/products` - добавление товара
- `GET /api/v1/products` - получение списка товаров
- `GET /api/v1/products/{product_id}` - получение информации о товаре
- `PUT /api/v1/products/{product_id}` - обновление информации о товаре
- `DELETE /api/v1/products/{product_id}` - удаление товара
- `POST /api/v1/products/bulk-s3` - пакетная загрузка товаров в S3

### Клиенты
- `POST /api/v1/customers` - добавление клиента
- `GET /api/v1/customers` - получение списка клиентов
- `GET /api/v1/customers/{customer_id}` - получение информации о клиенте
- `PUT /api/v1/customers/{customer_id}` - обновление информации о клиенте
- `DELETE /api/v1/customers/{customer_id}` - удаление клиента
- `POST /api/v1/customers/bulk-postgres` - пакетная загрузка клиентов в PostgreSQL

### Продавцы
- `POST /api/v1/sellers` - добавление продавца
- `GET /api/v1/sellers` - получение списка продавцов
- `GET /api/v1/sellers/{seller_id}` - получение информации о продавце
- `PUT /api/v1/sellers/{seller_id}` - обновление информации о продавце
- `DELETE /api/v1/sellers/{seller_id}` - удаление продавца

### Системные эндпоинты
- `GET /` - корневой эндпоинт
- `GET /health` - проверка статуса системы
- `GET /api/v1/health` - расширенная проверка здоровья API

## Airflow DAGs

Система содержит два основных DAG (графа задач) в Airflow:

### 1. api_data_processing
Обрабатывает данные, поступающие через API в режиме реального времени.

**Задачи**:
- `check_for_new_data` - проверка наличия новых данных в API
- `process_purchases_from_api` - обработка покупок из API
- `process_kafka_data` - обработка данных из Kafka с использованием PySpark
- `update_stage_layer` - обновление STAGE слоя

**Расписание**: каждые 30 минут

### 2. daily_shop_etl
Выполняет ежедневную обработку данных для аналитики.

**Задачи**:
- `upload_products_to_raw` - загрузка товаров в RAW слой
- `export_customers_to_raw` - экспорт клиентов из PostgreSQL в RAW слой
- `process_kafka_data` - обработка накопленных данных из Kafka
- `copy_raw_to_stage_with_validation` - копирование данных из RAW в STAGE с валидацией
- `run_spark_daily_job` - запуск Spark-задания для создания аналитических данных

**Расписание**: ежедневно в 2:00 AM

## Потоковая обработка данных

Система включает потоковую обработку данных о покупках с помощью Spark Streaming:

1. Данные о покупках поступают в Kafka через API
2. Spark Streaming считывает данные из Kafka
3. Данные обрабатываются и сохраняются в MinIO (RAW слой)
4. Аналитические агрегаты записываются в ClickHouse

Потоковая обработка настроена в контейнере `streaming-processor` и работает в фоновом режиме.

## Управление и мониторинг

### Просмотр логов
```bash
docker-compose -f infrastructure/docker-compose.yml logs -f [service_name]
```

### Остановка системы
```bash
docker-compose -f infrastructure/docker-compose.yml down
```

### Перезапуск отдельного сервиса
```bash
docker-compose -f infrastructure/docker-compose.yml restart [service_name]
```

## Описание основных модулей

### Data Ingestion
- `kafka_spark_processor.py` - обработка данных из Kafka с использованием PySpark
- `batch_spark_processor.py` - ежедневная батч-обработка данных с PySpark
- `postgres_to_s3.py` - экспорт данных из PostgreSQL в S3
- `s3_stage_loader.py` - загрузка данных из RAW в STAGE слой

### Batch Processing
- `daily_job.py` - основной скрипт для ежедневной обработки
- `aggregations.py` - создание аналитических агрегатов

### Streaming
- `purchase_processor.py` - потоковая обработка данных о покупках

### Utils
- `s3_client.py` - клиент для работы с S3/MinIO
- `ch_client.py` - клиент для ClickHouse

## Архитектура хранения данных

### RAW Layer (MinIO)
- `/shop-raw-data/purchases/` - сырые данные о покупках
- `/shop-raw-data/products/` - сырые данные о товарах
- `/shop-raw-data/customers/` - сырые данные о клиентах

### STAGE Layer (MinIO)
- `/shop-stage-data/purchases/` - валидированные данные о покупках
- `/shop-stage-data/products/` - валидированные данные о товарах
- `/shop-stage-data/customers/` - валидированные данные о клиентах

### ANALYTICS Layer
- **ClickHouse таблицы**:
  - `daily_purchase_stats` - дневная статистика покупок
  - `monthly_purchase_stats` - месячная статистика покупок
  
- **Iceberg таблицы (MinIO)**:
  - `/analytics/purchase_product_facts/` - денормализованные факты о покупках с информацией о товарах
