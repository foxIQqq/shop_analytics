# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Установка дополнительных зависимостей для обработки данных
RUN pip install --no-cache-dir pandas psycopg2-binary pyspark pyarrow minio

# Установка PYTHONPATH
ENV PYTHONPATH="/app"

# Копируем исходный код
COPY api/ ./api/
COPY utils/ ./utils/
COPY data_ingestion/ ./data_ingestion/
COPY configs/ ./configs/

# Экспонируем порт
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]