FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/app"

COPY api/ ./api/
COPY utils/ ./utils/
COPY data_ingestion/ ./data_ingestion/
COPY configs/ ./configs/

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]