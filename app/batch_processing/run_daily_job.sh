#!/bin/bash
export PYTHONPATH=/app

spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,com.clickhouse:clickhouse-jdbc:0.4.6 \
  /app/batch_processing/daily_job.py