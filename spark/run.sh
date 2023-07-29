#!/usr/bin/env bash

cd /app

source $(poetry env info --path)/bin/activate

/opt/bitnami/spark/bin/spark-submit \
    --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /app/jobs/$1