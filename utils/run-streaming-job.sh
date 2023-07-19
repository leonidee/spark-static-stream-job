#!/usr/bin/env bash

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1.jar,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1.jar /code/src/spark/stream.py

# curl https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.13/3.4.1/spark-streaming-kafka-0-10_2.13-3.4.1.jar --output /opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.13-3.4.1.jar
# curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.4.1/spark-sql-kafka-0-10_2.13-3.4.1.jar --output /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.4.1.jar
