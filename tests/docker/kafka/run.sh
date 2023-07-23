#!/usr/bin/env bash

docker run --rm -it --name kafka \
    --volume ./config/kafka/log4j.properties:/opt/bitnami/kafka/config/log4j.properties \
    -p 9092:9092 \
    -e ALLOW_PLAINTEXT_LISTENER=yes \
    -e KAFKA_CFG_BROKER_ID=1 \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://$(curl -s ipinfo.io/ip):9092 \
    -e KAFKA_CFG_DEFAULT_REPLICATION_FACTOR=1 \
    bitnami/kafka:3.4
