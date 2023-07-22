#!/usr/bin/env bash

docker build -t  kafka -f ./docker/kafka/Dockerfile .

docker run --rm -it --name kafka \
    --volume ./config/kafka/log4j.properties:/opt/bitnami/kafka/config/log4j.properties \
    -p 9092:9092 \
    kafka
