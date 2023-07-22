#!/usr/bin/env bash

docker exec -it kafka \
    /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic $1