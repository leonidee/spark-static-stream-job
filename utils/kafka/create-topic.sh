#!/usr/bin/env bash

docker exec -it kafka \
    /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --topic $1 --create \
    --partitions 3 --replication-factor 1
