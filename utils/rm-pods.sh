#!/usr/bin/env bash

docker rm spark-master \
    && docker rm spark-worker-1 \
    && docker rm spark-worker-2 \
    && docker rm spark-worker-3 \
    && docker rm spark-worker-4 \
    && docker rm kafka 