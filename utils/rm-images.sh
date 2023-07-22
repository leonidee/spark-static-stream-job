#!/usr/bin/env bash


docker rmi --force spark-static-stream-job-spark-master \
    && docker rmi --force spark-static-stream-job-spark-worker-1 \
    && docker rmi --force spark-static-stream-job-spark-worker-2 \
    && docker rmi --force spark-static-stream-job-spark-worker-3 \
    && docker rmi --force spark-static-stream-job-spark-worker-4 \
    && docker rmi --force kafka

