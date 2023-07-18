#!/usr/bin/env bash

docker rmi spark-static-stream-job-spark-master \
    && docker rmi spark-static-stream-job-spark-worker-1 \
    && docker rmi spark-static-stream-job-spark-worker-2