#!/usr/bin/env bash

docker build -t producer -f ./docker/producer/Dockerfile .

docker run --rm -it \
    --name producer \
    --volume ./src:/app/src \
    --volume ./.env:/app/.env \
    --volume ./producer:/app/producer \
    --volume ./config.yaml:/app/config.yaml \
    producer /app/producer/run.sh
