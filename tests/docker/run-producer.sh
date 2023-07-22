#!/usr/bin/env bash

docker build -t producer -f ./docker/producer/Dockerfile .

docker run --rm -it \
    --name producer-test \
    --volume ./src:/app/src \
    --volume ./.env:/app/.env \
    --volume ./utils:/app/utils \
    producer /bin/bash
