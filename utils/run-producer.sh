#!/usr/bin/env bash

if [[ -n "$VIRTUAL_ENV" ]]; then
    :
else
    poetry shell
fi

python /app/src/producer/producer.py 
