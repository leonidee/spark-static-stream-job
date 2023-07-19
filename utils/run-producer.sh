#!/usr/bin/env bash

if [[ -n "$VIRTUAL_ENV" ]]; then
    :
else
    poetry shell
fi

exec python src/producer/producer.py 