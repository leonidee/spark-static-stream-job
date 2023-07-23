#!/usr/bin/env bash

kafkacat -b $(curl -s ipinfo.io/ip):9092 \
    -t $1 -P -K: