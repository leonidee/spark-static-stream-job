#!/usr/bin/env bash

kafkacat -b $(curl -s ipinfo.io/ip):9092 \
    -t $1 -C -o begining \
    -f 'Message Key: %k\nMessage Value: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'