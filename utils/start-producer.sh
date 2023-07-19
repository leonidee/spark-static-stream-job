#!/usr/bin/env bash

kafkacat -b 158.160.6.228:9092 \
    -t base -P -K: