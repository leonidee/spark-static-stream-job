#!/usr/bin/env bash

kafkacat -b 158.160.78.165:9092 \
    -t base -P -K: