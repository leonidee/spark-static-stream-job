#!/usr/bin/env bash

# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
#     -X security.protocol=SASL_SSL \
#     -X sasl.mechanisms=SCRAM-SHA-512 \
#     -X sasl.username="kafka-admin" \
#     -X sasl.password="de-kafka-admin-2022" \
#     -X ssl.ca.location=/home/leonide/code/spark-static-stream-job/CA.pem \
#     -C -t student.topic.cohort12.leonidgrishenkov -o end \
#     -f 'Message Key: %k\nMessage Value: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'



kafkacat -b 158.160.78.165:9092 \
    -t base -C -o begining \
    -f 'Message Key: %k\nMessage Value: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'