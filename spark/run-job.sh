#!/usr/bin/env bash

JOB=$1

cd /app

source $(poetry env info --path)/bin/activate

/opt/bitnami/spark/bin/spark-submit $JOB