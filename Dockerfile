FROM docker.io/bitnami/spark:3.4.1

USER root
WORKDIR /code

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

