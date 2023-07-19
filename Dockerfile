# https://github.com/bitnami/containers/tree/main/bitnami/spark
FROM docker.io/bitnami/spark:3.4.1 

USER root
WORKDIR /code

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y git curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry $(which python3) -

ENV PATH="/opt/poetry/bin:$PATH"

# Install additional jars
# USER 1001
# RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar

# USER root
