# https://github.com/bitnami/containers/tree/main/bitnami/spark
FROM docker.io/bitnami/spark:3.4.1 

USER root
WORKDIR /code

COPY ./pyproject.toml /code/pyproject.toml
COPY ./poetry.lock /code/poetry.lock

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y git curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry $(which python3) -

ENV PATH="/opt/poetry/bin:$PATH"

RUN poetry env use $(which python3) \
    && poetry install
