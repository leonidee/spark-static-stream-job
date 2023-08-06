# https://github.com/bitnami/containers/tree/main/bitnami/spark
FROM docker.io/bitnami/spark:3.4.1-debian-11-r26

USER root
WORKDIR /app

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl build-essential gdb lcov pkg-config libbz2-dev \
    libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev libncurses5-dev \
    libreadline6-dev libsqlite3-dev libssl-dev lzma lzma-dev tk-dev uuid-dev zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build python from source
RUN mkdir /opt/python && mkdir /opt/python/python-3.11.1 \ 
    && curl https://www.python.org/ftp/python/3.11.1/Python-3.11.1.tgz --output /opt/python/python-3.11.1/source.tgz \
    && tar xvf /opt/python/python-3.11.1/source.tgz -C /opt/python/python-3.11.1 \
    && cd /opt/python/python-3.11.1/Python-3.11.1 \
    && ./configure --enable-optimizations --prefix=/opt/python/python-3.11.1 \
    && make -j 4 && make altinstall \
    && rm -rf /opt/python/python-3.11.1/source.tgz && rm -rf /opt/python/python-3.11.1/Python-3.11.1

ENV PATH="/opt/python/python-3.11.1/bin:$PATH" \
    PYTHONPATH=/opt/python/python-3.11.1/bin/python3.11

# Install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry /opt/python/python-3.11.1/bin/python3.11 -

ENV PATH="/opt/poetry/bin:$PATH"

# Install dependensies
COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock

RUN cd /app \
    && poetry env use /opt/python/python-3.11.1/bin/python3.11 \
    && source $(poetry env info --path)/bin/activate \
    && poetry install

# Create path for Spark checkpoints
# RUN mkdir /app/checkpoints
