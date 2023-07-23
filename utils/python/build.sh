#!/usr/bin/env bash

cd /opt
mkdir /opt/python
mkdir /opt/python/python-3.11.1
curl  https://www.python.org/ftp/python/3.11.1/Python-3.11.1.tgz --output /opt/python/python-3.11.1/source.tgz
tar xvf source.tgz 
apt-get update \
    && apt-get upgrade -y \ 
    && apt-get install -y build-essential gdb lcov pkg-config libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev libncurses5-dev libreadline6-dev libsqlite3-dev libssl-dev lzma lzma-dev tk-dev uuid-dev zlib1g-dev

cd Python-3.11.1
./configure --enable-optimizations --prefix=/opt/python/python-3.11.1
make -j 4
make altinstall
rm -rf /opt/python/python-3.11.1/source.tgz && rm -rf /opt/python/python-3.11.1/Python-3.11.1

touch ~/.bashrc
echo 'export PATH="/opt/python/python-3.11.1/bin:$PATH"' >> ~/.bashrc
echo export 'PYTHONPATH=/opt/python/python-3.11.1/bin/python3.11' >> ~/.bashrc
source ~/.bashrc