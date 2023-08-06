#!/usr/bin/env bash

cd /app
source $(poetry env info --path)/bin/activate
python /app/producer/__init__.py
