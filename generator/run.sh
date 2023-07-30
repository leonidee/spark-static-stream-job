#!/usr/bin/env bash

cd /app
source $(poetry env info --path)/bin/activate
python /app/generator/__init__.py