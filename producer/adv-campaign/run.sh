#!/usr/bin/env bash

# sleep 30s # Workaround for issue when producer starting before kafka is ready. ~20-30s will be enough
cd /app
source $(poetry env info --path)/bin/activate
python /app/producer/__init__.py
