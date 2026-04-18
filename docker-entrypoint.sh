#!/bin/sh
set -e

echo "Running database migrations..."
python run_migrations.py

echo "Starting application..."
exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port "${APP_PORT:-8000}" \
    --workers 1
