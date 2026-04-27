#!/bin/sh
set -e

PUID=${PUID:-1000}
PGID=${PGID:-1000}

# Create group if it doesn't already exist with this GID
if ! getent group "$PGID" > /dev/null 2>&1; then
    groupadd -g "$PGID" appgroup
fi

# Create user if it doesn't already exist with this UID
if ! getent passwd "$PUID" > /dev/null 2>&1; then
    useradd -u "$PUID" -g "$PGID" -d /app -s /sbin/nologin -M appuser
fi

# Ensure the data directory is owned by the target user
chown -R "$PUID:$PGID" /app/data

echo "Running database migrations as uid=${PUID} gid=${PGID}..."
gosu "$PUID:$PGID" python run_migrations.py

echo "Starting application as uid=${PUID} gid=${PGID}..."
exec gosu "$PUID:$PGID" uvicorn app.main:app \
    --host 0.0.0.0 \
    --port "${APP_PORT:-2112}" \
    --workers 1
