FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    cron \
    tzdata \
    gosu \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data/logs && \
    mkdir -p /app/data/m3u && \
    mkdir -p /app/data/vod && \
    mkdir -p /app/data/vod/movies && \
    mkdir -p /app/data/vod/series && \
    mkdir -p /app/data/vod/livetv && \
    mkdir -p /app/data/vod/unsorted && \
    mkdir -p /app/data/vod-offline

COPY . .

RUN chmod +x docker-entrypoint.sh

EXPOSE 2112

ENTRYPOINT ["./docker-entrypoint.sh"]
