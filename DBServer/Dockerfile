FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV DBSERVER_ROOT=/data
ENV DBSERVER_API_HOST=0.0.0.0
ENV DBSERVER_API_PORT=8080
ENV DBSERVER_ENABLE_MAINTENANCE=true
ENV DBSERVER_ENABLE_CYBER_INGEST=false
ENV DBSERVER_CYBER_DEVICE_IDS=1,2,3,4
ENV DBSERVER_CYBER_CHANNEL_TEMPLATE=omnisense/test/{device_id}/boxes
ENV DBSERVER_CYBER_NODE_NAME=dbserver_cyber_ingest
ENV DBSERVER_ENABLE_JOB_WORKER=true
ENV DBSERVER_ENABLE_EXPORT_WORKER=false

WORKDIR /app

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin dbserver

COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts

RUN python -m pip install --upgrade pip \
    && python -m pip install -e ".[duckdb,api,cyber]" \
    && chmod +x /app/scripts/docker-entrypoint.sh

USER dbserver

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3)" || exit 1

ENTRYPOINT ["/app/scripts/docker-entrypoint.sh"]
