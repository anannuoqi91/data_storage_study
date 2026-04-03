#!/bin/sh
set -eu

ROOT="${DBSERVER_ROOT:-/data}"
HOST="${DBSERVER_API_HOST:-0.0.0.0}"
PORT="${DBSERVER_API_PORT:-8080}"
LOG_LEVEL="${DBSERVER_LOG_LEVEL:-info}"

mkdir -p "$ROOT"

exec dbserver-api \
  --root "$ROOT" \
  --host "$HOST" \
  --port "$PORT" \
  --log-level "$LOG_LEVEL"
