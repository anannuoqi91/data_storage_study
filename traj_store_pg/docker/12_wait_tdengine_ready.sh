#!/usr/bin/env bash
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-16041}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-taosdata}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-60}"
HOST="${HOST:-127.0.0.1}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd curl
require_cmd grep

deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if output="$(curl -fsS --user "${DB_USER}:${DB_PASSWORD}" \
    -H 'Content-Type: text/plain; charset=utf-8' \
    --data 'SHOW DATABASES' \
    "http://${HOST}:${HTTP_PORT}/rest/sql" 2>/dev/null)"; then
    if echo "${output}" | grep -Eq '"code"[[:space:]]*:[[:space:]]*0|"status"[[:space:]]*:[[:space:]]*"succ"'; then
      echo "TDengine is ready on http://${HOST}:${HTTP_PORT}"
      exit 0
    fi
  fi
  sleep 1
done

echo "TDengine was not ready within ${READY_TIMEOUT_SECONDS} seconds" >&2
exit 1
