#!/usr/bin/env bash
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18123}"
DB_USER="${DB_USER:-bench}"
DB_PASSWORD="${DB_PASSWORD:-benchpass}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-60}"
HOST="${HOST:-127.0.0.1}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd curl

deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if output="$(curl -fsS --user "${DB_USER}:${DB_PASSWORD}" "http://${HOST}:${HTTP_PORT}/?query=SELECT%201" 2>/dev/null)"; then
    if [[ "${output}" == "1" ]]; then
      echo "ClickHouse is ready on http://${HOST}:${HTTP_PORT}"
      exit 0
    fi
  fi
  sleep 1
done

echo "ClickHouse was not ready within ${READY_TIMEOUT_SECONDS} seconds" >&2
exit 1
