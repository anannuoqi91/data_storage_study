#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-traj-store-pg-postgres-dev}"
DB_NAME="${DB_NAME:-traj_store_pg}"
DB_USER="${DB_USER:-bench}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-60}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd docker

deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if docker exec "${CONTAINER_NAME}" pg_isready -q -U "${DB_USER}" -d "${DB_NAME}"; then
    echo "PostgreSQL is ready in container ${CONTAINER_NAME}"
    exit 0
  fi
  sleep 1
done

echo "PostgreSQL was not ready within ${READY_TIMEOUT_SECONDS} seconds" >&2
exit 1
