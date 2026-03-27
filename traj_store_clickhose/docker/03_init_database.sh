#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="${SCRIPT_DIR}/sql"

CONTAINER_NAME="${CONTAINER_NAME:-traj-store-clickhose-dev}"
DB_NAME="${DB_NAME:-traj_store_clickhose}"
DB_USER="${DB_USER:-bench}"
DB_PASSWORD="${DB_PASSWORD:-benchpass}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

validate_identifier() {
  local value="$1"
  if [[ ! "${value}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "invalid ClickHouse identifier: ${value}" >&2
    echo "only letters, digits, and underscores are allowed, and the first character must be a letter or underscore" >&2
    exit 1
  fi
}

render_sql() {
  local sql_path="$1"
  sed "s/__DB_NAME__/${DB_NAME}/g" "${sql_path}"
}

apply_sql() {
  local sql_path="$1"
  echo "applying $(basename "${sql_path}")"
  render_sql "${sql_path}" | docker exec -i "${CONTAINER_NAME}" clickhouse-client \
    --user "${DB_USER}" \
    --password "${DB_PASSWORD}" \
    --multiquery
}

require_cmd docker
require_cmd sed
validate_identifier "${DB_NAME}"

if [[ -z "${DB_USER}" || -z "${DB_PASSWORD}" ]]; then
  echo "DB_USER and DB_PASSWORD must not be empty" >&2
  exit 1
fi

if [[ ! -f "${SQL_DIR}/01_create_database.sql" || ! -f "${SQL_DIR}/02_create_layout_tables.sql" ]]; then
  echo "missing SQL templates under ${SQL_DIR}" >&2
  exit 1
fi

"${SCRIPT_DIR}/02_wait_clickhouse_ready.sh"

apply_sql "${SQL_DIR}/01_create_database.sql"
apply_sql "${SQL_DIR}/02_create_layout_tables.sql"

docker exec -i "${CONTAINER_NAME}" clickhouse-client \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --query "SHOW TABLES FROM ${DB_NAME}"

echo "database and tables initialized for ${DB_NAME}"
