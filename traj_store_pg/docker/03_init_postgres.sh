#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${SCRIPT_DIR}/sql/postgres"
SCHEMA_DIR="${PROJECT_DIR}/schema/postgres/0001_init"

CONTAINER_NAME="${CONTAINER_NAME:-traj-store-pg-postgres-dev}"
DB_NAME="${DB_NAME:-traj_store_pg}"
DB_USER="${DB_USER:-bench}"
POSTGRES_PROFILE="${POSTGRES_PROFILE:-default}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

validate_identifier() {
  local value="$1"
  if [[ ! "${value}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "invalid PostgreSQL identifier: ${value}" >&2
    exit 1
  fi
}

validate_literal() {
  local value="$1"
  if [[ ! "${value}" =~ ^[A-Za-z0-9_]+$ ]]; then
    echo "invalid SQL literal token: ${value}" >&2
    exit 1
  fi
}

detect_toast_compression() {
  docker exec -i "${CONTAINER_NAME}" psql -At -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_settings WHERE name = 'default_toast_compression' AND 'lz4' = ANY(enumvals)) THEN 'lz4' ELSE current_setting('default_toast_compression') END"
}

apply_sql_file() {
  local sql_path="$1"
  local toast_compression="${2:-}"

  if grep -q '__TOAST_COMPRESSION__' "${sql_path}"; then
    validate_literal "${toast_compression}"
    sed "s/__TOAST_COMPRESSION__/${toast_compression}/g" "${sql_path}" |       docker exec -i "${CONTAINER_NAME}" psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}"
    return
  fi

  docker exec -i "${CONTAINER_NAME}" psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}" < "${sql_path}"
}

require_cmd docker
require_cmd grep
require_cmd sed
validate_identifier "${DB_NAME}"
validate_identifier "${DB_USER}"

case "${POSTGRES_PROFILE}" in
  default)
    SQL_TEMPLATE="${SQL_DIR}/01_create_tables.sql"
    POSTGRES_WAL_MODE="data_dir"
    ;;
  bench_low_wal_compressed)
    SQL_TEMPLATE="${SQL_DIR}/02_create_tables_bench_low_wal.sql"
    POSTGRES_WAL_MODE="tmpfs"
    ;;
  *)
    echo "unsupported POSTGRES_PROFILE: ${POSTGRES_PROFILE}" >&2
    echo "supported values: default, bench_low_wal_compressed" >&2
    exit 1
    ;;
esac

if [[ ! -f "${SQL_TEMPLATE}" || ! -f "${SCHEMA_DIR}/up.sql" ]]; then
  echo "missing PostgreSQL SQL templates under ${SQL_DIR} or ${SCHEMA_DIR}" >&2
  exit 1
fi

"${SCRIPT_DIR}/02_wait_postgres_ready.sh"

toast_compression="$(detect_toast_compression)"
validate_literal "${toast_compression}"

echo "applying PostgreSQL schema"
echo "postgres_profile=${POSTGRES_PROFILE}"
echo "postgres_wal_mode=${POSTGRES_WAL_MODE}"
echo "postgres_toast_compression=${toast_compression}"
apply_sql_file "${SQL_TEMPLATE}" "${toast_compression}"

echo "public tables:"
docker exec -i "${CONTAINER_NAME}" psql -At -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"

echo "table persistence:"
docker exec -i "${CONTAINER_NAME}" psql -At -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT relname || ':' || CASE relpersistence WHEN 'p' THEN 'permanent' WHEN 'u' THEN 'unlogged' WHEN 't' THEN 'temporary' ELSE relpersistence::text END FROM pg_class WHERE relname IN ('ingest_batches', 'trace_latest_state') ORDER BY relname"

echo "database and tables initialized for ${DB_NAME}"
