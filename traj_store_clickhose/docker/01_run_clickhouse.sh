#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="${IMAGE:-clickhouse/clickhouse-server:25.8}"
CONTAINER_NAME="${CONTAINER_NAME:-traj-store-clickhose-dev}"
DB_NAME="${DB_NAME:-traj_store_clickhose}"
DB_USER="${DB_USER:-bench}"
DB_PASSWORD="${DB_PASSWORD:-benchpass}"
HTTP_PORT="${HTTP_PORT:-18123}"
NATIVE_PORT="${NATIVE_PORT:-19000}"
DATA_DIR="${DATA_DIR:-${PROJECT_DIR}/data/clickhouse/dev/store}"
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/data/clickhouse/dev/logs}"

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

require_cmd docker
validate_identifier "${DB_NAME}"

if [[ -z "${DB_USER}" || -z "${DB_PASSWORD}" ]]; then
  echo "DB_USER and DB_PASSWORD must not be empty" >&2
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "container already exists: ${CONTAINER_NAME}" >&2
  echo "use docker/04_stop_clickhouse.sh first, or choose another CONTAINER_NAME" >&2
  exit 1
fi

mkdir -p "${DATA_DIR}" "${LOG_DIR}"
chmod 777 "${DATA_DIR}" "${LOG_DIR}"

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "pulling image ${IMAGE}"
  docker pull "${IMAGE}"
fi

docker run -d --rm \
  --name "${CONTAINER_NAME}" \
  --ulimit nofile=262144:262144 \
  -e "CLICKHOUSE_DB=${DB_NAME}" \
  -e "CLICKHOUSE_USER=${DB_USER}" \
  -e "CLICKHOUSE_PASSWORD=${DB_PASSWORD}" \
  -e "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1" \
  -p "${HTTP_PORT}:8123" \
  -p "${NATIVE_PORT}:9000" \
  -v "${DATA_DIR}:/var/lib/clickhouse" \
  -v "${LOG_DIR}:/var/log/clickhouse-server" \
  "${IMAGE}"

cat <<EOF
started ClickHouse container
container_name=${CONTAINER_NAME}
image=${IMAGE}
db_name=${DB_NAME}
db_user=${DB_USER}
http_port=${HTTP_PORT}
native_port=${NATIVE_PORT}
data_dir=${DATA_DIR}
log_dir=${LOG_DIR}

next steps:
  ${SCRIPT_DIR}/02_wait_clickhouse_ready.sh
  ${SCRIPT_DIR}/03_init_database.sh
EOF
