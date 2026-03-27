#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="${IMAGE:-tdengine/tdengine}"
CONTAINER_NAME="${CONTAINER_NAME:-traj-store-pg-tdengine-dev}"
DB_NAME="${DB_NAME:-traj_store_pg_td}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-taosdata}"
HTTP_PORT="${HTTP_PORT:-16041}"
ROOT_DIR="${ROOT_DIR:-${PROJECT_DIR}/data/tdengine/dev}"
DATA_DIR="${DATA_DIR:-${ROOT_DIR}/data}"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/logs}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

validate_identifier() {
  local value="$1"
  if [[ ! "${value}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "invalid TDengine identifier: ${value}" >&2
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
  echo "use docker/14_stop_tdengine.sh first, or choose another CONTAINER_NAME" >&2
  exit 1
fi

mkdir -p "${DATA_DIR}" "${LOG_DIR}"
chmod 777 "${ROOT_DIR}" "${DATA_DIR}" "${LOG_DIR}" 2>/dev/null || true

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "pulling image ${IMAGE}"
  docker pull "${IMAGE}"
fi

docker run -d --rm \
  --name "${CONTAINER_NAME}" \
  -p "${HTTP_PORT}:6041" \
  -v "${DATA_DIR}:/var/lib/taos" \
  -v "${LOG_DIR}:/var/log/taos" \
  "${IMAGE}"

cat <<EOF
started TDengine container
container_name=${CONTAINER_NAME}
image=${IMAGE}
db_name=${DB_NAME}
db_user=${DB_USER}
http_port=${HTTP_PORT}
root_dir=${ROOT_DIR}
data_dir=${DATA_DIR}
log_dir=${LOG_DIR}

next steps:
  ${SCRIPT_DIR}/12_wait_tdengine_ready.sh
  ${SCRIPT_DIR}/13_init_tdengine.sh
EOF
