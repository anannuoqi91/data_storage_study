#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="${IMAGE:-postgres:17}"
CONTAINER_NAME="${CONTAINER_NAME:-traj-store-pg-postgres-dev}"
DB_NAME="${DB_NAME:-traj_store_pg}"
DB_USER="${DB_USER:-bench}"
DB_PASSWORD="${DB_PASSWORD:-benchpass}"
POSTGRES_PORT="${POSTGRES_PORT:-15432}"
POSTGRES_PROFILE="${POSTGRES_PROFILE:-default}"
POSTGRES_WAL_TMPFS_SIZE="${POSTGRES_WAL_TMPFS_SIZE:-512m}"
POSTGRES_WAL_TMPFS_DIR="${POSTGRES_WAL_TMPFS_DIR:-/var/lib/postgresql/wal_tmpfs}"
ROOT_DIR="${ROOT_DIR:-${PROJECT_DIR}/data/postgres/dev}"
DATA_DIR="${DATA_DIR:-${ROOT_DIR}/data}"

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
    echo "only letters, digits, and underscores are allowed, and the first character must be a letter or underscore" >&2
    exit 1
  fi
}

require_cmd docker
validate_identifier "${DB_NAME}"
validate_identifier "${DB_USER}"

if [[ -z "${DB_PASSWORD}" ]]; then
  echo "DB_PASSWORD must not be empty" >&2
  exit 1
fi

case "${POSTGRES_PROFILE}" in
  default)
    POSTGRES_WAL_MODE="data_dir"
    docker_extra_args=()
    postgres_server_args=()
    ;;
  bench_low_wal_compressed)
    POSTGRES_WAL_MODE="tmpfs"
    docker_extra_args=(
      -e "POSTGRES_INITDB_WALDIR=${POSTGRES_WAL_TMPFS_DIR}"
      --tmpfs "${POSTGRES_WAL_TMPFS_DIR}:rw,size=${POSTGRES_WAL_TMPFS_SIZE}"
    )
    postgres_server_args=(
      -c wal_level=minimal
      -c wal_compression=on
      -c synchronous_commit=off
      -c archive_mode=off
      -c max_wal_senders=0
      -c autovacuum=off
    )
    ;;
  *)
    echo "unsupported POSTGRES_PROFILE: ${POSTGRES_PROFILE}" >&2
    echo "supported values: default, bench_low_wal_compressed" >&2
    exit 1
    ;;
esac

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "container already exists: ${CONTAINER_NAME}" >&2
  echo "use docker/04_stop_postgres.sh first, or choose another CONTAINER_NAME" >&2
  exit 1
fi

mkdir -p "${DATA_DIR}"
chmod 777 "${ROOT_DIR}" "${DATA_DIR}" 2>/dev/null || true

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "pulling image ${IMAGE}"
  docker pull "${IMAGE}"
fi

docker run -d --rm   --name "${CONTAINER_NAME}"   -e "POSTGRES_DB=${DB_NAME}"   -e "POSTGRES_USER=${DB_USER}"   -e "POSTGRES_PASSWORD=${DB_PASSWORD}"   -p "${POSTGRES_PORT}:5432"   -v "${DATA_DIR}:/var/lib/postgresql/data"   "${docker_extra_args[@]}"   "${IMAGE}"   "${postgres_server_args[@]}"

cat <<EOF
started PostgreSQL container
container_name=${CONTAINER_NAME}
image=${IMAGE}
db_name=${DB_NAME}
db_user=${DB_USER}
postgres_port=${POSTGRES_PORT}
postgres_profile=${POSTGRES_PROFILE}
postgres_wal_mode=${POSTGRES_WAL_MODE}
postgres_wal_tmpfs_dir=${POSTGRES_WAL_TMPFS_DIR}
root_dir=${ROOT_DIR}
data_dir=${DATA_DIR}

next steps:
  ${SCRIPT_DIR}/02_wait_postgres_ready.sh
  ${SCRIPT_DIR}/03_init_postgres.sh
EOF
