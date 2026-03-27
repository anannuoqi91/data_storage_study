#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="${IMAGE:-clickhouse/clickhouse-server:25.8}"
CONTAINER_NAME="${CONTAINER_NAME:-traj-store-clickhose-dev}"
ROOT_DIR="${ROOT_DIR:-${PROJECT_DIR}/data/clickhouse/dev}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd docker

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "stopping ${CONTAINER_NAME}"
  docker stop -t 5 "${CONTAINER_NAME}" >/dev/null
else
  echo "container not found: ${CONTAINER_NAME}"
fi

if [[ -d "${ROOT_DIR}" ]] && docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  uid="$(id -u)"
  gid="$(id -g)"
  docker run --rm \
    -v "${ROOT_DIR}:/work" \
    --entrypoint /bin/sh \
    "${IMAGE}" \
    -c "chown -R ${uid}:${gid} /work && chmod -R a+rX /work" \
    >/dev/null 2>&1 || true
fi

echo "stopped ${CONTAINER_NAME} and normalized permissions under ${ROOT_DIR}"
