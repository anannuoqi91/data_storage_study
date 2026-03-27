#!/usr/bin/env bash
set -euo pipefail

POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:17}"
TDENGINE_IMAGE="${TDENGINE_IMAGE:-tdengine/tdengine}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

ensure_image() {
  local image="$1"
  if docker image inspect "${image}" >/dev/null 2>&1; then
    echo "image already present: ${image}"
    return
  fi
  echo "pulling ${image}"
  docker pull "${image}"
}

require_cmd docker

ensure_image "${POSTGRES_IMAGE}"
ensure_image "${TDENGINE_IMAGE}"

echo "docker images are ready"
echo "postgres_image=${POSTGRES_IMAGE}"
echo "tdengine_image=${TDENGINE_IMAGE}"
