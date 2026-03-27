#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONTAINER_NAME="${CONTAINER_NAME:-traj-store-clickhose-dev}"
DB_NAME="${DB_NAME:-traj_store_clickhose}"
DB_USER="${DB_USER:-bench}"
DB_PASSWORD="${DB_PASSWORD:-benchpass}"
SMOKE_LAYOUT="${SMOKE_LAYOUT:-compact}"
SMOKE_TRACE_ID="${SMOKE_TRACE_ID:-}"

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

validate_uint32() {
  local value="$1"
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    echo "SMOKE_TRACE_ID must be an unsigned integer" >&2
    exit 1
  fi
  if (( 10#${value} > 4294967295 )); then
    echo "SMOKE_TRACE_ID must fit in UInt32" >&2
    exit 1
  fi
}

clickhouse_query() {
  local query="$1"
  docker exec -i "${CONTAINER_NAME}" clickhouse-client \
    --user "${DB_USER}" \
    --password "${DB_PASSWORD}" \
    --query "${query}"
}

require_cmd docker
validate_identifier "${DB_NAME}"

if [[ -z "${DB_USER}" || -z "${DB_PASSWORD}" ]]; then
  echo "DB_USER and DB_PASSWORD must not be empty" >&2
  exit 1
fi

if [[ -z "${SMOKE_TRACE_ID}" ]]; then
  SMOKE_TRACE_ID=$(( (10#$(date +%s%N)) % 4000000000 ))
fi
validate_uint32 "${SMOKE_TRACE_ID}"

case "${SMOKE_LAYOUT}" in
  compact)
    TABLE_NAME="box_info_compact_zstd"
    INSERT_SQL=$(cat <<SQL
INSERT INTO ${DB_NAME}.${TABLE_NAME} (
    trace_id,
    sample_date,
    sample_hour,
    sample_offset_ms,
    obj_type,
    position_x_mm,
    position_y_mm,
    position_z_mm,
    length_mm,
    width_mm,
    height_mm,
    speed_centi_kmh,
    spindle_centi_deg,
    lane_id,
    frame_id
) VALUES
    (${SMOKE_TRACE_ID}, '2025-03-14', 12, 12345, 1, 1234, 5678, 90, 4500, 1800, 1600, 3250, 9000, 2, 1001),
    (${SMOKE_TRACE_ID}, '2025-03-14', 12, 12445, 1, 1240, 5680, 90, 4500, 1800, 1600, 3260, 9010, 2, 1002)
SQL
)
    ;;
  raw)
    TABLE_NAME="box_info_raw_lz4"
    INSERT_SQL=$(cat <<SQL
INSERT INTO ${DB_NAME}.${TABLE_NAME} (
    trace_id,
    sample_timestamp,
    obj_type,
    position_x,
    position_y,
    position_z,
    length,
    width,
    height,
    speed_kmh,
    spindle,
    lane_id,
    frame_id
) VALUES
    (${SMOKE_TRACE_ID}, '2025-03-14 12:00:12.345', 1, 1.234, 5.678, 0.09, 4.5, 1.8, 1.6, 32.5, 90.0, 2, 1001),
    (${SMOKE_TRACE_ID}, '2025-03-14 12:00:12.445', 1, 1.240, 5.680, 0.09, 4.5, 1.8, 1.6, 32.6, 90.1, 2, 1002)
SQL
)
    ;;
  *)
    echo "unsupported SMOKE_LAYOUT: ${SMOKE_LAYOUT}" >&2
    echo "expected one of: compact, raw" >&2
    exit 1
    ;;
esac

"${SCRIPT_DIR}/02_wait_clickhouse_ready.sh"

if [[ "$(clickhouse_query "EXISTS TABLE ${DB_NAME}.${TABLE_NAME}")" != "1" ]]; then
  echo "table does not exist: ${DB_NAME}.${TABLE_NAME}" >&2
  echo "run docker/03_init_database.sh first" >&2
  exit 1
fi

docker exec -i "${CONTAINER_NAME}" clickhouse-client \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --multiquery <<SQL
${INSERT_SQL};
SQL

result="$(clickhouse_query "SELECT count(), min(frame_id), max(frame_id) FROM ${DB_NAME}.${TABLE_NAME} WHERE trace_id = ${SMOKE_TRACE_ID} FORMAT TSVRaw")"
IFS=$'	' read -r row_count min_frame max_frame <<< "${result}"

if [[ "${row_count}" != "2" || "${min_frame}" != "1001" || "${max_frame}" != "1002" ]]; then
  echo "smoke validation failed for ${DB_NAME}.${TABLE_NAME}" >&2
  echo "trace_id=${SMOKE_TRACE_ID} result=${result}" >&2
  exit 1
fi

echo "smoke validation passed"
echo "db_name=${DB_NAME}"
echo "table_name=${TABLE_NAME}"
echo "layout=${SMOKE_LAYOUT}"
echo "trace_id=${SMOKE_TRACE_ID}"
echo "rows=${row_count}"
echo "frame_range=${min_frame}-${max_frame}"
