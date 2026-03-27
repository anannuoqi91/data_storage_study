#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="${SCRIPT_DIR}/sql/tdengine"

DB_NAME="${DB_NAME:-traj_store_pg_td}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-taosdata}"
HTTP_PORT="${HTTP_PORT:-16041}"
HOST="${HOST:-127.0.0.1}"

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

apply_sql_file() {
  local sql_path="$1"
  python3 - "${HOST}" "${HTTP_PORT}" "${DB_USER}" "${DB_PASSWORD}" "${DB_NAME}" "${sql_path}" <<'PY2'
import base64
import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

host, port, user, password, db_name, sql_path = sys.argv[1:]
text = Path(sql_path).read_text(encoding='utf-8').replace('__DB_NAME__', db_name)
lines = []
for line in text.splitlines():
    if line.strip().startswith('--'):
        continue
    lines.append(line)
statements = [stmt.strip() for stmt in '\n'.join(lines).split(';') if stmt.strip()]
token = base64.b64encode(f'{user}:{password}'.encode('utf-8')).decode('ascii')
headers = {
    'Authorization': f'Basic {token}',
    'Content-Type': 'text/plain; charset=utf-8',
}
for index, stmt in enumerate(statements, 1):
    request = Request(
        f'http://{host}:{port}/rest/sql',
        data=stmt.encode('utf-8'),
        headers=headers,
        method='POST',
    )
    try:
        with urlopen(request, timeout=30.0) as response:
            body = response.read().decode('utf-8')
    except HTTPError as exc:
        body = exc.read().decode('utf-8', errors='replace')
        raise SystemExit(f'TDengine HTTP {exc.code} applying statement {index} from {sql_path}: {body}')
    except URLError as exc:
        raise SystemExit(f'TDengine request failed applying statement {index} from {sql_path}: {exc}')

    payload = json.loads(body)
    if isinstance(payload, dict) and payload.get('code') not in (None, 0):
        raise SystemExit(f'TDengine error applying statement {index} from {sql_path}: {payload}')
    print(f'applied {Path(sql_path).name} statement {index}')
PY2
}

require_cmd python3
validate_identifier "${DB_NAME}"

if [[ ! -f "${SQL_DIR}/01_create_database.sql" || ! -f "${SQL_DIR}/02_create_layout_tables.sql" ]]; then
  echo "missing TDengine SQL templates under ${SQL_DIR}" >&2
  exit 1
fi

"${SCRIPT_DIR}/12_wait_tdengine_ready.sh"

apply_sql_file "${SQL_DIR}/01_create_database.sql"
apply_sql_file "${SQL_DIR}/02_create_layout_tables.sql"

echo "database and stables initialized for ${DB_NAME}"
