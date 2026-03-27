#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import socket
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import psutil
import psycopg

from ingest_common import (
    ANGLE_SCALE,
    COORDINATE_SCALE,
    NETWORK_FIELD_COUNT,
    SIZE_SCALE,
    SPEED_SCALE,
    SUPPORTED_LAYOUT_CHOICES,
    SUPPORTED_POSTGRES_PROFILE_CHOICES,
    normalize_postgres_profile,
    normalize_storage_layout,
    safe_slug,
    sink_disk_usage,
    utc_now_iso,
    write_json,
)
from tdengine_http import TDengineRestClient


POSTGRES_TABLE_NAMES = ("ingest_batches", "trace_latest_state")

POSTGRES_DEFAULT_RESET_SQL = """
DROP TABLE IF EXISTS ingest_batches;
DROP TABLE IF EXISTS trace_latest_state;

CREATE TABLE ingest_batches (
    batch_id BIGINT PRIMARY KEY,
    storage_layout TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_trace_id INTEGER NOT NULL,
    max_trace_id INTEGER NOT NULL,
    min_sample_timestamp TIMESTAMPTZ NOT NULL,
    max_sample_timestamp TIMESTAMPTZ NOT NULL,
    min_frame_id INTEGER NOT NULL,
    max_frame_id INTEGER NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE trace_latest_state (
    trace_id INTEGER PRIMARY KEY,
    last_sample_timestamp TIMESTAMPTZ NOT NULL,
    last_frame_id INTEGER NOT NULL,
    obj_type SMALLINT NOT NULL,
    lane_id SMALLINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm INTEGER NOT NULL,
    width_mm INTEGER NOT NULL,
    height_mm INTEGER NOT NULL,
    speed_centi_kmh INTEGER NOT NULL,
    spindle_centi_deg INTEGER NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

POSTGRES_LOW_WAL_RESET_SQL_TEMPLATE = """
DROP TABLE IF EXISTS ingest_batches;
DROP TABLE IF EXISTS trace_latest_state;

CREATE UNLOGGED TABLE ingest_batches (
    batch_id BIGINT PRIMARY KEY,
    storage_layout TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_trace_id INTEGER NOT NULL,
    max_trace_id INTEGER NOT NULL,
    min_sample_timestamp TIMESTAMPTZ NOT NULL,
    max_sample_timestamp TIMESTAMPTZ NOT NULL,
    min_frame_id INTEGER NOT NULL,
    max_frame_id INTEGER NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (autovacuum_enabled = false);

CREATE UNLOGGED TABLE trace_latest_state (
    trace_id INTEGER PRIMARY KEY,
    last_sample_timestamp TIMESTAMPTZ NOT NULL,
    last_frame_id INTEGER NOT NULL,
    obj_type SMALLINT NOT NULL,
    lane_id SMALLINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm INTEGER NOT NULL,
    width_mm INTEGER NOT NULL,
    height_mm INTEGER NOT NULL,
    speed_centi_kmh INTEGER NOT NULL,
    spindle_centi_deg INTEGER NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (autovacuum_enabled = false);

ALTER TABLE ingest_batches ALTER COLUMN storage_layout SET STORAGE EXTENDED;
ALTER TABLE ingest_batches ALTER COLUMN storage_layout SET COMPRESSION {toast_compression};
"""


def build_postgres_reset_sql(postgres_profile: str, *, toast_compression: str) -> str:
    if postgres_profile == "default":
        return POSTGRES_DEFAULT_RESET_SQL
    if postgres_profile == "bench_low_wal_compressed":
        return POSTGRES_LOW_WAL_RESET_SQL_TEMPLATE.format(toast_compression=toast_compression)
    raise ValueError(f"Unsupported PostgreSQL profile: {postgres_profile}")

POSTGRES_BATCH_INSERT_SQL = """
INSERT INTO ingest_batches (
    batch_id,
    storage_layout,
    row_count,
    min_trace_id,
    max_trace_id,
    min_sample_timestamp,
    max_sample_timestamp,
    min_frame_id,
    max_frame_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (batch_id) DO NOTHING
"""

POSTGRES_TRACE_UPSERT_SQL = """
INSERT INTO trace_latest_state (
    trace_id,
    last_sample_timestamp,
    last_frame_id,
    obj_type,
    lane_id,
    position_x_mm,
    position_y_mm,
    position_z_mm,
    length_mm,
    width_mm,
    height_mm,
    speed_centi_kmh,
    spindle_centi_deg,
    updated_at_utc
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trace_id) DO UPDATE SET
    last_sample_timestamp = EXCLUDED.last_sample_timestamp,
    last_frame_id = EXCLUDED.last_frame_id,
    obj_type = EXCLUDED.obj_type,
    lane_id = EXCLUDED.lane_id,
    position_x_mm = EXCLUDED.position_x_mm,
    position_y_mm = EXCLUDED.position_y_mm,
    position_z_mm = EXCLUDED.position_z_mm,
    length_mm = EXCLUDED.length_mm,
    width_mm = EXCLUDED.width_mm,
    height_mm = EXCLUDED.height_mm,
    speed_centi_kmh = EXCLUDED.speed_centi_kmh,
    spindle_centi_deg = EXCLUDED.spindle_centi_deg,
    updated_at_utc = EXCLUDED.updated_at_utc
WHERE trace_latest_state.last_sample_timestamp <= EXCLUDED.last_sample_timestamp
"""

RAW_STABLE_SQL_TEMPLATE = """
CREATE STABLE IF NOT EXISTS {database}.box_info_raw (
    ts TIMESTAMP,
    obj_type TINYINT,
    position_x FLOAT,
    position_y FLOAT,
    position_z FLOAT,
    length FLOAT,
    width FLOAT,
    height FLOAT,
    speed_kmh FLOAT,
    spindle FLOAT,
    lane_id TINYINT,
    frame_id INT
) TAGS (trace_id INT)
"""

COMPACT_STABLE_SQL_TEMPLATE = """
CREATE STABLE IF NOT EXISTS {database}.box_info_compact (
    ts TIMESTAMP,
    obj_type TINYINT,
    lane_id TINYINT,
    frame_id INT,
    position_x_mm INT,
    position_y_mm INT,
    position_z_mm INT,
    length_mm INT,
    width_mm INT,
    height_mm INT,
    speed_centi_kmh INT,
    spindle_centi_deg INT
) TAGS (trace_id INT)
"""


@dataclass
class ReceiverStats:
    start_monotonic: float = field(default_factory=time.monotonic)
    rows_received: int = 0
    rows_inserted: int = 0
    batches_received: int = 0
    tdengine_insert_calls: int = 0
    postgres_write_calls: int = 0
    bytes_received: int = 0
    peak_rss_bytes: int = 0
    peak_cpu_percent: float = 0.0
    postgres_peak_rss_bytes: int = 0
    postgres_peak_cpu_percent: float = 0.0
    tdengine_peak_rss_bytes: int = 0
    tdengine_peak_cpu_percent: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(frozen=True)
class ReceiverLayout:
    storage_layout: str
    tdengine_database: str
    tdengine_supertable: str
    tdengine_create_sql: str
    tdengine_columns: tuple[str, ...]
    logical_encoding: str
    compact: bool


class ProcessTreeMonitor:
    def __init__(self, root_pid: int):
        self.root_pid = root_pid
        self._cache: dict[int, psutil.Process] = {}
        self._cpu_count = max(psutil.cpu_count() or 1, 1)

    def _iter_processes(self) -> list[psutil.Process]:
        try:
            root = psutil.Process(self.root_pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return []

        processes = [root]
        try:
            processes.extend(root.children(recursive=True))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        deduped: dict[int, psutil.Process] = {}
        for process in processes:
            deduped[process.pid] = process
        return list(deduped.values())

    def prime(self) -> None:
        for process in self._iter_processes():
            self._cache[process.pid] = process
            try:
                process.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

    def sample(self) -> tuple[float, int]:
        cpu_total = 0.0
        rss_total = 0
        next_cache: dict[int, psutil.Process] = {}
        for process in self._iter_processes():
            cached = self._cache.get(process.pid, process)
            next_cache[process.pid] = cached
            try:
                cpu_total += cached.cpu_percent(interval=None)
                rss_total += cached.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self._cache = next_cache
        return cpu_total / self._cpu_count, rss_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synchronously receive box batches and write them into PostgreSQL + TDengine.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--storage-layout", choices=SUPPORTED_LAYOUT_CHOICES, default="pg_tdengine_compact")
    parser.add_argument("--insert-row-target", type=int, default=10000)
    parser.add_argument("--postgres-profile", choices=SUPPORTED_POSTGRES_PROFILE_CHOICES, default="default")
    parser.add_argument("--postgres-wal-mode", choices=("data_dir", "tmpfs"), default="data_dir")
    parser.add_argument("--postgres-host", default="127.0.0.1")
    parser.add_argument("--postgres-port", type=int, default=15432)
    parser.add_argument("--postgres-database", default="traj_store_pg")
    parser.add_argument("--postgres-user", default="bench")
    parser.add_argument("--postgres-password", default="benchpass")
    parser.add_argument("--postgres-data-dir", required=True)
    parser.add_argument("--postgres-container-pid", type=int, required=True)
    parser.add_argument("--tdengine-host", default="127.0.0.1")
    parser.add_argument("--tdengine-port", type=int, default=16041)
    parser.add_argument("--tdengine-database", default="traj_store_pg_td")
    parser.add_argument("--tdengine-user", default="root")
    parser.add_argument("--tdengine-password", default="taosdata")
    parser.add_argument("--tdengine-data-dir", required=True)
    parser.add_argument("--tdengine-log-dir", required=True)
    parser.add_argument("--tdengine-container-pid", type=int, required=True)
    return parser.parse_args()


def initialize_layout(storage_layout: str, tdengine_database: str) -> ReceiverLayout:
    if storage_layout == "pg_tdengine_raw":
        return ReceiverLayout(
            storage_layout=storage_layout,
            tdengine_database=tdengine_database,
            tdengine_supertable="box_info_raw",
            tdengine_create_sql=RAW_STABLE_SQL_TEMPLATE.format(database=tdengine_database),
            tdengine_columns=(
                "ts",
                "obj_type",
                "position_x",
                "position_y",
                "position_z",
                "length",
                "width",
                "height",
                "speed_kmh",
                "spindle",
                "lane_id",
                "frame_id",
            ),
            logical_encoding="float/raw",
            compact=False,
        )
    if storage_layout == "pg_tdengine_compact":
        return ReceiverLayout(
            storage_layout=storage_layout,
            tdengine_database=tdengine_database,
            tdengine_supertable="box_info_compact",
            tdengine_create_sql=COMPACT_STABLE_SQL_TEMPLATE.format(database=tdengine_database),
            tdengine_columns=(
                "ts",
                "obj_type",
                "lane_id",
                "frame_id",
                "position_x_mm",
                "position_y_mm",
                "position_z_mm",
                "length_mm",
                "width_mm",
                "height_mm",
                "speed_centi_kmh",
                "spindle_centi_deg",
            ),
            logical_encoding="compact/int",
            compact=True,
        )
    raise ValueError(f"Unsupported storage layout: {storage_layout}")


def timestamp_ms_to_datetime(sample_timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(sample_timestamp_ms / 1000.0, tz=UTC)


def format_tdengine_timestamp(sample_timestamp_ms: int) -> str:
    dt = timestamp_ms_to_datetime(sample_timestamp_ms)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def sql_literal(value) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, float):
        text = format(value, ".6f").rstrip("0").rstrip(".")
        return text or "0"
    return str(value)


def tdengine_subtable_name(trace_id: int) -> str:
    return f"trace_{trace_id}"


def canonical_state_row(raw_row: list) -> tuple:
    (
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
        frame_id,
    ) = raw_row
    sample_dt = timestamp_ms_to_datetime(int(sample_timestamp))
    return (
        int(trace_id),
        sample_dt,
        int(frame_id),
        int(obj_type),
        int(lane_id),
        int(round(float(position_x) * COORDINATE_SCALE)),
        int(round(float(position_y) * COORDINATE_SCALE)),
        int(round(float(position_z) * COORDINATE_SCALE)),
        int(round(float(length) * SIZE_SCALE)),
        int(round(float(width) * SIZE_SCALE)),
        int(round(float(height) * SIZE_SCALE)),
        int(round(float(speed_kmh) * SPEED_SCALE)),
        int(round(float(spindle) * ANGLE_SCALE)),
        datetime.now(tz=UTC),
    )


def raw_tdengine_values(raw_row: list) -> tuple:
    (
        _trace_id,
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
        frame_id,
    ) = raw_row
    return (
        format_tdengine_timestamp(int(sample_timestamp)),
        int(obj_type),
        float(position_x),
        float(position_y),
        float(position_z),
        float(length),
        float(width),
        float(height),
        float(speed_kmh),
        float(spindle),
        int(lane_id),
        int(frame_id),
    )


def compact_tdengine_values(raw_row: list) -> tuple:
    (
        _trace_id,
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
        frame_id,
    ) = raw_row
    return (
        format_tdengine_timestamp(int(sample_timestamp)),
        int(obj_type),
        int(lane_id),
        int(frame_id),
        int(round(float(position_x) * COORDINATE_SCALE)),
        int(round(float(position_y) * COORDINATE_SCALE)),
        int(round(float(position_z) * COORDINATE_SCALE)),
        int(round(float(length) * SIZE_SCALE)),
        int(round(float(width) * SIZE_SCALE)),
        int(round(float(height) * SIZE_SCALE)),
        int(round(float(speed_kmh) * SPEED_SCALE)),
        int(round(float(spindle) * ANGLE_SCALE)),
    )


def build_tdengine_insert_sql(layout: ReceiverLayout, raw_rows: list[list]) -> str:
    grouped: dict[int, list[tuple]] = {}
    for raw_row in raw_rows:
        trace_id = int(raw_row[0])
        values = compact_tdengine_values(raw_row) if layout.compact else raw_tdengine_values(raw_row)
        grouped.setdefault(trace_id, []).append(values)

    segments: list[str] = []
    column_sql = ", ".join(layout.tdengine_columns)
    for trace_id in sorted(grouped):
        values_sql = " ".join(
            f"({', '.join(sql_literal(value) for value in row_values)})"
            for row_values in grouped[trace_id]
        )
        segments.append(
            f"{layout.tdengine_database}.{tdengine_subtable_name(trace_id)} "
            f"USING {layout.tdengine_database}.{layout.tdengine_supertable} "
            f"TAGS ({trace_id}) ({column_sql}) VALUES {values_sql}"
        )
    return "INSERT INTO " + " ".join(segments)


def postgres_batch_summary(batch_id: int, storage_layout: str, rows: list[list]) -> tuple:
    trace_ids = [int(row[0]) for row in rows]
    timestamps = [int(row[1]) for row in rows]
    frame_ids = [int(row[-1]) for row in rows]
    return (
        int(batch_id),
        storage_layout,
        len(rows),
        min(trace_ids),
        max(trace_ids),
        timestamp_ms_to_datetime(min(timestamps)),
        timestamp_ms_to_datetime(max(timestamps)),
        min(frame_ids),
        max(frame_ids),
    )


def query_postgres_counts(connection: psycopg.Connection) -> tuple[int, int]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT (SELECT count(*) FROM ingest_batches), (SELECT count(*) FROM trace_latest_state)")
        batch_rows, trace_rows = cursor.fetchone()
    return int(batch_rows), int(trace_rows)


def detect_postgres_toast_compression(connection: psycopg.Connection) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT CASE
                WHEN 'lz4' = ANY(enumvals) THEN 'lz4'
                ELSE current_setting('default_toast_compression')
            END
            FROM pg_settings
            WHERE name = 'default_toast_compression'
            """
        )
        value = cursor.fetchone()[0]
    return str(value)


def configure_postgres_session(
    connection: psycopg.Connection,
    *,
    postgres_profile: str,
    toast_compression: str,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('default_toast_compression', %s, false)", (toast_compression,))
        if postgres_profile == "bench_low_wal_compressed":
            cursor.execute("SELECT set_config('synchronous_commit', 'off', false)")
    connection.commit()


def query_postgres_runtime_settings(connection: psycopg.Connection) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                current_setting('wal_level'),
                current_setting('wal_compression'),
                current_setting('synchronous_commit'),
                current_setting('archive_mode'),
                current_setting('autovacuum'),
                current_setting('default_toast_compression')
            """
        )
        wal_level, wal_compression, synchronous_commit, archive_mode, autovacuum, default_toast_compression = cursor.fetchone()
    return {
        "wal_level": str(wal_level),
        "wal_compression": str(wal_compression),
        "synchronous_commit": str(synchronous_commit),
        "archive_mode": str(archive_mode),
        "autovacuum": str(autovacuum),
        "default_toast_compression": str(default_toast_compression),
    }


def query_postgres_table_persistence(connection: psycopg.Connection) -> dict[str, str]:
    persistence_map = {
        "p": "permanent",
        "u": "unlogged",
        "t": "temporary",
    }
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT relname, relpersistence FROM pg_class WHERE relname = ANY(%s)",
            (list(POSTGRES_TABLE_NAMES),),
        )
        rows = cursor.fetchall()
    result = {name: "unknown" for name in POSTGRES_TABLE_NAMES}
    for relname, relpersistence in rows:
        result[str(relname)] = persistence_map.get(str(relpersistence), str(relpersistence))
    return result


def sample_metrics_once(
    *,
    writer: csv.DictWriter,
    metrics_file,
    stats: ReceiverStats,
    receiver_process: psutil.Process,
    postgres_monitor: ProcessTreeMonitor,
    tdengine_monitor: ProcessTreeMonitor,
    sink_dir: Path,
    postgres_data_dir: Path,
    tdengine_data_dir: Path,
    postgres_metrics_conn: psycopg.Connection,
    tdengine_metrics_client: TDengineRestClient,
    layout: ReceiverLayout,
    pending_rows: list[list],
) -> None:
    usage = sink_disk_usage(
        sink_dir=sink_dir,
        postgres_data_dir=postgres_data_dir,
        tdengine_data_dir=tdengine_data_dir,
    )
    postgres_batch_rows, postgres_trace_rows = query_postgres_counts(postgres_metrics_conn)
    tdengine_rows = int(
        tdengine_metrics_client.query_single_value(
            f"SELECT COUNT(*) FROM {layout.tdengine_database}.{layout.tdengine_supertable}"
        )
        or 0
    )
    receiver_rss_bytes = receiver_process.memory_info().rss
    receiver_cpu_percent = receiver_process.cpu_percent(interval=None) / max(psutil.cpu_count() or 1, 1)
    postgres_cpu_percent, postgres_rss_bytes = postgres_monitor.sample()
    tdengine_cpu_percent, tdengine_rss_bytes = tdengine_monitor.sample()

    with stats.lock:
        stats.peak_rss_bytes = max(stats.peak_rss_bytes, receiver_rss_bytes)
        stats.peak_cpu_percent = max(stats.peak_cpu_percent, receiver_cpu_percent)
        stats.postgres_peak_rss_bytes = max(stats.postgres_peak_rss_bytes, postgres_rss_bytes)
        stats.postgres_peak_cpu_percent = max(stats.postgres_peak_cpu_percent, postgres_cpu_percent)
        stats.tdengine_peak_rss_bytes = max(stats.tdengine_peak_rss_bytes, tdengine_rss_bytes)
        stats.tdengine_peak_cpu_percent = max(stats.tdengine_peak_cpu_percent, tdengine_cpu_percent)
        rows_received = stats.rows_received
        rows_inserted = stats.rows_inserted
        batches_received = stats.batches_received
        tdengine_insert_calls = stats.tdengine_insert_calls
        postgres_write_calls = stats.postgres_write_calls
        bytes_received = stats.bytes_received
        elapsed_seconds = time.monotonic() - stats.start_monotonic

    writer.writerow(
        {
            "sampled_at_utc": utc_now_iso(),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "rows_received": rows_received,
            "rows_inserted": rows_inserted,
            "batches_received": batches_received,
            "tdengine_insert_calls": tdengine_insert_calls,
            "postgres_write_calls": postgres_write_calls,
            "bytes_received": bytes_received,
            "pending_rows": len(pending_rows),
            "avg_rows_per_second": round(rows_inserted / max(elapsed_seconds, 1e-9), 3),
            "receiver_cpu_percent": round(receiver_cpu_percent, 3),
            "receiver_rss_bytes": receiver_rss_bytes,
            "postgres_cpu_percent": round(postgres_cpu_percent, 3),
            "postgres_rss_bytes": postgres_rss_bytes,
            "tdengine_cpu_percent": round(tdengine_cpu_percent, 3),
            "tdengine_rss_bytes": tdengine_rss_bytes,
            "sink_bytes": usage.sink_bytes,
            "postgres_data_bytes": usage.postgres_data_bytes,
            "tdengine_data_bytes": usage.tdengine_data_bytes,
            "postgres_batch_rows": postgres_batch_rows,
            "postgres_trace_rows": postgres_trace_rows,
            "tdengine_rows": tdengine_rows,
        }
    )
    metrics_file.flush()


def run_sampler(
    *,
    stop_event: threading.Event,
    stats: ReceiverStats,
    receiver_process: psutil.Process,
    postgres_monitor: ProcessTreeMonitor,
    tdengine_monitor: ProcessTreeMonitor,
    sink_dir: Path,
    postgres_data_dir: Path,
    tdengine_data_dir: Path,
    postgres_metrics_conn: psycopg.Connection,
    tdengine_metrics_client: TDengineRestClient,
    layout: ReceiverLayout,
    pending_rows: list[list],
    metrics_path: Path,
    interval_seconds: float,
) -> None:
    receiver_process.cpu_percent(interval=None)
    postgres_monitor.prime()
    tdengine_monitor.prime()
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", newline="", encoding="utf-8") as metrics_file:
        writer = csv.DictWriter(
            metrics_file,
            fieldnames=[
                "sampled_at_utc",
                "elapsed_seconds",
                "rows_received",
                "rows_inserted",
                "batches_received",
                "tdengine_insert_calls",
                "postgres_write_calls",
                "bytes_received",
                "pending_rows",
                "avg_rows_per_second",
                "receiver_cpu_percent",
                "receiver_rss_bytes",
                "postgres_cpu_percent",
                "postgres_rss_bytes",
                "tdengine_cpu_percent",
                "tdengine_rss_bytes",
                "sink_bytes",
                "postgres_data_bytes",
                "tdengine_data_bytes",
                "postgres_batch_rows",
                "postgres_trace_rows",
                "tdengine_rows",
            ],
        )
        writer.writeheader()
        sample_metrics_once(
            writer=writer,
            metrics_file=metrics_file,
            stats=stats,
            receiver_process=receiver_process,
            postgres_monitor=postgres_monitor,
            tdengine_monitor=tdengine_monitor,
            sink_dir=sink_dir,
            postgres_data_dir=postgres_data_dir,
            tdengine_data_dir=tdengine_data_dir,
            postgres_metrics_conn=postgres_metrics_conn,
            tdengine_metrics_client=tdengine_metrics_client,
            layout=layout,
            pending_rows=pending_rows,
        )
        while not stop_event.wait(interval_seconds):
            sample_metrics_once(
                writer=writer,
                metrics_file=metrics_file,
                stats=stats,
                receiver_process=receiver_process,
                postgres_monitor=postgres_monitor,
                tdengine_monitor=tdengine_monitor,
                sink_dir=sink_dir,
                postgres_data_dir=postgres_data_dir,
                tdengine_data_dir=tdengine_data_dir,
                postgres_metrics_conn=postgres_metrics_conn,
                tdengine_metrics_client=tdengine_metrics_client,
                layout=layout,
                pending_rows=pending_rows,
            )
        sample_metrics_once(
            writer=writer,
            metrics_file=metrics_file,
            stats=stats,
            receiver_process=receiver_process,
            postgres_monitor=postgres_monitor,
            tdengine_monitor=tdengine_monitor,
            sink_dir=sink_dir,
            postgres_data_dir=postgres_data_dir,
            tdengine_data_dir=tdengine_data_dir,
            postgres_metrics_conn=postgres_metrics_conn,
            tdengine_metrics_client=tdengine_metrics_client,
            layout=layout,
            pending_rows=pending_rows,
        )


def main() -> None:
    args = parse_args()
    storage_layout = normalize_storage_layout(args.storage_layout)
    postgres_profile = normalize_postgres_profile(args.postgres_profile)
    layout = initialize_layout(storage_layout, args.tdengine_database)

    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    sink_dir = run_dir / "sink"
    sink_dir.mkdir(parents=True, exist_ok=True)

    postgres_data_dir = Path(args.postgres_data_dir).resolve()
    tdengine_data_dir = Path(args.tdengine_data_dir).resolve()
    tdengine_log_dir = Path(args.tdengine_log_dir).resolve()
    postgres_data_dir.mkdir(parents=True, exist_ok=True)
    tdengine_data_dir.mkdir(parents=True, exist_ok=True)
    tdengine_log_dir.mkdir(parents=True, exist_ok=True)

    metrics_path = run_dir / "receiver_metrics.csv"
    ready_path = run_dir / "receiver_ready.json"
    summary_path = run_dir / "receiver_summary.json"
    metadata_path = sink_dir / "storage_layout.json"

    postgres_conn = psycopg.connect(
        host=args.postgres_host,
        port=args.postgres_port,
        dbname=args.postgres_database,
        user=args.postgres_user,
        password=args.postgres_password,
        autocommit=False,
    )
    postgres_metrics_conn = psycopg.connect(
        host=args.postgres_host,
        port=args.postgres_port,
        dbname=args.postgres_database,
        user=args.postgres_user,
        password=args.postgres_password,
        autocommit=True,
    )
    tdengine_client = TDengineRestClient(
        host=args.tdengine_host,
        port=args.tdengine_port,
        user=args.tdengine_user,
        password=args.tdengine_password,
    )
    tdengine_metrics_client = TDengineRestClient(
        host=args.tdengine_host,
        port=args.tdengine_port,
        user=args.tdengine_user,
        password=args.tdengine_password,
    )

    tdengine_client.execute(f"CREATE DATABASE IF NOT EXISTS {args.tdengine_database}")
    tdengine_client.execute(layout.tdengine_create_sql)

    toast_compression = detect_postgres_toast_compression(postgres_metrics_conn)
    configure_postgres_session(
        postgres_conn,
        postgres_profile=postgres_profile,
        toast_compression=toast_compression,
    )
    configure_postgres_session(
        postgres_metrics_conn,
        postgres_profile=postgres_profile,
        toast_compression=toast_compression,
    )
    with postgres_conn.cursor() as cursor:
        cursor.execute(build_postgres_reset_sql(postgres_profile, toast_compression=toast_compression))
    postgres_conn.commit()

    postgres_runtime_settings = query_postgres_runtime_settings(postgres_metrics_conn)
    postgres_table_persistence = query_postgres_table_persistence(postgres_metrics_conn)

    metadata = {
        "storage_layout": layout.storage_layout,
        "sink_kind": "postgres_tdengine",
        "postgres_profile": postgres_profile,
        "postgres_wal_mode": args.postgres_wal_mode,
        "postgres_tables": list(POSTGRES_TABLE_NAMES),
        "postgres_table_persistence": postgres_table_persistence,
        "postgres_runtime_settings": postgres_runtime_settings,
        "postgres_toast_compression": toast_compression,
        "tdengine_database": layout.tdengine_database,
        "tdengine_supertable": layout.tdengine_supertable,
        "tdengine_columns": list(layout.tdengine_columns),
        "insert_row_target": args.insert_row_target,
        "logical_encoding": layout.logical_encoding,
        "compact_schema": layout.compact,
        "postgres_database": args.postgres_database,
        "postgres_user": args.postgres_user,
        "postgres_host": args.postgres_host,
        "postgres_port": args.postgres_port,
        "tdengine_user": args.tdengine_user,
        "tdengine_host": args.tdengine_host,
        "tdengine_port": args.tdengine_port,
    }
    write_json(metadata_path, metadata)

    stats = ReceiverStats()
    receiver_process = psutil.Process()
    postgres_monitor = ProcessTreeMonitor(args.postgres_container_pid)
    tdengine_monitor = ProcessTreeMonitor(args.tdengine_container_pid)
    stop_event = threading.Event()
    pending_rows: list[list] = []
    pending_batch_summaries: list[tuple] = []

    sampler_thread = threading.Thread(
        target=run_sampler,
        kwargs={
            "stop_event": stop_event,
            "stats": stats,
            "receiver_process": receiver_process,
            "postgres_monitor": postgres_monitor,
            "tdengine_monitor": tdengine_monitor,
            "sink_dir": sink_dir,
            "postgres_data_dir": postgres_data_dir,
            "tdengine_data_dir": tdengine_data_dir,
            "postgres_metrics_conn": postgres_metrics_conn,
            "tdengine_metrics_client": tdengine_metrics_client,
            "layout": layout,
            "pending_rows": pending_rows,
            "metrics_path": metrics_path,
            "interval_seconds": args.metric_interval_seconds,
        },
        daemon=True,
    )
    sampler_thread.start()

    def flush_pending_rows() -> int:
        if not pending_rows:
            return 0

        tdengine_sql = build_tdengine_insert_sql(layout, pending_rows)
        latest_states: dict[int, tuple] = {}
        for raw_row in pending_rows:
            latest_states[int(raw_row[0])] = canonical_state_row(raw_row)

        tdengine_client.execute(tdengine_sql)
        with postgres_conn.cursor() as cursor:
            cursor.executemany(POSTGRES_BATCH_INSERT_SQL, pending_batch_summaries)
            cursor.executemany(POSTGRES_TRACE_UPSERT_SQL, list(latest_states.values()))
        postgres_conn.commit()

        inserted = len(pending_rows)
        pending_rows.clear()
        pending_batch_summaries.clear()
        with stats.lock:
            stats.rows_inserted += inserted
            stats.tdengine_insert_calls += 1
            stats.postgres_write_calls += 1
        return inserted

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((args.host, args.port))
        server_sock.listen(1)

        write_json(
            ready_path,
            {
                "ready_at_utc": utc_now_iso(),
                "host": args.host,
                "port": args.port,
                "run_dir": str(run_dir),
                "sink_dir": str(sink_dir),
                "postgres_database": args.postgres_database,
                "postgres_profile": postgres_profile,
                "postgres_wal_mode": args.postgres_wal_mode,
                "tdengine_database": args.tdengine_database,
                "postgres_data_dir": str(postgres_data_dir),
                "tdengine_data_dir": str(tdengine_data_dir),
                "storage_layout": layout.storage_layout,
                "receiver_label": safe_slug(run_dir.name),
            },
        )

        conn, _client_addr = server_sock.accept()
        with conn:
            reader = conn.makefile("rb")
            while True:
                raw_line = reader.readline()
                if not raw_line:
                    break

                message = json.loads(raw_line.decode("utf-8"))
                if message.get("type") != "box_batch":
                    continue

                rows = message.get("rows", [])
                if rows:
                    pending_batch_summaries.append(
                        postgres_batch_summary(
                            int(message.get("batch_id", 0)),
                            layout.storage_layout,
                            rows,
                        )
                    )
                for row in rows:
                    if len(row) != NETWORK_FIELD_COUNT:
                        raise ValueError(f"Expected {NETWORK_FIELD_COUNT} fields, got {len(row)}")
                    pending_rows.append(row)

                if len(pending_rows) >= max(args.insert_row_target, 1):
                    flush_pending_rows()

                with stats.lock:
                    stats.rows_received += len(rows)
                    stats.batches_received += 1
                    stats.bytes_received += len(raw_line)

    flush_pending_rows()
    ingest_elapsed_seconds = max(time.monotonic() - stats.start_monotonic, 1e-9)
    tdengine_row_count = int(
        tdengine_client.query_single_value(
            f"SELECT COUNT(*) FROM {layout.tdengine_database}.{layout.tdengine_supertable}"
        )
        or 0
    )
    postgres_batch_rows, postgres_trace_rows = query_postgres_counts(postgres_metrics_conn)
    total_elapsed_seconds = max(time.monotonic() - stats.start_monotonic, 1e-9)
    usage = sink_disk_usage(
        sink_dir=sink_dir,
        postgres_data_dir=postgres_data_dir,
        tdengine_data_dir=tdengine_data_dir,
    )

    stop_event.set()
    sampler_thread.join(timeout=5)

    receiver_summary = {
        "completed_at_utc": utc_now_iso(),
        "sink_kind": "postgres_tdengine",
        "storage_layout": layout.storage_layout,
        "postgres_profile": postgres_profile,
        "postgres_wal_mode": args.postgres_wal_mode,
        "rows_received": stats.rows_received,
        "rows_inserted": stats.rows_inserted,
        "rows_in_sink": tdengine_row_count,
        "tdengine_rows_in_sink": tdengine_row_count,
        "postgres_batch_rows": postgres_batch_rows,
        "postgres_trace_rows": postgres_trace_rows,
        "batches_received": stats.batches_received,
        "tdengine_insert_calls": stats.tdengine_insert_calls,
        "postgres_write_calls": stats.postgres_write_calls,
        "bytes_received": stats.bytes_received,
        "ingest_elapsed_seconds": ingest_elapsed_seconds,
        "elapsed_seconds": total_elapsed_seconds,
        "avg_rows_per_second_ingest": stats.rows_inserted / ingest_elapsed_seconds,
        "avg_rows_per_second_total": stats.rows_inserted / total_elapsed_seconds,
        "receiver_peak_rss_bytes": stats.peak_rss_bytes,
        "receiver_peak_cpu_percent": stats.peak_cpu_percent,
        "postgres_peak_rss_bytes": stats.postgres_peak_rss_bytes,
        "postgres_peak_cpu_percent": stats.postgres_peak_cpu_percent,
        "postgres_runtime_settings": postgres_runtime_settings,
        "postgres_table_persistence": postgres_table_persistence,
        "postgres_toast_compression": toast_compression,
        "tdengine_peak_rss_bytes": stats.tdengine_peak_rss_bytes,
        "tdengine_peak_cpu_percent": stats.tdengine_peak_cpu_percent,
        "sink_dir": str(sink_dir),
        "postgres_data_dir": str(postgres_data_dir),
        "tdengine_data_dir": str(tdengine_data_dir),
        "sink_bytes_post_run": usage.sink_bytes,
        "postgres_data_bytes_post_run": usage.postgres_data_bytes,
        "tdengine_data_bytes_post_run": usage.tdengine_data_bytes,
        "bytes_per_row_post_run": usage.sink_bytes / max(tdengine_row_count, 1),
        "bytes_per_row_postgres_data_post_run": usage.postgres_data_bytes / max(tdengine_row_count, 1),
        "bytes_per_row_tdengine_data_post_run": usage.tdengine_data_bytes / max(tdengine_row_count, 1),
        "storage_metadata_path": str(metadata_path),
        "tdengine_supertable": layout.tdengine_supertable,
        "postgres_tables": list(POSTGRES_TABLE_NAMES),
    }
    write_json(summary_path, receiver_summary)

    postgres_metrics_conn.close()
    postgres_conn.close()


if __name__ == "__main__":
    main()
