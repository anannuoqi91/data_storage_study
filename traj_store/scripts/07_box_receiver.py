#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import socket
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import psutil

from ingest_common import (
    ANGLE_SCALE,
    COORDINATE_SCALE,
    NETWORK_FIELD_COUNT,
    SIZE_SCALE,
    SPEED_SCALE,
    CompactEncodingConfig,
    SUPPORTED_LAYOUT_CHOICES,
    compact_box_row,
    normalize_storage_layout,
    parquet_output_path,
    raw_box_row,
    sink_disk_usage,
    sql_quote,
    utc_now_iso,
    utc_partition_fields,
    write_json,
)


DUCKDB_RAW_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS od;

CREATE TABLE IF NOT EXISTS od.box_info_ingest (
    box_id BIGINT,
    trace_id BIGINT NOT NULL,
    sample_timestamp BIGINT NOT NULL,
    obj_type SMALLINT NOT NULL,
    position_x REAL NOT NULL,
    position_y REAL NOT NULL,
    position_z REAL NOT NULL,
    length REAL NOT NULL,
    width REAL NOT NULL,
    height REAL NOT NULL,
    speed_kmh REAL NOT NULL,
    spindle REAL NOT NULL,
    lane_id INTEGER NOT NULL,
    frame_id BIGINT NOT NULL
);
"""

DUCKDB_COMPACT_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS od;

CREATE TABLE IF NOT EXISTS od.box_info_ingest_compact (
    box_id INTEGER,
    trace_id INTEGER NOT NULL,
    sample_offset_ms INTEGER NOT NULL,
    obj_type SMALLINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm SMALLINT NOT NULL,
    width_mm SMALLINT NOT NULL,
    height_mm SMALLINT NOT NULL,
    speed_centi_kmh SMALLINT NOT NULL,
    spindle_centi_deg INTEGER NOT NULL,
    lane_id SMALLINT NOT NULL,
    frame_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS od.box_info_ingest_compact_meta (
    base_timestamp_ms BIGINT,
    coordinate_scale INTEGER NOT NULL,
    size_scale INTEGER NOT NULL,
    speed_scale INTEGER NOT NULL,
    angle_scale INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);
"""

DUCKDB_COMPACT_VIEW_SQL = """
CREATE OR REPLACE VIEW od.box_info_ingest_compact_view AS
SELECT
    c.box_id,
    c.trace_id,
    m.base_timestamp_ms + c.sample_offset_ms AS sample_timestamp,
    c.obj_type,
    c.position_x_mm / CAST(m.coordinate_scale AS DOUBLE) AS position_x,
    c.position_y_mm / CAST(m.coordinate_scale AS DOUBLE) AS position_y,
    c.position_z_mm / CAST(m.coordinate_scale AS DOUBLE) AS position_z,
    c.length_mm / CAST(m.size_scale AS DOUBLE) AS length,
    c.width_mm / CAST(m.size_scale AS DOUBLE) AS width,
    c.height_mm / CAST(m.size_scale AS DOUBLE) AS height,
    c.speed_centi_kmh / CAST(m.speed_scale AS DOUBLE) AS speed_kmh,
    c.spindle_centi_deg / CAST(m.angle_scale AS DOUBLE) AS spindle,
    c.lane_id,
    c.frame_id
FROM od.box_info_ingest_compact AS c
CROSS JOIN od.box_info_ingest_compact_meta AS m
"""

PARQUET_RAW_BUFFER_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS box_info_parquet_buffer (
    trace_id INTEGER NOT NULL,
    sample_timestamp BIGINT NOT NULL,
    obj_type SMALLINT NOT NULL,
    position_x REAL NOT NULL,
    position_y REAL NOT NULL,
    position_z REAL NOT NULL,
    length REAL NOT NULL,
    width REAL NOT NULL,
    height REAL NOT NULL,
    speed_kmh REAL NOT NULL,
    spindle REAL NOT NULL,
    lane_id SMALLINT NOT NULL,
    frame_id INTEGER NOT NULL
);
"""

PARQUET_COMPACT_BUFFER_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS box_info_parquet_compact_buffer (
    trace_id INTEGER NOT NULL,
    sample_offset_ms INTEGER NOT NULL,
    obj_type SMALLINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm SMALLINT NOT NULL,
    width_mm SMALLINT NOT NULL,
    height_mm SMALLINT NOT NULL,
    speed_centi_kmh SMALLINT NOT NULL,
    spindle_centi_deg INTEGER NOT NULL,
    lane_id SMALLINT NOT NULL,
    frame_id INTEGER NOT NULL
);
"""

DUCKDB_RAW_INSERT_SQL = """
INSERT INTO od.box_info_ingest
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

DUCKDB_COMPACT_INSERT_SQL = """
INSERT INTO od.box_info_ingest_compact
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

PARQUET_RAW_INSERT_SQL = """
INSERT INTO box_info_parquet_buffer
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

PARQUET_COMPACT_INSERT_SQL = """
INSERT INTO box_info_parquet_compact_buffer
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass
class ReceiverStats:
    start_monotonic: float = field(default_factory=time.monotonic)
    rows_inserted: int = 0
    rows_flushed: int = 0
    batches_inserted: int = 0
    flushes_completed: int = 0
    bytes_received: int = 0
    peak_rss_bytes: int = 0
    peak_cpu_percent: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(frozen=True)
class ReceiverLayout:
    storage_layout: str
    sink_kind: str
    target_name: str
    insert_sql: str
    buffer_table: str | None = None
    parquet_compression: str | None = None
    decoded_view: str | None = None
    compact: bool = False
    omits_box_id: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synchronously receive box batches and write them to DuckDB or Parquet.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--storage-layout", choices=SUPPORTED_LAYOUT_CHOICES, default="parquet_compact_zstd")
    parser.add_argument("--flush-row-target", type=int, default=10000)
    parser.add_argument("--row-group-size", type=int, default=100000)
    return parser.parse_args()


def initialize_layout(con: duckdb.DuckDBPyConnection, storage_layout: str) -> ReceiverLayout:
    if storage_layout == "duckdb_raw":
        con.execute(DUCKDB_RAW_SCHEMA_SQL)
        return ReceiverLayout(
            storage_layout=storage_layout,
            sink_kind="duckdb",
            target_name="od.box_info_ingest",
            insert_sql=DUCKDB_RAW_INSERT_SQL,
        )

    if storage_layout == "duckdb_compact":
        con.execute(DUCKDB_COMPACT_SCHEMA_SQL)
        return ReceiverLayout(
            storage_layout=storage_layout,
            sink_kind="duckdb",
            target_name="od.box_info_ingest_compact",
            insert_sql=DUCKDB_COMPACT_INSERT_SQL,
            decoded_view="od.box_info_ingest_compact_view",
            compact=True,
        )

    if storage_layout in {"parquet_snappy", "parquet_zstd"}:
        con.execute(PARQUET_RAW_BUFFER_SCHEMA_SQL)
        return ReceiverLayout(
            storage_layout=storage_layout,
            sink_kind="parquet",
            target_name="parquet/box_info",
            insert_sql=PARQUET_RAW_INSERT_SQL,
            buffer_table="box_info_parquet_buffer",
            parquet_compression="SNAPPY" if storage_layout == "parquet_snappy" else "ZSTD",
            omits_box_id=True,
        )

    if storage_layout == "parquet_compact_zstd":
        con.execute(PARQUET_COMPACT_BUFFER_SCHEMA_SQL)
        return ReceiverLayout(
            storage_layout=storage_layout,
            sink_kind="parquet",
            target_name="parquet/box_info_compact",
            insert_sql=PARQUET_COMPACT_INSERT_SQL,
            buffer_table="box_info_parquet_compact_buffer",
            parquet_compression="ZSTD",
            compact=True,
            omits_box_id=True,
        )

    raise ValueError(f"Unsupported storage layout: {storage_layout}")


def sample_metrics_once(
    *,
    writer: csv.DictWriter,
    metrics_file,
    stats: ReceiverStats,
    process: psutil.Process,
    sink_dir: Path,
    db_path: Path | None,
    parquet_dir: Path | None,
) -> None:
    usage = sink_disk_usage(sink_dir=sink_dir, db_path=db_path, parquet_dir=parquet_dir)
    rss_bytes = process.memory_info().rss
    cpu_percent = process.cpu_percent(interval=None) / max(psutil.cpu_count() or 1, 1)

    with stats.lock:
        stats.peak_rss_bytes = max(stats.peak_rss_bytes, rss_bytes)
        stats.peak_cpu_percent = max(stats.peak_cpu_percent, cpu_percent)
        rows_inserted = stats.rows_inserted
        rows_flushed = stats.rows_flushed
        batches_inserted = stats.batches_inserted
        flushes_completed = stats.flushes_completed
        bytes_received = stats.bytes_received
        elapsed_seconds = time.monotonic() - stats.start_monotonic

    writer.writerow(
        {
            "sampled_at_utc": utc_now_iso(),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "rows_inserted": rows_inserted,
            "rows_flushed": rows_flushed,
            "batches_inserted": batches_inserted,
            "flushes_completed": flushes_completed,
            "bytes_received": bytes_received,
            "avg_rows_per_second": round(rows_inserted / max(elapsed_seconds, 1e-9), 3),
            "cpu_percent": round(cpu_percent, 3),
            "rss_bytes": rss_bytes,
            "sink_bytes": usage.sink_bytes,
            "parquet_bytes": usage.parquet_bytes,
            "duckdb_bytes": usage.duckdb_bytes,
            "wal_bytes": usage.wal_bytes,
        }
    )
    metrics_file.flush()


def run_sampler(
    *,
    stop_event: threading.Event,
    stats: ReceiverStats,
    process: psutil.Process,
    sink_dir: Path,
    db_path: Path | None,
    parquet_dir: Path | None,
    metrics_path: Path,
    interval_seconds: float,
) -> None:
    process.cpu_percent(interval=None)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", newline="", encoding="utf-8") as metrics_file:
        writer = csv.DictWriter(
            metrics_file,
            fieldnames=[
                "sampled_at_utc",
                "elapsed_seconds",
                "rows_inserted",
                "rows_flushed",
                "batches_inserted",
                "flushes_completed",
                "bytes_received",
                "avg_rows_per_second",
                "cpu_percent",
                "rss_bytes",
                "sink_bytes",
                "parquet_bytes",
                "duckdb_bytes",
                "wal_bytes",
            ],
        )
        writer.writeheader()
        sample_metrics_once(
            writer=writer,
            metrics_file=metrics_file,
            stats=stats,
            process=process,
            sink_dir=sink_dir,
            db_path=db_path,
            parquet_dir=parquet_dir,
        )
        while not stop_event.wait(interval_seconds):
            sample_metrics_once(
                writer=writer,
                metrics_file=metrics_file,
                stats=stats,
                process=process,
                sink_dir=sink_dir,
                db_path=db_path,
                parquet_dir=parquet_dir,
            )
        sample_metrics_once(
            writer=writer,
            metrics_file=metrics_file,
            stats=stats,
            process=process,
            sink_dir=sink_dir,
            db_path=db_path,
            parquet_dir=parquet_dir,
        )


def parquet_copy_sql(layout: ReceiverLayout, destination_path: Path, row_group_size: int) -> str:
    if layout.storage_layout == "parquet_compact_zstd":
        select_sql = (
            "SELECT trace_id, sample_offset_ms, obj_type, position_x_mm, position_y_mm, position_z_mm, "
            "length_mm, width_mm, height_mm, speed_centi_kmh, spindle_centi_deg, lane_id, frame_id "
            f"FROM {layout.buffer_table} ORDER BY sample_offset_ms, trace_id"
        )
    else:
        select_sql = (
            "SELECT trace_id, sample_timestamp, obj_type, position_x, position_y, position_z, length, width, "
            "height, speed_kmh, spindle, lane_id, frame_id "
            f"FROM {layout.buffer_table} ORDER BY sample_timestamp, trace_id"
        )

    return (
        f"COPY ({select_sql}) TO {sql_quote(str(destination_path))} "
        f"(FORMAT PARQUET, COMPRESSION {layout.parquet_compression}, ROW_GROUP_SIZE {row_group_size})"
    )


def count_parquet_rows(con: duckdb.DuckDBPyConnection, parquet_dir: Path | None) -> int:
    if parquet_dir is None:
        return 0
    files = sorted(parquet_dir.rglob("*.parquet"))
    if not files:
        return 0
    files_sql = ", ".join(sql_quote(str(path)) for path in files)
    return con.execute(f"SELECT count(*) FROM read_parquet([{files_sql}])").fetchone()[0]


def main() -> None:
    args = parse_args()
    storage_layout = normalize_storage_layout(args.storage_layout)
    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    sink_dir = run_dir / "sink"
    sink_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = run_dir / "receiver_metrics.csv"
    ready_path = run_dir / "receiver_ready.json"
    summary_path = run_dir / "receiver_summary.json"
    metadata_path = sink_dir / "storage_layout.json"

    db_path: Path | None
    parquet_dir: Path | None
    if storage_layout.startswith("duckdb"):
        db_path = sink_dir / "receiver.duckdb"
        parquet_dir = None
        con = duckdb.connect(str(db_path))
    else:
        db_path = None
        parquet_dir = sink_dir / "parquet"
        parquet_dir.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(":memory:")

    con.execute("PRAGMA threads=1")
    layout = initialize_layout(con, storage_layout)

    stats = ReceiverStats()
    process = psutil.Process()
    stop_event = threading.Event()
    sampler_thread = threading.Thread(
        target=run_sampler,
        kwargs={
            "stop_event": stop_event,
            "stats": stats,
            "process": process,
            "sink_dir": sink_dir,
            "db_path": db_path,
            "parquet_dir": parquet_dir,
            "metrics_path": metrics_path,
            "interval_seconds": args.metric_interval_seconds,
        },
        daemon=True,
    )
    sampler_thread.start()

    encoding = CompactEncodingConfig()
    next_box_id = 1
    base_timestamp_ms: int | None = None
    next_chunk_index = 1
    pending_rows: list[tuple[str, str, tuple]] = []

    def flush_pending_rows() -> None:
        nonlocal next_chunk_index, pending_rows
        if layout.sink_kind != "parquet" or not pending_rows:
            return

        grouped_rows: dict[tuple[str, str], list[tuple]] = defaultdict(list)
        for part_date, part_hour, db_row in pending_rows:
            grouped_rows[(part_date, part_hour)].append(db_row)

        flushed_rows = 0
        for (part_date, part_hour), rows_to_flush in sorted(grouped_rows.items()):
            con.execute(f"DELETE FROM {layout.buffer_table}")
            con.executemany(layout.insert_sql, rows_to_flush)

            destination_path = parquet_output_path(parquet_dir, part_date, part_hour, next_chunk_index)
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(parquet_copy_sql(layout, destination_path, args.row_group_size))

            flushed_rows += len(rows_to_flush)
            next_chunk_index += 1

        con.execute(f"DELETE FROM {layout.buffer_table}")
        pending_rows = []

        with stats.lock:
            stats.rows_flushed += flushed_rows
            stats.flushes_completed += len(grouped_rows)

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
                "db_path": str(db_path) if db_path is not None else None,
                "parquet_dir": str(parquet_dir) if parquet_dir is not None else None,
                "storage_layout": layout.storage_layout,
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
                db_rows = []
                for row in rows:
                    if len(row) != NETWORK_FIELD_COUNT:
                        raise ValueError(f"Expected {NETWORK_FIELD_COUNT} fields, got {len(row)}")

                    sample_timestamp_ms = int(row[1])
                    if layout.compact and base_timestamp_ms is None:
                        base_timestamp_ms = sample_timestamp_ms
                        if layout.storage_layout == "duckdb_compact":
                            con.execute("DELETE FROM od.box_info_ingest_compact_meta")
                            con.execute(
                                """
                                INSERT INTO od.box_info_ingest_compact_meta
                                (base_timestamp_ms, coordinate_scale, size_scale, speed_scale, angle_scale)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    base_timestamp_ms,
                                    encoding.coordinate_scale,
                                    encoding.size_scale,
                                    encoding.speed_scale,
                                    encoding.angle_scale,
                                ),
                            )
                            con.execute(DUCKDB_COMPACT_VIEW_SQL)

                    if layout.storage_layout == "duckdb_raw":
                        db_rows.append((next_box_id, *raw_box_row(raw_row=row)))
                    elif layout.storage_layout == "duckdb_compact":
                        if base_timestamp_ms is None:
                            raise RuntimeError("base_timestamp_ms was not initialized")
                        db_rows.append((next_box_id, *compact_box_row(raw_row=row, base_timestamp_ms=base_timestamp_ms, encoding=encoding)))
                    elif layout.storage_layout in {"parquet_snappy", "parquet_zstd"}:
                        part_date, part_hour = utc_partition_fields(sample_timestamp_ms)
                        pending_rows.append((part_date, part_hour, raw_box_row(raw_row=row)))
                    elif layout.storage_layout == "parquet_compact_zstd":
                        if base_timestamp_ms is None:
                            raise RuntimeError("base_timestamp_ms was not initialized")
                        part_date, part_hour = utc_partition_fields(sample_timestamp_ms)
                        pending_rows.append(
                            (
                                part_date,
                                part_hour,
                                compact_box_row(raw_row=row, base_timestamp_ms=base_timestamp_ms, encoding=encoding),
                            )
                        )
                    else:
                        raise RuntimeError(f"Unhandled storage layout: {layout.storage_layout}")

                    next_box_id += 1

                if db_rows:
                    con.execute("BEGIN")
                    try:
                        con.executemany(layout.insert_sql, db_rows)
                        con.execute("COMMIT")
                    except Exception:
                        con.execute("ROLLBACK")
                        raise

                if layout.sink_kind == "parquet" and len(pending_rows) >= max(args.flush_row_target, 1):
                    flush_pending_rows()

                with stats.lock:
                    stats.rows_inserted += len(rows)
                    stats.batches_inserted += 1
                    stats.bytes_received += len(raw_line)

    flush_pending_rows()

    if layout.sink_kind == "duckdb":
        con.execute("CHECKPOINT")
        row_count = con.execute(f"SELECT count(*) FROM {layout.target_name}").fetchone()[0]
    else:
        row_count = count_parquet_rows(con, parquet_dir)

    elapsed_seconds = max(time.monotonic() - stats.start_monotonic, 1e-9)
    usage = sink_disk_usage(sink_dir=sink_dir, db_path=db_path, parquet_dir=parquet_dir)

    stop_event.set()
    sampler_thread.join(timeout=5)

    metadata = {
        "storage_layout": layout.storage_layout,
        "sink_kind": layout.sink_kind,
        "sort_keys": ["sample_timestamp", "trace_id"] if not layout.compact else ["sample_offset_ms", "trace_id"],
        "partitioning": ["date", "hour"] if layout.sink_kind == "parquet" else [],
        "flush_row_target": args.flush_row_target,
        "row_group_size": args.row_group_size,
        "parquet_compression": layout.parquet_compression,
        "omits_synthetic_box_id": layout.omits_box_id,
    }
    if layout.compact:
        metadata.update(
            {
                "base_timestamp_ms": base_timestamp_ms,
                "coordinate_scale": COORDINATE_SCALE,
                "size_scale": SIZE_SCALE,
                "speed_scale": SPEED_SCALE,
                "angle_scale": ANGLE_SCALE,
            }
        )
    write_json(metadata_path, metadata)

    with stats.lock:
        summary = {
            "completed_at_utc": utc_now_iso(),
            "storage_layout": layout.storage_layout,
            "sink_kind": layout.sink_kind,
            "target_name": layout.target_name,
            "decoded_view": layout.decoded_view,
            "rows_inserted": stats.rows_inserted,
            "rows_flushed": stats.rows_flushed if layout.sink_kind == "parquet" else stats.rows_inserted,
            "rows_in_sink": row_count,
            "batches_inserted": stats.batches_inserted,
            "flushes_completed": stats.flushes_completed,
            "bytes_received": stats.bytes_received,
            "elapsed_seconds": elapsed_seconds,
            "avg_rows_per_second": stats.rows_inserted / elapsed_seconds,
            "peak_rss_bytes": stats.peak_rss_bytes,
            "peak_cpu_percent": stats.peak_cpu_percent,
            "sink_dir": str(sink_dir),
            "db_path": str(db_path) if db_path is not None else None,
            "parquet_dir": str(parquet_dir) if parquet_dir is not None else None,
            "duckdb_bytes_post_flush": usage.duckdb_bytes,
            "wal_bytes_post_flush": usage.wal_bytes,
            "parquet_bytes_post_flush": usage.parquet_bytes,
            "sink_bytes_post_flush": usage.sink_bytes,
            "bytes_per_row_post_flush": usage.sink_bytes / max(row_count, 1),
            "storage_metadata_path": str(metadata_path),
            "db_bytes_post_checkpoint": usage.duckdb_bytes,
            "wal_bytes_post_checkpoint": usage.wal_bytes,
            "total_disk_bytes_post_checkpoint": usage.sink_bytes,
            "bytes_per_row_post_checkpoint": usage.sink_bytes / max(row_count, 1),
        }

        if layout.compact:
            summary.update(
                {
                    "base_timestamp_ms": base_timestamp_ms,
                    "coordinate_scale": COORDINATE_SCALE,
                    "size_scale": SIZE_SCALE,
                    "speed_scale": SPEED_SCALE,
                    "angle_scale": ANGLE_SCALE,
                }
            )

    write_json(summary_path, summary)
    print("\n".join(f"{key}={value}" for key, value in summary.items()))


if __name__ == "__main__":
    main()
