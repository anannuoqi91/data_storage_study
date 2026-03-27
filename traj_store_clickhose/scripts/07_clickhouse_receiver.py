#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import psutil

from clickhouse_http import ClickHouseClient
from ingest_common import (
    ANGLE_SCALE,
    COORDINATE_SCALE,
    NETWORK_FIELD_COUNT,
    SIZE_SCALE,
    SPEED_SCALE,
    SUPPORTED_LAYOUT_CHOICES,
    format_datetime64_ms,
    normalize_storage_layout,
    safe_slug,
    sink_disk_usage,
    utc_hour_partition_fields,
    utc_now_iso,
    write_json,
)


RAW_TABLE_SQL = """
CREATE TABLE box_info_raw_lz4 (
    trace_id UInt32,
    sample_timestamp DateTime64(3, 'UTC'),
    obj_type UInt8,
    position_x Float32,
    position_y Float32,
    position_z Float32,
    length Float32,
    width Float32,
    height Float32,
    speed_kmh Float32,
    spindle Float32,
    lane_id UInt8,
    frame_id UInt32
)
ENGINE = MergeTree
PARTITION BY toDate(sample_timestamp)
ORDER BY (sample_timestamp, trace_id, frame_id)
SETTINGS index_granularity = 8192
"""

COMPACT_TABLE_SQL = """
CREATE TABLE box_info_compact_zstd (
    trace_id UInt32 CODEC(ZSTD(3)),
    sample_date Date CODEC(ZSTD(1)),
    sample_hour UInt8 CODEC(ZSTD(1)),
    sample_offset_ms UInt32 CODEC(ZSTD(3)),
    obj_type UInt8 CODEC(ZSTD(1)),
    position_x_mm Int32 CODEC(ZSTD(3)),
    position_y_mm Int32 CODEC(ZSTD(3)),
    position_z_mm Int32 CODEC(ZSTD(3)),
    length_mm UInt16 CODEC(ZSTD(1)),
    width_mm UInt16 CODEC(ZSTD(1)),
    height_mm UInt16 CODEC(ZSTD(1)),
    speed_centi_kmh UInt16 CODEC(ZSTD(1)),
    spindle_centi_deg UInt16 CODEC(ZSTD(1)),
    lane_id UInt8 CODEC(ZSTD(1)),
    frame_id UInt32 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY (sample_date, sample_hour)
ORDER BY (sample_date, sample_hour, sample_offset_ms, trace_id, frame_id)
SETTINGS index_granularity = 8192
"""


@dataclass
class ReceiverStats:
    start_monotonic: float = field(default_factory=time.monotonic)
    rows_received: int = 0
    rows_inserted: int = 0
    batches_received: int = 0
    insert_calls: int = 0
    bytes_received: int = 0
    peak_rss_bytes: int = 0
    peak_cpu_percent: float = 0.0
    clickhouse_peak_rss_bytes: int = 0
    clickhouse_peak_cpu_percent: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(frozen=True)
class TableMetrics:
    rows: int
    active_parts: int
    active_bytes_on_disk: int
    active_compressed_bytes: int
    active_uncompressed_bytes: int


@dataclass(frozen=True)
class ReceiverLayout:
    storage_layout: str
    table_name: str
    create_table_sql: str
    columns: tuple[str, ...]
    partitioning: tuple[str, ...]
    sort_keys: tuple[str, ...]
    logical_codec: str
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
    parser = argparse.ArgumentParser(description="Synchronously receive box batches and write them into ClickHouse.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--storage-layout", choices=SUPPORTED_LAYOUT_CHOICES, default="clickhouse_compact_zstd")
    parser.add_argument("--insert-row-target", type=int, default=10000)
    parser.add_argument("--clickhouse-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-port", type=int, default=18123)
    parser.add_argument("--clickhouse-database", default="traj_store_clickhose")
    parser.add_argument("--clickhouse-user", default="bench")
    parser.add_argument("--clickhouse-password", default="benchpass")
    parser.add_argument("--clickhouse-data-dir", required=True)
    parser.add_argument("--clickhouse-container-pid", type=int, required=True)
    parser.add_argument("--optimize-final", action="store_true")
    return parser.parse_args()


def initialize_layout(storage_layout: str) -> ReceiverLayout:
    if storage_layout == "clickhouse_raw_lz4":
        return ReceiverLayout(
            storage_layout=storage_layout,
            table_name="box_info_raw_lz4",
            create_table_sql=RAW_TABLE_SQL,
            columns=(
                "trace_id",
                "sample_timestamp",
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
            partitioning=("toDate(sample_timestamp)",),
            sort_keys=("sample_timestamp", "trace_id", "frame_id"),
            logical_codec="LZ4(default)",
            compact=False,
        )

    if storage_layout == "clickhouse_compact_zstd":
        return ReceiverLayout(
            storage_layout=storage_layout,
            table_name="box_info_compact_zstd",
            create_table_sql=COMPACT_TABLE_SQL,
            columns=(
                "trace_id",
                "sample_date",
                "sample_hour",
                "sample_offset_ms",
                "obj_type",
                "position_x_mm",
                "position_y_mm",
                "position_z_mm",
                "length_mm",
                "width_mm",
                "height_mm",
                "speed_centi_kmh",
                "spindle_centi_deg",
                "lane_id",
                "frame_id",
            ),
            partitioning=("sample_date", "sample_hour"),
            sort_keys=("sample_date", "sample_hour", "sample_offset_ms", "trace_id", "frame_id"),
            logical_codec="ZSTD",
            compact=True,
        )

    raise ValueError(f"Unsupported storage layout: {storage_layout}")


def raw_clickhouse_row(raw_row: list) -> tuple:
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
    return (
        int(trace_id),
        format_datetime64_ms(int(sample_timestamp)),
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


def compact_clickhouse_row(raw_row: list) -> tuple:
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
    sample_date, sample_hour, sample_offset_ms = utc_hour_partition_fields(int(sample_timestamp))
    return (
        int(trace_id),
        sample_date,
        int(sample_hour),
        int(sample_offset_ms),
        int(obj_type),
        int(round(float(position_x) * COORDINATE_SCALE)),
        int(round(float(position_y) * COORDINATE_SCALE)),
        int(round(float(position_z) * COORDINATE_SCALE)),
        int(round(float(length) * SIZE_SCALE)),
        int(round(float(width) * SIZE_SCALE)),
        int(round(float(height) * SIZE_SCALE)),
        int(round(float(speed_kmh) * SPEED_SCALE)),
        int(round(float(spindle) * ANGLE_SCALE)),
        int(lane_id),
        int(frame_id),
    )


def fetch_table_metrics(client: ClickHouseClient, table_name: str) -> TableMetrics:
    query = f"""
    SELECT
        toUInt64(coalesce(sum(rows), 0)),
        toUInt64(count()),
        toUInt64(coalesce(sum(bytes_on_disk), 0)),
        toUInt64(coalesce(sum(data_compressed_bytes), 0)),
        toUInt64(coalesce(sum(data_uncompressed_bytes), 0))
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = '{table_name}'
      AND active
    """
    row = client.query_tsv_row(query)
    if not row:
        return TableMetrics(0, 0, 0, 0, 0)
    return TableMetrics(*(int(value) for value in row))


def sample_metrics_once(
    *,
    writer: csv.DictWriter,
    metrics_file,
    stats: ReceiverStats,
    receiver_process: psutil.Process,
    clickhouse_monitor: ProcessTreeMonitor,
    sink_dir: Path,
    clickhouse_data_dir: Path,
    client: ClickHouseClient,
    layout: ReceiverLayout,
    pending_rows: list[tuple],
) -> None:
    usage = sink_disk_usage(sink_dir=sink_dir, clickhouse_data_dir=clickhouse_data_dir)
    table_metrics = fetch_table_metrics(client, layout.table_name)
    receiver_rss_bytes = receiver_process.memory_info().rss
    receiver_cpu_percent = receiver_process.cpu_percent(interval=None) / max(psutil.cpu_count() or 1, 1)
    clickhouse_cpu_percent, clickhouse_rss_bytes = clickhouse_monitor.sample()

    with stats.lock:
        stats.peak_rss_bytes = max(stats.peak_rss_bytes, receiver_rss_bytes)
        stats.peak_cpu_percent = max(stats.peak_cpu_percent, receiver_cpu_percent)
        stats.clickhouse_peak_rss_bytes = max(stats.clickhouse_peak_rss_bytes, clickhouse_rss_bytes)
        stats.clickhouse_peak_cpu_percent = max(stats.clickhouse_peak_cpu_percent, clickhouse_cpu_percent)
        rows_received = stats.rows_received
        rows_inserted = stats.rows_inserted
        batches_received = stats.batches_received
        insert_calls = stats.insert_calls
        bytes_received = stats.bytes_received
        elapsed_seconds = time.monotonic() - stats.start_monotonic

    writer.writerow(
        {
            "sampled_at_utc": utc_now_iso(),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "rows_received": rows_received,
            "rows_inserted": rows_inserted,
            "batches_received": batches_received,
            "insert_calls": insert_calls,
            "bytes_received": bytes_received,
            "pending_rows": len(pending_rows),
            "avg_rows_per_second": round(rows_inserted / max(elapsed_seconds, 1e-9), 3),
            "receiver_cpu_percent": round(receiver_cpu_percent, 3),
            "receiver_rss_bytes": receiver_rss_bytes,
            "clickhouse_cpu_percent": round(clickhouse_cpu_percent, 3),
            "clickhouse_rss_bytes": clickhouse_rss_bytes,
            "sink_bytes": usage.sink_bytes,
            "clickhouse_data_bytes": usage.clickhouse_data_bytes,
            "table_rows": table_metrics.rows,
            "active_parts": table_metrics.active_parts,
            "active_bytes_on_disk": table_metrics.active_bytes_on_disk,
            "active_compressed_bytes": table_metrics.active_compressed_bytes,
            "active_uncompressed_bytes": table_metrics.active_uncompressed_bytes,
        }
    )
    metrics_file.flush()


def run_sampler(
    *,
    stop_event: threading.Event,
    stats: ReceiverStats,
    receiver_process: psutil.Process,
    clickhouse_monitor: ProcessTreeMonitor,
    sink_dir: Path,
    clickhouse_data_dir: Path,
    client: ClickHouseClient,
    layout: ReceiverLayout,
    pending_rows: list[tuple],
    metrics_path: Path,
    interval_seconds: float,
) -> None:
    receiver_process.cpu_percent(interval=None)
    clickhouse_monitor.prime()
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
                "insert_calls",
                "bytes_received",
                "pending_rows",
                "avg_rows_per_second",
                "receiver_cpu_percent",
                "receiver_rss_bytes",
                "clickhouse_cpu_percent",
                "clickhouse_rss_bytes",
                "sink_bytes",
                "clickhouse_data_bytes",
                "table_rows",
                "active_parts",
                "active_bytes_on_disk",
                "active_compressed_bytes",
                "active_uncompressed_bytes",
            ],
        )
        writer.writeheader()
        sample_metrics_once(
            writer=writer,
            metrics_file=metrics_file,
            stats=stats,
            receiver_process=receiver_process,
            clickhouse_monitor=clickhouse_monitor,
            sink_dir=sink_dir,
            clickhouse_data_dir=clickhouse_data_dir,
            client=client,
            layout=layout,
            pending_rows=pending_rows,
        )
        while not stop_event.wait(interval_seconds):
            sample_metrics_once(
                writer=writer,
                metrics_file=metrics_file,
                stats=stats,
                receiver_process=receiver_process,
                clickhouse_monitor=clickhouse_monitor,
                sink_dir=sink_dir,
                clickhouse_data_dir=clickhouse_data_dir,
                client=client,
                layout=layout,
                pending_rows=pending_rows,
            )
        sample_metrics_once(
            writer=writer,
            metrics_file=metrics_file,
            stats=stats,
            receiver_process=receiver_process,
            clickhouse_monitor=clickhouse_monitor,
            sink_dir=sink_dir,
            clickhouse_data_dir=clickhouse_data_dir,
            client=client,
            layout=layout,
            pending_rows=pending_rows,
        )


def main() -> None:
    args = parse_args()
    storage_layout = normalize_storage_layout(args.storage_layout)
    layout = initialize_layout(storage_layout)

    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    sink_dir = run_dir / "sink"
    sink_dir.mkdir(parents=True, exist_ok=True)
    clickhouse_data_dir = Path(args.clickhouse_data_dir).resolve()
    clickhouse_data_dir.mkdir(parents=True, exist_ok=True)

    metrics_path = run_dir / "receiver_metrics.csv"
    ready_path = run_dir / "receiver_ready.json"
    summary_path = run_dir / "receiver_summary.json"
    metadata_path = sink_dir / "storage_layout.json"

    client = ClickHouseClient(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        database=args.clickhouse_database,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    metrics_client = ClickHouseClient(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        database=args.clickhouse_database,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    client.execute(f"CREATE DATABASE IF NOT EXISTS {args.clickhouse_database}", database="default")
    client.execute(f"DROP TABLE IF EXISTS {layout.table_name}")
    client.execute(layout.create_table_sql)

    metadata = {
        "storage_layout": layout.storage_layout,
        "sink_kind": "clickhouse",
        "table_name": layout.table_name,
        "partitioning": list(layout.partitioning),
        "sort_keys": list(layout.sort_keys),
        "insert_row_target": args.insert_row_target,
        "logical_codec": layout.logical_codec,
        "compact_schema": layout.compact,
        "clickhouse_database": args.clickhouse_database,
        "clickhouse_user": args.clickhouse_user,
        "clickhouse_host": args.clickhouse_host,
        "clickhouse_port": args.clickhouse_port,
        "optimize_final": args.optimize_final,
    }
    write_json(metadata_path, metadata)

    stats = ReceiverStats()
    receiver_process = psutil.Process()
    clickhouse_monitor = ProcessTreeMonitor(args.clickhouse_container_pid)
    stop_event = threading.Event()
    pending_rows: list[tuple] = []

    sampler_thread = threading.Thread(
        target=run_sampler,
        kwargs={
            "stop_event": stop_event,
            "stats": stats,
            "receiver_process": receiver_process,
            "clickhouse_monitor": clickhouse_monitor,
            "sink_dir": sink_dir,
            "clickhouse_data_dir": clickhouse_data_dir,
            "client": metrics_client,
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
        inserted = client.insert_tsv(layout.table_name, layout.columns, pending_rows)
        pending_rows.clear()
        with stats.lock:
            stats.rows_inserted += inserted
            stats.insert_calls += 1
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
                "clickhouse_database": args.clickhouse_database,
                "clickhouse_data_dir": str(clickhouse_data_dir),
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
                for row in rows:
                    if len(row) != NETWORK_FIELD_COUNT:
                        raise ValueError(f"Expected {NETWORK_FIELD_COUNT} fields, got {len(row)}")
                    if layout.compact:
                        pending_rows.append(compact_clickhouse_row(row))
                    else:
                        pending_rows.append(raw_clickhouse_row(row))

                if len(pending_rows) >= max(args.insert_row_target, 1):
                    flush_pending_rows()

                with stats.lock:
                    stats.rows_received += len(rows)
                    stats.batches_received += 1
                    stats.bytes_received += len(raw_line)

    flush_pending_rows()
    ingest_elapsed_seconds = max(time.monotonic() - stats.start_monotonic, 1e-9)
    optimize_elapsed_seconds = 0.0
    if args.optimize_final:
        optimize_start = time.monotonic()
        client.execute(f"OPTIMIZE TABLE {layout.table_name} FINAL")
        optimize_elapsed_seconds = time.monotonic() - optimize_start

    table_metrics = fetch_table_metrics(client, layout.table_name)
    row_count = int(client.query_single_value(f"SELECT count() FROM {layout.table_name}"))
    total_elapsed_seconds = max(time.monotonic() - stats.start_monotonic, 1e-9)
    usage = sink_disk_usage(sink_dir=sink_dir, clickhouse_data_dir=clickhouse_data_dir)

    stop_event.set()
    sampler_thread.join(timeout=5)

    with stats.lock:
        summary = {
            "completed_at_utc": utc_now_iso(),
            "storage_layout": layout.storage_layout,
            "sink_kind": "clickhouse",
            "table_name": layout.table_name,
            "rows_received": stats.rows_received,
            "rows_inserted": stats.rows_inserted,
            "rows_in_sink": row_count,
            "batches_received": stats.batches_received,
            "insert_calls": stats.insert_calls,
            "bytes_received": stats.bytes_received,
            "ingest_elapsed_seconds": ingest_elapsed_seconds,
            "elapsed_seconds": total_elapsed_seconds,
            "avg_rows_per_second_ingest": stats.rows_inserted / ingest_elapsed_seconds,
            "avg_rows_per_second_total": stats.rows_inserted / total_elapsed_seconds,
            "receiver_peak_rss_bytes": stats.peak_rss_bytes,
            "receiver_peak_cpu_percent": stats.peak_cpu_percent,
            "clickhouse_peak_rss_bytes": stats.clickhouse_peak_rss_bytes,
            "clickhouse_peak_cpu_percent": stats.clickhouse_peak_cpu_percent,
            "active_parts_post_run": table_metrics.active_parts,
            "active_rows_post_run": table_metrics.rows,
            "active_bytes_on_disk_post_run": table_metrics.active_bytes_on_disk,
            "active_compressed_bytes_post_run": table_metrics.active_compressed_bytes,
            "active_uncompressed_bytes_post_run": table_metrics.active_uncompressed_bytes,
            "sink_dir": str(sink_dir),
            "clickhouse_data_dir": str(clickhouse_data_dir),
            "sink_bytes_post_run": usage.sink_bytes,
            "clickhouse_data_bytes_post_run": usage.clickhouse_data_bytes,
            "bytes_per_row_clickhouse_data_post_run": usage.clickhouse_data_bytes / max(row_count, 1),
            "bytes_per_row_active_bytes_post_run": table_metrics.active_bytes_on_disk / max(row_count, 1),
            "logical_codec": layout.logical_codec,
            "optimize_final": args.optimize_final,
            "optimize_final_seconds": optimize_elapsed_seconds,
            "storage_metadata_path": str(metadata_path),
        }

    write_json(summary_path, summary)
    print("\n".join(f"{key}={value}" for key, value in summary.items()))


if __name__ == "__main__":
    main()
