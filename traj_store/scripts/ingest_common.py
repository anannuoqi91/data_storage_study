from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
BENCHMARK_ROOT = BASE_DIR / "data" / "benchmarks"
DEFAULT_START_TS_MS = 1741910400000
NETWORK_FIELD_COUNT = 13

COORDINATE_SCALE = 1000
SIZE_SCALE = 1000
SPEED_SCALE = 100
ANGLE_SCALE = 100

DUCKDB_LAYOUTS = ("duckdb_raw", "duckdb_compact")
PARQUET_LAYOUTS = ("parquet_snappy", "parquet_zstd", "parquet_compact_zstd")
LEGACY_LAYOUT_ALIASES = {
    "raw": "duckdb_raw",
    "compact": "duckdb_compact",
}
SUPPORTED_LAYOUTS = DUCKDB_LAYOUTS + PARQUET_LAYOUTS
SUPPORTED_LAYOUT_CHOICES = SUPPORTED_LAYOUTS + tuple(LEGACY_LAYOUT_ALIASES.keys())


@dataclass(frozen=True)
class BoxStreamConfig:
    fps: int = 10
    vehicles_per_frame: int = 200
    batch_frames: int = 5
    start_ts_ms: int = DEFAULT_START_TS_MS
    seed: int = 7
    pace: str = "realtime"


@dataclass(frozen=True)
class StorageUsage:
    sink_bytes: int
    duckdb_bytes: int
    wal_bytes: int
    parquet_bytes: int


@dataclass(frozen=True)
class CompactEncodingConfig:
    coordinate_scale: int = COORDINATE_SCALE
    size_scale: int = SIZE_SCALE
    speed_scale: int = SPEED_SCALE
    angle_scale: int = ANGLE_SCALE


def normalize_storage_layout(storage_layout: str) -> str:
    return LEGACY_LAYOUT_ALIASES.get(storage_layout, storage_layout)


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def make_run_dir(run_name: str | None = None) -> Path:
    BENCHMARK_ROOT.mkdir(parents=True, exist_ok=True)
    base_name = run_name or datetime.now(tz=UTC).strftime("ingest_%Y%m%dT%H%M%SZ")
    candidate = BENCHMARK_ROOT / base_name
    suffix = 1
    while candidate.exists():
        candidate = BENCHMARK_ROOT / f"{base_name}_{suffix:02d}"
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def path_size_bytes(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(entry.stat().st_size for entry in path.rglob("*") if entry.is_file())


def sink_disk_usage(sink_dir: Path, db_path: Path | None = None, parquet_dir: Path | None = None) -> StorageUsage:
    wal_path = Path(f"{db_path}.wal") if db_path is not None else None
    duckdb_bytes = path_size_bytes(db_path)
    wal_bytes = path_size_bytes(wal_path)
    parquet_bytes = path_size_bytes(parquet_dir)
    sink_bytes = path_size_bytes(sink_dir)
    return StorageUsage(
        sink_bytes=sink_bytes,
        duckdb_bytes=duckdb_bytes,
        wal_bytes=wal_bytes,
        parquet_bytes=parquet_bytes,
    )


def sender_summary_dict(*, batches_sent: int, rows_sent: int, bytes_sent: int, elapsed_seconds: float) -> dict:
    return {
        "batches_sent": batches_sent,
        "rows_sent": rows_sent,
        "bytes_sent": bytes_sent,
        "elapsed_seconds": elapsed_seconds,
        "avg_rows_per_second": rows_sent / max(elapsed_seconds, 1e-9),
        "completed_at_utc": utc_now_iso(),
    }


def generate_box_batch(start_frame_id: int, config: BoxStreamConfig, rng: random.Random) -> tuple[list[list], int]:
    rows: list[list] = []
    next_frame_id = start_frame_id

    base_speeds = {
        100000 + vehicle_idx: 35.0 + (vehicle_idx % 7) * 8.0
        for vehicle_idx in range(config.vehicles_per_frame)
    }

    for _ in range(config.batch_frames):
        frame_offset_ms = int(round(next_frame_id * 1000.0 / config.fps))
        sample_timestamp = config.start_ts_ms + frame_offset_ms

        for vehicle_idx in range(config.vehicles_per_frame):
            trace_id = 100000 + vehicle_idx
            lane_id = vehicle_idx % 6 + 1
            obj_type = 1 if vehicle_idx % 10 else 2

            base_speed = base_speeds[trace_id]
            burst = 18.0 if vehicle_idx % 25 == 0 and 60 <= next_frame_id <= 120 else 0.0
            speed_kmh = round(base_speed + burst + rng.uniform(-3.0, 3.0), 2)

            position_x = round(vehicle_idx * 4.2 + next_frame_id * (speed_kmh / 36.0), 3)
            position_y = round(lane_id * 3.6 + rng.uniform(-0.15, 0.15), 3)
            position_z = round(rng.uniform(0.0, 0.3), 3)
            length = round(4.3 + (vehicle_idx % 5) * 0.2, 2)
            width = round(1.75 + (vehicle_idx % 3) * 0.08, 2)
            height = round(1.45 + (vehicle_idx % 4) * 0.05, 2)
            spindle = round((lane_id * 11.0 + next_frame_id * 0.7) % 360, 2)

            rows.append(
                [
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
                    next_frame_id,
                ]
            )

        next_frame_id += 1

    return rows, next_frame_id


def encode_batch_message(batch_id: int, rows: list[list]) -> bytes:
    payload = {
        "type": "box_batch",
        "batch_id": batch_id,
        "row_count": len(rows),
        "rows": rows,
    }
    return (json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def utc_partition_fields(sample_timestamp_ms: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(sample_timestamp_ms / 1000.0, tz=UTC)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def parquet_output_path(parquet_root: Path, part_date: str, part_hour: str, chunk_index: int) -> Path:
    return parquet_root / f"date={part_date}" / f"hour={part_hour}" / f"chunk_{chunk_index:06d}.parquet"


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def raw_box_row(*, raw_row: list) -> tuple:
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
        int(sample_timestamp),
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


def compact_box_row(*, raw_row: list, base_timestamp_ms: int, encoding: CompactEncodingConfig) -> tuple:
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
        int(sample_timestamp - base_timestamp_ms),
        int(obj_type),
        int(round(position_x * encoding.coordinate_scale)),
        int(round(position_y * encoding.coordinate_scale)),
        int(round(position_z * encoding.coordinate_scale)),
        int(round(length * encoding.size_scale)),
        int(round(width * encoding.size_scale)),
        int(round(height * encoding.size_scale)),
        int(round(speed_kmh * encoding.speed_scale)),
        int(round(spindle * encoding.angle_scale)),
        int(lane_id),
        int(frame_id),
    )
