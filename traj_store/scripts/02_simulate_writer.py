#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import duckdb

from ingest_common import ANGLE_SCALE, COORDINATE_SCALE, SIZE_SCALE, SPEED_SCALE
from schema_versioning import (
    WRITER_DB_PATH,
    dataset_root,
    ensure_dataset_manifest,
    ensure_writer_schema_latest,
    resolve_dataset_version_and_path,
)


BASE_DIR = Path(__file__).resolve().parents[1]
BOX_INFO_DATASET = "box_info"
BOX_INFO_SCHEMA_VERSION = "box_info.v2"


@dataclass(frozen=True)
class SimulationConfig:
    fps: int = 10
    vehicles_per_frame: int = 200
    sim_seconds: int = 30
    flush_every_frames: int = 50
    flush_lag_ms: int = 1000
    start_ts_ms: int = 1741910400000
    seed: int = 7


def ensure_dirs(box_info_path: Path) -> None:
    WRITER_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    dataset_root("box_info").mkdir(parents=True, exist_ok=True)
    dataset_root("events").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "data" / "tmp").mkdir(parents=True, exist_ok=True)
    box_info_path.mkdir(parents=True, exist_ok=True)


def generate_rows(config: SimulationConfig) -> Iterable[tuple]:
    rng = random.Random(config.seed)
    total_frames = config.fps * config.sim_seconds

    base_speeds = {
        100000 + vehicle_idx: 35.0 + (vehicle_idx % 7) * 8.0
        for vehicle_idx in range(config.vehicles_per_frame)
    }

    for frame_id in range(total_frames):
        frame_offset_ms = int(round(frame_id * 1000.0 / config.fps))
        sample_timestamp = config.start_ts_ms + frame_offset_ms

        for vehicle_idx in range(config.vehicles_per_frame):
            trace_id = 100000 + vehicle_idx
            lane_id = vehicle_idx % 6 + 1
            obj_type = 1 if vehicle_idx % 10 else 2

            base_speed = base_speeds[trace_id]
            burst = 18.0 if vehicle_idx % 25 == 0 and 60 <= frame_id <= 120 else 0.0
            speed_kmh = round(base_speed + burst + rng.uniform(-3.0, 3.0), 2)

            position_x = round(vehicle_idx * 4.2 + frame_id * (speed_kmh / 36.0), 3)
            position_y = round(lane_id * 3.6 + rng.uniform(-0.15, 0.15), 3)
            position_z = round(rng.uniform(0.0, 0.3), 3)
            length = round(4.3 + (vehicle_idx % 5) * 0.2, 2)
            width = round(1.75 + (vehicle_idx % 3) * 0.08, 2)
            height = round(1.45 + (vehicle_idx % 4) * 0.05, 2)
            spindle = round((lane_id * 11.0 + frame_id * 0.7) % 360, 2)

            yield (
                trace_id,
                sample_timestamp,
                obj_type,
                int(round(position_x * COORDINATE_SCALE)),
                int(round(position_y * COORDINATE_SCALE)),
                int(round(position_z * COORDINATE_SCALE)),
                int(round(length * SIZE_SCALE)),
                int(round(width * SIZE_SCALE)),
                int(round(height * SIZE_SCALE)),
                int(round(speed_kmh * SPEED_SCALE)),
                int(round(spindle * ANGLE_SCALE)),
                lane_id,
                frame_id,
            )


def insert_rows(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    con.executemany(
        """
        INSERT INTO od.box_info_staging
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def flush_to_parquet(
    con: duckdb.DuckDBPyConnection,
    box_info_path: Path,
    *,
    flush_lag_ms: int,
    force: bool = False,
) -> int:
    flushed_before_ts = con.execute(
        """
        SELECT flushed_before_ts
        FROM od.flush_checkpoint
        WHERE id = 1
        """
    ).fetchone()[0]

    max_ts = con.execute(
        """
        SELECT max(sample_timestamp)
        FROM od.box_info_staging
        """
    ).fetchone()[0]

    if max_ts is None:
        return 0

    safe_flush_ts = max_ts if force else max_ts - flush_lag_ms
    if safe_flush_ts <= flushed_before_ts:
        return 0

    row_count = con.execute(
        f"""
        SELECT count(*)
        FROM od.box_info_staging
        WHERE sample_timestamp > {flushed_before_ts}
          AND sample_timestamp <= {safe_flush_ts}
        """
    ).fetchone()[0]
    if row_count == 0:
        return 0

    export_sql = f"""
    COPY (
        WITH staged AS (
            SELECT
                trace_id,
                sample_timestamp,
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
                frame_id,
                strftime(make_timestamp_ms(sample_timestamp), '%Y-%m-%d') AS date,
                strftime(make_timestamp_ms(sample_timestamp), '%H') AS hour
            FROM od.box_info_staging
            WHERE sample_timestamp > {flushed_before_ts}
              AND sample_timestamp <= {safe_flush_ts}
        )
        SELECT
            trace_id,
            CAST(
                sample_timestamp - epoch_ms(strptime(date || ' ' || hour || ':00:00', '%Y-%m-%d %H:%M:%S'))
                AS INTEGER
            ) AS sample_offset_ms,
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
            frame_id,
            date,
            hour
        FROM staged
        ORDER BY date, hour, sample_timestamp, trace_id, frame_id
    )
    TO '{box_info_path.as_posix()}'
    (
        FORMAT parquet,
        PARTITION_BY (date, hour),
        COMPRESSION zstd,
        APPEND,
        FILENAME_PATTERN 'part_{{uuid}}'
    )
    """
    con.execute(export_sql)

    con.execute(
        f"""
        DELETE FROM od.box_info_staging
        WHERE sample_timestamp <= {safe_flush_ts}
        """
    )

    con.execute(
        f"""
        UPDATE od.flush_checkpoint
        SET flushed_before_ts = {safe_flush_ts},
            updated_at = current_timestamp
        WHERE id = 1
        """
    )

    con.execute(
        f"""
        INSERT INTO od.flush_log (range_start_ts, range_end_ts, row_count)
        VALUES ({flushed_before_ts + 1}, {safe_flush_ts}, {row_count})
        """
    )

    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate a single-writer DuckDB trajectory ingest.")
    parser.add_argument("--fps", type=int, default=SimulationConfig.fps)
    parser.add_argument("--vehicles-per-frame", type=int, default=SimulationConfig.vehicles_per_frame)
    parser.add_argument("--sim-seconds", type=int, default=SimulationConfig.sim_seconds)
    parser.add_argument("--flush-every-frames", type=int, default=SimulationConfig.flush_every_frames)
    parser.add_argument("--flush-lag-ms", type=int, default=SimulationConfig.flush_lag_ms)
    parser.add_argument("--start-ts-ms", type=int, default=SimulationConfig.start_ts_ms)
    parser.add_argument("--seed", type=int, default=SimulationConfig.seed)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = SimulationConfig(
        fps=args.fps,
        vehicles_per_frame=args.vehicles_per_frame,
        sim_seconds=args.sim_seconds,
        flush_every_frames=args.flush_every_frames,
        flush_lag_ms=args.flush_lag_ms,
        start_ts_ms=args.start_ts_ms,
        seed=args.seed,
    )

    box_info_version, box_info_path = resolve_dataset_version_and_path(BOX_INFO_DATASET, create_if_missing=True)
    ensure_dirs(box_info_path)

    con = duckdb.connect(str(WRITER_DB_PATH))
    writer_version = ensure_writer_schema_latest(
        con,
        note="auto-ensure latest writer schema via 02_simulate_writer.py",
        db_path=WRITER_DB_PATH,
    )

    batch: list[tuple] = []
    total_inserted = 0
    total_flushed = 0
    last_trace_id = 100000 + config.vehicles_per_frame - 1

    for row in generate_rows(config):
        batch.append(row)
        total_inserted += 1

        frame_id = row[-1]
        if len(batch) >= config.vehicles_per_frame:
            insert_rows(con, batch)
            batch.clear()

        if (frame_id + 1) % config.flush_every_frames == 0 and row[0] == last_trace_id:
            total_flushed += flush_to_parquet(con, box_info_path, flush_lag_ms=config.flush_lag_ms)

    if batch:
        insert_rows(con, batch)

    total_flushed += flush_to_parquet(con, box_info_path, flush_lag_ms=config.flush_lag_ms, force=True)

    remaining_rows = con.execute("SELECT count(*) FROM od.box_info_staging").fetchone()[0]
    sample_day = datetime.fromtimestamp(config.start_ts_ms / 1000.0, tz=UTC).strftime("%Y-%m-%d")
    manifest_path = ensure_dataset_manifest(
        "box_info",
        schema_version=BOX_INFO_SCHEMA_VERSION,
        producer_script="02_simulate_writer.py",
        source_dependencies={
            "writer.duckdb": {
                "current_version": writer_version,
                "db_path": str(WRITER_DB_PATH),
            }
        },
        note="Updated after compact box_info parquet flush",
        extra={
            "active_version": box_info_version,
            "sample_day": sample_day,
            "omits_synthetic_box_id": True,
            "timestamp_encoding": "partition(date/hour) + sample_offset_ms",
            "sort_keys": ["sample_offset_ms", "trace_id"],
            "coordinate_scale": COORDINATE_SCALE,
            "size_scale": SIZE_SCALE,
            "speed_scale": SPEED_SCALE,
            "angle_scale": ANGLE_SCALE,
        },
    )
    con.close()

    print(
        "\n".join(
            [
                f"writer_db={WRITER_DB_PATH}",
                f"writer_schema_version={writer_version}",
                f"box_info_lake={box_info_path}",
                f"box_info_version={box_info_version}",
                f"box_info_manifest={manifest_path}",
                f"sample_day={sample_day}",
                f"rows_inserted={total_inserted}",
                f"rows_flushed={total_flushed}",
                f"staging_rows_remaining={remaining_rows}",
            ]
        )
    )


if __name__ == "__main__":
    main()
