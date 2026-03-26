#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "duckdb" / "writer.duckdb"
BOX_INFO_PATH = BASE_DIR / "data" / "lake" / "box_info"
INIT_SQL_PATH = BASE_DIR / "scripts" / "01_init.sql"


@dataclass(frozen=True)
class SimulationConfig:
    fps: int = 10
    vehicles_per_frame: int = 200
    sim_seconds: int = 30
    flush_every_frames: int = 50
    flush_lag_ms: int = 1000
    start_ts_ms: int = 1741910400000
    seed: int = 7


def ensure_dirs() -> None:
    (BASE_DIR / "data" / "duckdb").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "data" / "lake" / "box_info").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "data" / "lake" / "events").mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "data" / "tmp").mkdir(parents=True, exist_ok=True)


def apply_init_sql(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(INIT_SQL_PATH.read_text(encoding="utf-8"))


def generate_rows(config: SimulationConfig) -> Iterable[tuple]:
    rng = random.Random(config.seed)
    total_frames = config.fps * config.sim_seconds
    box_id = 1

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
                box_id,
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
            )
            box_id += 1


def insert_rows(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    con.executemany(
        """
        INSERT INTO od.box_info_staging
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def flush_to_parquet(
    con: duckdb.DuckDBPyConnection,
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
        SELECT
            box_id,
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
            strftime(make_timestamp_ms(sample_timestamp), '%Y-%m-%d') AS date,
            strftime(make_timestamp_ms(sample_timestamp), '%H') AS hour
        FROM od.box_info_staging
        WHERE sample_timestamp > {flushed_before_ts}
          AND sample_timestamp <= {safe_flush_ts}
        ORDER BY sample_timestamp, trace_id, frame_id
    )
    TO '{BOX_INFO_PATH.as_posix()}'
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

    ensure_dirs()
    con = duckdb.connect(str(DB_PATH))
    apply_init_sql(con)

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

        if (frame_id + 1) % config.flush_every_frames == 0 and row[1] == last_trace_id:
            total_flushed += flush_to_parquet(con, flush_lag_ms=config.flush_lag_ms)

    if batch:
        insert_rows(con, batch)

    total_flushed += flush_to_parquet(con, flush_lag_ms=config.flush_lag_ms, force=True)

    remaining_rows = con.execute("SELECT count(*) FROM od.box_info_staging").fetchone()[0]
    sample_day = datetime.fromtimestamp(config.start_ts_ms / 1000.0, tz=UTC).strftime("%Y-%m-%d")

    print(
        "\n".join(
            [
                f"writer_db={DB_PATH}",
                f"box_info_lake={BOX_INFO_PATH}",
                f"sample_day={sample_day}",
                f"rows_inserted={total_inserted}",
                f"rows_flushed={total_flushed}",
                f"staging_rows_remaining={remaining_rows}",
            ]
        )
    )


if __name__ == "__main__":
    main()
