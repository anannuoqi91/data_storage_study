#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "duckdb" / "writer.duckdb"
BOX_INFO_GLOB = BASE_DIR / "data" / "lake" / "box_info" / "**" / "*.parquet"
EVENTS_PATH = BASE_DIR / "data" / "lake" / "events"
INIT_SQL_PATH = BASE_DIR / "scripts" / "01_init.sql"


def ensure_dirs() -> None:
    EVENTS_PATH.mkdir(parents=True, exist_ok=True)


def apply_init_sql(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(INIT_SQL_PATH.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize overspeed events from box_info parquet.")
    parser.add_argument("--date", required=True, help="Day to materialize, e.g. 2025-03-14")
    parser.add_argument("--speed-threshold", type=float, default=80.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()

    box_info_files = list((BASE_DIR / "data" / "lake" / "box_info").rglob("*.parquet"))
    if not box_info_files:
        raise SystemExit("No box_info parquet files found. Run 02_simulate_writer.py first.")

    con = duckdb.connect(str(DB_PATH))
    apply_init_sql(con)

    box_info_path = BOX_INFO_GLOB.as_posix()
    events_path = EVENTS_PATH.as_posix()

    existing_event_scan = "SELECT NULL::VARCHAR AS event_type, NULL::BIGINT AS trace_id, NULL::BIGINT AS event_timestamp WHERE FALSE"
    if any(EVENTS_PATH.rglob("*.parquet")):
        existing_event_scan = f"""
        SELECT
            event_type,
            trace_id,
            event_timestamp
        FROM read_parquet('{events_path}/**/*.parquet', hive_partitioning = true)
        """

    export_sql = f"""
    COPY (
        WITH candidate_events AS (
            SELECT
                trace_id,
                min(sample_timestamp) AS event_timestamp
            FROM read_parquet('{box_info_path}', hive_partitioning = true)
            WHERE date = '{args.date}'
              AND speed_kmh > {args.speed_threshold}
            GROUP BY trace_id
        ),
        source_rows AS (
            SELECT
                b.trace_id,
                b.sample_timestamp,
                b.frame_id,
                b.speed_kmh,
                b.lane_id,
                b.date AS source_box_date,
                b.hour AS source_box_hour
            FROM read_parquet('{box_info_path}', hive_partitioning = true) AS b
            JOIN candidate_events AS c
              ON b.trace_id = c.trace_id
             AND b.sample_timestamp = c.event_timestamp
            WHERE b.date = '{args.date}'
        ),
        event_rows AS (
            SELECT
                hash('overspeed', s.trace_id, s.sample_timestamp, s.frame_id)::VARCHAR AS event_id,
                'overspeed' AS event_type,
                s.trace_id,
                s.sample_timestamp AS event_timestamp,
                strftime(make_timestamp_ms(s.sample_timestamp), '%Y-%m-%d') AS date,
                strftime(make_timestamp_ms(s.sample_timestamp), '%H') AS hour,
                s.speed_kmh,
                s.lane_id,
                s.trace_id AS source_trace_id,
                s.sample_timestamp AS source_sample_timestamp,
                s.frame_id AS source_frame_id,
                s.source_box_date,
                s.source_box_hour,
                current_timestamp AS generated_at
            FROM source_rows AS s
        ),
        existing_events AS (
            {existing_event_scan}
        )
        SELECT
            e.event_id,
            e.event_type,
            e.trace_id,
            e.event_timestamp,
            e.date,
            e.hour,
            e.speed_kmh,
            e.lane_id,
            e.source_trace_id,
            e.source_sample_timestamp,
            e.source_frame_id,
            e.source_box_date,
            e.source_box_hour,
            e.generated_at
        FROM event_rows AS e
        LEFT JOIN existing_events AS x
          ON e.event_type = x.event_type
         AND e.trace_id = x.trace_id
         AND e.event_timestamp = x.event_timestamp
        WHERE x.trace_id IS NULL
        ORDER BY event_timestamp, trace_id
    )
    TO '{events_path}'
    (
        FORMAT parquet,
        PARTITION_BY (event_type, date, hour),
        COMPRESSION zstd,
        APPEND,
        FILENAME_PATTERN 'part_{{uuid}}'
    )
    """
    con.execute(export_sql)

    event_files = list(EVENTS_PATH.rglob("*.parquet"))
    if event_files:
        max_event_ts = con.execute(
            f"""
            SELECT coalesce(max(event_timestamp), 0)
            FROM read_parquet('{events_path}/**/*.parquet', hive_partitioning = true)
            WHERE event_type = 'overspeed'
            """
        ).fetchone()[0]
        event_count = con.execute(
            f"""
            SELECT count(*)
            FROM read_parquet('{events_path}/**/*.parquet', hive_partitioning = true)
            WHERE event_type = 'overspeed'
              AND date = '{args.date}'
            """
        ).fetchone()[0]
    else:
        max_event_ts = 0
        event_count = 0

    con.execute(
        f"""
        UPDATE od.event_materialization_checkpoint
        SET processed_before_ts = {max_event_ts},
            updated_at = current_timestamp
        WHERE event_type = 'overspeed'
        """
    )

    print(
        "\n".join(
            [
                f"events_lake={EVENTS_PATH}",
                f"event_type=overspeed",
                f"date={args.date}",
                f"speed_threshold={args.speed_threshold}",
                f"event_count_for_day={event_count}",
            ]
        )
    )


if __name__ == "__main__":
    main()
