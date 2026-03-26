#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from ingest_common import SPEED_SCALE
from schema_versioning import (
    WRITER_DB_PATH,
    ensure_dataset_manifest,
    ensure_writer_schema_latest,
    load_dataset_manifest,
    resolve_dataset_scan_glob,
    resolve_dataset_version_and_path,
)


BASE_DIR = Path(__file__).resolve().parents[1]
EVENTS_DATASET = "events"
BOX_INFO_DATASET = "box_info"


def ensure_dirs(events_path: Path) -> None:
    events_path.mkdir(parents=True, exist_ok=True)


def box_info_decoded_scan_sql(*, box_info_glob: str, date: str) -> str:
    return f"""
        SELECT
            trace_id,
            epoch_ms(strptime(date || ' ' || hour || ':00:00', '%Y-%m-%d %H:%M:%S')) + sample_offset_ms AS sample_timestamp,
            frame_id,
            speed_centi_kmh / CAST({SPEED_SCALE} AS DOUBLE) AS speed_kmh,
            lane_id,
            date,
            hour
        FROM read_parquet('{box_info_glob}', hive_partitioning = true)
        WHERE date = '{date}'
    """


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize overspeed events from box_info parquet.")
    parser.add_argument("--date", required=True, help="Day to materialize, e.g. 2025-03-14")
    parser.add_argument("--speed-threshold", type=float, default=80.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    box_info_version, box_info_path = resolve_dataset_version_and_path(BOX_INFO_DATASET)
    if not any(box_info_path.rglob("*.parquet")):
        raise SystemExit("No box_info parquet files found in the active dataset version. Run 02_simulate_writer.py first.")

    events_version, events_path = resolve_dataset_version_and_path(EVENTS_DATASET, create_if_missing=True)
    ensure_dirs(events_path)

    con = duckdb.connect(str(WRITER_DB_PATH))
    writer_version = ensure_writer_schema_latest(
        con,
        note="auto-ensure latest writer schema via 04_build_events.py",
        db_path=WRITER_DB_PATH,
    )

    box_info_glob = resolve_dataset_scan_glob(BOX_INFO_DATASET)
    events_glob = resolve_dataset_scan_glob(EVENTS_DATASET)
    decoded_box_info_scan = box_info_decoded_scan_sql(box_info_glob=box_info_glob, date=args.date)

    existing_event_scan = "SELECT NULL::VARCHAR AS event_type, NULL::BIGINT AS trace_id, NULL::BIGINT AS event_timestamp WHERE FALSE"
    if any(events_path.rglob("*.parquet")):
        existing_event_scan = f"""
        SELECT
            event_type,
            trace_id,
            event_timestamp
        FROM read_parquet('{events_glob}', hive_partitioning = true)
        """

    export_sql = f"""
    COPY (
        WITH box_info_decoded AS (
            {decoded_box_info_scan}
        ),
        candidate_events AS (
            SELECT
                trace_id,
                min(sample_timestamp) AS event_timestamp
            FROM box_info_decoded
            WHERE speed_kmh > {args.speed_threshold}
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
            FROM box_info_decoded AS b
            JOIN candidate_events AS c
              ON b.trace_id = c.trace_id
             AND b.sample_timestamp = c.event_timestamp
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
    TO '{events_path.as_posix()}'
    (
        FORMAT parquet,
        PARTITION_BY (event_type, date, hour),
        COMPRESSION zstd,
        APPEND,
        FILENAME_PATTERN 'part_{{uuid}}'
    )
    """
    con.execute(export_sql)

    if any(events_path.rglob("*.parquet")):
        max_event_ts = con.execute(
            f"""
            SELECT coalesce(max(event_timestamp), 0)
            FROM read_parquet('{events_glob}', hive_partitioning = true)
            WHERE event_type = 'overspeed'
            """
        ).fetchone()[0]
        event_count = con.execute(
            f"""
            SELECT count(*)
            FROM read_parquet('{events_glob}', hive_partitioning = true)
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

    box_info_manifest = load_dataset_manifest(BOX_INFO_DATASET)
    events_manifest_path = ensure_dataset_manifest(
        EVENTS_DATASET,
        schema_version="events.v1",
        producer_script="04_build_events.py",
        source_dependencies={
            "box_info": {
                "active_version": box_info_version,
                "manifest": box_info_manifest,
            },
            "writer.duckdb": {
                "current_version": writer_version,
                "db_path": str(WRITER_DB_PATH),
            },
        },
        note="Updated after overspeed event materialization from compact box_info parquet",
        extra={
            "active_version": events_version,
            "event_type": "overspeed",
            "speed_threshold": args.speed_threshold,
        },
    )
    con.close()

    print(
        "\n".join(
            [
                f"events_lake={events_path}",
                f"events_version={events_version}",
                f"events_manifest={events_manifest_path}",
                f"box_info_version={box_info_version}",
                f"writer_schema_version={writer_version}",
                f"event_type=overspeed",
                f"date={args.date}",
                f"speed_threshold={args.speed_threshold}",
                f"event_count_for_day={event_count}",
            ]
        )
    )


if __name__ == "__main__":
    main()
