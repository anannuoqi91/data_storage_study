#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from ingest_common import ANGLE_SCALE, COORDINATE_SCALE, SIZE_SCALE, SPEED_SCALE
from schema_versioning import resolve_dataset_scan_glob, resolve_dataset_version_and_path


BASE_DIR = Path(__file__).resolve().parents[1]
HOT_DB_PATH = BASE_DIR / "data" / "tmp" / "hot_day.duckdb"


def box_info_decoded_scan_sql(*, box_info_glob: str) -> str:
    return f"""
        SELECT
            trace_id,
            epoch_ms(strptime(date || ' ' || hour || ':00:00', '%Y-%m-%d %H:%M:%S')) + sample_offset_ms AS sample_timestamp,
            obj_type,
            position_x_mm / CAST({COORDINATE_SCALE} AS DOUBLE) AS position_x,
            position_y_mm / CAST({COORDINATE_SCALE} AS DOUBLE) AS position_y,
            position_z_mm / CAST({COORDINATE_SCALE} AS DOUBLE) AS position_z,
            length_mm / CAST({SIZE_SCALE} AS DOUBLE) AS length,
            width_mm / CAST({SIZE_SCALE} AS DOUBLE) AS width,
            height_mm / CAST({SIZE_SCALE} AS DOUBLE) AS height,
            speed_centi_kmh / CAST({SPEED_SCALE} AS DOUBLE) AS speed_kmh,
            spindle_centi_deg / CAST({ANGLE_SCALE} AS DOUBLE) AS spindle,
            lane_id,
            frame_id,
            date,
            hour
        FROM read_parquet('{box_info_glob}', hive_partitioning = true)
    """


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a one-day indexed DuckDB cache.")
    parser.add_argument("--date", required=True, help="Day to import, e.g. 2025-03-14")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    box_info_version, box_info_path = resolve_dataset_version_and_path("box_info")
    if not any(box_info_path.rglob("*.parquet")):
        raise SystemExit("No box_info parquet files found in the active dataset version. Run 02_simulate_writer.py first.")

    events_version, events_path = resolve_dataset_version_and_path("events", create_if_missing=True)

    HOT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    day_suffix = args.date.replace("-", "")
    box_table = f"box_info_{day_suffix}"
    event_table = f"events_overspeed_{day_suffix}"

    box_info_glob = resolve_dataset_scan_glob("box_info")
    events_glob = resolve_dataset_scan_glob("events")
    decoded_box_info_scan = box_info_decoded_scan_sql(box_info_glob=box_info_glob)

    con = duckdb.connect(str(HOT_DB_PATH))
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {box_table} AS
        SELECT
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
        FROM (
            {decoded_box_info_scan}
        ) AS box_info_decoded
        WHERE date = '{args.date}'
        """
    )

    con.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{box_table}_trace_ts
        ON {box_table}(trace_id, sample_timestamp)
        """
    )
    con.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{box_table}_trace_frame
        ON {box_table}(trace_id, frame_id)
        """
    )

    if any(events_path.rglob("*.parquet")):
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {event_table} AS
            SELECT
                event_id,
                event_type,
                trace_id,
                event_timestamp,
                speed_kmh,
                lane_id,
                source_trace_id,
                source_sample_timestamp,
                source_frame_id,
                source_box_date,
                source_box_hour
            FROM read_parquet('{events_glob}', hive_partitioning = true)
            WHERE event_type = 'overspeed'
              AND date = '{args.date}'
            """
        )
        con.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{event_table}_trace_ts
            ON {event_table}(trace_id, event_timestamp)
            """
        )

    row_count = con.execute(f"SELECT count(*) FROM {box_table}").fetchone()[0]
    con.close()
    print(
        "\n".join(
            [
                f"hot_db={HOT_DB_PATH}",
                f"box_table={box_table}",
                f"box_info_version={box_info_version}",
                f"events_version={events_version}",
                f"date={args.date}",
                f"box_row_count={row_count}",
            ]
        )
    )


if __name__ == "__main__":
    main()
