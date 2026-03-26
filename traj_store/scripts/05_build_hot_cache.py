#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
HOT_DB_PATH = BASE_DIR / "data" / "tmp" / "hot_day.duckdb"
BOX_INFO_GLOB = BASE_DIR / "data" / "lake" / "box_info" / "**" / "*.parquet"
EVENTS_GLOB = BASE_DIR / "data" / "lake" / "events" / "**" / "*.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a one-day indexed DuckDB cache.")
    parser.add_argument("--date", required=True, help="Day to import, e.g. 2025-03-14")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    box_info_root = BASE_DIR / "data" / "lake" / "box_info"
    if not any(box_info_root.rglob("*.parquet")):
        raise SystemExit("No box_info parquet files found. Run 02_simulate_writer.py first.")

    HOT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    day_suffix = args.date.replace("-", "")
    box_table = f"box_info_{day_suffix}"
    event_table = f"events_overspeed_{day_suffix}"

    con = duckdb.connect(str(HOT_DB_PATH))
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {box_table} AS
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
            frame_id
        FROM read_parquet('{BOX_INFO_GLOB.as_posix()}', hive_partitioning = true)
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

    if any((BASE_DIR / "data" / "lake" / "events").rglob("*.parquet")):
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
            FROM read_parquet('{EVENTS_GLOB.as_posix()}', hive_partitioning = true)
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
    print(
        "\n".join(
            [
                f"hot_db={HOT_DB_PATH}",
                f"box_table={box_table}",
                f"date={args.date}",
                f"box_row_count={row_count}",
            ]
        )
    )


if __name__ == "__main__":
    main()
