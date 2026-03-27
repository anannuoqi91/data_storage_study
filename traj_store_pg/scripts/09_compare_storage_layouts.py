#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ingest_common import (
    SUPPORTED_LAYOUTS,
    SUPPORTED_POSTGRES_PROFILE_CHOICES,
    normalize_postgres_profile,
    utc_now_iso,
    write_json,
)


SCRIPTS_DIR = Path(__file__).resolve().parent
BASE_DIR = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare PostgreSQL + TDengine storage layouts under the same ingest workload.")
    parser.add_argument("--layouts", nargs="+", default=list(SUPPORTED_LAYOUTS))
    parser.add_argument("--duration-seconds", type=float, default=15.0)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--vehicles-per-frame", type=int, default=200)
    parser.add_argument("--batch-frames", type=int, default=5)
    parser.add_argument("--insert-row-target", type=int, default=10000)
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--postgres-profile", choices=SUPPORTED_POSTGRES_PROFILE_CHOICES, default="default")
    parser.add_argument("--postgres-image", default="postgres:17")
    parser.add_argument("--postgres-port", type=int, default=15432)
    parser.add_argument("--postgres-database", default="traj_store_pg")
    parser.add_argument("--tdengine-image", default="tdengine/tdengine")
    parser.add_argument("--tdengine-http-port", type=int, default=16041)
    parser.add_argument("--tdengine-database", default="traj_store_pg_td")
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    postgres_profile = normalize_postgres_profile(args.postgres_profile)
    results = []

    for index, layout in enumerate(args.layouts):
        run_name = f"compare_{layout}_{postgres_profile}"
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "08_run_ingest_benchmark.py"),
            "--run-name",
            run_name,
            "--storage-layout",
            layout,
            "--duration-seconds",
            str(args.duration_seconds),
            "--fps",
            str(args.fps),
            "--vehicles-per-frame",
            str(args.vehicles_per_frame),
            "--batch-frames",
            str(args.batch_frames),
            "--insert-row-target",
            str(args.insert_row_target),
            "--metric-interval-seconds",
            str(args.metric_interval_seconds),
            "--postgres-profile",
            postgres_profile,
            "--postgres-image",
            args.postgres_image,
            "--postgres-port",
            str(args.postgres_port + index),
            "--postgres-database",
            args.postgres_database,
            "--tdengine-image",
            args.tdengine_image,
            "--tdengine-http-port",
            str(args.tdengine_http_port + index),
            "--tdengine-database",
            args.tdengine_database,
        ]
        subprocess.run(cmd, cwd=BASE_DIR.parent, check=True)

        run_candidates = sorted((BASE_DIR / "data" / "benchmarks").glob(f"{run_name}*"))
        latest_run_dir = run_candidates[-1]
        benchmark_summary = load_json(latest_run_dir / "benchmark_summary.json")
        receiver = benchmark_summary["receiver"]
        results.append(
            {
                "layout": layout,
                "postgres_profile": benchmark_summary.get("postgres_profile", postgres_profile),
                "postgres_wal_mode": receiver.get("postgres_wal_mode"),
                "postgres_toast_compression": receiver.get("postgres_toast_compression"),
                "postgres_table_persistence": receiver.get("postgres_table_persistence", {}),
                "run_dir": str(latest_run_dir),
                "rows_in_sink": receiver["rows_in_sink"],
                "avg_rows_per_second_ingest": receiver["avg_rows_per_second_ingest"],
                "avg_rows_per_second_total": receiver["avg_rows_per_second_total"],
                "postgres_peak_cpu_percent": receiver["postgres_peak_cpu_percent"],
                "postgres_peak_rss_bytes": receiver["postgres_peak_rss_bytes"],
                "tdengine_peak_cpu_percent": receiver["tdengine_peak_cpu_percent"],
                "tdengine_peak_rss_bytes": receiver["tdengine_peak_rss_bytes"],
                "postgres_data_bytes_post_run": receiver["postgres_data_bytes_post_run"],
                "tdengine_data_bytes_post_run": receiver["tdengine_data_bytes_post_run"],
                "bytes_per_row_post_run": receiver["bytes_per_row_post_run"],
                "bytes_per_row_postgres_data_post_run": receiver["bytes_per_row_postgres_data_post_run"],
                "bytes_per_row_tdengine_data_post_run": receiver["bytes_per_row_tdengine_data_post_run"],
            }
        )

    summary = {
        "completed_at_utc": utc_now_iso(),
        "layouts": args.layouts,
        "postgres_profile": postgres_profile,
        "results": results,
    }
    output_path = BASE_DIR / "data" / "benchmarks" / "comparison_summary.json"
    write_json(output_path, summary)

    print(f"comparison_summary={output_path}")
    for row in results:
        print(
            " | ".join(
                [
                    f"layout={row['layout']}",
                    f"postgres_profile={row['postgres_profile']}",
                    f"postgres_wal_mode={row['postgres_wal_mode']}",
                    f"rows_in_sink={row['rows_in_sink']}",
                    f"avg_rows_per_second_ingest={row['avg_rows_per_second_ingest']}",
                    f"postgres_peak_cpu_percent={row['postgres_peak_cpu_percent']}",
                    f"tdengine_peak_cpu_percent={row['tdengine_peak_cpu_percent']}",
                    f"bytes_per_row_postgres_data_post_run={row['bytes_per_row_postgres_data_post_run']}",
                    f"bytes_per_row_tdengine_data_post_run={row['bytes_per_row_tdengine_data_post_run']}",
                ]
            )
        )


if __name__ == "__main__":
    main()
