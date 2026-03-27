#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ingest_common import SUPPORTED_LAYOUTS, utc_now_iso, write_json


SCRIPTS_DIR = Path(__file__).resolve().parent
BASE_DIR = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare ClickHouse storage layouts under the same ingest workload.")
    parser.add_argument("--layouts", nargs="+", default=list(SUPPORTED_LAYOUTS))
    parser.add_argument("--duration-seconds", type=float, default=15.0)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--vehicles-per-frame", type=int, default=200)
    parser.add_argument("--batch-frames", type=int, default=5)
    parser.add_argument("--insert-row-target", type=int, default=10000)
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--clickhouse-image", default="clickhouse/clickhouse-server:25.8")
    parser.add_argument("--clickhouse-http-port", type=int, default=18123)
    parser.add_argument("--clickhouse-native-port", type=int, default=19000)
    parser.add_argument("--clickhouse-database", default="traj_store_clickhose")
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    results = []

    for index, layout in enumerate(args.layouts):
        run_name = f"compare_{layout}"
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
            "--clickhouse-image",
            args.clickhouse_image,
            "--clickhouse-http-port",
            str(args.clickhouse_http_port + index),
            "--clickhouse-native-port",
            str(args.clickhouse_native_port + index),
            "--clickhouse-database",
            args.clickhouse_database,
        ]
        subprocess.run(cmd, cwd=BASE_DIR.parent, check=True)

        run_candidates = sorted((BASE_DIR / "data" / "benchmarks").glob(f"{run_name}*"))
        latest_run_dir = run_candidates[-1]
        benchmark_summary = load_json(latest_run_dir / "benchmark_summary.json")
        receiver = benchmark_summary["receiver"]
        results.append(
            {
                "layout": layout,
                "run_dir": str(latest_run_dir),
                "rows_in_sink": receiver["rows_in_sink"],
                "avg_rows_per_second_ingest": receiver["avg_rows_per_second_ingest"],
                "avg_rows_per_second_total": receiver["avg_rows_per_second_total"],
                "clickhouse_peak_cpu_percent": receiver["clickhouse_peak_cpu_percent"],
                "clickhouse_peak_rss_bytes": receiver["clickhouse_peak_rss_bytes"],
                "clickhouse_data_bytes_post_run": receiver["clickhouse_data_bytes_post_run"],
                "active_bytes_on_disk_post_run": receiver["active_bytes_on_disk_post_run"],
                "bytes_per_row_clickhouse_data_post_run": receiver["bytes_per_row_clickhouse_data_post_run"],
                "bytes_per_row_active_bytes_post_run": receiver["bytes_per_row_active_bytes_post_run"],
            }
        )

    summary = {
        "completed_at_utc": utc_now_iso(),
        "layouts": args.layouts,
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
                    f"rows_in_sink={row['rows_in_sink']}",
                    f"avg_rows_per_second_ingest={row['avg_rows_per_second_ingest']}",
                    f"clickhouse_peak_cpu_percent={row['clickhouse_peak_cpu_percent']}",
                    f"bytes_per_row_clickhouse_data_post_run={row['bytes_per_row_clickhouse_data_post_run']}",
                ]
            )
        )


if __name__ == "__main__":
    main()
