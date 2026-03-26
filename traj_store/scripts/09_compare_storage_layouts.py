#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ingest_common import SUPPORTED_LAYOUTS, make_run_dir, normalize_storage_layout, utc_now_iso, write_json


BASE_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
DEFAULT_LAYOUTS = list(SUPPORTED_LAYOUTS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the same ingest workload across multiple storage layouts.")
    parser.add_argument("--run-name", default="")
    parser.add_argument("--layouts", nargs="+", default=DEFAULT_LAYOUTS)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--base-port", type=int, default=9200)
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--vehicles-per-frame", type=int, default=200)
    parser.add_argument("--batch-frames", type=int, default=5)
    parser.add_argument("--start-ts-ms", type=int, default=1741910400000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--pace", choices=("realtime", "max"), default="realtime")
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--ready-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--flush-row-target", type=int, default=10000)
    parser.add_argument("--row-group-size", type=int, default=100000)
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def extract_run_dir(stdout: str) -> str:
    for line in stdout.splitlines():
        if line.startswith("run_dir="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("run_dir not found in benchmark output")


def format_result_line(result: dict) -> str:
    return (
        f"{result['storage_layout']:<22} "
        f"rows/s={result['avg_rows_per_second']:<10.2f} "
        f"cpu={result['peak_cpu_percent']:<7.2f} "
        f"rss_mb={result['peak_rss_bytes'] / (1024 * 1024):<8.2f} "
        f"sink_mb={result['sink_bytes_post_flush'] / (1024 * 1024):<8.3f} "
        f"parquet_mb={result['parquet_bytes_post_flush'] / (1024 * 1024):<8.3f} "
        f"bytes/row={result['bytes_per_row_post_flush']:.3f}"
    )


def main() -> None:
    args = parse_args()
    compare_dir = make_run_dir(args.run_name or "compare_storage_layouts")
    results = []

    for index, layout_arg in enumerate(args.layouts):
        storage_layout = normalize_storage_layout(layout_arg)
        port = args.base_port + index
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "08_run_ingest_benchmark.py"),
            "--run-name",
            f"{compare_dir.name}_{storage_layout}",
            "--host",
            args.host,
            "--port",
            str(port),
            "--duration-seconds",
            str(args.duration_seconds),
            "--fps",
            str(args.fps),
            "--vehicles-per-frame",
            str(args.vehicles_per_frame),
            "--batch-frames",
            str(args.batch_frames),
            "--start-ts-ms",
            str(args.start_ts_ms),
            "--seed",
            str(args.seed),
            "--pace",
            args.pace,
            "--metric-interval-seconds",
            str(args.metric_interval_seconds),
            "--ready-timeout-seconds",
            str(args.ready_timeout_seconds),
            "--storage-layout",
            storage_layout,
            "--flush-row-target",
            str(args.flush_row_target),
            "--row-group-size",
            str(args.row_group_size),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=BASE_DIR.parent)
        if proc.returncode != 0:
            raise RuntimeError(
                f"benchmark failed for {storage_layout}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )

        run_dir = Path(extract_run_dir(proc.stdout))
        benchmark_summary = load_json(run_dir / "benchmark_summary.json")
        receiver = benchmark_summary["receiver"]
        results.append(
            {
                "storage_layout": storage_layout,
                "run_dir": str(run_dir),
                "avg_rows_per_second": receiver["avg_rows_per_second"],
                "peak_cpu_percent": receiver["peak_cpu_percent"],
                "peak_rss_bytes": receiver["peak_rss_bytes"],
                "sink_bytes_post_flush": receiver["sink_bytes_post_flush"],
                "parquet_bytes_post_flush": receiver["parquet_bytes_post_flush"],
                "duckdb_bytes_post_flush": receiver["duckdb_bytes_post_flush"],
                "wal_bytes_post_flush": receiver["wal_bytes_post_flush"],
                "bytes_per_row_post_flush": receiver["bytes_per_row_post_flush"],
                "rows_inserted": receiver["rows_inserted"],
                "rows_in_sink": receiver["rows_in_sink"],
                "rows_match": benchmark_summary["rows_match"],
            }
        )

    summary = {
        "completed_at_utc": utc_now_iso(),
        "comparison_dir": str(compare_dir),
        "workload": {
            "duration_seconds": args.duration_seconds,
            "fps": args.fps,
            "vehicles_per_frame": args.vehicles_per_frame,
            "batch_frames": args.batch_frames,
            "pace": args.pace,
            "flush_row_target": args.flush_row_target,
            "row_group_size": args.row_group_size,
        },
        "results": results,
    }
    write_json(compare_dir / "compare_summary.json", summary)

    print(f"comparison_dir={compare_dir}")
    for result in results:
        print(format_result_line(result))


if __name__ == "__main__":
    main()
