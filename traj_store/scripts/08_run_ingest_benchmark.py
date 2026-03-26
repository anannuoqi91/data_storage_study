#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from ingest_common import SUPPORTED_LAYOUT_CHOICES, make_run_dir, normalize_storage_layout, utc_now_iso, write_json


BASE_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local sender/receiver ingest benchmark.")
    parser.add_argument("--run-name", default="")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--vehicles-per-frame", type=int, default=200)
    parser.add_argument("--batch-frames", type=int, default=5)
    parser.add_argument("--start-ts-ms", type=int, default=1741910400000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--pace", choices=("realtime", "max"), default="realtime")
    parser.add_argument("--metric-interval-seconds", type=float, default=1.0)
    parser.add_argument("--ready-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--storage-layout", choices=SUPPORTED_LAYOUT_CHOICES, default="parquet_compact_zstd")
    parser.add_argument("--flush-row-target", type=int, default=10000)
    parser.add_argument("--row-group-size", type=int, default=100000)
    return parser.parse_args()


def wait_for_receiver_ready(receiver_proc: subprocess.Popen, ready_path: Path, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if ready_path.exists():
            return
        if receiver_proc.poll() is not None:
            raise RuntimeError(f"receiver exited early with code {receiver_proc.returncode}")
        time.sleep(0.1)
    raise TimeoutError(f"receiver did not become ready within {timeout_seconds} seconds")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    storage_layout = normalize_storage_layout(args.storage_layout)
    run_dir = make_run_dir(args.run_name or None)
    ready_path = run_dir / "receiver_ready.json"

    receiver_stdout = (run_dir / "receiver.stdout.log").open("w", encoding="utf-8")
    receiver_stderr = (run_dir / "receiver.stderr.log").open("w", encoding="utf-8")
    sender_stdout = (run_dir / "sender.stdout.log").open("w", encoding="utf-8")
    sender_stderr = (run_dir / "sender.stderr.log").open("w", encoding="utf-8")

    receiver_cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "07_box_receiver.py"),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--run-dir",
        str(run_dir),
        "--metric-interval-seconds",
        str(args.metric_interval_seconds),
        "--storage-layout",
        storage_layout,
        "--flush-row-target",
        str(args.flush_row_target),
        "--row-group-size",
        str(args.row_group_size),
    ]
    sender_cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "06_box_sender.py"),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--run-dir",
        str(run_dir),
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
    ]

    receiver_proc = subprocess.Popen(receiver_cmd, stdout=receiver_stdout, stderr=receiver_stderr, cwd=BASE_DIR.parent)

    try:
        wait_for_receiver_ready(receiver_proc, ready_path, args.ready_timeout_seconds)
        sender_result = subprocess.run(sender_cmd, stdout=sender_stdout, stderr=sender_stderr, cwd=BASE_DIR.parent)
        if sender_result.returncode != 0:
            raise RuntimeError(f"sender exited with code {sender_result.returncode}")

        receiver_timeout = max(args.duration_seconds * 3, 30.0)
        receiver_returncode = receiver_proc.wait(timeout=receiver_timeout)
        if receiver_returncode != 0:
            raise RuntimeError(f"receiver exited with code {receiver_returncode}")
    finally:
        receiver_stdout.close()
        receiver_stderr.close()
        sender_stdout.close()
        sender_stderr.close()
        if receiver_proc.poll() is None:
            receiver_proc.terminate()
            receiver_proc.wait(timeout=5)

    sender_summary = load_json(run_dir / "sender_summary.json")
    receiver_summary = load_json(run_dir / "receiver_summary.json")

    benchmark_summary = {
        "completed_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "storage_layout": storage_layout,
        "sender": sender_summary,
        "receiver": receiver_summary,
        "rows_gap": sender_summary["rows_sent"] - receiver_summary["rows_inserted"],
        "rows_match": sender_summary["rows_sent"] == receiver_summary["rows_inserted"],
    }
    write_json(run_dir / "benchmark_summary.json", benchmark_summary)

    print(f"run_dir={run_dir}")
    print(f"storage_layout={storage_layout}")
    print(f"rows_sent={sender_summary['rows_sent']}")
    print(f"rows_inserted={receiver_summary['rows_inserted']}")
    print(f"rows_in_sink={receiver_summary['rows_in_sink']}")
    print(f"rows_match={benchmark_summary['rows_match']}")
    print(f"avg_rows_per_second={receiver_summary['avg_rows_per_second']}")
    print(f"peak_rss_bytes={receiver_summary['peak_rss_bytes']}")
    print(f"peak_cpu_percent={receiver_summary['peak_cpu_percent']}")
    print(f"sink_bytes_post_flush={receiver_summary['sink_bytes_post_flush']}")
    print(f"parquet_bytes_post_flush={receiver_summary['parquet_bytes_post_flush']}")
    print(f"duckdb_bytes_post_flush={receiver_summary['duckdb_bytes_post_flush']}")
    print(f"wal_bytes_post_flush={receiver_summary['wal_bytes_post_flush']}")
    print(f"bytes_per_row_post_flush={receiver_summary['bytes_per_row_post_flush']}")


if __name__ == "__main__":
    main()
