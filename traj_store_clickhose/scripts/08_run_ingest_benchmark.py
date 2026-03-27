#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from clickhouse_http import ClickHouseClient
from ingest_common import SUPPORTED_LAYOUT_CHOICES, make_run_dir, normalize_storage_layout, safe_slug, sink_disk_usage, utc_now_iso, write_json


BASE_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local sender/receiver ClickHouse ingest benchmark.")
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
    parser.add_argument("--ready-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--storage-layout", choices=SUPPORTED_LAYOUT_CHOICES, default="clickhouse_compact_zstd")
    parser.add_argument("--insert-row-target", type=int, default=10000)
    parser.add_argument("--clickhouse-image", default="clickhouse/clickhouse-server:25.8")
    parser.add_argument("--clickhouse-http-port", type=int, default=18123)
    parser.add_argument("--clickhouse-native-port", type=int, default=19000)
    parser.add_argument("--clickhouse-database", default="traj_store_clickhose")
    parser.add_argument("--clickhouse-user", default="bench")
    parser.add_argument("--clickhouse-password", default="benchpass")
    parser.add_argument("--no-auto-pull", action="store_true")
    parser.add_argument("--no-optimize-final", action="store_true")
    parser.add_argument("--keep-container", action="store_true")
    return parser.parse_args()


def run_command(args: list[str], *, cwd: Path | None = None, stdout=None, stderr=None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=cwd, stdout=stdout, stderr=stderr, check=check, text=True)


def docker_image_exists(image: str) -> bool:
    result = run_command(["docker", "image", "inspect", image], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def ensure_clickhouse_image(image: str, *, auto_pull: bool) -> None:
    if docker_image_exists(image):
        return
    if not auto_pull:
        raise RuntimeError(f"Docker image {image} not found locally. Re-run without --no-auto-pull or pull it manually.")
    run_command(["docker", "pull", image])


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


def start_clickhouse_container(
    *,
    container_name: str,
    image: str,
    clickhouse_data_dir: Path,
    clickhouse_log_dir: Path,
    http_port: int,
    native_port: int,
    database: str,
    user: str,
    password: str,
) -> None:
    clickhouse_data_dir.mkdir(parents=True, exist_ok=True)
    clickhouse_log_dir.mkdir(parents=True, exist_ok=True)
    clickhouse_data_dir.chmod(0o777)
    clickhouse_log_dir.chmod(0o777)
    run_command(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "--ulimit",
            "nofile=262144:262144",
            "-e",
            f"CLICKHOUSE_DB={database}",
            "-e",
            f"CLICKHOUSE_USER={user}",
            "-e",
            f"CLICKHOUSE_PASSWORD={password}",
            "-e",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
            "-p",
            f"{http_port}:8123",
            "-p",
            f"{native_port}:9000",
            "-v",
            f"{clickhouse_data_dir}:/var/lib/clickhouse",
            "-v",
            f"{clickhouse_log_dir}:/var/log/clickhouse-server",
            image,
        ]
    )


def stop_clickhouse_container(container_name: str) -> None:
    run_command(["docker", "stop", "-t", "5", container_name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_clickhouse_container_pid(container_name: str) -> int:
    result = run_command(["docker", "inspect", "--format", "{{.State.Pid}}", container_name], stdout=subprocess.PIPE)
    return int(result.stdout.strip())


def capture_clickhouse_logs(container_name: str, destination: Path) -> None:
    with destination.open("w", encoding="utf-8") as output:
        run_command(["docker", "logs", container_name], stdout=output, stderr=subprocess.STDOUT, check=False)


def fix_output_permissions(image: str, target_dir: Path) -> None:
    uid = os.getuid()
    gid = os.getgid()
    run_command(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{target_dir}:/work",
            "--entrypoint",
            "/bin/sh",
            image,
            "-c",
            f"chown -R {uid}:{gid} /work && chmod -R a+rX /work",
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )




def refresh_post_stop_sizes(run_dir: Path, sink_dir: Path, clickhouse_data_dir: Path) -> None:
    receiver_summary_path = run_dir / "receiver_summary.json"
    benchmark_summary_path = run_dir / "benchmark_summary.json"
    if not receiver_summary_path.exists():
        return

    receiver_summary = load_json(receiver_summary_path)
    usage = sink_disk_usage(sink_dir=sink_dir, clickhouse_data_dir=clickhouse_data_dir)
    row_count = max(int(receiver_summary.get("rows_in_sink", 0)), 1)
    receiver_summary["sink_bytes_post_run"] = usage.sink_bytes
    receiver_summary["clickhouse_data_bytes_post_run"] = usage.clickhouse_data_bytes
    receiver_summary["bytes_per_row_clickhouse_data_post_run"] = usage.clickhouse_data_bytes / row_count
    write_json(receiver_summary_path, receiver_summary)

    if benchmark_summary_path.exists():
        benchmark_summary = load_json(benchmark_summary_path)
        benchmark_summary["receiver"] = receiver_summary
        write_json(benchmark_summary_path, benchmark_summary)

def main() -> None:
    args = parse_args()
    storage_layout = normalize_storage_layout(args.storage_layout)
    run_dir = make_run_dir(args.run_name or None)
    ready_path = run_dir / "receiver_ready.json"
    sink_dir = run_dir / "sink"
    clickhouse_data_dir = sink_dir / "clickhouse_store"
    clickhouse_log_dir = sink_dir / "clickhouse_logs"
    container_name = f"traj-store-clickhose-{safe_slug(run_dir.name, max_length=32)}"

    receiver_stdout = (run_dir / "receiver.stdout.log").open("w", encoding="utf-8")
    receiver_stderr = (run_dir / "receiver.stderr.log").open("w", encoding="utf-8")
    sender_stdout = (run_dir / "sender.stdout.log").open("w", encoding="utf-8")
    sender_stderr = (run_dir / "sender.stderr.log").open("w", encoding="utf-8")

    receiver_proc: subprocess.Popen | None = None
    container_started = False
    try:
        ensure_clickhouse_image(args.clickhouse_image, auto_pull=not args.no_auto_pull)
        start_clickhouse_container(
            container_name=container_name,
            image=args.clickhouse_image,
            clickhouse_data_dir=clickhouse_data_dir,
            clickhouse_log_dir=clickhouse_log_dir,
            http_port=args.clickhouse_http_port,
            native_port=args.clickhouse_native_port,
            database=args.clickhouse_database,
            user=args.clickhouse_user,
            password=args.clickhouse_password,
        )
        container_started = True

        client = ClickHouseClient(
            host=args.host,
            port=args.clickhouse_http_port,
            database="default",
            user=args.clickhouse_user,
            password=args.clickhouse_password,
        )
        client.wait_until_ready(timeout_seconds=args.ready_timeout_seconds, database="default")
        clickhouse_container_pid = get_clickhouse_container_pid(container_name)

        receiver_cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "07_clickhouse_receiver.py"),
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
            "--insert-row-target",
            str(args.insert_row_target),
            "--clickhouse-host",
            args.host,
            "--clickhouse-port",
            str(args.clickhouse_http_port),
            "--clickhouse-database",
            args.clickhouse_database,
            "--clickhouse-user",
            args.clickhouse_user,
            "--clickhouse-password",
            args.clickhouse_password,
            "--clickhouse-data-dir",
            str(clickhouse_data_dir),
            "--clickhouse-container-pid",
            str(clickhouse_container_pid),
        ]
        if not args.no_optimize_final:
            receiver_cmd.append("--optimize-final")

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
        wait_for_receiver_ready(receiver_proc, ready_path, args.ready_timeout_seconds)

        sender_result = subprocess.run(sender_cmd, stdout=sender_stdout, stderr=sender_stderr, cwd=BASE_DIR.parent)
        if sender_result.returncode != 0:
            raise RuntimeError(f"sender exited with code {sender_result.returncode}")

        receiver_timeout = max(args.duration_seconds * 6, 60.0)
        receiver_returncode = receiver_proc.wait(timeout=receiver_timeout)
        if receiver_returncode != 0:
            raise RuntimeError(f"receiver exited with code {receiver_returncode}")

        sender_summary = load_json(run_dir / "sender_summary.json")
        receiver_summary = load_json(run_dir / "receiver_summary.json")

        benchmark_summary = {
            "completed_at_utc": utc_now_iso(),
            "run_dir": str(run_dir),
            "storage_layout": storage_layout,
            "clickhouse_image": args.clickhouse_image,
            "clickhouse_http_port": args.clickhouse_http_port,
            "clickhouse_native_port": args.clickhouse_native_port,
            "clickhouse_database": args.clickhouse_database,
            "clickhouse_user": args.clickhouse_user,
            "sender": sender_summary,
            "receiver": receiver_summary,
            "rows_gap": sender_summary["rows_sent"] - receiver_summary["rows_received"],
            "rows_match": sender_summary["rows_sent"] == receiver_summary["rows_received"] == receiver_summary["rows_in_sink"],
        }
        write_json(run_dir / "benchmark_summary.json", benchmark_summary)

        print(f"run_dir={run_dir}")
        print(f"storage_layout={storage_layout}")
        print(f"rows_sent={sender_summary['rows_sent']}")
        print(f"rows_received={receiver_summary['rows_received']}")
        print(f"rows_in_sink={receiver_summary['rows_in_sink']}")
        print(f"rows_match={benchmark_summary['rows_match']}")
        print(f"avg_rows_per_second_ingest={receiver_summary['avg_rows_per_second_ingest']}")
        print(f"avg_rows_per_second_total={receiver_summary['avg_rows_per_second_total']}")
        print(f"receiver_peak_rss_bytes={receiver_summary['receiver_peak_rss_bytes']}")
        print(f"receiver_peak_cpu_percent={receiver_summary['receiver_peak_cpu_percent']}")
        print(f"clickhouse_peak_rss_bytes={receiver_summary['clickhouse_peak_rss_bytes']}")
        print(f"clickhouse_peak_cpu_percent={receiver_summary['clickhouse_peak_cpu_percent']}")
        print(f"clickhouse_data_bytes_post_run={receiver_summary['clickhouse_data_bytes_post_run']}")
        print(f"active_bytes_on_disk_post_run={receiver_summary['active_bytes_on_disk_post_run']}")
        print(f"bytes_per_row_clickhouse_data_post_run={receiver_summary['bytes_per_row_clickhouse_data_post_run']}")
        print(f"bytes_per_row_active_bytes_post_run={receiver_summary['bytes_per_row_active_bytes_post_run']}")
    finally:
        receiver_stdout.close()
        receiver_stderr.close()
        sender_stdout.close()
        sender_stderr.close()
        if container_started:
            capture_clickhouse_logs(container_name, run_dir / "clickhouse.container.log")
        if receiver_proc is not None and receiver_proc.poll() is None:
            receiver_proc.terminate()
            receiver_proc.wait(timeout=5)
        if container_started and not args.keep_container:
            stop_clickhouse_container(container_name)
            fix_output_permissions(args.clickhouse_image, sink_dir)
            refresh_post_stop_sizes(run_dir, sink_dir, clickhouse_data_dir)


if __name__ == "__main__":
    main()
