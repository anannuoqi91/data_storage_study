#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg

from ingest_common import (
    SUPPORTED_LAYOUT_CHOICES,
    SUPPORTED_POSTGRES_PROFILE_CHOICES,
    make_run_dir,
    normalize_postgres_profile,
    normalize_storage_layout,
    safe_slug,
    sink_disk_usage,
    utc_now_iso,
    write_json,
)
from tdengine_http import TDengineRestClient


BASE_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
POSTGRES_WAL_TMPFS_SIZE = "512m"
POSTGRES_WAL_TMPFS_DIR = "/var/lib/postgresql/wal_tmpfs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local sender/receiver PostgreSQL + TDengine ingest benchmark.")
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
    parser.add_argument("--storage-layout", choices=SUPPORTED_LAYOUT_CHOICES, default="pg_tdengine_compact")
    parser.add_argument("--insert-row-target", type=int, default=10000)
    parser.add_argument("--postgres-profile", choices=SUPPORTED_POSTGRES_PROFILE_CHOICES, default="default")
    parser.add_argument("--postgres-image", default="postgres:17")
    parser.add_argument("--postgres-port", type=int, default=15432)
    parser.add_argument("--postgres-database", default="traj_store_pg")
    parser.add_argument("--postgres-user", default="bench")
    parser.add_argument("--postgres-password", default="benchpass")
    parser.add_argument("--tdengine-image", default="tdengine/tdengine")
    parser.add_argument("--tdengine-http-port", type=int, default=16041)
    parser.add_argument("--tdengine-database", default="traj_store_pg_td")
    parser.add_argument("--tdengine-user", default="root")
    parser.add_argument("--tdengine-password", default="taosdata")
    parser.add_argument("--no-auto-pull", action="store_true")
    parser.add_argument("--keep-containers", action="store_true")
    return parser.parse_args()


def run_command(args: list[str], *, cwd: Path | None = None, stdout=None, stderr=None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=cwd, stdout=stdout, stderr=stderr, check=check, text=True)


def docker_image_exists(image: str) -> bool:
    result = run_command(["docker", "image", "inspect", image], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def ensure_docker_image(image: str, *, auto_pull: bool) -> None:
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


def prepare_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o777)


def postgres_wal_mode(postgres_profile: str) -> str:
    if postgres_profile == "bench_low_wal_compressed":
        return "tmpfs"
    return "data_dir"


def postgres_server_args(postgres_profile: str) -> list[str]:
    if postgres_profile != "bench_low_wal_compressed":
        return []
    return [
        "-c",
        "wal_level=minimal",
        "-c",
        "wal_compression=on",
        "-c",
        "synchronous_commit=off",
        "-c",
        "archive_mode=off",
        "-c",
        "max_wal_senders=0",
        "-c",
        "autovacuum=off",
    ]


def start_postgres_container(
    *,
    container_name: str,
    image: str,
    data_dir: Path,
    port: int,
    database: str,
    user: str,
    password: str,
    postgres_profile: str,
) -> None:
    prepare_dir(data_dir)
    command = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-e",
        f"POSTGRES_DB={database}",
        "-e",
        f"POSTGRES_USER={user}",
        "-e",
        f"POSTGRES_PASSWORD={password}",
        "-p",
        f"{port}:5432",
        "-v",
        f"{data_dir}:/var/lib/postgresql/data",
    ]
    if postgres_wal_mode(postgres_profile) == "tmpfs":
        command.extend(["-e", f"POSTGRES_INITDB_WALDIR={POSTGRES_WAL_TMPFS_DIR}"])
        command.extend(["--tmpfs", f"{POSTGRES_WAL_TMPFS_DIR}:rw,size={POSTGRES_WAL_TMPFS_SIZE}"])
    command.append(image)
    command.extend(postgres_server_args(postgres_profile))
    run_command(command)


def start_tdengine_container(
    *,
    container_name: str,
    image: str,
    data_dir: Path,
    log_dir: Path,
    http_port: int,
) -> None:
    prepare_dir(data_dir)
    prepare_dir(log_dir)
    run_command(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-p",
            f"{http_port}:6041",
            "-v",
            f"{data_dir}:/var/lib/taos",
            "-v",
            f"{log_dir}:/var/log/taos",
            image,
        ]
    )


def stop_container(container_name: str) -> None:
    run_command(["docker", "stop", "-t", "5", container_name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_container_pid(container_name: str) -> int:
    result = run_command(["docker", "inspect", "--format", "{{.State.Pid}}", container_name], stdout=subprocess.PIPE)
    return int(result.stdout.strip())


def capture_container_logs(container_name: str, destination: Path) -> None:
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


def wait_for_postgres_ready(*, host: str, port: int, database: str, user: str, password: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
                connect_timeout=3,
                autocommit=True,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5)
    raise TimeoutError(f"PostgreSQL did not become ready within {timeout_seconds} seconds: {last_error}")


def wait_for_tdengine_ready(*, host: str, port: int, user: str, password: str, timeout_seconds: float) -> None:
    client = TDengineRestClient(host=host, port=port, user=user, password=password)
    client.wait_until_ready(timeout_seconds=timeout_seconds)


def refresh_post_stop_sizes(run_dir: Path, sink_dir: Path, postgres_data_dir: Path, tdengine_data_dir: Path) -> None:
    receiver_summary_path = run_dir / "receiver_summary.json"
    benchmark_summary_path = run_dir / "benchmark_summary.json"
    if not receiver_summary_path.exists():
        return

    receiver_summary = load_json(receiver_summary_path)
    usage = sink_disk_usage(
        sink_dir=sink_dir,
        postgres_data_dir=postgres_data_dir,
        tdengine_data_dir=tdengine_data_dir,
    )
    row_count = max(int(receiver_summary.get("rows_in_sink", 0)), 1)
    receiver_summary["sink_bytes_post_run"] = usage.sink_bytes
    receiver_summary["postgres_data_bytes_post_run"] = usage.postgres_data_bytes
    receiver_summary["tdengine_data_bytes_post_run"] = usage.tdengine_data_bytes
    receiver_summary["bytes_per_row_post_run"] = usage.sink_bytes / row_count
    receiver_summary["bytes_per_row_postgres_data_post_run"] = usage.postgres_data_bytes / row_count
    receiver_summary["bytes_per_row_tdengine_data_post_run"] = usage.tdengine_data_bytes / row_count
    write_json(receiver_summary_path, receiver_summary)

    if benchmark_summary_path.exists():
        benchmark_summary = load_json(benchmark_summary_path)
        benchmark_summary["receiver"] = receiver_summary
        write_json(benchmark_summary_path, benchmark_summary)


def main() -> None:
    args = parse_args()
    storage_layout = normalize_storage_layout(args.storage_layout)
    postgres_profile = normalize_postgres_profile(args.postgres_profile)
    postgres_wal_mode_value = postgres_wal_mode(postgres_profile)
    run_dir = make_run_dir(args.run_name or None)
    ready_path = run_dir / "receiver_ready.json"
    sink_dir = run_dir / "sink"
    postgres_data_dir = sink_dir / "postgres_data"
    tdengine_data_dir = sink_dir / "tdengine_data"
    tdengine_log_dir = sink_dir / "tdengine_logs"
    postgres_container_name = f"traj-store-pg-pg-{safe_slug(run_dir.name, max_length=24)}"
    tdengine_container_name = f"traj-store-pg-td-{safe_slug(run_dir.name, max_length=24)}"

    receiver_stdout = (run_dir / "receiver.stdout.log").open("w", encoding="utf-8")
    receiver_stderr = (run_dir / "receiver.stderr.log").open("w", encoding="utf-8")
    sender_stdout = (run_dir / "sender.stdout.log").open("w", encoding="utf-8")
    sender_stderr = (run_dir / "sender.stderr.log").open("w", encoding="utf-8")

    receiver_proc: subprocess.Popen | None = None
    postgres_started = False
    tdengine_started = False
    try:
        ensure_docker_image(args.postgres_image, auto_pull=not args.no_auto_pull)
        ensure_docker_image(args.tdengine_image, auto_pull=not args.no_auto_pull)

        start_postgres_container(
            container_name=postgres_container_name,
            image=args.postgres_image,
            data_dir=postgres_data_dir,
            port=args.postgres_port,
            database=args.postgres_database,
            user=args.postgres_user,
            password=args.postgres_password,
            postgres_profile=postgres_profile,
        )
        postgres_started = True

        start_tdengine_container(
            container_name=tdengine_container_name,
            image=args.tdengine_image,
            data_dir=tdengine_data_dir,
            log_dir=tdengine_log_dir,
            http_port=args.tdengine_http_port,
        )
        tdengine_started = True

        wait_for_postgres_ready(
            host=args.host,
            port=args.postgres_port,
            database=args.postgres_database,
            user=args.postgres_user,
            password=args.postgres_password,
            timeout_seconds=args.ready_timeout_seconds,
        )
        wait_for_tdengine_ready(
            host=args.host,
            port=args.tdengine_http_port,
            user=args.tdengine_user,
            password=args.tdengine_password,
            timeout_seconds=args.ready_timeout_seconds,
        )

        postgres_container_pid = get_container_pid(postgres_container_name)
        tdengine_container_pid = get_container_pid(tdengine_container_name)

        receiver_cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "07_pg_tdengine_receiver.py"),
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
            "--postgres-profile",
            postgres_profile,
            "--postgres-wal-mode",
            postgres_wal_mode_value,
            "--postgres-host",
            args.host,
            "--postgres-port",
            str(args.postgres_port),
            "--postgres-database",
            args.postgres_database,
            "--postgres-user",
            args.postgres_user,
            "--postgres-password",
            args.postgres_password,
            "--postgres-data-dir",
            str(postgres_data_dir),
            "--postgres-container-pid",
            str(postgres_container_pid),
            "--tdengine-host",
            args.host,
            "--tdengine-port",
            str(args.tdengine_http_port),
            "--tdengine-database",
            args.tdengine_database,
            "--tdengine-user",
            args.tdengine_user,
            "--tdengine-password",
            args.tdengine_password,
            "--tdengine-data-dir",
            str(tdengine_data_dir),
            "--tdengine-log-dir",
            str(tdengine_log_dir),
            "--tdengine-container-pid",
            str(tdengine_container_pid),
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
            "postgres_profile": postgres_profile,
            "postgres_wal_mode": postgres_wal_mode_value,
            "postgres_image": args.postgres_image,
            "postgres_port": args.postgres_port,
            "postgres_database": args.postgres_database,
            "postgres_user": args.postgres_user,
            "tdengine_image": args.tdengine_image,
            "tdengine_http_port": args.tdengine_http_port,
            "tdengine_database": args.tdengine_database,
            "tdengine_user": args.tdengine_user,
            "sender": sender_summary,
            "receiver": receiver_summary,
            "rows_gap": sender_summary["rows_sent"] - receiver_summary["rows_received"],
            "rows_match": sender_summary["rows_sent"] == receiver_summary["rows_received"] == receiver_summary["rows_in_sink"],
        }
        write_json(run_dir / "benchmark_summary.json", benchmark_summary)

        print(f"run_dir={run_dir}")
        print(f"storage_layout={storage_layout}")
        print(f"postgres_profile={postgres_profile}")
        print(f"postgres_wal_mode={postgres_wal_mode_value}")
        print(f"rows_sent={sender_summary['rows_sent']}")
        print(f"rows_received={receiver_summary['rows_received']}")
        print(f"rows_in_sink={receiver_summary['rows_in_sink']}")
        print(f"rows_match={benchmark_summary['rows_match']}")
        print(f"avg_rows_per_second_ingest={receiver_summary['avg_rows_per_second_ingest']}")
        print(f"avg_rows_per_second_total={receiver_summary['avg_rows_per_second_total']}")
        print(f"receiver_peak_rss_bytes={receiver_summary['receiver_peak_rss_bytes']}")
        print(f"receiver_peak_cpu_percent={receiver_summary['receiver_peak_cpu_percent']}")
        print(f"postgres_peak_rss_bytes={receiver_summary['postgres_peak_rss_bytes']}")
        print(f"postgres_peak_cpu_percent={receiver_summary['postgres_peak_cpu_percent']}")
        print(f"tdengine_peak_rss_bytes={receiver_summary['tdengine_peak_rss_bytes']}")
        print(f"tdengine_peak_cpu_percent={receiver_summary['tdengine_peak_cpu_percent']}")
        print(f"postgres_data_bytes_post_run={receiver_summary['postgres_data_bytes_post_run']}")
        print(f"tdengine_data_bytes_post_run={receiver_summary['tdengine_data_bytes_post_run']}")
        print(f"bytes_per_row_post_run={receiver_summary['bytes_per_row_post_run']}")
        print(f"bytes_per_row_tdengine_data_post_run={receiver_summary['bytes_per_row_tdengine_data_post_run']}")
    finally:
        receiver_stdout.close()
        receiver_stderr.close()
        sender_stdout.close()
        sender_stderr.close()
        if postgres_started:
            capture_container_logs(postgres_container_name, run_dir / "postgres.container.log")
        if tdengine_started:
            capture_container_logs(tdengine_container_name, run_dir / "tdengine.container.log")
        if receiver_proc is not None and receiver_proc.poll() is None:
            receiver_proc.terminate()
            receiver_proc.wait(timeout=5)
        if postgres_started and not args.keep_containers:
            stop_container(postgres_container_name)
        if tdengine_started and not args.keep_containers:
            stop_container(tdengine_container_name)
        if (postgres_started or tdengine_started) and not args.keep_containers:
            fix_output_permissions(args.postgres_image, sink_dir)
            refresh_post_stop_sizes(run_dir, sink_dir, postgres_data_dir, tdengine_data_dir)


if __name__ == "__main__":
    main()
