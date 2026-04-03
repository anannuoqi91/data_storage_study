from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
CLI_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
SAMPLE_JSONL = REPO_ROOT / "examples" / "sample_box_frames.jsonl"


def run_cli(*args: str, root: Path, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    return subprocess.run(
        [str(CLI_PYTHON), "-u", "-m", "dbserver.cli", "--root", str(root), *args],
        cwd=str(REPO_ROOT if cwd is None else cwd),
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )


def build_hot_jsonl(path: Path) -> int:
    now_ms = int(time.time() * 1000)
    path.write_text(
        "\n".join(
            [
                (
                    '{"device_id":1,"frame_id":7001,"event_time_ms":%d,'
                    '"boxes":[{"trace_id":8080,"obj_type":1,"position_x_mm":100,'
                    '"position_y_mm":200,"position_z_mm":0,"length_mm":4000,'
                    '"width_mm":1800,"height_mm":1500,"speed_centi_kmh_100":1000,'
                    '"spindle_centi_deg_100":9000,"lane_id":1}]}'
                )
                % now_ms,
                (
                    '{"device_id":1,"frame_id":7002,"event_time_ms":%d,'
                    '"boxes":[{"trace_id":8080,"obj_type":1,"position_x_mm":120,'
                    '"position_y_mm":210,"position_z_mm":0,"length_mm":4000,'
                    '"width_mm":1800,"height_mm":1500,"speed_centi_kmh_100":1010,'
                    '"spindle_centi_deg_100":9010,"lane_id":1}]}'
                )
                % (now_ms + 100),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return now_ms


def build_old_multiwindow_jsonl(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                (
                    '{"device_id":1,"frame_id":8001,"event_time_ms":1711922400100,'
                    '"boxes":[{"trace_id":9001,"obj_type":1,"position_x_mm":100,'
                    '"position_y_mm":200,"position_z_mm":0,"length_mm":4000,'
                    '"width_mm":1800,"height_mm":1500,"speed_centi_kmh_100":1000,'
                    '"spindle_centi_deg_100":9000,"lane_id":1}]}'
                ),
                (
                    '{"device_id":1,"frame_id":8002,"event_time_ms":1711922400200,'
                    '"boxes":[{"trace_id":9001,"obj_type":1,"position_x_mm":120,'
                    '"position_y_mm":210,"position_z_mm":0,"length_mm":4000,'
                    '"width_mm":1800,"height_mm":1500,"speed_centi_kmh_100":1010,'
                    '"spindle_centi_deg_100":9010,"lane_id":1}]}'
                ),
                (
                    '{"device_id":1,"frame_id":8101,"event_time_ms":1711923000100,'
                    '"boxes":[{"trace_id":9001,"obj_type":1,"position_x_mm":140,'
                    '"position_y_mm":220,"position_z_mm":0,"length_mm":4000,'
                    '"width_mm":1800,"height_mm":1500,"speed_centi_kmh_100":1020,'
                    '"spindle_centi_deg_100":9020,"lane_id":1}]}'
                ),
                (
                    '{"device_id":1,"frame_id":8102,"event_time_ms":1711923000200,'
                    '"boxes":[{"trace_id":9001,"obj_type":1,"position_x_mm":160,'
                    '"position_y_mm":230,"position_z_mm":0,"length_mm":4000,'
                    '"width_mm":1800,"height_mm":1500,"speed_centi_kmh_100":1030,'
                    '"spindle_centi_deg_100":9030,"lane_id":1}]}'
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )


class DbserverCliSmokeTest(unittest.TestCase):
    def test_ingest_query_status_and_metrics(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-smoke-") as temp_dir:
            root = Path(temp_dir)
            run_cli("init-metadata", root=root)
            ingest = run_cli("ingest-jsonl", "--input", str(SAMPLE_JSONL), root=root)
            self.assertIn("published_windows=2", ingest.stdout)

            trace_query = run_cli(
                "exec-trace-lookup-query",
                "--device-id",
                "1",
                "--trace-id",
                "42",
                root=root,
            )
            self.assertIn('"trace_id": 42', trace_query.stdout)

            rollup_query = run_cli(
                "exec-rollup-query",
                "--device-id",
                "1",
                "--start-ms",
                "1711922400000",
                "--end-ms",
                "1711922999999",
                root=root,
            )
            self.assertIn('"box_count": 2', rollup_query.stdout)

            status_output = run_cli("status-json", root=root)
            status_payload = json.loads(status_output.stdout)
            self.assertEqual(status_payload["service"], "dbserver")
            self.assertEqual(status_payload["current_release"]["schema_version_id"], "box_info_v1")

            metrics_output = run_cli("metrics-text", root=root)
            self.assertIn("dbserver_up 1", metrics_output.stdout)
            self.assertIn("dbserver_release_count", metrics_output.stdout)

    def test_async_export_queue(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-export-") as temp_dir:
            root = Path(temp_dir)
            run_cli("init-metadata", root=root)
            run_cli("ingest-jsonl", "--input", str(SAMPLE_JSONL), root=root)

            submitted = run_cli(
                "submit-detail-export",
                "--device-id",
                "1",
                "--start-ms",
                "1711922400000",
                "--end-ms",
                "1711922999999",
                root=root,
            )
            self.assertIn("status=pending", submitted.stdout)
            export_id = None
            for line in submitted.stdout.splitlines():
                if line.startswith("export_id="):
                    export_id = line.split("=", 1)[1]
                    break
            self.assertIsNotNone(export_id)

            listed_pending = run_cli("list-exports", root=root)
            self.assertIn(f"export_id={export_id}", listed_pending.stdout)
            self.assertIn("status=pending", listed_pending.stdout)

            processed = run_cli("run-export-queue", root=root)
            self.assertIn("processed_jobs=1", processed.stdout)
            self.assertIn("finished_jobs=1", processed.stdout)

            listed_finished = run_cli("list-exports", root=root)
            self.assertIn(f"export_id={export_id}", listed_finished.stdout)
            self.assertIn("status=finished", listed_finished.stdout)

            export_path = root / "exports" / "finished" / f"{export_id}.parquet"
            manifest_path = root / "exports" / "finished" / f"{export_id}.json"
            self.assertTrue(export_path.exists())
            self.assertTrue(manifest_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "finished")
            self.assertEqual(manifest["row_count"], 3)

            status_output = run_cli("status-json", root=root)
            status_payload = json.loads(status_output.stdout)
            self.assertEqual(status_payload["export_counts"]["finished"], 1)

            metrics_output = run_cli("metrics-text", root=root)
            self.assertIn('dbserver_export_job_count{status="finished"} 1', metrics_output.stdout)

    def test_trace_index_cooling_via_maintenance(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-hotcool-") as temp_dir:
            root = Path(temp_dir)
            hot_jsonl = root / "hot.jsonl"
            now_ms = build_hot_jsonl(hot_jsonl)

            run_cli("init-metadata", root=root)
            run_cli("ingest-jsonl", "--input", str(hot_jsonl), root=root)
            self.assertTrue((root / "live" / "trace_index_hot").exists())

            future_ms = now_ms + 31 * 24 * 60 * 60 * 1000
            maintenance = run_cli("run-maintenance", "--now-ms", str(future_ms), root=root)
            self.assertIn("trace_cooling_triggered=True", maintenance.stdout)
            self.assertTrue((root / "live" / "trace_index_cold").exists())

            trace_query = run_cli(
                "exec-trace-lookup-query",
                "--device-id",
                "1",
                "--trace-id",
                "8080",
                root=root,
            )
            self.assertIn('"trace_id": 8080', trace_query.stdout)

            releases = run_cli("list-releases", root=root)
            self.assertIn("event_type=trace_index_cool", releases.stdout)

    def test_seal_rewrites_derived_data_to_hourly_files(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-seal-derived-") as temp_dir:
            root = Path(temp_dir)
            input_jsonl = root / "multiwindow.jsonl"
            build_old_multiwindow_jsonl(input_jsonl)

            run_cli("init-metadata", root=root)
            ingest = run_cli("ingest-jsonl", "--input", str(input_jsonl), root=root)
            self.assertIn("published_windows=2", ingest.stdout)

            box_dir = root / "live" / "box_info" / "date=2024-03-31" / "hour=22" / "device_id=1"
            trace_dir = root / "live" / "trace_index_cold" / "date=2024-03-31" / "device_id=1"
            rollup_dir = root / "live" / "obj_type_rollup" / "date=2024-03-31" / "hour=22" / "device_id=1"

            self.assertEqual(len(list(box_dir.glob("*.parquet"))), 2)
            self.assertEqual(len(list(trace_dir.glob("*.parquet"))), 2)
            self.assertEqual(len(list(rollup_dir.glob("*.parquet"))), 2)

            seal = run_cli(
                "seal-hour",
                "--date-utc",
                "2024-03-31",
                "--hour-utc",
                "22",
                "--device-id",
                "1",
                root=root,
            )
            self.assertIn("triggered=True", seal.stdout)
            self.assertIn("replaced_group_count=6", seal.stdout)

            self.assertEqual(len(list(box_dir.glob("*.parquet"))), 1)
            self.assertEqual(len(list(trace_dir.glob("*.parquet"))), 1)
            self.assertEqual(len(list(rollup_dir.glob("*.parquet"))), 1)

            trace_query = run_cli(
                "exec-trace-lookup-query",
                "--device-id",
                "1",
                "--trace-id",
                "9001",
                root=root,
            )
            self.assertIn('"trace_id": 9001', trace_query.stdout)

            rollup_query = run_cli(
                "exec-rollup-query",
                "--device-id",
                "1",
                "--start-ms",
                "1711922400000",
                "--end-ms",
                "1711923599999",
                root=root,
            )
            self.assertEqual(rollup_query.stdout.count('"box_count": 2'), 2)

            second_seal = run_cli(
                "seal-hour",
                "--date-utc",
                "2024-03-31",
                "--hour-utc",
                "22",
                "--device-id",
                "1",
                root=root,
            )
            self.assertIn("triggered=False", second_seal.stdout)
            self.assertIn("reason=target partition is already sealed", second_seal.stdout)

    def test_serve_mode_keeps_running_with_status_port_option(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-serve-") as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir(parents=True, exist_ok=True)
            run_cli("init-metadata", root=root)

            env = os.environ.copy()
            env["PYTHONPATH"] = str(REPO_ROOT / "src")
            proc = subprocess.Popen(
                [
                    str(CLI_PYTHON),
                    "-u",
                    "-m",
                    "dbserver.cli",
                    "--root",
                    str(root),
                    "serve",
                    "--input-dir",
                    str(input_dir),
                    "--poll-interval-seconds",
                    "0.2",
                    "--max-runtime-seconds",
                    "30",
                    "--status-port",
                    "0",
                ],
                cwd=str(REPO_ROOT),
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                host = None
                port = None
                status_error = None
                seen_lines: list[str] = []
                deadline = time.time() + 10
                assert proc.stdout is not None
                while time.time() < deadline and (status_error is None and (host is None or port is None)):
                    line = proc.stdout.readline()
                    if not line:
                        time.sleep(0.1)
                        continue
                    line = line.strip()
                    seen_lines.append(line)
                    if line.startswith("status_host="):
                        host = line.split("=", 1)[1]
                    if line.startswith("status_port="):
                        port = int(line.split("=", 1)[1])
                    if line.startswith("status_interface_error="):
                        status_error = line.split("=", 1)[1]
                self.assertTrue(
                    host is not None or status_error is not None,
                    msg="\n".join(seen_lines),
                )

                if host is not None and port is not None:
                    with urlopen(f"http://{host}:{port}/healthz", timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        payload = json.loads(response.read().decode("utf-8"))
                        self.assertEqual(payload["status"], "ok")

                    with urlopen(f"http://{host}:{port}/status", timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        payload = json.loads(response.read().decode("utf-8"))
                        self.assertEqual(payload["service"], "dbserver")
                        self.assertEqual(payload["runtime"]["mode"], "serve")

                    with urlopen(f"http://{host}:{port}/metrics", timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        payload = response.read().decode("utf-8")
                        self.assertIn("dbserver_up 1", payload)
                        self.assertIn("dbserver_runtime_buffered_windows", payload)
                else:
                    self.assertIsNotNone(status_error, msg="\n".join(seen_lines))
                    self.assertIn("Operation not permitted", status_error)
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                stderr = "" if proc.stderr is None else proc.stderr.read()
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                self.assertEqual(proc.returncode, 0, msg=stderr)


if __name__ == "__main__":
    unittest.main()
