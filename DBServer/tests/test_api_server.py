from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
API_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
CLI_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_until(predicate, *, timeout_seconds: float = 10.0, step_seconds: float = 0.1) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(step_seconds)
    return False


def _start_api_server(root: Path, *, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    return subprocess.Popen(
        [
            str(API_PYTHON),
            "-u",
            "-m",
            "dbserver.api.server",
            "--root",
            str(root),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--publish-interval-seconds",
            "0.05",
            "--no-run-maintenance",
            "--log-level",
            "warning",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _run_cli(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    return subprocess.run(
        [
            str(CLI_PYTHON),
            "-m",
            "dbserver.cli",
            "--root",
            str(root),
            *args,
        ],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


class _CallbackSink:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests: list[dict[str, object]] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.port = _free_port()

    def start(self) -> None:
        sink = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                with sink._lock:
                    sink.requests.append(
                        {
                            "path": self.path,
                            "headers": {key: value for key, value in self.headers.items()},
                            "body": json.loads(body.decode("utf-8")),
                        }
                    )
                self.send_response(204)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:  # noqa: A003
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/callbacks/dbserver"


def _get_json(url: str) -> dict[str, object]:
    with urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_json(url: str, payload: dict[str, object]) -> tuple[int, dict[str, object]]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


class DbserverApiServerTest(unittest.TestCase):
    def test_ingest_endpoint_publishes_release_and_exposes_metrics(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-api-") as temp_dir:
            root = Path(temp_dir)
            port = _free_port()
            proc = _start_api_server(root, port=port)
            try:
                def _health_ready() -> bool:
                    try:
                        payload = _get_json(f"http://127.0.0.1:{port}/healthz")
                    except URLError:
                        return False
                    return payload["status"] == "ok"

                self.assertTrue(_wait_until(_health_ready))

                status_code, payload = _post_json(
                    f"http://127.0.0.1:{port}/v1/ingest/box-frames",
                    {
                        "source": "api-test",
                        "frames": [
                            {
                                "device_id": 1,
                                "frame_id": 1001,
                                "event_time_ms": 1711922400100,
                                "boxes": [
                                    {
                                        "trace_id": 42,
                                        "obj_type": 1,
                                        "position_x_mm": 1200,
                                        "position_y_mm": 3400,
                                        "position_z_mm": 0,
                                        "length_mm": 4500,
                                        "width_mm": 1800,
                                        "height_mm": 1600,
                                        "speed_centi_kmh_100": 1250,
                                        "spindle_centi_deg_100": 9050,
                                        "lane_id": 2,
                                    }
                                ],
                            },
                            {
                                "device_id": 1,
                                "frame_id": 1002,
                                "event_time_ms": 1711922400200,
                                "boxes": [
                                    {
                                        "trace_id": 42,
                                        "obj_type": 1,
                                        "position_x_mm": 1215,
                                        "position_y_mm": 3410,
                                        "position_z_mm": 0,
                                        "length_mm": 4500,
                                        "width_mm": 1800,
                                        "height_mm": 1600,
                                        "speed_centi_kmh_100": 1255,
                                        "spindle_centi_deg_100": 9052,
                                        "lane_id": 2,
                                    }
                                ],
                            },
                        ],
                    },
                )
                self.assertEqual(status_code, 200)
                self.assertTrue(payload["accepted"])
                self.assertEqual(payload["frames_accepted"], 2)
                self.assertEqual(payload["frames_rejected"], 0)
                self.assertEqual(payload["buffered_windows"], 1)

                latest_status: dict[str, object] = {}

                def _release_created() -> bool:
                    nonlocal latest_status
                    latest_status = _get_json(f"http://127.0.0.1:{port}/status")
                    return latest_status["current_release_id"] is not None

                self.assertTrue(_wait_until(_release_created))
                self.assertIsNotNone(latest_status["current_release_id"])
                self.assertTrue((root / "live" / "box_info").exists())

                with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    metrics = response.read().decode("utf-8")
                self.assertIn("dbserver_up 1", metrics)
                self.assertIn("dbserver_release_count", metrics)
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                stderr = "" if proc.stderr is None else proc.stderr.read()
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                self.assertIn(proc.returncode, (0, -signal.SIGTERM), msg=stderr)

    def test_export_job_api_completes_and_invokes_callback(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-api-export-") as temp_dir:
            root = Path(temp_dir)
            port = _free_port()
            callback_sink = _CallbackSink()
            callback_sink.start()
            proc = _start_api_server(root, port=port)
            try:
                def _health_ready() -> bool:
                    try:
                        payload = _get_json(f"http://127.0.0.1:{port}/healthz")
                    except URLError:
                        return False
                    return payload["status"] == "ok"

                self.assertTrue(_wait_until(_health_ready))

                status_code, _ = _post_json(
                    f"http://127.0.0.1:{port}/v1/ingest/box-frames",
                    {
                        "source": "api-test",
                        "frames": [
                            {
                                "device_id": 1,
                                "frame_id": 1001,
                                "event_time_ms": 1711922400100,
                                "boxes": [
                                    {
                                        "trace_id": 42,
                                        "obj_type": 1,
                                        "position_x_mm": 1200,
                                        "position_y_mm": 3400,
                                        "position_z_mm": 0,
                                        "length_mm": 4500,
                                        "width_mm": 1800,
                                        "height_mm": 1600,
                                        "speed_centi_kmh_100": 1250,
                                        "spindle_centi_deg_100": 9050,
                                        "lane_id": 2,
                                    }
                                ],
                            }
                        ],
                    },
                )
                self.assertEqual(status_code, 200)

                latest_status: dict[str, object] = {}

                def _release_created() -> bool:
                    nonlocal latest_status
                    latest_status = _get_json(f"http://127.0.0.1:{port}/v1/status")
                    return latest_status["current_release_id"] is not None

                self.assertTrue(_wait_until(_release_created))

                status_code, accepted = _post_json(
                    f"http://127.0.0.1:{port}/v1/jobs/export",
                    {
                        "request_id": "export-001",
                        "export_kind": "detail",
                        "device_id": 1,
                        "start_ms": 1711922400000,
                        "end_ms": 1711922999999,
                        "callback": {
                            "url": callback_sink.url,
                            "secret": "shared-secret",
                            "headers": {"X-Caller": "api-test"},
                        },
                    },
                )
                self.assertEqual(status_code, 202)
                job_id = str(accepted["job_id"])
                self.assertFalse(bool(accepted["reused_existing"]))

                status_code, duplicate = _post_json(
                    f"http://127.0.0.1:{port}/v1/jobs/export",
                    {
                        "request_id": "export-001",
                        "export_kind": "detail",
                        "device_id": 1,
                        "start_ms": 1711922400000,
                        "end_ms": 1711922999999,
                    },
                )
                self.assertEqual(status_code, 202)
                self.assertEqual(duplicate["job_id"], job_id)
                self.assertTrue(bool(duplicate["reused_existing"]))

                latest_job: dict[str, object] = {}

                def _job_finished() -> bool:
                    nonlocal latest_job
                    latest_job = _get_json(f"http://127.0.0.1:{port}/v1/jobs/{job_id}")
                    callback_payload = dict(latest_job["callback"])
                    return (
                        latest_job["status"] == "succeeded"
                        and callback_payload["status"] == "delivered"
                    )

                self.assertTrue(_wait_until(_job_finished, timeout_seconds=15.0))
                self.assertTrue(Path(str(dict(latest_job["result"])["output_path"])).exists())
                self.assertEqual(len(callback_sink.requests), 1)
                callback_request = callback_sink.requests[0]
                self.assertEqual(callback_request["body"]["job_id"], job_id)
                self.assertEqual(callback_request["body"]["status"], "succeeded")
                self.assertEqual(callback_request["headers"]["X-Caller"], "api-test")
                self.assertIn("X-Dbserver-Signature", callback_request["headers"])
            finally:
                callback_sink.stop()
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                stderr = "" if proc.stderr is None else proc.stderr.read()
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                self.assertIn(proc.returncode, (0, -signal.SIGTERM), msg=stderr)

    def test_delete_job_api_completes_and_invokes_callback(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-api-delete-") as temp_dir:
            root = Path(temp_dir)
            input_path = root / "seed.jsonl"
            _write_jsonl(
                input_path,
                [
                    {
                        "device_id": 1,
                        "frame_id": 1,
                        "event_time_ms": 1711922400100,
                        "boxes": [
                            {
                                "trace_id": 7,
                                "obj_type": 1,
                                "position_x_mm": 1,
                                "position_y_mm": 2,
                                "position_z_mm": 0,
                                "length_mm": 4,
                                "width_mm": 5,
                                "height_mm": 6,
                                "speed_centi_kmh_100": 7,
                                "spindle_centi_deg_100": 8,
                                "lane_id": 1,
                            }
                        ],
                    }
                ],
            )
            _run_cli(root, "init-metadata")
            _run_cli(
                root,
                "ingest-jsonl",
                "--input",
                str(input_path),
                "--now-ms",
                "1711923600000",
            )
            _run_cli(
                root,
                "seal-ready-hours",
                "--now-ms",
                "1711929600000",
            )

            callback_sink = _CallbackSink()
            callback_sink.start()
            port = _free_port()
            proc = _start_api_server(root, port=port)
            try:
                def _health_ready() -> bool:
                    try:
                        payload = _get_json(f"http://127.0.0.1:{port}/healthz")
                    except URLError:
                        return False
                    return payload["status"] == "ok"

                self.assertTrue(_wait_until(_health_ready))

                status_code, accepted = _post_json(
                    f"http://127.0.0.1:{port}/v1/jobs/delete",
                    {
                        "request_id": "delete-001",
                        "before_ms": 1711930000000,
                        "callback": {
                            "url": callback_sink.url,
                            "headers": {"X-Delete-Test": "1"},
                        },
                    },
                )
                self.assertEqual(status_code, 202)
                job_id = str(accepted["job_id"])

                latest_job: dict[str, object] = {}

                def _job_finished() -> bool:
                    nonlocal latest_job
                    latest_job = _get_json(f"http://127.0.0.1:{port}/v1/jobs/{job_id}")
                    callback_payload = dict(latest_job["callback"])
                    return (
                        latest_job["status"] == "succeeded"
                        and callback_payload["status"] == "delivered"
                    )

                self.assertTrue(_wait_until(_job_finished, timeout_seconds=15.0))
                result = dict(latest_job["result"])
                self.assertEqual(result["deleted_group_count"], 3)
                self.assertIsNotNone(result["release_id"])

                current_status = _get_json(f"http://127.0.0.1:{port}/v1/status")
                self.assertEqual(current_status["current_release_id"], result["release_id"])
                self.assertEqual(len(callback_sink.requests), 1)
                self.assertEqual(callback_sink.requests[0]["body"]["job_id"], job_id)
                self.assertEqual(callback_sink.requests[0]["headers"]["X-Delete-Test"], "1")
            finally:
                callback_sink.stop()
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                stderr = "" if proc.stderr is None else proc.stderr.read()
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                self.assertIn(proc.returncode, (0, -signal.SIGTERM), msg=stderr)

    def test_invalid_ingest_is_quarantined(self) -> None:
        with tempfile.TemporaryDirectory(prefix="dbserver-api-bad-") as temp_dir:
            root = Path(temp_dir)
            port = _free_port()
            proc = _start_api_server(root, port=port)
            try:
                def _health_ready() -> bool:
                    try:
                        payload = _get_json(f"http://127.0.0.1:{port}/healthz")
                    except URLError:
                        return False
                    return payload["status"] == "ok"

                self.assertTrue(_wait_until(_health_ready))

                status_code, payload = _post_json(
                    f"http://127.0.0.1:{port}/v1/ingest/box-frames",
                    {
                        "source": "api-test",
                        "frames": [
                            {
                                "device_id": 1,
                                "frame_id": 1,
                                "event_time_ms": 1000,
                                "boxes": [
                                    {
                                        "trace_id": 1,
                                        "obj_type": 1,
                                        "position_x_mm": 1,
                                        "position_y_mm": 1,
                                        "position_z_mm": 0,
                                        "length_mm": 1,
                                        "width_mm": 1,
                                        "height_mm": 1,
                                        "speed_centi_kmh_100": 1,
                                        "spindle_centi_deg_100": 1,
                                        "lane_id": 1,
                                    }
                                ],
                            }
                        ],
                    },
                )
                self.assertEqual(status_code, 200)
                self.assertFalse(payload["accepted"])
                self.assertEqual(payload["frames_accepted"], 0)
                self.assertEqual(payload["frames_rejected"], 1)

                quarantine_path = root / "quarantine" / "bad_timestamps" / "rejected.jsonl"
                self.assertTrue(_wait_until(quarantine_path.exists, timeout_seconds=3.0))
                records = [json.loads(line) for line in quarantine_path.read_text(encoding="utf-8").splitlines()]
                self.assertEqual(len(records), 1)
                self.assertIn("event_time_ms", records[0]["reason"])
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                stderr = "" if proc.stderr is None else proc.stderr.read()
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                self.assertIn(proc.returncode, (0, -signal.SIGTERM), msg=stderr)
