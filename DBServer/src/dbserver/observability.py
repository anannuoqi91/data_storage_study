from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from time import time_ns
from typing import Any, Mapping
from urllib.parse import urlparse

from dbserver import __version__
from dbserver.config import AppConfig
from dbserver.metadata.store import MetadataStore


def _now_ms() -> int:
    return time_ns() // 1_000_000


def _directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            total += entry.stat().st_size
    return total


def _collect_export_counts(exports_root: Path) -> dict[str, int]:
    counts = {
        "pending": 0,
        "running": 0,
        "finished": 0,
        "failed": 0,
    }
    pending_dir = exports_root / "pending"
    finished_dir = exports_root / "finished"
    for path in pending_dir.glob("*.json"):
        if path.name.endswith(".working.json"):
            counts["running"] += 1
        else:
            counts["pending"] += 1
    for path in finished_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        status = payload.get("status")
        if status == "failed":
            counts["failed"] += 1
        elif status == "finished":
            counts["finished"] += 1
    return counts


def _collect_job_counts(jobs_root: Path) -> tuple[dict[str, int], dict[str, int]]:
    job_counts = {
        "pending": 0,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
    }
    callback_counts = {
        "not_configured": 0,
        "pending": 0,
        "delivered": 0,
        "failed": 0,
    }
    for status_name in ("pending", "running", "finished"):
        for path in jobs_root.joinpath(status_name).glob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            status = payload.get("status")
            if status in job_counts:
                job_counts[str(status)] += 1
            callback_status = payload.get("callback_status", "not_configured")
            if callback_status in callback_counts:
                callback_counts[str(callback_status)] += 1
    return job_counts, callback_counts


class ObservabilityState:
    def __init__(self) -> None:
        self._lock = Lock()
        self._snapshot: dict[str, Any] = {
            "service": "dbserver",
            "version": __version__,
            "generated_at_ms": _now_ms(),
            "status": "starting",
        }

    def replace(self, snapshot: Mapping[str, Any]) -> None:
        with self._lock:
            self._snapshot = copy.deepcopy(dict(snapshot))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._snapshot)


@dataclass(slots=True)
class StatusServer:
    host: str
    port: int
    state: ObservabilityState
    _server: ThreadingHTTPServer | None = None
    _thread: Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        state = self.state

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/healthz":
                    body = json.dumps(
                        {
                            "service": "dbserver",
                            "status": "ok",
                            "generated_at_ms": _now_ms(),
                        },
                        sort_keys=True,
                    ).encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if parsed.path == "/status":
                    body = json.dumps(state.snapshot(), sort_keys=True).encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if parsed.path == "/metrics":
                    body = render_metrics_text(state.snapshot()).encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_error(HTTPStatus.NOT_FOUND, "unsupported path")

            def log_message(self, format: str, *args: object) -> None:  # noqa: A003
                return

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None

    @property
    def bound_host(self) -> str:
        if self._server is None:
            return self.host
        return str(self._server.server_address[0])

    @property
    def bound_port(self) -> int:
        if self._server is None:
            return self.port
        return int(self._server.server_address[1])


def collect_status_snapshot(
    config: AppConfig,
    metadata: MetadataStore,
    *,
    runtime: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at_ms = _now_ms()
    current_release_id = metadata.get_current_release()
    current_release = None
    if current_release_id is not None:
        release = metadata.get_release(current_release_id)
        if release is not None:
            current_release = {
                "release_id": release.release_id,
                "parent_release_id": release.parent_release_id,
                "schema_version_id": release.schema_version_id,
                "created_at_ms": release.created_at_ms,
                "event_type": release.event_type.value,
                "retention_floor_ms": release.retention_floor_ms,
                "note": release.note,
            }

    partitions = metadata.list_partition_states()
    partition_counts: dict[str, int] = {}
    for record in partitions:
        partition_counts.setdefault(record.state.value, 0)
        partition_counts[record.state.value] += 1

    active_queries = metadata.list_active_queries()
    storage_bytes = {
        "live": _directory_size_bytes(config.paths.live),
        "staging": _directory_size_bytes(config.paths.staging),
        "metadata": _directory_size_bytes(config.paths.metadata),
        "quarantine": _directory_size_bytes(config.paths.quarantine),
        "exports": _directory_size_bytes(config.paths.exports),
    }
    storage_bytes["root"] = sum(storage_bytes.values())
    export_counts = _collect_export_counts(config.paths.exports)
    job_counts, callback_counts = _collect_job_counts(config.paths.jobs)

    snapshot = {
        "service": "dbserver",
        "version": __version__,
        "generated_at_ms": generated_at_ms,
        "status": "ok",
        "root": str(config.paths.root),
        "current_release_id": current_release_id,
        "current_release": current_release,
        "release_count": len(metadata.list_releases()),
        "retention_floor_ms": metadata.get_current_retention_floor_ms(),
        "partition_total": len(partitions),
        "partition_state_counts": partition_counts,
        "active_query_count": len(active_queries),
        "active_queries": [
            {
                "query_id": handle.query_id,
                "release_id": handle.release_id,
                "query_kind": handle.query_kind.value,
                "started_at_ms": handle.started_at_ms,
                "owner_pid": handle.owner_pid,
                "target_range_start_ms": None if handle.target_range is None else handle.target_range.start_ms,
                "target_range_end_ms": None if handle.target_range is None else handle.target_range.end_ms,
            }
            for handle in active_queries
        ],
        "storage_bytes": storage_bytes,
        "export_counts": export_counts,
        "job_counts": job_counts,
        "callback_counts": callback_counts,
        "runtime": dict(runtime or {}),
    }
    return snapshot


def render_metrics_text(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# HELP dbserver_up Whether DBServer observability endpoint is up.",
        "# TYPE dbserver_up gauge",
        "dbserver_up 1",
    ]
    release_count = int(snapshot.get("release_count", 0))
    lines.extend(
        [
            "# HELP dbserver_release_count Number of known releases.",
            "# TYPE dbserver_release_count gauge",
            f"dbserver_release_count {release_count}",
            "# HELP dbserver_active_query_count Number of active queries tracked for delete protection.",
            "# TYPE dbserver_active_query_count gauge",
            f"dbserver_active_query_count {int(snapshot.get('active_query_count', 0))}",
            "# HELP dbserver_partition_total Number of known partition states.",
            "# TYPE dbserver_partition_total gauge",
            f"dbserver_partition_total {int(snapshot.get('partition_total', 0))}",
        ]
    )
    for state, count in sorted(dict(snapshot.get("partition_state_counts", {})).items()):
        lines.append(
            f'dbserver_partition_state_count{{state="{state}"}} {int(count)}'
        )
    for area, value in sorted(dict(snapshot.get("storage_bytes", {})).items()):
        lines.append(f'dbserver_storage_bytes{{area="{area}"}} {int(value)}')
    for state, value in sorted(dict(snapshot.get("export_counts", {})).items()):
        lines.append(f'dbserver_export_job_count{{status="{state}"}} {int(value)}')
    for state, value in sorted(dict(snapshot.get("job_counts", {})).items()):
        lines.append(f'dbserver_job_count{{status="{state}"}} {int(value)}')
    for state, value in sorted(dict(snapshot.get("callback_counts", {})).items()):
        lines.append(f'dbserver_job_callback_count{{status="{state}"}} {int(value)}')

    runtime = dict(snapshot.get("runtime", {}))
    numeric_runtime_keys = (
        "buffered_windows",
        "buffered_boxes",
        "processed_files_total",
        "failed_files_total",
        "frames_total",
        "frames_accepted",
        "frames_rejected",
        "boxes_accepted",
        "published_windows_last_cycle",
        "published_rows_last_cycle",
        "last_publish_now_ms",
        "serve_started_at_ms",
        "recovered_stale_queries",
    )
    for key in numeric_runtime_keys:
        value = runtime.get(key)
        if value is None:
            continue
        metric_name = "dbserver_runtime_" + key
        lines.append(f"{metric_name} {int(value)}")
    return "\n".join(lines) + "\n"
