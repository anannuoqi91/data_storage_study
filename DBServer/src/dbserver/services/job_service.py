from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, replace
from pathlib import Path
from time import time_ns
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import CallbackStatus, JobStatus, JobType, TimeRange
from dbserver.runtime_logging import RuntimeLogger
from dbserver.services.export_service import ExportJobRecord, ExportService
from dbserver.services.ops_service import OpsService
from dbserver.storage.layout import StorageLayout


def _now_ms() -> int:
    return time_ns() // 1_000_000


@dataclass(slots=True, frozen=True)
class CallbackSpec:
    url: str
    headers: dict[str, str]
    secret: str | None = None


@dataclass(slots=True, frozen=True)
class JobRecord:
    job_id: str
    job_type: JobType
    status: JobStatus
    created_at_ms: int
    request_id: str | None
    parameters: dict[str, object]
    callback: CallbackSpec | None = None
    result: dict[str, object] | None = None
    error: str | None = None
    started_at_ms: int | None = None
    finished_at_ms: int | None = None
    callback_status: CallbackStatus = CallbackStatus.NOT_CONFIGURED
    callback_attempt_count: int = 0
    callback_next_attempt_at_ms: int | None = None
    callback_delivered_at_ms: int | None = None
    callback_last_error: str | None = None
    callback_last_status_code: int | None = None

    def to_json(self) -> str:
        payload: dict[str, object] = {
            "job_id": self.job_id,
            "job_type": self.job_type.value,
            "status": self.status.value,
            "created_at_ms": self.created_at_ms,
            "request_id": self.request_id,
            "parameters": self.parameters,
            "result": self.result,
            "error": self.error,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "callback_status": self.callback_status.value,
            "callback_attempt_count": self.callback_attempt_count,
            "callback_next_attempt_at_ms": self.callback_next_attempt_at_ms,
            "callback_delivered_at_ms": self.callback_delivered_at_ms,
            "callback_last_error": self.callback_last_error,
            "callback_last_status_code": self.callback_last_status_code,
        }
        if self.callback is None:
            payload["callback"] = None
        else:
            payload["callback"] = {
                "url": self.callback.url,
                "headers": self.callback.headers,
                "secret": self.callback.secret,
            }
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_path(cls, path: Path) -> "JobRecord":
        payload = json.loads(path.read_text(encoding="utf-8"))
        callback_payload = payload.get("callback")
        callback = None
        if isinstance(callback_payload, dict):
            callback = CallbackSpec(
                url=str(callback_payload["url"]),
                headers={
                    str(key): str(value)
                    for key, value in dict(callback_payload.get("headers", {})).items()
                },
                secret=(
                    None
                    if callback_payload.get("secret") is None
                    else str(callback_payload["secret"])
                ),
            )
        return cls(
            job_id=str(payload["job_id"]),
            job_type=JobType(str(payload["job_type"])),
            status=JobStatus(str(payload["status"])),
            created_at_ms=int(payload["created_at_ms"]),
            request_id=None if payload.get("request_id") is None else str(payload["request_id"]),
            parameters=dict(payload.get("parameters", {})),
            callback=callback,
            result=None if payload.get("result") is None else dict(payload["result"]),
            error=None if payload.get("error") is None else str(payload["error"]),
            started_at_ms=payload.get("started_at_ms"),
            finished_at_ms=payload.get("finished_at_ms"),
            callback_status=CallbackStatus(str(payload.get("callback_status", "not_configured"))),
            callback_attempt_count=int(payload.get("callback_attempt_count", 0)),
            callback_next_attempt_at_ms=payload.get("callback_next_attempt_at_ms"),
            callback_delivered_at_ms=payload.get("callback_delivered_at_ms"),
            callback_last_error=payload.get("callback_last_error"),
            callback_last_status_code=payload.get("callback_last_status_code"),
        )

    def to_public_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "job_id": self.job_id,
            "job_type": self.job_type.value,
            "status": self.status.value,
            "created_at_ms": self.created_at_ms,
            "request_id": self.request_id,
            "parameters": self.parameters,
            "result": self.result,
            "error": self.error,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "callback": {
                "configured": self.callback is not None,
                "status": self.callback_status.value,
                "attempt_count": self.callback_attempt_count,
                "next_attempt_at_ms": self.callback_next_attempt_at_ms,
                "delivered_at_ms": self.callback_delivered_at_ms,
                "last_error": self.callback_last_error,
                "last_status_code": self.callback_last_status_code,
                "url": None if self.callback is None else self.callback.url,
            },
        }
        return payload


@dataclass(slots=True, frozen=True)
class JobSubmitResult:
    record: JobRecord
    reused_existing: bool


@dataclass(slots=True, frozen=True)
class JobQueueResult:
    processed_jobs: int
    succeeded_jobs: int
    failed_jobs: int
    callback_delivered_jobs: int
    job_ids: tuple[str, ...]


class JobService:
    def __init__(
        self,
        metadata_store: MetadataStore,
        journal: ReleaseJournal,
        layout: StorageLayout,
        runtime_logger: RuntimeLogger,
        export_service: ExportService,
        *,
        callback_timeout_seconds: float = 5.0,
        callback_retry_seconds: float = 30.0,
        callback_max_attempts: int = 5,
    ) -> None:
        self.metadata_store = metadata_store
        self.journal = journal
        self.layout = layout
        self.runtime_logger = runtime_logger
        self.export_service = export_service
        self.callback_timeout_seconds = max(callback_timeout_seconds, 0.1)
        self.callback_retry_seconds = max(callback_retry_seconds, 0.1)
        self.callback_max_attempts = max(callback_max_attempts, 1)

    def recover_running_jobs(self) -> int:
        recovered = 0
        for path in sorted(self.layout.paths.jobs.joinpath("running").glob("*.json")):
            record = JobRecord.from_path(path)
            pending = replace(
                record,
                status=JobStatus.PENDING,
                started_at_ms=None,
                finished_at_ms=None,
                error=None,
                result=None,
            )
            self.layout.job_pending_path(record.job_id).write_text(
                pending.to_json(),
                encoding="utf-8",
            )
            path.unlink(missing_ok=True)
            recovered += 1
        if recovered:
            self.runtime_logger.append(
                "job_queue_recovered",
                {"recovered_job_count": recovered},
            )
        return recovered

    def submit_export_job(
        self,
        *,
        export_kind: str,
        device_id: int,
        start_ms: int | None = None,
        end_ms: int | None = None,
        trace_id: int | None = None,
        lane_id: int | None = None,
        obj_type: int | None = None,
        request_id: str | None = None,
        callback: CallbackSpec | None = None,
    ) -> JobSubmitResult:
        existing = self._find_by_request_id(request_id)
        if existing is not None:
            return JobSubmitResult(record=existing, reused_existing=True)
        parameters: dict[str, object] = {
            "export_kind": export_kind,
            "device_id": device_id,
        }
        if start_ms is not None:
            parameters["start_ms"] = start_ms
        if end_ms is not None:
            parameters["end_ms"] = end_ms
        if trace_id is not None:
            parameters["trace_id"] = trace_id
        if lane_id is not None:
            parameters["lane_id"] = lane_id
        if obj_type is not None:
            parameters["obj_type"] = obj_type
        record = self._create_job_record(
            job_type=JobType.EXPORT,
            request_id=request_id,
            parameters=parameters,
            callback=callback,
        )
        return JobSubmitResult(record=self._write_pending_job(record), reused_existing=False)

    def submit_delete_job(
        self,
        *,
        before_ms: int,
        request_id: str | None = None,
        query_wait_timeout_seconds: float = 0.0,
        query_poll_interval_seconds: float = 0.5,
        callback: CallbackSpec | None = None,
    ) -> JobSubmitResult:
        existing = self._find_by_request_id(request_id)
        if existing is not None:
            return JobSubmitResult(record=existing, reused_existing=True)
        record = self._create_job_record(
            job_type=JobType.DELETE,
            request_id=request_id,
            parameters={
                "before_ms": before_ms,
                "query_wait_timeout_seconds": query_wait_timeout_seconds,
                "query_poll_interval_seconds": query_poll_interval_seconds,
            },
            callback=callback,
        )
        return JobSubmitResult(record=self._write_pending_job(record), reused_existing=False)

    def list_jobs(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        for dirname in ("pending", "running", "finished"):
            for path in sorted(self.layout.paths.jobs.joinpath(dirname).glob("*.json")):
                records.append(JobRecord.from_path(path))
        records.sort(key=lambda item: (item.created_at_ms, item.job_id))
        return records

    def get_job(self, job_id: str) -> JobRecord | None:
        for path in (
            self.layout.job_pending_path(job_id),
            self.layout.job_running_path(job_id),
            self.layout.job_finished_path(job_id),
        ):
            if path.exists():
                return JobRecord.from_path(path)
        return None

    def process_pending_jobs(self, *, max_jobs: int | None = None) -> JobQueueResult:
        pending_items: list[tuple[JobRecord, Path]] = []
        for path in self.layout.paths.jobs.joinpath("pending").glob("*.json"):
            pending_items.append((JobRecord.from_path(path), path))
        pending_items.sort(key=lambda item: (item[0].created_at_ms, item[0].job_id))
        if max_jobs is not None:
            pending_items = pending_items[:max_jobs]

        processed_jobs = 0
        succeeded_jobs = 0
        failed_jobs = 0
        callback_delivered_jobs = 0
        job_ids: list[str] = []
        for record, pending_path in pending_items:
            running_path = self.layout.job_running_path(record.job_id)
            try:
                pending_path.replace(running_path)
            except FileNotFoundError:
                continue
            running = replace(
                record,
                status=JobStatus.RUNNING,
                started_at_ms=_now_ms(),
                finished_at_ms=None,
                error=None,
                result=None,
            )
            running_path.write_text(running.to_json(), encoding="utf-8")
            terminal = self._execute_job(running)
            running_path.unlink(missing_ok=True)
            finished_path = self.layout.job_finished_path(running.job_id)
            terminal = self._deliver_callback_if_due(terminal, allow_retry=True)
            finished_path.write_text(terminal.to_json(), encoding="utf-8")
            processed_jobs += 1
            job_ids.append(terminal.job_id)
            if terminal.status is JobStatus.SUCCEEDED:
                succeeded_jobs += 1
            else:
                failed_jobs += 1
            if terminal.callback_status is CallbackStatus.DELIVERED:
                callback_delivered_jobs += 1
        callback_delivered_jobs += self.deliver_due_callbacks(max_jobs=max_jobs)
        return JobQueueResult(
            processed_jobs=processed_jobs,
            succeeded_jobs=succeeded_jobs,
            failed_jobs=failed_jobs,
            callback_delivered_jobs=callback_delivered_jobs,
            job_ids=tuple(job_ids),
        )

    def deliver_due_callbacks(self, *, max_jobs: int | None = None) -> int:
        delivered = 0
        due: list[tuple[JobRecord, Path]] = []
        now_ms = _now_ms()
        for path in sorted(self.layout.paths.jobs.joinpath("finished").glob("*.json")):
            record = JobRecord.from_path(path)
            if record.callback is None or record.callback_status is CallbackStatus.DELIVERED:
                continue
            if record.callback_status is CallbackStatus.FAILED:
                continue
            if record.callback_next_attempt_at_ms is not None and record.callback_next_attempt_at_ms > now_ms:
                continue
            due.append((record, path))
        if max_jobs is not None:
            due = due[:max_jobs]
        for record, path in due:
            updated = self._deliver_callback_if_due(record, allow_retry=True)
            path.write_text(updated.to_json(), encoding="utf-8")
            if updated.callback_status is CallbackStatus.DELIVERED:
                delivered += 1
        return delivered

    def _create_job_record(
        self,
        *,
        job_type: JobType,
        request_id: str | None,
        parameters: dict[str, object],
        callback: CallbackSpec | None,
    ) -> JobRecord:
        callback_status = (
            CallbackStatus.NOT_CONFIGURED
            if callback is None
            else CallbackStatus.PENDING
        )
        callback_next_attempt_at_ms = None if callback is None else _now_ms()
        return JobRecord(
            job_id=uuid4().hex,
            job_type=job_type,
            status=JobStatus.PENDING,
            created_at_ms=_now_ms(),
            request_id=request_id,
            parameters=parameters,
            callback=callback,
            callback_status=callback_status,
            callback_next_attempt_at_ms=callback_next_attempt_at_ms,
        )

    def _write_pending_job(self, record: JobRecord) -> JobRecord:
        path = self.layout.job_pending_path(record.job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(record.to_json(), encoding="utf-8")
        self.runtime_logger.append(
            "job_submitted",
            {
                "job_id": record.job_id,
                "job_type": record.job_type.value,
                "request_id": record.request_id,
                "parameters": record.parameters,
                "callback_configured": record.callback is not None,
            },
            ts_ms=record.created_at_ms,
        )
        return record

    def _execute_job(self, record: JobRecord) -> JobRecord:
        try:
            if record.job_type is JobType.EXPORT:
                return self._execute_export_job(record)
            if record.job_type is JobType.DELETE:
                return self._execute_delete_job(record)
            raise ValueError(f"unsupported job type: {record.job_type.value}")
        except Exception as exc:
            finished_at_ms = _now_ms()
            self.runtime_logger.append(
                "job_failed",
                {
                    "job_id": record.job_id,
                    "job_type": record.job_type.value,
                    "error": str(exc),
                },
                ts_ms=finished_at_ms,
            )
            return replace(
                record,
                status=JobStatus.FAILED,
                finished_at_ms=finished_at_ms,
                error=str(exc),
                result=None,
            )

    def _execute_export_job(self, record: JobRecord) -> JobRecord:
        export_kind = str(record.parameters["export_kind"])
        device_id = int(record.parameters["device_id"])
        export_record: ExportJobRecord
        if export_kind == "detail":
            time_range = TimeRange(
                start_ms=int(record.parameters["start_ms"]),
                end_ms=int(record.parameters["end_ms"]),
            )
            export_record = self.export_service.build_detail_job_record(
                device_id=device_id,
                time_range=time_range,
                export_id=record.job_id,
            )
        elif export_kind == "trace":
            time_range = None
            if "start_ms" in record.parameters and "end_ms" in record.parameters:
                time_range = TimeRange(
                    start_ms=int(record.parameters["start_ms"]),
                    end_ms=int(record.parameters["end_ms"]),
                )
            export_record = self.export_service.build_trace_job_record(
                device_id=device_id,
                trace_id=int(record.parameters["trace_id"]),
                time_range=time_range,
                export_id=record.job_id,
            )
        elif export_kind == "rollup":
            time_range = TimeRange(
                start_ms=int(record.parameters["start_ms"]),
                end_ms=int(record.parameters["end_ms"]),
            )
            export_record = self.export_service.build_rollup_job_record(
                device_id=device_id,
                time_range=time_range,
                lane_id=(
                    None
                    if record.parameters.get("lane_id") is None
                    else int(record.parameters["lane_id"])
                ),
                obj_type=(
                    None
                    if record.parameters.get("obj_type") is None
                    else int(record.parameters["obj_type"])
                ),
                export_id=record.job_id,
            )
        else:
            raise ValueError(f"unsupported export_kind: {export_kind}")

        terminal = self.export_service.execute_job_record(export_record)
        status = JobStatus.SUCCEEDED if terminal.status == "finished" else JobStatus.FAILED
        return replace(
            record,
            status=status,
            finished_at_ms=terminal.finished_at_ms,
            result=(
                None
                if status is JobStatus.FAILED
                else {
                    "export_kind": export_kind,
                    "format": terminal.format,
                    "release_id": terminal.release_id,
                    "row_count": terminal.row_count,
                    "output_path": terminal.output_path,
                }
            ),
            error=terminal.error,
        )

    def _execute_delete_job(self, record: JobRecord) -> JobRecord:
        before_ms = int(record.parameters["before_ms"])
        ops_service = OpsService(self.metadata_store, self.journal)
        plan = ops_service.build_delete_plan(
            retention_floor_ms=before_ms,
            schema_version_id=self._current_schema_version_id(),
            note="api delete job",
        )
        deleted_partition_count = self._count_deleted_partitions(plan.delete_groups)
        deleted_group_count = len(plan.delete_groups)
        deleted_bytes_estimate = sum(group.total_bytes for group in plan.delete_groups)
        if deleted_group_count == 0:
            finished_at_ms = _now_ms()
            self.runtime_logger.append(
                "job_delete_noop",
                {
                    "job_id": record.job_id,
                    "before_ms": before_ms,
                },
                ts_ms=finished_at_ms,
            )
            return replace(
                record,
                status=JobStatus.SUCCEEDED,
                finished_at_ms=finished_at_ms,
                result={
                    "before_ms": before_ms,
                    "deleted_partition_count": 0,
                    "deleted_group_count": 0,
                    "deleted_bytes_estimate": 0,
                    "release_id": None,
                },
            )
        release = ops_service.commit_delete_plan_with_wait(
            plan,
            query_wait_timeout_ms=int(float(record.parameters["query_wait_timeout_seconds"]) * 1000),
            query_poll_interval_ms=int(float(record.parameters["query_poll_interval_seconds"]) * 1000),
        )
        self.runtime_logger.append(
            "job_delete_finished",
            {
                "job_id": record.job_id,
                "before_ms": before_ms,
                "release_id": release.release_id,
                "deleted_partition_count": deleted_partition_count,
                "deleted_group_count": deleted_group_count,
                "deleted_bytes_estimate": deleted_bytes_estimate,
            },
            ts_ms=release.created_at_ms,
        )
        return replace(
            record,
            status=JobStatus.SUCCEEDED,
            finished_at_ms=release.created_at_ms,
            result={
                "before_ms": before_ms,
                "deleted_partition_count": deleted_partition_count,
                "deleted_group_count": deleted_group_count,
                "deleted_bytes_estimate": deleted_bytes_estimate,
                "release_id": release.release_id,
            },
        )

    def _deliver_callback_if_due(self, record: JobRecord, *, allow_retry: bool) -> JobRecord:
        if record.callback is None:
            return record
        if record.callback_status is CallbackStatus.DELIVERED:
            return record
        if record.callback_status is CallbackStatus.FAILED:
            return record
        now_ms = _now_ms()
        if record.callback_next_attempt_at_ms is not None and record.callback_next_attempt_at_ms > now_ms:
            return record
        payload = json.dumps(record.to_public_dict(), sort_keys=True).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-DBServer-Job-Id": record.job_id,
        }
        headers.update(record.callback.headers)
        if record.callback.secret:
            digest = hmac.new(
                record.callback.secret.encode("utf-8"),
                payload,
                hashlib.sha256,
            ).hexdigest()
            headers["X-DBServer-Signature"] = "sha256=" + digest
        request = Request(
            record.callback.url,
            data=payload,
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.callback_timeout_seconds) as response:
                status_code = int(response.status)
                if status_code < 200 or status_code >= 300:
                    raise RuntimeError(f"callback returned unexpected status {status_code}")
        except (HTTPError, URLError, RuntimeError) as exc:
            attempt_count = record.callback_attempt_count + 1
            next_attempt_at_ms = (
                None
                if not allow_retry or attempt_count >= self.callback_max_attempts
                else now_ms + int(self.callback_retry_seconds * 1000)
            )
            callback_status = (
                CallbackStatus.FAILED
                if next_attempt_at_ms is None
                else CallbackStatus.PENDING
            )
            updated = replace(
                record,
                callback_status=callback_status,
                callback_attempt_count=attempt_count,
                callback_next_attempt_at_ms=next_attempt_at_ms,
                callback_last_error=str(exc),
                callback_last_status_code=(
                    exc.code if isinstance(exc, HTTPError) else record.callback_last_status_code
                ),
            )
            self.runtime_logger.append(
                "job_callback_delivery_failed",
                {
                    "job_id": record.job_id,
                    "job_type": record.job_type.value,
                    "attempt_count": attempt_count,
                    "callback_url": record.callback.url,
                    "error": str(exc),
                    "will_retry": updated.callback_status is CallbackStatus.PENDING,
                },
                ts_ms=now_ms,
            )
            return updated
        updated = replace(
            record,
            callback_status=CallbackStatus.DELIVERED,
            callback_attempt_count=record.callback_attempt_count + 1,
            callback_next_attempt_at_ms=None,
            callback_delivered_at_ms=now_ms,
            callback_last_error=None,
            callback_last_status_code=200,
        )
        self.runtime_logger.append(
            "job_callback_delivered",
            {
                "job_id": record.job_id,
                "job_type": record.job_type.value,
                "attempt_count": updated.callback_attempt_count,
                "callback_url": record.callback.url,
            },
            ts_ms=now_ms,
        )
        return updated

    def _find_by_request_id(self, request_id: str | None) -> JobRecord | None:
        if request_id is None or not request_id.strip():
            return None
        for record in self.list_jobs():
            if record.request_id == request_id:
                return record
        return None

    def _current_schema_version_id(self) -> str:
        release_id = self.metadata_store.get_current_release()
        if release_id is None:
            raise ValueError("no current release exists")
        release = self.metadata_store.get_release(release_id)
        if release is None:
            raise ValueError("current release metadata is missing")
        return release.schema_version_id

    def _count_deleted_partitions(self, groups: tuple[object, ...]) -> int:
        seen: set[tuple[str, int, int]] = set()
        for group in groups:
            date_utc = getattr(group, "date_utc", None)
            hour_utc = getattr(group, "hour_utc", None)
            device_id = getattr(group, "device_id", None)
            if date_utc is None or hour_utc is None or device_id is None:
                continue
            seen.add((str(date_utc), int(hour_utc), int(device_id)))
        return len(seen)
