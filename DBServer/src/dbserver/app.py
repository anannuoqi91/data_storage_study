from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from threading import Event, RLock, Thread
from time import monotonic, time_ns
from typing import Iterable

from dbserver.adapters.cyber import CyberBoxesSubscriber
from dbserver.config import AppConfig, build_config
from dbserver.domain.records import FrameBatch
from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import DatasetKind, PartitionKey, PartitionState, PartitionStateRecord
from dbserver.observability import collect_status_snapshot, render_metrics_text
from dbserver.runtime_logging import RuntimeLogger
from dbserver.services.derived_data_service import DerivedDataService
from dbserver.services.export_service import ExportQueueResult, ExportService
from dbserver.services.ingest_service import IngestService
from dbserver.services.job_service import CallbackSpec, JobQueueResult, JobRecord, JobService, JobSubmitResult
from dbserver.services.maintenance_service import MaintenanceRunResult, MaintenanceService
from dbserver.services.publish_service import PublishService
from dbserver.services.retention_service import RetentionService
from dbserver.services.seal_service import SealService
from dbserver.services.validator_service import ValidationError, ValidatorService
from dbserver.storage.layout import StorageLayout
from dbserver.storage.parquet_writer import BoxInfoParquetWriter
from dbserver.storage.structured_parquet_writer import StructuredParquetWriter


def _now_ms() -> int:
    return time_ns() // 1_000_000


def _default_box_info_schema_json() -> str:
    schema = {
        "table_name": "box_info",
        "columns": [
            {"name": "trace_id", "type": "INTEGER"},
            {"name": "device_id", "type": "USMALLINT"},
            {"name": "event_time_ms", "type": "BIGINT"},
            {"name": "obj_type", "type": "UTINYINT"},
            {"name": "position_x_mm", "type": "INTEGER"},
            {"name": "position_y_mm", "type": "INTEGER"},
            {"name": "position_z_mm", "type": "INTEGER"},
            {"name": "length_mm", "type": "USMALLINT"},
            {"name": "width_mm", "type": "USMALLINT"},
            {"name": "height_mm", "type": "USMALLINT"},
            {"name": "speed_centi_kmh_100", "type": "USMALLINT"},
            {"name": "spindle_centi_deg_100", "type": "USMALLINT"},
            {"name": "lane_id", "type": "UTINYINT"},
            {"name": "frame_id", "type": "INTEGER"},
        ],
        "derived_columns": [
            {"name": "date_utc", "from": "event_time_ms"},
            {"name": "hour_utc", "from": "event_time_ms"},
            {"name": "sample_offset_ms", "from": "event_time_ms"},
        ],
    }
    return json.dumps(schema, sort_keys=True)


def _resolve_schema_version_id(metadata: MetadataStore, requested: str | None) -> str:
    if requested:
        return requested
    current_release_id = metadata.get_current_release()
    if current_release_id is None:
        return "box_info_v1"
    current_release = metadata.get_release(current_release_id)
    if current_release is None:
        return "box_info_v1"
    return current_release.schema_version_id


def _ensure_schema_version(metadata: MetadataStore, schema_version_id: str) -> None:
    if metadata.schema_version_exists(schema_version_id):
        return
    metadata.upsert_schema_version(
        schema_version_id=schema_version_id,
        created_at_ms=_now_ms(),
        table_name="box_info",
        schema_json=_default_box_info_schema_json(),
        partition_rule="date/hour directories derived from event_time_ms in UTC",
        sort_rule="event_time_ms, frame_id, trace_id",
        compression_codec="ZSTD",
        is_current=True,
    )


def _append_quarantine_record(config: AppConfig, *, frame_summary: dict[str, object], reason: str) -> None:
    if "event_time_ms" in reason:
        target_dir = config.paths.quarantine / "bad_timestamps"
    else:
        target_dir = config.paths.quarantine / "bad_frames"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / "rejected.jsonl"
    payload = {
        "ts_ms": _now_ms(),
        "reason": reason,
        "frame": frame_summary,
    }
    with target_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def _recover_orphan_files(metadata: MetadataStore, layout: StorageLayout) -> dict[str, int]:
    referenced = {Path(path) for path in metadata.list_all_file_group_paths()}
    detected_live = 0
    deleted_staging = 0
    for path in layout.iter_live_data_files():
        if path not in referenced:
            detected_live += 1
    for path in layout.iter_staging_files():
        path.unlink(missing_ok=True)
        deleted_staging += 1
    return {
        "detected_live": detected_live,
        "deleted_staging": deleted_staging,
    }


def _recover_stale_active_queries(
    metadata: MetadataStore,
    runtime_logger: RuntimeLogger,
    *,
    command_name: str,
) -> int:
    stale_handles = metadata.prune_stale_active_queries()
    if stale_handles:
        runtime_logger.append(
            "stale_active_queries_recovered",
            {
                "command": command_name,
                "query_ids": [handle.query_id for handle in stale_handles],
                "owner_pids": [handle.owner_pid for handle in stale_handles],
            },
        )
    return len(stale_handles)


def _build_derived_data_service(
    *,
    config: AppConfig,
    metadata: MetadataStore,
    journal: ReleaseJournal,
    layout: StorageLayout,
) -> DerivedDataService:
    return DerivedDataService(
        config=config,
        metadata_store=metadata,
        journal=journal,
        layout=layout,
        writer=StructuredParquetWriter(config.paths.staging),
    )


def _build_export_service(
    *,
    metadata: MetadataStore,
    layout: StorageLayout,
    runtime_logger: RuntimeLogger,
) -> ExportService:
    return ExportService(
        metadata_store=metadata,
        layout=layout,
        runtime_logger=runtime_logger,
    )


def _build_job_service(
    *,
    metadata: MetadataStore,
    journal: ReleaseJournal,
    layout: StorageLayout,
    runtime_logger: RuntimeLogger,
    export_service: ExportService,
    callback_timeout_seconds: float,
    callback_retry_seconds: float,
    callback_max_attempts: int,
) -> JobService:
    return JobService(
        metadata_store=metadata,
        journal=journal,
        layout=layout,
        runtime_logger=runtime_logger,
        export_service=export_service,
        callback_timeout_seconds=callback_timeout_seconds,
        callback_retry_seconds=callback_retry_seconds,
        callback_max_attempts=callback_max_attempts,
    )


@dataclass(slots=True, frozen=True)
class FrameIngestResult:
    frames_total: int
    frames_accepted: int
    frames_rejected: int
    boxes_accepted: int


@dataclass(slots=True, frozen=True)
class PublishRunResult:
    publish_now_ms: int
    published_windows: int
    published_rows: int
    release_ids: tuple[str, ...]


@dataclass(slots=True)
class ApplicationRuntime:
    validator: ValidatorService
    ingest_service: IngestService
    publish_service: PublishService
    maintenance_service: MaintenanceService
    export_service: ExportService
    job_service: JobService
    partition_states: dict[PartitionKey, PartitionStateRecord]
    recovery: dict[str, int]


@dataclass(slots=True, frozen=True)
class ApplicationSettings:
    root: str | Path = "storage"
    schema_version_id: str | None = None
    publish_interval_seconds: float = 1.0
    run_maintenance: bool = True
    maintenance_interval_seconds: float = 60.0
    enable_cyber_ingest: bool = False
    cyber_device_ids: tuple[int, ...] = (1, 2, 3, 4)
    cyber_channel_template: str = "omnisense/test/{device_id}/boxes"
    cyber_node_name: str = "dbserver_cyber_ingest"
    cyber_queue_maxsize: int = 1024
    run_job_worker: bool = True
    max_jobs_per_cycle: int = 1
    run_export_worker: bool = False
    max_export_jobs_per_cycle: int = 1
    callback_timeout_seconds: float = 5.0
    callback_retry_seconds: float = 30.0
    callback_max_attempts: int = 5
    low_watermark_bytes: int | None = None
    high_watermark_bytes: int | None = None
    query_wait_timeout_seconds: float = 0.0
    query_poll_interval_seconds: float = 0.5
    ingest_note: str = "api ingest batch"
    seal_note: str = "api maintenance seal ready hours"
    retention_note: str = "api maintenance automatic retention cleanup"
    mode: str = "api"


@dataclass(slots=True, frozen=True)
class ApiIngestResult:
    accepted_at_ms: int
    frames_total: int
    frames_accepted: int
    frames_rejected: int
    boxes_accepted: int
    buffered_windows: int
    buffered_boxes: int

    @property
    def accepted(self) -> bool:
        return self.frames_total > 0 and self.frames_rejected == 0


def _load_partition_state_cache(
    metadata: MetadataStore,
) -> dict[PartitionKey, PartitionStateRecord]:
    return {record.partition: record for record in metadata.list_partition_states()}


def _prime_validator_from_partition_states(
    validator: ValidatorService,
    partition_states: dict[PartitionKey, PartitionStateRecord],
) -> None:
    for record in partition_states.values():
        validator.prime_device_state(
            record.partition.device_id,
            max_event_time_ms=record.last_event_time_ms,
        )


def _terminal_partition_reason(state: PartitionState | None) -> str | None:
    if state is PartitionState.SEALING:
        return "partition is currently sealing"
    if state is PartitionState.SEALED:
        return "partition is already sealed"
    if state is PartitionState.DELETING:
        return "partition is currently deleting"
    if state is PartitionState.DELETED:
        return "partition is already deleted"
    return None


def _stage_partition_open_state(
    partition_states: dict[PartitionKey, PartitionStateRecord],
    dirty_states: dict[PartitionKey, PartitionStateRecord],
    partition: PartitionKey,
    event_time_ms: int,
) -> None:
    existing = partition_states.get(partition)
    if existing is None:
        updated = PartitionStateRecord(
            partition=partition,
            state=PartitionState.OPEN,
            last_event_time_ms=event_time_ms,
            watermark_ms=None,
            last_publish_release_id=None,
            last_seal_release_id=None,
        )
    else:
        updated = PartitionStateRecord(
            partition=partition,
            state=(
                PartitionState.PUBLISHABLE
                if existing.state is PartitionState.PUBLISHABLE
                else PartitionState.OPEN
            ),
            last_event_time_ms=(
                event_time_ms
                if existing.last_event_time_ms is None
                else max(existing.last_event_time_ms, event_time_ms)
            ),
            watermark_ms=existing.watermark_ms,
            last_publish_release_id=existing.last_publish_release_id,
            last_seal_release_id=existing.last_seal_release_id,
        )
    partition_states[partition] = updated
    dirty_states[partition] = updated


def _flush_partition_states(
    metadata: MetadataStore,
    dirty_states: dict[PartitionKey, PartitionStateRecord],
) -> None:
    for partition in sorted(
        dirty_states,
        key=lambda key: (key.date_utc, key.hour_utc, key.device_id),
    ):
        record = dirty_states[partition]
        metadata.upsert_partition_state(
            record.partition,
            record.state,
            last_event_time_ms=record.last_event_time_ms,
            watermark_ms=record.watermark_ms,
            last_publish_release_id=record.last_publish_release_id,
            last_seal_release_id=record.last_seal_release_id,
        )


def _ingest_frames(
    *,
    frames: Iterable[FrameBatch],
    validator: ValidatorService,
    ingest_service: IngestService,
    partition_states: dict[PartitionKey, PartitionStateRecord],
    metadata: MetadataStore,
    config: AppConfig,
    now_ms: int | None,
) -> FrameIngestResult:
    frames_total = 0
    frames_accepted = 0
    frames_rejected = 0
    boxes_accepted = 0
    dirty_states: dict[PartitionKey, PartitionStateRecord] = {}

    for frame in frames:
        frames_total += 1
        partition = PartitionKey.from_event_time(frame.device_id, frame.event_time_ms)
        try:
            existing_state = partition_states.get(partition)
            rejection_reason = _terminal_partition_reason(
                None if existing_state is None else existing_state.state
            )
            if rejection_reason is not None:
                raise ValidationError(rejection_reason)
            validator.validate_frame(frame, now_ms=now_ms)
            ingest_service.ingest_frame(frame)
            _stage_partition_open_state(
                partition_states,
                dirty_states,
                partition,
                frame.event_time_ms,
            )
        except ValidationError as exc:
            frames_rejected += 1
            _append_quarantine_record(
                config,
                frame_summary={
                    "device_id": frame.device_id,
                    "frame_id": frame.frame_id,
                    "event_time_ms": frame.event_time_ms,
                    "box_count": frame.box_count,
                },
                reason=str(exc),
            )
            continue
        frames_accepted += 1
        boxes_accepted += frame.box_count

    _flush_partition_states(metadata, dirty_states)
    return FrameIngestResult(
        frames_total=frames_total,
        frames_accepted=frames_accepted,
        frames_rejected=frames_rejected,
        boxes_accepted=boxes_accepted,
    )


def _resolve_publish_now_ms(config: AppConfig, ingest_service: IngestService, requested: int | None) -> int:
    if requested is not None:
        return requested
    max_window_end_ms = ingest_service.max_buffered_window_end_ms()
    if max_window_end_ms is None:
        return _now_ms()
    return max_window_end_ms + config.runtime.late_arrival_window_seconds * 1000 + 1


def _publish_ready_windows(
    *,
    config: AppConfig,
    ingest_service: IngestService,
    publish_service: PublishService,
    schema_version_id: str,
    note: str,
    now_ms: int | None,
    force_flush: bool,
) -> PublishRunResult:
    publish_now_ms = (
        _resolve_publish_now_ms(config, ingest_service, now_ms)
        if force_flush
        else (_now_ms() if now_ms is None else now_ms)
    )
    plans = ingest_service.build_publish_plans(now_ms=publish_now_ms)
    published_windows = 0
    published_rows = 0
    release_ids: list[str] = []
    for plan in plans:
        release = publish_service.publish_box_info_plan(
            plan,
            schema_version_id=schema_version_id,
            note=note,
        )
        release_ids.append(release.release_id)
        published_windows += 1
        published_rows += len(plan.records)
        ingest_service.mark_window_published(plan.window)
    return PublishRunResult(
        publish_now_ms=publish_now_ms,
        published_windows=published_windows,
        published_rows=published_rows,
        release_ids=tuple(release_ids),
    )


def _run_maintenance_cycle(
    *,
    runtime: ApplicationRuntime,
    metadata: MetadataStore,
    schema_version_id: str,
    now_ms: int | None,
    low_watermark_bytes: int | None,
    high_watermark_bytes: int | None,
    query_wait_timeout_seconds: float,
    query_poll_interval_seconds: float,
    seal_note: str,
    retention_note: str,
) -> MaintenanceRunResult:
    result = runtime.maintenance_service.run_cycle(
        schema_version_id=schema_version_id,
        now_ms=now_ms,
        low_watermark_bytes=low_watermark_bytes,
        high_watermark_bytes=high_watermark_bytes,
        query_wait_timeout_ms=int(query_wait_timeout_seconds * 1000),
        query_poll_interval_ms=int(query_poll_interval_seconds * 1000),
        seal_note=seal_note,
        retention_note=retention_note,
    )
    runtime.partition_states = _load_partition_state_cache(metadata)
    return result


class DbserverApplication:
    def __init__(self, settings: ApplicationSettings) -> None:
        self.settings = settings
        self.config = build_config(settings.root)
        self.layout = StorageLayout(self.config.paths)
        self.metadata = MetadataStore(self.config.paths.metadata_db)
        self.runtime_logger = RuntimeLogger(self.config.paths.runtime_log_path)
        self._lock = RLock()
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._started = False
        self._background_error: str | None = None
        self._cyber_subscriber: CyberBoxesSubscriber | None = None
        self.schema_version_id: str | None = None
        self.runtime: ApplicationRuntime | None = None
        self.runtime_status: dict[str, object] = {
            "mode": settings.mode,
            "status": "starting",
        }

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self.config.paths.ensure()
            self.metadata.init_schema()
            self.schema_version_id = _resolve_schema_version_id(self.metadata, self.settings.schema_version_id)
            _ensure_schema_version(self.metadata, self.schema_version_id)
            self.runtime = self._build_runtime()
            recovered_stale_queries = _recover_stale_active_queries(
                self.metadata,
                self.runtime_logger,
                command_name="api_server",
            )
            recovered_jobs = self.runtime.job_service.recover_running_jobs()
            started_at_ms = _now_ms()
            self.runtime_status = {
                "mode": self.settings.mode,
                "schema_version_id": self.schema_version_id,
                "service_started_at_ms": started_at_ms,
                "publish_interval_seconds": self.settings.publish_interval_seconds,
                "maintenance_enabled": self.settings.run_maintenance,
                "maintenance_interval_seconds": self.settings.maintenance_interval_seconds,
                "cyber_ingest_enabled": self.settings.enable_cyber_ingest,
                "cyber_ingest_channels": [
                    self.settings.cyber_channel_template.format(device_id=device_id, num=device_id)
                    for device_id in self.settings.cyber_device_ids
                ],
                "job_worker_enabled": self.settings.run_job_worker,
                "max_jobs_per_cycle": self.settings.max_jobs_per_cycle,
                "export_worker_enabled": self.settings.run_export_worker,
                "buffered_windows": self.runtime.ingest_service.buffered_window_count(),
                "buffered_boxes": self.runtime.ingest_service.buffered_box_count(),
                "frames_total": 0,
                "frames_accepted": 0,
                "frames_rejected": 0,
                "boxes_accepted": 0,
                "published_windows_last_cycle": 0,
                "published_rows_last_cycle": 0,
                "last_publish_now_ms": 0,
                "recovered_stale_queries": recovered_stale_queries,
                "recovered_running_jobs": recovered_jobs,
                "detected_live_orphans": self.runtime.recovery["detected_live"],
                "recovered_staging_orphans": self.runtime.recovery["deleted_staging"],
                "background_error": None,
                "stop_requested": False,
                "stop_reason": None,
            }
            self.runtime_logger.append(
                "api_service_started",
                {
                    "schema_version_id": self.schema_version_id,
                    "root": str(self.config.paths.root),
                    "publish_interval_seconds": self.settings.publish_interval_seconds,
                    "maintenance_enabled": self.settings.run_maintenance,
                    "maintenance_interval_seconds": self.settings.maintenance_interval_seconds,
                    "job_worker_enabled": self.settings.run_job_worker,
                    "max_jobs_per_cycle": self.settings.max_jobs_per_cycle,
                    "export_worker_enabled": self.settings.run_export_worker,
                    "recovered_running_jobs": recovered_jobs,
                    "detected_live_orphans": self.runtime.recovery["detected_live"],
                    "recovered_staging_orphans": self.runtime.recovery["deleted_staging"],
                    "recovered_stale_queries": recovered_stale_queries,
                },
                ts_ms=started_at_ms,
            )
            self._stop_event.clear()
            self._background_error = None
            self._thread = Thread(target=self._run_background_loop, daemon=True, name="dbserver-api-loop")
            self._thread.start()
            self._started = True
            if self.settings.enable_cyber_ingest:
                try:
                    self._start_cyber_ingest()
                except Exception:
                    self.stop()
                    raise

    def stop(self) -> None:
        cyber_subscriber = self._cyber_subscriber
        if cyber_subscriber is not None:
            cyber_subscriber.stop()
            self._cyber_subscriber = None
        thread = self._thread
        self._stop_event.set()
        if thread is not None:
            thread.join(timeout=max(self.settings.publish_interval_seconds * 4, 2.0))
        with self._lock:
            if not self._started:
                return
            self.runtime_status["stop_requested"] = True
            self.runtime_status["stop_reason"] = "shutdown"
            self.runtime_logger.append(
                "api_service_stopped",
                {
                    "schema_version_id": self.schema_version_id,
                    "background_error": self._background_error,
                    "frames_total": self.runtime_status.get("frames_total", 0),
                    "frames_accepted": self.runtime_status.get("frames_accepted", 0),
                    "frames_rejected": self.runtime_status.get("frames_rejected", 0),
                    "boxes_accepted": self.runtime_status.get("boxes_accepted", 0),
                },
            )
            self._started = False
            self._thread = None

    def ingest_frames(self, frames: Iterable[FrameBatch], *, source: str = "rest", now_ms: int | None = None) -> ApiIngestResult:
        materialized_frames = tuple(frames)
        if not materialized_frames:
            raise ValueError("frames payload must not be empty")
        with self._lock:
            runtime = self._require_runtime()
            frame_result = _ingest_frames(
                frames=materialized_frames,
                validator=runtime.validator,
                ingest_service=runtime.ingest_service,
                partition_states=runtime.partition_states,
                metadata=self.metadata,
                config=self.config,
                now_ms=now_ms,
            )
            self.runtime_status["buffered_windows"] = runtime.ingest_service.buffered_window_count()
            self.runtime_status["buffered_boxes"] = runtime.ingest_service.buffered_box_count()
            self.runtime_status["frames_total"] = int(self.runtime_status.get("frames_total", 0)) + frame_result.frames_total
            self.runtime_status["frames_accepted"] = (
                int(self.runtime_status.get("frames_accepted", 0)) + frame_result.frames_accepted
            )
            self.runtime_status["frames_rejected"] = (
                int(self.runtime_status.get("frames_rejected", 0)) + frame_result.frames_rejected
            )
            self.runtime_status["boxes_accepted"] = (
                int(self.runtime_status.get("boxes_accepted", 0)) + frame_result.boxes_accepted
            )
            accepted_at_ms = _now_ms()
            self.runtime_logger.append(
                "api_ingest_completed",
                {
                    "source": source,
                    "frames_total": frame_result.frames_total,
                    "frames_accepted": frame_result.frames_accepted,
                    "frames_rejected": frame_result.frames_rejected,
                    "boxes_accepted": frame_result.boxes_accepted,
                    "buffered_windows": runtime.ingest_service.buffered_window_count(),
                    "buffered_boxes": runtime.ingest_service.buffered_box_count(),
                },
                ts_ms=accepted_at_ms,
            )
            return ApiIngestResult(
                accepted_at_ms=accepted_at_ms,
                frames_total=frame_result.frames_total,
                frames_accepted=frame_result.frames_accepted,
                frames_rejected=frame_result.frames_rejected,
                boxes_accepted=frame_result.boxes_accepted,
                buffered_windows=runtime.ingest_service.buffered_window_count(),
                buffered_boxes=runtime.ingest_service.buffered_box_count(),
            )

    def status_snapshot(self) -> dict[str, object]:
        with self._lock:
            runtime = self._require_runtime()
            self.runtime_status["buffered_windows"] = runtime.ingest_service.buffered_window_count()
            self.runtime_status["buffered_boxes"] = runtime.ingest_service.buffered_box_count()
            if self._cyber_subscriber is not None:
                cyber_snapshot = self._cyber_subscriber.snapshot().to_dict()
                self.runtime_status["cyber_ingest"] = cyber_snapshot
                self.runtime_status["background_error"] = cyber_snapshot.get("last_error")
            snapshot = collect_status_snapshot(
                self.config,
                self.metadata,
                runtime=self.runtime_status,
            )
            if self._background_error is not None:
                snapshot["status"] = "degraded"
                snapshot["background_error"] = self._background_error
            return snapshot

    def metrics_text(self) -> str:
        return render_metrics_text(self.status_snapshot())

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
        with self._lock:
            runtime = self._require_runtime()
            return runtime.job_service.submit_export_job(
                export_kind=export_kind,
                device_id=device_id,
                start_ms=start_ms,
                end_ms=end_ms,
                trace_id=trace_id,
                lane_id=lane_id,
                obj_type=obj_type,
                request_id=request_id,
                callback=callback,
            )

    def submit_delete_job(
        self,
        *,
        before_ms: int,
        request_id: str | None = None,
        query_wait_timeout_seconds: float = 0.0,
        query_poll_interval_seconds: float = 0.5,
        callback: CallbackSpec | None = None,
    ) -> JobSubmitResult:
        with self._lock:
            runtime = self._require_runtime()
            return runtime.job_service.submit_delete_job(
                before_ms=before_ms,
                request_id=request_id,
                query_wait_timeout_seconds=query_wait_timeout_seconds,
                query_poll_interval_seconds=query_poll_interval_seconds,
                callback=callback,
            )

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._lock:
            runtime = self._require_runtime()
            return runtime.job_service.get_job(job_id)

    def list_jobs(self) -> list[JobRecord]:
        with self._lock:
            runtime = self._require_runtime()
            return runtime.job_service.list_jobs()

    def health_payload(self) -> tuple[dict[str, object], int]:
        if not self._started:
            return {
                "service": "dbserver",
                "status": "starting",
                "generated_at_ms": _now_ms(),
            }, 503
        cyber_error = None
        if self._cyber_subscriber is not None:
            cyber_error = self._cyber_subscriber.snapshot().last_error
        if self._background_error is not None:
            return {
                "service": "dbserver",
                "status": "degraded",
                "generated_at_ms": _now_ms(),
                "background_error": self._background_error,
            }, 503
        if cyber_error is not None:
            return {
                "service": "dbserver",
                "status": "degraded",
                "generated_at_ms": _now_ms(),
                "background_error": cyber_error,
            }, 503
        return {
            "service": "dbserver",
            "status": "ok",
            "generated_at_ms": _now_ms(),
        }, 200

    def _start_cyber_ingest(self) -> None:
        subscriber = CyberBoxesSubscriber(
            device_ids=self.settings.cyber_device_ids,
            channel_template=self.settings.cyber_channel_template,
            node_name=self.settings.cyber_node_name,
            queue_maxsize=self.settings.cyber_queue_maxsize,
        )
        subscriber.start(
            lambda frame, channel: self.ingest_frames((frame,), source=channel),
        )
        self._cyber_subscriber = subscriber
        self.runtime_logger.append(
            "cyber_ingest_started",
            {
                "node_name": self.settings.cyber_node_name,
                "device_ids": list(self.settings.cyber_device_ids),
                "channel_template": self.settings.cyber_channel_template,
                "queue_maxsize": self.settings.cyber_queue_maxsize,
            },
        )

    def _build_runtime(self) -> ApplicationRuntime:
        journal = ReleaseJournal(self.config.paths.journal_dir)
        derived_data_service = _build_derived_data_service(
            config=self.config,
            metadata=self.metadata,
            journal=journal,
            layout=self.layout,
        )
        validator = ValidatorService(
            late_arrival_window_ms=self.config.runtime.late_arrival_window_seconds * 1000,
            max_devices=self.config.runtime.max_devices,
        )
        ingest_service = IngestService(self.config, self.layout)
        publish_service = PublishService(
            metadata_store=self.metadata,
            journal=journal,
            box_info_writer=BoxInfoParquetWriter(self.layout.staging_root(DatasetKind.BOX_INFO)),
            derived_data_service=derived_data_service,
            layout=self.layout,
        )
        seal_service = SealService(
            metadata_store=self.metadata,
            journal=journal,
            box_info_writer=BoxInfoParquetWriter(self.layout.staging_root(DatasetKind.BOX_INFO)),
            derived_data_service=derived_data_service,
            layout=self.layout,
            late_arrival_window_seconds=self.config.runtime.late_arrival_window_seconds,
        )
        retention_service = RetentionService(self.config, self.metadata, journal)
        maintenance_service = MaintenanceService(seal_service, derived_data_service, retention_service)
        export_service = _build_export_service(
            metadata=self.metadata,
            layout=self.layout,
            runtime_logger=self.runtime_logger,
        )
        job_service = _build_job_service(
            metadata=self.metadata,
            journal=journal,
            layout=self.layout,
            runtime_logger=self.runtime_logger,
            export_service=export_service,
            callback_timeout_seconds=self.settings.callback_timeout_seconds,
            callback_retry_seconds=self.settings.callback_retry_seconds,
            callback_max_attempts=self.settings.callback_max_attempts,
        )
        recovery = _recover_orphan_files(self.metadata, self.layout)
        partition_states = _load_partition_state_cache(self.metadata)
        _prime_validator_from_partition_states(validator, partition_states)
        return ApplicationRuntime(
            validator=validator,
            ingest_service=ingest_service,
            publish_service=publish_service,
            maintenance_service=maintenance_service,
            export_service=export_service,
            job_service=job_service,
            partition_states=partition_states,
            recovery=recovery,
        )

    def _run_background_loop(self) -> None:
        next_maintenance_at = monotonic()
        try:
            while not self._stop_event.is_set():
                publish_result: PublishRunResult | None = None
                maintenance_result: MaintenanceRunResult | None = None
                job_result: JobQueueResult | None = None
                export_result: ExportQueueResult | None = None
                with self._lock:
                    runtime = self._require_runtime()
                    publish_result = _publish_ready_windows(
                        config=self.config,
                        ingest_service=runtime.ingest_service,
                        publish_service=runtime.publish_service,
                        schema_version_id=self._require_schema_version_id(),
                        note=self.settings.ingest_note,
                        now_ms=None,
                        force_flush=False,
                    )
                    self.runtime_status["buffered_windows"] = runtime.ingest_service.buffered_window_count()
                    self.runtime_status["buffered_boxes"] = runtime.ingest_service.buffered_box_count()
                    self.runtime_status["published_windows_last_cycle"] = publish_result.published_windows
                    self.runtime_status["published_rows_last_cycle"] = publish_result.published_rows
                    self.runtime_status["last_publish_now_ms"] = publish_result.publish_now_ms

                    maintenance_due = self.settings.run_maintenance and monotonic() >= next_maintenance_at
                    if maintenance_due:
                        maintenance_result = _run_maintenance_cycle(
                            runtime=runtime,
                            metadata=self.metadata,
                            schema_version_id=self._require_schema_version_id(),
                            now_ms=None,
                            low_watermark_bytes=self.settings.low_watermark_bytes,
                            high_watermark_bytes=self.settings.high_watermark_bytes,
                            query_wait_timeout_seconds=self.settings.query_wait_timeout_seconds,
                            query_poll_interval_seconds=self.settings.query_poll_interval_seconds,
                            seal_note=self.settings.seal_note,
                            retention_note=self.settings.retention_note,
                        )
                        next_maintenance_at = monotonic() + max(
                            self.settings.maintenance_interval_seconds,
                            0.1,
                        )
                        self.runtime_status["last_maintenance"] = {
                            "seal_triggered": maintenance_result.seal_result.triggered,
                            "seal_reason": maintenance_result.seal_result.reason,
                            "trace_cooling_triggered": (
                                maintenance_result.trace_index_cooling_result.triggered
                            ),
                            "trace_cooling_reason": (
                                maintenance_result.trace_index_cooling_result.reason
                            ),
                            "retention_triggered": (
                                None
                                if maintenance_result.retention_result is None
                                else maintenance_result.retention_result.triggered
                            ),
                            "retention_reason": (
                                None
                                if maintenance_result.retention_result is None
                                else maintenance_result.retention_result.reason
                            ),
                        }
                    if self.settings.run_job_worker:
                        job_result = runtime.job_service.process_pending_jobs(
                            max_jobs=self.settings.max_jobs_per_cycle,
                        )
                        self.runtime_status["last_job_queue_run"] = {
                            "processed_jobs": job_result.processed_jobs,
                            "succeeded_jobs": job_result.succeeded_jobs,
                            "failed_jobs": job_result.failed_jobs,
                            "callback_delivered_jobs": job_result.callback_delivered_jobs,
                            "job_ids": list(job_result.job_ids),
                        }
                    if self.settings.run_export_worker:
                        export_result = runtime.export_service.process_pending_jobs(
                            max_jobs=self.settings.max_export_jobs_per_cycle,
                        )
                        self.runtime_status["last_export_queue_run"] = {
                            "processed_jobs": export_result.processed_jobs,
                            "finished_jobs": export_result.finished_jobs,
                            "failed_jobs": export_result.failed_jobs,
                            "export_ids": list(export_result.export_ids),
                        }

                    if (
                        publish_result.published_windows > 0
                        or maintenance_result is not None
                        or (export_result is not None and export_result.processed_jobs > 0)
                    ):
                        self.runtime_logger.append(
                            "api_background_cycle_completed",
                            {
                                "published_windows": publish_result.published_windows,
                                "published_rows": publish_result.published_rows,
                                "release_ids": list(publish_result.release_ids),
                                "buffered_windows": runtime.ingest_service.buffered_window_count(),
                                "buffered_boxes": runtime.ingest_service.buffered_box_count(),
                                "maintenance_ran": maintenance_result is not None,
                                "trace_cooling_triggered": (
                                    None
                                    if maintenance_result is None
                                    else maintenance_result.trace_index_cooling_result.triggered
                                ),
                                "export_processed_jobs": (
                                    None if export_result is None else export_result.processed_jobs
                                ),
                                "export_finished_jobs": (
                                    None if export_result is None else export_result.finished_jobs
                                ),
                            },
                        )
                self._stop_event.wait(max(self.settings.publish_interval_seconds, 0.05))
        except Exception as exc:
            with self._lock:
                self._background_error = str(exc)
                self.runtime_status["background_error"] = self._background_error
                self.runtime_logger.append(
                    "api_background_loop_failed",
                    {
                        "error": self._background_error,
                    },
                )
            self._stop_event.set()

    def _require_runtime(self) -> ApplicationRuntime:
        if self.runtime is None:
            raise RuntimeError("application runtime is not initialized")
        return self.runtime

    def _require_schema_version_id(self) -> str:
        if self.schema_version_id is None:
            raise RuntimeError("schema_version_id is not initialized")
        return self.schema_version_id
