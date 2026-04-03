from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep, time_ns
from uuid import uuid4

from dbserver.adapters.jsonl import JsonlFrameSource
from dbserver.config import AppConfig, build_config
from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import (
    DatasetKind,
    PartitionKey,
    PartitionState,
    PartitionStateRecord,
    QueryHandle,
    QueryKind,
    TimeRange,
)
from dbserver.observability import (
    ObservabilityState,
    StatusServer,
    collect_status_snapshot,
    render_metrics_text,
)
from dbserver.runtime_logging import RuntimeLogger
from dbserver.services.derived_data_service import DerivedDataService
from dbserver.services.export_service import ExportQueueResult, ExportService
from dbserver.services.ingest_service import IngestService
from dbserver.services.maintenance_service import MaintenanceRunResult, MaintenanceService
from dbserver.services.ops_service import DeleteConflictError, OpsService
from dbserver.services.publish_service import PublishService
from dbserver.services.query_service import QueryService
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


def _resolve_publish_now_ms(config: AppConfig, ingest_service: IngestService, requested: int | None) -> int:
    if requested is not None:
        return requested
    max_window_end_ms = ingest_service.max_buffered_window_end_ms()
    if max_window_end_ms is None:
        return _now_ms()
    return max_window_end_ms + config.runtime.late_arrival_window_seconds * 1000 + 1


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


def _directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            total += entry.stat().st_size
    return total


def _format_bytes(byte_count: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(byte_count)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{byte_count}B"


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
class IngestCommandRuntime:
    validator: ValidatorService
    ingest_service: IngestService
    publish_service: PublishService
    maintenance_service: MaintenanceService
    export_service: ExportService
    partition_states: dict[PartitionKey, PartitionStateRecord]
    recovery: dict[str, int]


@dataclass(slots=True, frozen=True)
class PendingFileRunResult:
    processed_files: int
    failed_files: int
    frames_total: int
    frames_accepted: int
    frames_rejected: int
    boxes_accepted: int


@dataclass(slots=True)
class ServiceStopController:
    stop_requested: bool = False
    stop_reason: str | None = None

    def request_stop(self, reason: str) -> None:
        if self.stop_requested:
            return
        self.stop_requested = True
        self.stop_reason = reason


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
    frames,
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


def _build_ingest_command_runtime(
    *,
    config: AppConfig,
    layout: StorageLayout,
    metadata: MetadataStore,
    runtime_logger: RuntimeLogger,
) -> IngestCommandRuntime:
    journal = ReleaseJournal(config.paths.journal_dir)
    derived_data_service = _build_derived_data_service(
        config=config,
        metadata=metadata,
        journal=journal,
        layout=layout,
    )
    validator = ValidatorService(
        late_arrival_window_ms=config.runtime.late_arrival_window_seconds * 1000,
        max_devices=config.runtime.max_devices,
    )
    ingest_service = IngestService(config, layout)
    publish_service = PublishService(
        metadata_store=metadata,
        journal=journal,
        box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
        derived_data_service=derived_data_service,
        layout=layout,
    )
    seal_service = SealService(
        metadata_store=metadata,
        journal=journal,
        box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
        derived_data_service=derived_data_service,
        layout=layout,
        late_arrival_window_seconds=config.runtime.late_arrival_window_seconds,
    )
    retention_service = RetentionService(config, metadata, journal)
    maintenance_service = MaintenanceService(seal_service, derived_data_service, retention_service)
    export_service = _build_export_service(
        metadata=metadata,
        layout=layout,
        runtime_logger=runtime_logger,
    )
    recovery = _recover_orphan_files(metadata, layout)
    partition_states = _load_partition_state_cache(metadata)
    _prime_validator_from_partition_states(validator, partition_states)
    return IngestCommandRuntime(
        validator=validator,
        ingest_service=ingest_service,
        publish_service=publish_service,
        maintenance_service=maintenance_service,
        export_service=export_service,
        partition_states=partition_states,
        recovery=recovery,
    )


def _move_input_file(source_path: Path, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / source_path.name
    if target_path.exists():
        target_path = target_dir / f"{source_path.stem}-{_now_ms()}{source_path.suffix}"
    shutil.move(str(source_path), str(target_path))
    return target_path


def _execute_sql(sql: str) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    import duckdb  # type: ignore

    connection = duckdb.connect()
    try:
        cursor = connection.execute(sql)
        columns = tuple(item[0] for item in cursor.description)
        rows = cursor.fetchall()
    finally:
        connection.close()
    return columns, rows


def _optional_time_range(start_ms: int | None, end_ms: int | None) -> TimeRange | None:
    if start_ms is None and end_ms is None:
        return None
    if start_ms is None or end_ms is None:
        raise ValueError("start-ms and end-ms must be provided together")
    return TimeRange(start_ms=start_ms, end_ms=end_ms)


def _process_pending_input_files(
    *,
    input_dir: Path,
    glob_pattern: str,
    archive_dir: Path,
    failed_dir: Path,
    max_files_per_cycle: int | None,
    runtime: IngestCommandRuntime,
    metadata: MetadataStore,
    config: AppConfig,
    runtime_logger: RuntimeLogger,
    now_ms: int | None,
    cycle_count: int,
    processed_event_type: str,
    failed_event_type: str,
    stop_controller: ServiceStopController | None = None,
) -> PendingFileRunResult:
    pending_files = sorted(path for path in input_dir.glob(glob_pattern) if path.is_file())
    if max_files_per_cycle is not None:
        pending_files = pending_files[:max_files_per_cycle]

    frames_total = 0
    frames_accepted = 0
    frames_rejected = 0
    boxes_accepted = 0
    processed_files = 0
    failed_files = 0

    for input_path in pending_files:
        if stop_controller is not None and stop_controller.stop_requested:
            break
        source = JsonlFrameSource(input_path)
        try:
            frame_result = _ingest_frames(
                frames=source.iter_frames(),
                validator=runtime.validator,
                ingest_service=runtime.ingest_service,
                partition_states=runtime.partition_states,
                metadata=metadata,
                config=config,
                now_ms=now_ms,
            )
        except ValueError as exc:
            failed_path = _move_input_file(input_path, failed_dir)
            failed_files += 1
            runtime_logger.append(
                failed_event_type,
                {
                    "input": str(input_path),
                    "failed_path": str(failed_path),
                    "error": str(exc),
                    "cycle": cycle_count,
                },
            )
            continue

        archived_path = _move_input_file(input_path, archive_dir)
        processed_files += 1
        frames_total += frame_result.frames_total
        frames_accepted += frame_result.frames_accepted
        frames_rejected += frame_result.frames_rejected
        boxes_accepted += frame_result.boxes_accepted
        runtime_logger.append(
            processed_event_type,
            {
                "input": str(input_path),
                "archived_path": str(archived_path),
                "cycle": cycle_count,
                "frames_total": frame_result.frames_total,
                "frames_accepted": frame_result.frames_accepted,
                "frames_rejected": frame_result.frames_rejected,
                "boxes_accepted": frame_result.boxes_accepted,
            },
        )

    return PendingFileRunResult(
        processed_files=processed_files,
        failed_files=failed_files,
        frames_total=frames_total,
        frames_accepted=frames_accepted,
        frames_rejected=frames_rejected,
        boxes_accepted=boxes_accepted,
    )


def _run_maintenance_cycle(
    *,
    runtime: IngestCommandRuntime,
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


def _sleep_with_stop(
    duration_seconds: float,
    stop_controller: ServiceStopController,
    *,
    step_seconds: float = 0.2,
) -> None:
    deadline = monotonic() + max(duration_seconds, 0.0)
    while not stop_controller.stop_requested:
        remaining = deadline - monotonic()
        if remaining <= 0:
            return
        sleep(min(remaining, step_seconds))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DBServer skeleton CLI")
    parser.add_argument("--root", default="storage", help="Storage root path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("show-layout", help="Print key storage paths")

    init_parser = subparsers.add_parser("init-metadata", help="Initialize metadata.db schema")
    init_parser.add_argument("--schema-version-id", default="box_info_v1")

    subparsers.add_parser("current-release", help="Show current release id")
    subparsers.add_parser("status-json", help="Print a JSON status snapshot")
    subparsers.add_parser("metrics-text", help="Print Prometheus-style metrics text")
    subparsers.add_parser("list-releases", help="List known releases in reverse chronological order")
    subparsers.add_parser("list-partitions", help="List partition states in metadata")
    subparsers.add_parser("list-active-queries", help="List active query handles tracked for delete protection")
    disk_parser = subparsers.add_parser("disk-usage", help="Show directory usage by top-level area")
    disk_parser.add_argument("--low-watermark-bytes", type=int)
    disk_parser.add_argument("--high-watermark-bytes", type=int)

    ingest_parser = subparsers.add_parser("ingest-jsonl", help="Validate, buffer, and publish JSONL frames")
    ingest_parser.add_argument("--input", required=True, help="Local JSONL file path")
    ingest_parser.add_argument("--schema-version-id")
    ingest_parser.add_argument("--now-ms", type=int)
    ingest_parser.add_argument("--note", default="ingest jsonl batch")

    loop_parser = subparsers.add_parser(
        "run-ingest-loop",
        help="Continuously ingest sorted JSONL files from a directory and publish ready windows",
    )
    loop_parser.add_argument("--input-dir", required=True, help="Directory containing pending JSONL files")
    loop_parser.add_argument("--glob", default="*.jsonl", help="Glob pattern used to discover pending files")
    loop_parser.add_argument("--archive-dir", help="Directory for successfully processed files")
    loop_parser.add_argument("--failed-dir", help="Directory for files that fail parsing")
    loop_parser.add_argument("--schema-version-id")
    loop_parser.add_argument("--now-ms", type=int)
    loop_parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    loop_parser.add_argument("--max-cycles", type=int)
    loop_parser.add_argument("--max-files-per-cycle", type=int)
    loop_parser.add_argument("--note", default="ingest loop batch")
    loop_parser.add_argument("--run-maintenance", action="store_true")
    loop_parser.add_argument("--low-watermark-bytes", type=int)
    loop_parser.add_argument("--high-watermark-bytes", type=int)
    loop_parser.add_argument("--seal-note", default="loop seal ready hours")
    loop_parser.add_argument("--retention-note", default="loop automatic retention cleanup")

    serve_parser = subparsers.add_parser(
        "serve",
        help="Run a long-lived single-process ingest service over a local input directory",
    )
    serve_parser.add_argument("--input-dir", required=True, help="Directory containing pending JSONL files")
    serve_parser.add_argument("--glob", default="*.jsonl", help="Glob pattern used to discover pending JSONL files")
    serve_parser.add_argument("--archive-dir", help="Directory for successfully processed files")
    serve_parser.add_argument("--failed-dir", help="Directory for files that fail parsing")
    serve_parser.add_argument("--schema-version-id")
    serve_parser.add_argument("--now-ms", type=int)
    serve_parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    serve_parser.add_argument("--max-files-per-cycle", type=int)
    serve_parser.add_argument("--max-runtime-seconds", type=float)
    serve_parser.add_argument("--status-host", default="127.0.0.1")
    serve_parser.add_argument("--status-port", type=int)
    serve_parser.add_argument("--run-maintenance", action="store_true")
    serve_parser.add_argument("--run-export-worker", action="store_true")
    serve_parser.add_argument("--max-export-jobs-per-cycle", type=int, default=1)
    serve_parser.add_argument("--maintenance-interval-seconds", type=float, default=60.0)
    serve_parser.add_argument("--low-watermark-bytes", type=int)
    serve_parser.add_argument("--high-watermark-bytes", type=int)
    serve_parser.add_argument("--query-wait-timeout-seconds", type=float, default=0.0)
    serve_parser.add_argument("--query-poll-interval-seconds", type=float, default=0.5)
    serve_parser.add_argument("--note", default="serve ingest batch")
    serve_parser.add_argument("--seal-note", default="serve seal ready hours")
    serve_parser.add_argument("--retention-note", default="serve automatic retention cleanup")

    detail_parser = subparsers.add_parser("plan-detail-query", help="Build a detail query plan")
    detail_parser.add_argument("--device-id", required=True, type=int)
    detail_parser.add_argument("--start-ms", required=True, type=int)
    detail_parser.add_argument("--end-ms", required=True, type=int)

    trace_parser = subparsers.add_parser(
        "plan-trace-query",
        help="Build a trace query plan by scanning current box_info detail files",
    )
    trace_parser.add_argument("--device-id", required=True, type=int)
    trace_parser.add_argument("--trace-id", required=True, type=int)
    trace_parser.add_argument("--start-ms", type=int)
    trace_parser.add_argument("--end-ms", type=int)

    submit_detail_export_parser = subparsers.add_parser(
        "submit-detail-export",
        help="Create an async export job for device_id + time_range detail rows",
    )
    submit_detail_export_parser.add_argument("--device-id", required=True, type=int)
    submit_detail_export_parser.add_argument("--start-ms", required=True, type=int)
    submit_detail_export_parser.add_argument("--end-ms", required=True, type=int)

    submit_trace_export_parser = subparsers.add_parser(
        "submit-trace-export",
        help="Create an async export job for device_id + trace_id detail rows",
    )
    submit_trace_export_parser.add_argument("--device-id", required=True, type=int)
    submit_trace_export_parser.add_argument("--trace-id", required=True, type=int)
    submit_trace_export_parser.add_argument("--start-ms", type=int)
    submit_trace_export_parser.add_argument("--end-ms", type=int)

    submit_rollup_export_parser = subparsers.add_parser(
        "submit-rollup-export",
        help="Create an async export job for obj_type rollup rows",
    )
    submit_rollup_export_parser.add_argument("--device-id", required=True, type=int)
    submit_rollup_export_parser.add_argument("--start-ms", required=True, type=int)
    submit_rollup_export_parser.add_argument("--end-ms", required=True, type=int)
    submit_rollup_export_parser.add_argument("--lane-id", type=int)
    submit_rollup_export_parser.add_argument("--obj-type", type=int)

    subparsers.add_parser("list-exports", help="List async export jobs")

    run_export_queue_parser = subparsers.add_parser(
        "run-export-queue",
        help="Process pending async export jobs serially",
    )
    run_export_queue_parser.add_argument("--max-jobs", type=int)

    trace_lookup_parser = subparsers.add_parser(
        "plan-trace-lookup-query",
        help="Build a trace segment lookup query plan from trace_index datasets",
    )
    trace_lookup_parser.add_argument("--device-id", required=True, type=int)
    trace_lookup_parser.add_argument("--trace-id", required=True, type=int)
    trace_lookup_parser.add_argument("--start-ms", type=int)
    trace_lookup_parser.add_argument("--end-ms", type=int)

    rollup_parser = subparsers.add_parser(
        "plan-rollup-query",
        help="Build an obj_type rollup query plan from current rollup datasets",
    )
    rollup_parser.add_argument("--device-id", required=True, type=int)
    rollup_parser.add_argument("--start-ms", required=True, type=int)
    rollup_parser.add_argument("--end-ms", required=True, type=int)
    rollup_parser.add_argument("--lane-id", type=int)
    rollup_parser.add_argument("--obj-type", type=int)

    exec_detail_parser = subparsers.add_parser(
        "exec-detail-query",
        help="Execute a device_id + time_range detail query against the current release",
    )
    exec_detail_parser.add_argument("--device-id", required=True, type=int)
    exec_detail_parser.add_argument("--start-ms", required=True, type=int)
    exec_detail_parser.add_argument("--end-ms", required=True, type=int)
    exec_detail_parser.add_argument("--limit", type=int)
    exec_detail_parser.add_argument("--hold-seconds", type=float, default=0.0)

    exec_trace_parser = subparsers.add_parser(
        "exec-trace-query",
        help="Execute a device_id + trace_id query by scanning current box_info detail files",
    )
    exec_trace_parser.add_argument("--device-id", required=True, type=int)
    exec_trace_parser.add_argument("--trace-id", required=True, type=int)
    exec_trace_parser.add_argument("--start-ms", type=int)
    exec_trace_parser.add_argument("--end-ms", type=int)
    exec_trace_parser.add_argument("--limit", type=int)
    exec_trace_parser.add_argument("--hold-seconds", type=float, default=0.0)

    exec_trace_lookup_parser = subparsers.add_parser(
        "exec-trace-lookup-query",
        help="Execute a device_id + trace_id segment lookup against trace_index datasets",
    )
    exec_trace_lookup_parser.add_argument("--device-id", required=True, type=int)
    exec_trace_lookup_parser.add_argument("--trace-id", required=True, type=int)
    exec_trace_lookup_parser.add_argument("--start-ms", type=int)
    exec_trace_lookup_parser.add_argument("--end-ms", type=int)
    exec_trace_lookup_parser.add_argument("--limit", type=int)
    exec_trace_lookup_parser.add_argument("--hold-seconds", type=float, default=0.0)

    exec_rollup_parser = subparsers.add_parser(
        "exec-rollup-query",
        help="Execute an obj_type rollup query against the current release",
    )
    exec_rollup_parser.add_argument("--device-id", required=True, type=int)
    exec_rollup_parser.add_argument("--start-ms", required=True, type=int)
    exec_rollup_parser.add_argument("--end-ms", required=True, type=int)
    exec_rollup_parser.add_argument("--lane-id", type=int)
    exec_rollup_parser.add_argument("--obj-type", type=int)
    exec_rollup_parser.add_argument("--limit", type=int)
    exec_rollup_parser.add_argument("--hold-seconds", type=float, default=0.0)

    delete_plan_parser = subparsers.add_parser("plan-delete", help="Build a retention delete plan")
    delete_plan_parser.add_argument("--before-ms", required=True, type=int)
    delete_plan_parser.add_argument("--schema-version-id")

    delete_parser = subparsers.add_parser("delete-before", help="Delete sealed data before a UTC ms boundary")
    delete_parser.add_argument("--before-ms", required=True, type=int)
    delete_parser.add_argument("--schema-version-id")
    delete_parser.add_argument("--query-wait-timeout-seconds", type=float, default=0.0)
    delete_parser.add_argument("--query-poll-interval-seconds", type=float, default=0.5)

    rollback_parser = subparsers.add_parser(
        "rollback-release",
        help="Switch the current release pointer to a historical release snapshot",
    )
    rollback_parser.add_argument("--target-release-id", required=True)
    rollback_parser.add_argument("--retention-floor-ms", required=True, type=int)
    rollback_parser.add_argument("--schema-version-id")
    rollback_parser.add_argument("--note", default="manual rollback")

    seal_ready_parser = subparsers.add_parser(
        "seal-ready-hours",
        help="Seal all ready box_info hour/device partitions in the current release",
    )
    seal_ready_parser.add_argument("--schema-version-id")
    seal_ready_parser.add_argument("--now-ms", type=int)
    seal_ready_parser.add_argument("--note", default="seal ready hours")

    seal_hour_parser = subparsers.add_parser(
        "seal-hour",
        help="Seal a specific box_info UTC hour/device partition in the current release",
    )
    seal_hour_parser.add_argument("--date-utc", required=True)
    seal_hour_parser.add_argument("--hour-utc", required=True, type=int)
    seal_hour_parser.add_argument("--device-id", required=True, type=int)
    seal_hour_parser.add_argument("--schema-version-id")
    seal_hour_parser.add_argument("--note", default="manual seal partition")

    enforce_parser = subparsers.add_parser(
        "enforce-watermarks",
        help="Delete oldest sealed partitions until usage drops below the target watermark",
    )
    enforce_parser.add_argument("--schema-version-id")
    enforce_parser.add_argument("--low-watermark-bytes", required=True, type=int)
    enforce_parser.add_argument("--high-watermark-bytes", required=True, type=int)
    enforce_parser.add_argument("--query-wait-timeout-seconds", type=float, default=0.0)
    enforce_parser.add_argument("--query-poll-interval-seconds", type=float, default=0.5)
    enforce_parser.add_argument("--note", default="automatic retention cleanup")

    rebuild_trace_index_parser = subparsers.add_parser(
        "rebuild-trace-index",
        help="Rebuild trace_index datasets from current box_info file groups",
    )
    rebuild_trace_index_parser.add_argument("--schema-version-id")
    rebuild_trace_index_parser.add_argument("--now-ms", type=int)
    rebuild_trace_index_parser.add_argument("--note", default="manual rebuild trace_index")

    rebuild_rollup_parser = subparsers.add_parser(
        "rebuild-rollup",
        help="Rebuild obj_type rollup datasets from current box_info file groups",
    )
    rebuild_rollup_parser.add_argument("--schema-version-id")
    rebuild_rollup_parser.add_argument("--now-ms", type=int)
    rebuild_rollup_parser.add_argument("--note", default="manual rebuild rollup")

    maintenance_parser = subparsers.add_parser(
        "run-maintenance",
        help="Run seal-ready-hours first, then optional watermark-based retention cleanup",
    )
    maintenance_parser.add_argument("--schema-version-id")
    maintenance_parser.add_argument("--now-ms", type=int)
    maintenance_parser.add_argument("--low-watermark-bytes", type=int)
    maintenance_parser.add_argument("--high-watermark-bytes", type=int)
    maintenance_parser.add_argument("--query-wait-timeout-seconds", type=float, default=0.0)
    maintenance_parser.add_argument("--query-poll-interval-seconds", type=float, default=0.5)
    maintenance_parser.add_argument("--seal-note", default="maintenance seal ready hours")
    maintenance_parser.add_argument(
        "--retention-note",
        default="maintenance automatic retention cleanup",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = build_config(args.root)
    config.paths.ensure()
    layout = StorageLayout(config.paths)

    if args.command == "show-layout":
        print(f"root={config.paths.root}")
        print(f"live={config.paths.live}")
        print(f"staging={config.paths.staging}")
        print(f"metadata_db={config.paths.metadata_db}")
        print(f"journal_dir={config.paths.journal_dir}")
        print(f"exports={config.paths.exports}")
        print(f"quarantine={config.paths.quarantine}")
        return

    metadata = MetadataStore(config.paths.metadata_db)
    runtime_logger = RuntimeLogger(config.paths.runtime_log_path)
    recovered_stale_queries = 0

    try:
        if args.command != "show-layout":
            metadata.init_schema()
            recovered_stale_queries = _recover_stale_active_queries(
                metadata,
                runtime_logger,
                command_name=args.command,
            )

        if args.command == "init-metadata":
            _ensure_schema_version(metadata, args.schema_version_id)
            print(f"metadata_db={config.paths.metadata_db}")
            print(f"schema_version_id={args.schema_version_id}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "current-release":
            current_release_id = metadata.get_current_release()
            print(f"release_id={current_release_id}")
            if current_release_id is not None:
                release = metadata.get_release(current_release_id)
                if release is not None:
                    print(f"schema_version_id={release.schema_version_id}")
                    print(f"retention_floor_ms={release.retention_floor_ms}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "status-json":
            snapshot = collect_status_snapshot(
                config,
                metadata,
                runtime={"recovered_stale_queries": recovered_stale_queries},
            )
            print(json.dumps(snapshot, sort_keys=True))
            return

        if args.command == "metrics-text":
            snapshot = collect_status_snapshot(
                config,
                metadata,
                runtime={"recovered_stale_queries": recovered_stale_queries},
            )
            print(render_metrics_text(snapshot), end="")
            return

        if args.command == "list-releases":
            for release in metadata.list_releases():
                print(
                    " ".join(
                        [
                            f"release_id={release.release_id}",
                            f"parent_release_id={release.parent_release_id}",
                            f"schema_version_id={release.schema_version_id}",
                            f"created_at_ms={release.created_at_ms}",
                            f"event_type={release.event_type.value}",
                            f"retention_floor_ms={release.retention_floor_ms}",
                            f"note={json.dumps(release.note, ensure_ascii=True)}",
                        ]
                    )
                )
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "submit-detail-export":
            export_service = _build_export_service(
                metadata=metadata,
                layout=layout,
                runtime_logger=runtime_logger,
            )
            job = export_service.submit_detail_export(
                device_id=args.device_id,
                time_range=TimeRange(start_ms=args.start_ms, end_ms=args.end_ms),
            )
            print(f"export_id={job.export_id}")
            print(f"status={job.status}")
            print(f"release_id={job.release_id}")
            print(f"query_kind={job.query_kind}")
            print(f"file_count={len(job.files)}")
            return

        if args.command == "submit-trace-export":
            export_service = _build_export_service(
                metadata=metadata,
                layout=layout,
                runtime_logger=runtime_logger,
            )
            job = export_service.submit_trace_export(
                device_id=args.device_id,
                trace_id=args.trace_id,
                time_range=_optional_time_range(args.start_ms, args.end_ms),
            )
            print(f"export_id={job.export_id}")
            print(f"status={job.status}")
            print(f"release_id={job.release_id}")
            print(f"query_kind={job.query_kind}")
            print(f"file_count={len(job.files)}")
            return

        if args.command == "submit-rollup-export":
            export_service = _build_export_service(
                metadata=metadata,
                layout=layout,
                runtime_logger=runtime_logger,
            )
            job = export_service.submit_rollup_export(
                device_id=args.device_id,
                time_range=TimeRange(start_ms=args.start_ms, end_ms=args.end_ms),
                lane_id=args.lane_id,
                obj_type=args.obj_type,
            )
            print(f"export_id={job.export_id}")
            print(f"status={job.status}")
            print(f"release_id={job.release_id}")
            print(f"query_kind={job.query_kind}")
            print(f"file_count={len(job.files)}")
            return

        if args.command == "list-exports":
            export_service = _build_export_service(
                metadata=metadata,
                layout=layout,
                runtime_logger=runtime_logger,
            )
            for job in export_service.list_jobs():
                print(
                    " ".join(
                        [
                            f"export_id={job.export_id}",
                            f"status={job.status}",
                            f"release_id={job.release_id}",
                            f"query_kind={job.query_kind}",
                            f"created_at_ms={job.created_at_ms}",
                            f"row_count={job.row_count}",
                            f"output_path={job.output_path}",
                            f"error={json.dumps(job.error, ensure_ascii=True)}",
                        ]
                    )
                )
            return

        if args.command == "run-export-queue":
            export_service = _build_export_service(
                metadata=metadata,
                layout=layout,
                runtime_logger=runtime_logger,
            )
            result = export_service.process_pending_jobs(max_jobs=args.max_jobs)
            print(f"processed_jobs={result.processed_jobs}")
            print(f"finished_jobs={result.finished_jobs}")
            print(f"failed_jobs={result.failed_jobs}")
            if result.export_ids:
                print(f"export_ids={','.join(result.export_ids)}")
            return

        if args.command == "list-partitions":
            for record in metadata.list_partition_states():
                print(
                    " ".join(
                        [
                            f"date_utc={record.partition.date_utc}",
                            f"hour_utc={record.partition.hour_utc:02d}",
                            f"device_id={record.partition.device_id}",
                            f"state={record.state.value}",
                            f"last_event_time_ms={record.last_event_time_ms}",
                            f"watermark_ms={record.watermark_ms}",
                            f"last_publish_release_id={record.last_publish_release_id}",
                            f"last_seal_release_id={record.last_seal_release_id}",
                        ]
                    )
                )
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "list-active-queries":
            for handle in metadata.list_active_queries():
                print(
                    " ".join(
                        [
                            f"query_id={handle.query_id}",
                            f"release_id={handle.release_id}",
                            f"query_kind={handle.query_kind.value}",
                            f"started_at_ms={handle.started_at_ms}",
                            f"owner_pid={handle.owner_pid}",
                            "target_range_start_ms="
                            f"{None if handle.target_range is None else handle.target_range.start_ms}",
                            "target_range_end_ms="
                            f"{None if handle.target_range is None else handle.target_range.end_ms}",
                        ]
                    )
                )
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "disk-usage":
            sizes = {
                "live": _directory_size_bytes(config.paths.live),
                "staging": _directory_size_bytes(config.paths.staging),
                "metadata": _directory_size_bytes(config.paths.metadata),
                "quarantine": _directory_size_bytes(config.paths.quarantine),
                "exports": _directory_size_bytes(config.paths.exports),
            }
            total = sum(sizes.values())
            current_release_id: str | None = None
            if config.paths.metadata_db.exists():
                try:
                    current_release_id = metadata.get_current_release()
                except ModuleNotFoundError:
                    current_release_id = "unavailable_without_duckdb"
                except Exception:
                    current_release_id = "unavailable_due_to_metadata_lock"
            print(f"root={config.paths.root}")
            print(f"current_release={current_release_id}")
            print(f"total_bytes={total}")
            print(f"total_human={_format_bytes(total)}")
            low = args.low_watermark_bytes
            high = args.high_watermark_bytes
            if low is not None:
                print(f"low_watermark_bytes={low}")
                print(f"low_watermark_triggered={total >= low}")
            if high is not None:
                print(f"high_watermark_bytes={high}")
                print(f"high_watermark_cleared={total <= high}")
            for key, value in sizes.items():
                print(f"{key}_bytes={value}")
                print(f"{key}_human={_format_bytes(value)}")
            return

        if args.command == "ingest-jsonl":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)

            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            validator = ValidatorService(
                late_arrival_window_ms=config.runtime.late_arrival_window_seconds * 1000,
                max_devices=config.runtime.max_devices,
            )
            ingest_service = IngestService(config, layout)
            publish_service = PublishService(
                metadata_store=metadata,
                journal=journal,
                box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
                derived_data_service=derived_data_service,
                layout=layout,
            )
            recovery = _recover_orphan_files(metadata, layout)
            partition_states = _load_partition_state_cache(metadata)
            _prime_validator_from_partition_states(validator, partition_states)

            source = JsonlFrameSource(Path(args.input))
            frame_result = _ingest_frames(
                frames=source.iter_frames(),
                validator=validator,
                ingest_service=ingest_service,
                partition_states=partition_states,
                metadata=metadata,
                config=config,
                now_ms=args.now_ms,
            )
            publish_result = _publish_ready_windows(
                config=config,
                ingest_service=ingest_service,
                publish_service=publish_service,
                schema_version_id=schema_version_id,
                note=args.note,
                now_ms=args.now_ms,
                force_flush=True,
            )

            runtime_logger.append(
                "ingest_jsonl_completed",
                {
                    "input": str(Path(args.input)),
                    "schema_version_id": schema_version_id,
                    "frames_total": frame_result.frames_total,
                    "frames_accepted": frame_result.frames_accepted,
                    "frames_rejected": frame_result.frames_rejected,
                    "boxes_accepted": frame_result.boxes_accepted,
                    "published_windows": publish_result.published_windows,
                    "published_rows": publish_result.published_rows,
                    "buffered_windows": ingest_service.buffered_window_count(),
                    "publish_now_ms": publish_result.publish_now_ms,
                    "detected_live_orphans": recovery["detected_live"],
                    "recovered_staging_orphans": recovery["deleted_staging"],
                },
            )

            print(f"schema_version_id={schema_version_id}")
            print(f"frames_total={frame_result.frames_total}")
            print(f"frames_accepted={frame_result.frames_accepted}")
            print(f"frames_rejected={frame_result.frames_rejected}")
            print(f"boxes_accepted={frame_result.boxes_accepted}")
            print(f"publish_now_ms={publish_result.publish_now_ms}")
            print(f"published_windows={publish_result.published_windows}")
            print(f"published_rows={publish_result.published_rows}")
            print(f"buffered_windows={ingest_service.buffered_window_count()}")
            print(f"buffered_boxes={ingest_service.buffered_box_count()}")
            print(f"detected_live_orphans={recovery['detected_live']}")
            print(f"recovered_staging_orphans={recovery['deleted_staging']}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            if publish_result.release_ids:
                print(f"release_ids={','.join(publish_result.release_ids)}")
            return

        if args.command == "serve":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)

            input_dir = Path(args.input_dir)
            input_dir.mkdir(parents=True, exist_ok=True)
            archive_dir = Path(args.archive_dir) if args.archive_dir else input_dir / "processed"
            failed_dir = Path(args.failed_dir) if args.failed_dir else input_dir / "failed"
            runtime = _build_ingest_command_runtime(
                config=config,
                layout=layout,
                metadata=metadata,
                runtime_logger=runtime_logger,
            )
            maintenance_enabled = args.run_maintenance or (
                args.low_watermark_bytes is not None and args.high_watermark_bytes is not None
            )
            export_worker_enabled = args.run_export_worker
            stop_controller = ServiceStopController()
            started_at_ms = _now_ms()
            started_monotonic = monotonic()
            next_maintenance_at = started_monotonic
            cycle_count = 0
            processed_files_total = 0
            failed_files_total = 0
            frames_total = 0
            frames_accepted = 0
            frames_rejected = 0
            boxes_accepted = 0
            status_state = ObservabilityState()
            status_server: StatusServer | None = None
            status_server_error: str | None = None
            runtime_status: dict[str, object] = {
                "mode": "serve",
                "schema_version_id": schema_version_id,
                "input_dir": str(input_dir),
                "glob": args.glob,
                "poll_interval_seconds": args.poll_interval_seconds,
                "maintenance_enabled": maintenance_enabled,
                "export_worker_enabled": export_worker_enabled,
                "serve_started_at_ms": started_at_ms,
                "buffered_windows": runtime.ingest_service.buffered_window_count(),
                "buffered_boxes": runtime.ingest_service.buffered_box_count(),
                "processed_files_total": 0,
                "failed_files_total": 0,
                "frames_total": 0,
                "frames_accepted": 0,
                "frames_rejected": 0,
                "boxes_accepted": 0,
                "published_windows_last_cycle": 0,
                "published_rows_last_cycle": 0,
                "last_publish_now_ms": 0,
                "recovered_stale_queries": recovered_stale_queries,
                "detected_live_orphans": runtime.recovery["detected_live"],
                "recovered_staging_orphans": runtime.recovery["deleted_staging"],
                "stop_requested": False,
                "stop_reason": None,
            }

            def _refresh_status_state() -> None:
                status_state.replace(
                    collect_status_snapshot(
                        config,
                        metadata,
                        runtime=runtime_status,
                    )
                )

            def _handle_stop_signal(signum: int, _frame: object) -> None:
                signal_name = signal.Signals(signum).name
                stop_controller.request_stop(f"signal:{signal_name}")

            previous_sigint = signal.getsignal(signal.SIGINT)
            previous_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGINT, _handle_stop_signal)
            signal.signal(signal.SIGTERM, _handle_stop_signal)

            _refresh_status_state()
            if args.status_port is not None:
                try:
                    status_server = StatusServer(args.status_host, args.status_port, status_state)
                    status_server.start()
                except OSError as exc:
                    status_server = None
                    status_server_error = str(exc)
                    runtime_status["status_interface_error"] = status_server_error
                else:
                    runtime_status["status_host"] = status_server.bound_host
                    runtime_status["status_port"] = status_server.bound_port
                _refresh_status_state()

            runtime_logger.append(
                "serve_started",
                {
                    "schema_version_id": schema_version_id,
                    "input_dir": str(input_dir),
                    "glob": args.glob,
                    "poll_interval_seconds": args.poll_interval_seconds,
                    "maintenance_enabled": maintenance_enabled,
                    "export_worker_enabled": export_worker_enabled,
                    "maintenance_interval_seconds": args.maintenance_interval_seconds,
                    "detected_live_orphans": runtime.recovery["detected_live"],
                    "recovered_staging_orphans": runtime.recovery["deleted_staging"],
                    "status_host": None if status_server is None else status_server.bound_host,
                    "status_port": None if status_server is None else status_server.bound_port,
                    "status_interface_error": status_server_error,
                },
                ts_ms=started_at_ms,
            )

            print("status=running")
            print(f"schema_version_id={schema_version_id}")
            print(f"input_dir={input_dir}")
            print(f"maintenance_enabled={maintenance_enabled}")
            print(f"export_worker_enabled={export_worker_enabled}")
            print(f"detected_live_orphans={runtime.recovery['detected_live']}")
            print(f"recovered_staging_orphans={runtime.recovery['deleted_staging']}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            if status_server is not None:
                print(f"status_host={status_server.bound_host}")
                print(f"status_port={status_server.bound_port}")
            if status_server_error is not None:
                print(f"status_interface_error={status_server_error}")

            try:
                while not stop_controller.stop_requested:
                    cycle_count += 1
                    pending_result = _process_pending_input_files(
                        input_dir=input_dir,
                        glob_pattern=args.glob,
                        archive_dir=archive_dir,
                        failed_dir=failed_dir,
                        max_files_per_cycle=args.max_files_per_cycle,
                        runtime=runtime,
                        metadata=metadata,
                        config=config,
                        runtime_logger=runtime_logger,
                        now_ms=args.now_ms,
                        cycle_count=cycle_count,
                        processed_event_type="serve_file_processed",
                        failed_event_type="serve_file_failed",
                        stop_controller=stop_controller,
                    )
                    processed_files_total += pending_result.processed_files
                    failed_files_total += pending_result.failed_files
                    frames_total += pending_result.frames_total
                    frames_accepted += pending_result.frames_accepted
                    frames_rejected += pending_result.frames_rejected
                    boxes_accepted += pending_result.boxes_accepted
                    runtime_status["processed_files_total"] = processed_files_total
                    runtime_status["failed_files_total"] = failed_files_total
                    runtime_status["frames_total"] = frames_total
                    runtime_status["frames_accepted"] = frames_accepted
                    runtime_status["frames_rejected"] = frames_rejected
                    runtime_status["boxes_accepted"] = boxes_accepted

                    publish_result = _publish_ready_windows(
                        config=config,
                        ingest_service=runtime.ingest_service,
                        publish_service=runtime.publish_service,
                        schema_version_id=schema_version_id,
                        note=args.note,
                        now_ms=args.now_ms,
                        force_flush=False,
                    )
                    runtime.partition_states = _load_partition_state_cache(metadata)
                    runtime_status["buffered_windows"] = runtime.ingest_service.buffered_window_count()
                    runtime_status["buffered_boxes"] = runtime.ingest_service.buffered_box_count()
                    runtime_status["published_windows_last_cycle"] = publish_result.published_windows
                    runtime_status["published_rows_last_cycle"] = publish_result.published_rows
                    runtime_status["last_publish_now_ms"] = publish_result.publish_now_ms

                    maintenance_result = None
                    maintenance_due = maintenance_enabled and monotonic() >= next_maintenance_at
                    if maintenance_due:
                        maintenance_result = _run_maintenance_cycle(
                            runtime=runtime,
                            metadata=metadata,
                            schema_version_id=schema_version_id,
                            now_ms=args.now_ms,
                            low_watermark_bytes=args.low_watermark_bytes,
                            high_watermark_bytes=args.high_watermark_bytes,
                            query_wait_timeout_seconds=args.query_wait_timeout_seconds,
                            query_poll_interval_seconds=args.query_poll_interval_seconds,
                            seal_note=args.seal_note,
                            retention_note=args.retention_note,
                        )
                        next_maintenance_at = monotonic() + max(args.maintenance_interval_seconds, 0.1)
                    if maintenance_result is not None:
                        runtime_status["last_maintenance"] = {
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
                    export_result: ExportQueueResult | None = None
                    if export_worker_enabled:
                        export_result = runtime.export_service.process_pending_jobs(
                            max_jobs=args.max_export_jobs_per_cycle,
                        )
                        runtime_status["last_export_queue_run"] = {
                            "processed_jobs": export_result.processed_jobs,
                            "finished_jobs": export_result.finished_jobs,
                            "failed_jobs": export_result.failed_jobs,
                            "export_ids": list(export_result.export_ids),
                        }
                    _refresh_status_state()

                    runtime_logger.append(
                        "serve_cycle_completed",
                        {
                            "cycle": cycle_count,
                            "processed_files": pending_result.processed_files,
                            "failed_files": pending_result.failed_files,
                            "frames_total": pending_result.frames_total,
                            "frames_accepted": pending_result.frames_accepted,
                            "frames_rejected": pending_result.frames_rejected,
                            "boxes_accepted": pending_result.boxes_accepted,
                            "published_windows": publish_result.published_windows,
                            "published_rows": publish_result.published_rows,
                            "buffered_windows": runtime.ingest_service.buffered_window_count(),
                            "buffered_boxes": runtime.ingest_service.buffered_box_count(),
                            "publish_now_ms": publish_result.publish_now_ms,
                            "release_ids": list(publish_result.release_ids),
                            "maintenance_due": maintenance_due,
                            "trace_cooling_triggered": (
                                None
                                if maintenance_result is None
                                else maintenance_result.trace_index_cooling_result.triggered
                            ),
                            "export_worker_enabled": export_worker_enabled,
                            "export_processed_jobs": (
                                None if export_result is None else export_result.processed_jobs
                            ),
                            "export_finished_jobs": (
                                None if export_result is None else export_result.finished_jobs
                            ),
                            "export_failed_jobs": (
                                None if export_result is None else export_result.failed_jobs
                            ),
                            "stop_requested": stop_controller.stop_requested,
                        },
                    )

                    print(f"cycle={cycle_count}")
                    print(f"processed_files={pending_result.processed_files}")
                    print(f"failed_files={pending_result.failed_files}")
                    print(f"frames_total={pending_result.frames_total}")
                    print(f"frames_accepted={pending_result.frames_accepted}")
                    print(f"frames_rejected={pending_result.frames_rejected}")
                    print(f"boxes_accepted={pending_result.boxes_accepted}")
                    print(f"published_windows={publish_result.published_windows}")
                    print(f"published_rows={publish_result.published_rows}")
                    print(f"buffered_windows={runtime.ingest_service.buffered_window_count()}")
                    print(f"buffered_boxes={runtime.ingest_service.buffered_box_count()}")
                    print(f"publish_now_ms={publish_result.publish_now_ms}")
                    if publish_result.release_ids:
                        print(f"release_ids={','.join(publish_result.release_ids)}")
                    print(f"maintenance_due={maintenance_due}")
                    print(f"export_worker_enabled={export_worker_enabled}")
                    if export_result is not None:
                        print(f"export_processed_jobs={export_result.processed_jobs}")
                        print(f"export_finished_jobs={export_result.finished_jobs}")
                        print(f"export_failed_jobs={export_result.failed_jobs}")
                    if maintenance_result is not None:
                        print(f"seal_triggered={maintenance_result.seal_result.triggered}")
                        print(f"seal_reason={maintenance_result.seal_result.reason}")
                        print(
                            "trace_cooling_triggered="
                            f"{maintenance_result.trace_index_cooling_result.triggered}"
                        )
                        print(
                            "trace_cooling_reason="
                            f"{maintenance_result.trace_index_cooling_result.reason}"
                        )
                        print(
                            "retention_triggered="
                            f"{None if maintenance_result.retention_result is None else maintenance_result.retention_result.triggered}"
                        )

                    if (
                        args.max_runtime_seconds is not None
                        and monotonic() - started_monotonic >= args.max_runtime_seconds
                    ):
                        stop_controller.request_stop("max_runtime_reached")
                    if stop_controller.stop_requested:
                        break
                    _sleep_with_stop(args.poll_interval_seconds, stop_controller)

                runtime_logger.append(
                    "serve_stopped",
                    {
                        "schema_version_id": schema_version_id,
                        "stop_reason": stop_controller.stop_reason,
                        "cycles": cycle_count,
                        "processed_files_total": processed_files_total,
                        "failed_files_total": failed_files_total,
                        "frames_total": frames_total,
                        "frames_accepted": frames_accepted,
                        "frames_rejected": frames_rejected,
                        "boxes_accepted": boxes_accepted,
                        "buffered_windows": runtime.ingest_service.buffered_window_count(),
                        "buffered_boxes": runtime.ingest_service.buffered_box_count(),
                    },
                )
                runtime_status["stop_requested"] = True
                runtime_status["stop_reason"] = stop_controller.stop_reason
                runtime_status["buffered_windows"] = runtime.ingest_service.buffered_window_count()
                runtime_status["buffered_boxes"] = runtime.ingest_service.buffered_box_count()
                _refresh_status_state()
                print(f"stop_reason={stop_controller.stop_reason}")
                print(f"processed_files_total={processed_files_total}")
                print(f"failed_files_total={failed_files_total}")
                print(f"frames_total_overall={frames_total}")
                print(f"frames_accepted_overall={frames_accepted}")
                print(f"frames_rejected_overall={frames_rejected}")
                print(f"boxes_accepted_overall={boxes_accepted}")
                print(f"buffered_windows={runtime.ingest_service.buffered_window_count()}")
                print(f"buffered_boxes={runtime.ingest_service.buffered_box_count()}")
                return
            finally:
                if status_server is not None:
                    status_server.stop()
                signal.signal(signal.SIGINT, previous_sigint)
                signal.signal(signal.SIGTERM, previous_sigterm)

        if args.command == "run-ingest-loop":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)

            input_dir = Path(args.input_dir)
            input_dir.mkdir(parents=True, exist_ok=True)
            archive_dir = Path(args.archive_dir) if args.archive_dir else input_dir / "processed"
            failed_dir = Path(args.failed_dir) if args.failed_dir else input_dir / "failed"

            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            validator = ValidatorService(
                late_arrival_window_ms=config.runtime.late_arrival_window_seconds * 1000,
                max_devices=config.runtime.max_devices,
            )
            ingest_service = IngestService(config, layout)
            publish_service = PublishService(
                metadata_store=metadata,
                journal=journal,
                box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
                derived_data_service=derived_data_service,
                layout=layout,
            )
            seal_service = SealService(
                metadata_store=metadata,
                journal=journal,
                box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
                derived_data_service=derived_data_service,
                layout=layout,
                late_arrival_window_seconds=config.runtime.late_arrival_window_seconds,
            )
            retention_service = RetentionService(config, metadata, journal)
            maintenance_service = MaintenanceService(
                seal_service,
                derived_data_service,
                retention_service,
            )
            recovery = _recover_orphan_files(metadata, layout)
            partition_states = _load_partition_state_cache(metadata)
            _prime_validator_from_partition_states(validator, partition_states)

            cycle_count = 0
            processed_files_total = 0
            failed_files_total = 0

            while True:
                cycle_count += 1
                pending_files = sorted(path for path in input_dir.glob(args.glob) if path.is_file())
                if args.max_files_per_cycle is not None:
                    pending_files = pending_files[: args.max_files_per_cycle]

                cycle_frames_total = 0
                cycle_frames_accepted = 0
                cycle_frames_rejected = 0
                cycle_boxes_accepted = 0
                cycle_processed_files = 0
                cycle_failed_files = 0

                for input_path in pending_files:
                    source = JsonlFrameSource(input_path)
                    try:
                        frame_result = _ingest_frames(
                            frames=source.iter_frames(),
                            validator=validator,
                            ingest_service=ingest_service,
                            partition_states=partition_states,
                            metadata=metadata,
                            config=config,
                            now_ms=args.now_ms,
                        )
                    except ValueError as exc:
                        failed_path = _move_input_file(input_path, failed_dir)
                        cycle_failed_files += 1
                        failed_files_total += 1
                        runtime_logger.append(
                            "ingest_loop_file_failed",
                            {
                                "input": str(input_path),
                                "failed_path": str(failed_path),
                                "error": str(exc),
                                "cycle": cycle_count,
                            },
                        )
                        continue

                    archived_path = _move_input_file(input_path, archive_dir)
                    cycle_processed_files += 1
                    processed_files_total += 1
                    cycle_frames_total += frame_result.frames_total
                    cycle_frames_accepted += frame_result.frames_accepted
                    cycle_frames_rejected += frame_result.frames_rejected
                    cycle_boxes_accepted += frame_result.boxes_accepted
                    runtime_logger.append(
                        "ingest_loop_file_processed",
                        {
                            "input": str(input_path),
                            "archived_path": str(archived_path),
                            "cycle": cycle_count,
                            "frames_total": frame_result.frames_total,
                            "frames_accepted": frame_result.frames_accepted,
                            "frames_rejected": frame_result.frames_rejected,
                            "boxes_accepted": frame_result.boxes_accepted,
                        },
                    )

                publish_result = _publish_ready_windows(
                    config=config,
                    ingest_service=ingest_service,
                    publish_service=publish_service,
                    schema_version_id=schema_version_id,
                    note=args.note,
                    now_ms=args.now_ms,
                    force_flush=False,
                )
                partition_states = _load_partition_state_cache(metadata)

                maintenance_result = None
                if args.run_maintenance or (
                    args.low_watermark_bytes is not None and args.high_watermark_bytes is not None
                ):
                    maintenance_result = maintenance_service.run_cycle(
                        schema_version_id=schema_version_id,
                        now_ms=args.now_ms,
                        low_watermark_bytes=args.low_watermark_bytes,
                        high_watermark_bytes=args.high_watermark_bytes,
                        seal_note=args.seal_note,
                        retention_note=args.retention_note,
                    )
                    partition_states = _load_partition_state_cache(metadata)

                runtime_logger.append(
                    "ingest_loop_cycle_completed",
                    {
                        "cycle": cycle_count,
                        "processed_files": cycle_processed_files,
                        "failed_files": cycle_failed_files,
                        "frames_total": cycle_frames_total,
                        "frames_accepted": cycle_frames_accepted,
                        "frames_rejected": cycle_frames_rejected,
                        "boxes_accepted": cycle_boxes_accepted,
                        "published_windows": publish_result.published_windows,
                        "published_rows": publish_result.published_rows,
                        "buffered_windows": ingest_service.buffered_window_count(),
                        "buffered_boxes": ingest_service.buffered_box_count(),
                        "publish_now_ms": publish_result.publish_now_ms,
                        "release_ids": list(publish_result.release_ids),
                        "maintenance_triggered": None if maintenance_result is None else True,
                        "trace_cooling_triggered": (
                            None
                            if maintenance_result is None
                            else maintenance_result.trace_index_cooling_result.triggered
                        ),
                    },
                )

                print(f"cycle={cycle_count}")
                print(f"processed_files={cycle_processed_files}")
                print(f"failed_files={cycle_failed_files}")
                print(f"frames_total={cycle_frames_total}")
                print(f"frames_accepted={cycle_frames_accepted}")
                print(f"frames_rejected={cycle_frames_rejected}")
                print(f"boxes_accepted={cycle_boxes_accepted}")
                print(f"published_windows={publish_result.published_windows}")
                print(f"published_rows={publish_result.published_rows}")
                print(f"buffered_windows={ingest_service.buffered_window_count()}")
                print(f"buffered_boxes={ingest_service.buffered_box_count()}")
                print(f"publish_now_ms={publish_result.publish_now_ms}")
                print(f"detected_live_orphans={recovery['detected_live']}")
                print(f"recovered_staging_orphans={recovery['deleted_staging']}")
                print(f"recovered_stale_queries={recovered_stale_queries}")
                if publish_result.release_ids:
                    print(f"release_ids={','.join(publish_result.release_ids)}")
                if maintenance_result is not None:
                    print(f"seal_triggered={maintenance_result.seal_result.triggered}")
                    print(f"seal_reason={maintenance_result.seal_result.reason}")
                    print(
                        "trace_cooling_triggered="
                        f"{maintenance_result.trace_index_cooling_result.triggered}"
                    )
                    print(
                        "trace_cooling_reason="
                        f"{maintenance_result.trace_index_cooling_result.reason}"
                    )
                    print(f"retention_triggered={None if maintenance_result.retention_result is None else maintenance_result.retention_result.triggered}")

                if args.max_cycles is not None and cycle_count >= args.max_cycles:
                    print(f"processed_files_total={processed_files_total}")
                    print(f"failed_files_total={failed_files_total}")
                    return

                sleep(args.poll_interval_seconds)
            return

        if args.command == "plan-detail-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            plan = query_service.plan_device_time_range_query(
                device_id=args.device_id,
                time_range=TimeRange(start_ms=args.start_ms, end_ms=args.end_ms),
            )
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(plan.sql)
            return

        if args.command == "plan-trace-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            time_range = _optional_time_range(args.start_ms, args.end_ms)
            plan = query_service.plan_device_trace_query(
                device_id=args.device_id,
                trace_id=args.trace_id,
                time_range=time_range,
            )
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(plan.sql)
            return

        if args.command == "plan-trace-lookup-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            time_range = _optional_time_range(args.start_ms, args.end_ms)
            plan = query_service.plan_trace_lookup_query(
                device_id=args.device_id,
                trace_id=args.trace_id,
                time_range=time_range,
            )
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(plan.sql)
            return

        if args.command == "plan-rollup-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            plan = query_service.plan_rollup_query(
                device_id=args.device_id,
                time_range=TimeRange(start_ms=args.start_ms, end_ms=args.end_ms),
                lane_id=args.lane_id,
                obj_type=args.obj_type,
            )
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(plan.sql)
            return

        if args.command == "exec-detail-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            time_range = TimeRange(start_ms=args.start_ms, end_ms=args.end_ms)
            plan = query_service.plan_device_time_range_query(
                device_id=args.device_id,
                time_range=time_range,
            )
            sql = plan.sql
            if args.limit is not None:
                sql = f"SELECT * FROM ({plan.sql}) AS detail_query LIMIT {args.limit}"
            query_id = uuid4().hex
            handle = QueryHandle(
                query_id=query_id,
                release_id=plan.release_id,
                query_kind=QueryKind.DETAIL,
                started_at_ms=_now_ms(),
                target_range=time_range,
                owner_pid=os.getpid(),
            )
            metadata.start_query(handle)
            try:
                if args.hold_seconds > 0:
                    sleep(args.hold_seconds)
                columns, rows = _execute_sql(sql)
            finally:
                metadata.finish_query(query_id)
            runtime_logger.append(
                "detail_query_completed",
                {
                    "query_id": query_id,
                    "release_id": plan.release_id,
                    "device_id": args.device_id,
                    "start_ms": args.start_ms,
                    "end_ms": args.end_ms,
                    "row_count": len(rows),
                    "limit": args.limit,
                },
            )
            print(f"query_id={query_id}")
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(f"row_count={len(rows)}")
            for row in rows:
                payload = dict(zip(columns, row, strict=True))
                print(json.dumps(payload, sort_keys=True, default=str))
            return

        if args.command == "exec-trace-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            time_range = _optional_time_range(args.start_ms, args.end_ms)
            plan = query_service.plan_device_trace_query(
                device_id=args.device_id,
                trace_id=args.trace_id,
                time_range=time_range,
            )
            sql = plan.sql
            if args.limit is not None:
                sql = f"SELECT * FROM ({plan.sql}) AS trace_query LIMIT {args.limit}"
            query_id = uuid4().hex
            handle = QueryHandle(
                query_id=query_id,
                release_id=plan.release_id,
                query_kind=QueryKind.TRACE,
                started_at_ms=_now_ms(),
                target_range=time_range,
                owner_pid=os.getpid(),
            )
            metadata.start_query(handle)
            try:
                if args.hold_seconds > 0:
                    sleep(args.hold_seconds)
                columns, rows = _execute_sql(sql)
            finally:
                metadata.finish_query(query_id)
            runtime_logger.append(
                "trace_query_completed",
                {
                    "query_id": query_id,
                    "release_id": plan.release_id,
                    "device_id": args.device_id,
                    "trace_id": args.trace_id,
                    "start_ms": None if time_range is None else time_range.start_ms,
                    "end_ms": None if time_range is None else time_range.end_ms,
                    "row_count": len(rows),
                    "limit": args.limit,
                },
            )
            print(f"query_id={query_id}")
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(f"row_count={len(rows)}")
            for row in rows:
                payload = dict(zip(columns, row, strict=True))
                print(json.dumps(payload, sort_keys=True, default=str))
            return

        if args.command == "exec-trace-lookup-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            time_range = _optional_time_range(args.start_ms, args.end_ms)
            plan = query_service.plan_trace_lookup_query(
                device_id=args.device_id,
                trace_id=args.trace_id,
                time_range=time_range,
            )
            sql = plan.sql
            if args.limit is not None:
                sql = f"SELECT * FROM ({plan.sql}) AS trace_lookup_query LIMIT {args.limit}"
            query_id = uuid4().hex
            handle = QueryHandle(
                query_id=query_id,
                release_id=plan.release_id,
                query_kind=QueryKind.TRACE,
                started_at_ms=_now_ms(),
                target_range=time_range,
                owner_pid=os.getpid(),
            )
            metadata.start_query(handle)
            try:
                if args.hold_seconds > 0:
                    sleep(args.hold_seconds)
                columns, rows = _execute_sql(sql)
            finally:
                metadata.finish_query(query_id)
            runtime_logger.append(
                "trace_lookup_query_completed",
                {
                    "query_id": query_id,
                    "release_id": plan.release_id,
                    "device_id": args.device_id,
                    "trace_id": args.trace_id,
                    "start_ms": None if time_range is None else time_range.start_ms,
                    "end_ms": None if time_range is None else time_range.end_ms,
                    "row_count": len(rows),
                    "limit": args.limit,
                },
            )
            print(f"query_id={query_id}")
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(f"row_count={len(rows)}")
            for row in rows:
                payload = dict(zip(columns, row, strict=True))
                print(json.dumps(payload, sort_keys=True, default=str))
            return

        if args.command == "exec-rollup-query":
            metadata.init_schema()
            query_service = QueryService(metadata)
            time_range = TimeRange(start_ms=args.start_ms, end_ms=args.end_ms)
            plan = query_service.plan_rollup_query(
                device_id=args.device_id,
                time_range=time_range,
                lane_id=args.lane_id,
                obj_type=args.obj_type,
            )
            sql = plan.sql
            if args.limit is not None:
                sql = f"SELECT * FROM ({plan.sql}) AS rollup_query LIMIT {args.limit}"
            query_id = uuid4().hex
            handle = QueryHandle(
                query_id=query_id,
                release_id=plan.release_id,
                query_kind=QueryKind.AGGREGATE,
                started_at_ms=_now_ms(),
                target_range=time_range,
                owner_pid=os.getpid(),
            )
            metadata.start_query(handle)
            try:
                if args.hold_seconds > 0:
                    sleep(args.hold_seconds)
                columns, rows = _execute_sql(sql)
            finally:
                metadata.finish_query(query_id)
            runtime_logger.append(
                "rollup_query_completed",
                {
                    "query_id": query_id,
                    "release_id": plan.release_id,
                    "device_id": args.device_id,
                    "start_ms": args.start_ms,
                    "end_ms": args.end_ms,
                    "lane_id": args.lane_id,
                    "obj_type": args.obj_type,
                    "row_count": len(rows),
                    "limit": args.limit,
                },
            )
            print(f"query_id={query_id}")
            print(f"release_id={plan.release_id}")
            print(f"files={len(plan.files)}")
            print(f"row_count={len(rows)}")
            for row in rows:
                payload = dict(zip(columns, row, strict=True))
                print(json.dumps(payload, sort_keys=True, default=str))
            return

        if args.command == "plan-delete":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            ops_service = OpsService(metadata, journal)
            plan = ops_service.build_delete_plan(
                retention_floor_ms=args.before_ms,
                schema_version_id=schema_version_id,
            )
            print(f"source_release_id={plan.source_release_id}")
            print(f"new_release_id={plan.new_release.release_id}")
            print(f"keep_groups={len(plan.keep_groups)}")
            print(f"delete_groups={len(plan.delete_groups)}")
            print(f"delete_bytes_estimate={sum(group.total_bytes for group in plan.delete_groups)}")
            return

        if args.command == "delete-before":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            ops_service = OpsService(metadata, journal)
            plan = ops_service.build_delete_plan(
                retention_floor_ms=args.before_ms,
                schema_version_id=schema_version_id,
            )
            if not plan.delete_groups:
                print("delete_groups=0")
                print("status=noop")
                return
            try:
                release = ops_service.commit_delete_plan_with_wait(
                    plan,
                    query_wait_timeout_ms=int(args.query_wait_timeout_seconds * 1000),
                    query_poll_interval_ms=int(args.query_poll_interval_seconds * 1000),
                )
            except DeleteConflictError as exc:
                print(f"status=blocked")
                print(f"reason={exc}")
                print(f"recovered_stale_queries={recovered_stale_queries}")
                return
            runtime_logger.append(
                "delete_before_completed",
                {
                    "release_id": release.release_id,
                    "source_release_id": plan.source_release_id,
                    "delete_groups": len(plan.delete_groups),
                    "retention_floor_ms": release.retention_floor_ms,
                },
            )
            print(f"release_id={release.release_id}")
            print(f"source_release_id={plan.source_release_id}")
            print(f"delete_groups={len(plan.delete_groups)}")
            print(f"delete_bytes_estimate={sum(group.total_bytes for group in plan.delete_groups)}")
            print(f"retention_floor_ms={release.retention_floor_ms}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "rollback-release":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            ops_service = OpsService(metadata, journal)
            release = ops_service.rollback_to_release(
                target_release_id=args.target_release_id,
                schema_version_id=schema_version_id,
                retention_floor_ms=args.retention_floor_ms,
                note=args.note,
            )
            runtime_logger.append(
                "rollback_release_completed",
                {
                    "release_id": release.release_id,
                    "target_release_id": args.target_release_id,
                    "schema_version_id": schema_version_id,
                    "retention_floor_ms": args.retention_floor_ms,
                },
            )
            print(f"release_id={release.release_id}")
            print(f"target_release_id={args.target_release_id}")
            print(f"schema_version_id={schema_version_id}")
            print(f"retention_floor_ms={release.retention_floor_ms}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "seal-ready-hours":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            seal_service = SealService(
                metadata_store=metadata,
                journal=journal,
                box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
                derived_data_service=derived_data_service,
                layout=layout,
                late_arrival_window_seconds=config.runtime.late_arrival_window_seconds,
            )
            recovery = _recover_orphan_files(metadata, layout)
            result = seal_service.seal_ready_partitions(
                schema_version_id=schema_version_id,
                now_ms=args.now_ms,
                note=args.note,
            )
            runtime_logger.append(
                "seal_ready_hours_completed",
                {
                    "schema_version_id": schema_version_id,
                    "triggered": result.triggered,
                    "partition_count": result.partition_count,
                    "replaced_group_count": result.replaced_group_count,
                    "sealed_row_count": result.sealed_row_count,
                    "new_release_id": result.new_release_id,
                    "reason": result.reason,
                    "detected_live_orphans": recovery["detected_live"],
                    "recovered_staging_orphans": recovery["deleted_staging"],
                },
            )
            print(f"triggered={result.triggered}")
            print(f"reason={result.reason}")
            print(f"partition_count={result.partition_count}")
            print(f"replaced_group_count={result.replaced_group_count}")
            print(f"sealed_row_count={result.sealed_row_count}")
            print(f"detected_live_orphans={recovery['detected_live']}")
            print(f"recovered_staging_orphans={recovery['deleted_staging']}")
            print(f"new_release_id={result.new_release_id}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "seal-hour":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            seal_service = SealService(
                metadata_store=metadata,
                journal=journal,
                box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
                derived_data_service=derived_data_service,
                layout=layout,
                late_arrival_window_seconds=config.runtime.late_arrival_window_seconds,
            )
            recovery = _recover_orphan_files(metadata, layout)
            result = seal_service.seal_partition(
                date_utc=args.date_utc,
                hour_utc=args.hour_utc,
                device_id=args.device_id,
                schema_version_id=schema_version_id,
                note=args.note,
            )
            runtime_logger.append(
                "seal_hour_completed",
                {
                    "schema_version_id": schema_version_id,
                    "date_utc": args.date_utc,
                    "hour_utc": args.hour_utc,
                    "device_id": args.device_id,
                    "triggered": result.triggered,
                    "partition_count": result.partition_count,
                    "replaced_group_count": result.replaced_group_count,
                    "sealed_row_count": result.sealed_row_count,
                    "new_release_id": result.new_release_id,
                    "reason": result.reason,
                    "detected_live_orphans": recovery["detected_live"],
                    "recovered_staging_orphans": recovery["deleted_staging"],
                },
            )
            print(f"triggered={result.triggered}")
            print(f"reason={result.reason}")
            print(f"partition_count={result.partition_count}")
            print(f"replaced_group_count={result.replaced_group_count}")
            print(f"sealed_row_count={result.sealed_row_count}")
            print(f"detected_live_orphans={recovery['detected_live']}")
            print(f"recovered_staging_orphans={recovery['deleted_staging']}")
            print(f"new_release_id={result.new_release_id}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "enforce-watermarks":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            retention_service = RetentionService(config, metadata, journal)
            result = retention_service.enforce_watermarks(
                schema_version_id=schema_version_id,
                low_watermark_bytes=args.low_watermark_bytes,
                high_watermark_bytes=args.high_watermark_bytes,
                query_wait_timeout_ms=int(args.query_wait_timeout_seconds * 1000),
                query_poll_interval_ms=int(args.query_poll_interval_seconds * 1000),
                note=args.note,
            )
            runtime_logger.append(
                "enforce_watermarks_completed",
                {
                    "schema_version_id": schema_version_id,
                    "triggered": result.triggered,
                    "low_watermark_bytes": result.low_watermark_bytes,
                    "high_watermark_bytes": result.high_watermark_bytes,
                    "usage_before_bytes": result.usage_before_bytes,
                    "usage_after_bytes": result.usage_after_bytes,
                    "deleted_partition_count": result.deleted_partition_count,
                    "deleted_group_count": result.deleted_group_count,
                    "deleted_estimated_bytes": result.deleted_estimated_bytes,
                    "new_release_id": result.new_release_id,
                    "retention_floor_ms": result.retention_floor_ms,
                    "reason": result.reason,
                },
            )
            print(f"triggered={result.triggered}")
            print(f"reason={result.reason}")
            print(f"usage_before_bytes={result.usage_before_bytes}")
            print(f"usage_after_bytes={result.usage_after_bytes}")
            print(f"low_watermark_bytes={result.low_watermark_bytes}")
            print(f"high_watermark_bytes={result.high_watermark_bytes}")
            print(f"deleted_partition_count={result.deleted_partition_count}")
            print(f"deleted_group_count={result.deleted_group_count}")
            print(f"deleted_estimated_bytes={result.deleted_estimated_bytes}")
            print(f"new_release_id={result.new_release_id}")
            print(f"retention_floor_ms={result.retention_floor_ms}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "rebuild-trace-index":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            result = derived_data_service.rebuild_current_release(
                schema_version_id=schema_version_id,
                rebuild_trace_index=True,
                rebuild_rollup=False,
                now_ms=args.now_ms,
                note=args.note,
            )
            runtime_logger.append(
                "rebuild_trace_index_completed",
                {
                    "schema_version_id": schema_version_id,
                    "triggered": result.triggered,
                    "rebuilt_trace_index_groups": result.rebuilt_trace_index_groups,
                    "removed_group_count": result.removed_group_count,
                    "new_release_id": result.new_release_id,
                    "reason": result.reason,
                },
            )
            print(f"triggered={result.triggered}")
            print(f"reason={result.reason}")
            print(f"rebuilt_trace_index_groups={result.rebuilt_trace_index_groups}")
            print(f"removed_group_count={result.removed_group_count}")
            print(f"new_release_id={result.new_release_id}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "rebuild-rollup":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            result = derived_data_service.rebuild_current_release(
                schema_version_id=schema_version_id,
                rebuild_trace_index=False,
                rebuild_rollup=True,
                now_ms=args.now_ms,
                note=args.note,
            )
            runtime_logger.append(
                "rebuild_rollup_completed",
                {
                    "schema_version_id": schema_version_id,
                    "triggered": result.triggered,
                    "rebuilt_rollup_groups": result.rebuilt_rollup_groups,
                    "removed_group_count": result.removed_group_count,
                    "new_release_id": result.new_release_id,
                    "reason": result.reason,
                },
            )
            print(f"triggered={result.triggered}")
            print(f"reason={result.reason}")
            print(f"rebuilt_rollup_groups={result.rebuilt_rollup_groups}")
            print(f"removed_group_count={result.removed_group_count}")
            print(f"new_release_id={result.new_release_id}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            return

        if args.command == "run-maintenance":
            metadata.init_schema()
            schema_version_id = _resolve_schema_version_id(metadata, args.schema_version_id)
            _ensure_schema_version(metadata, schema_version_id)
            journal = ReleaseJournal(config.paths.journal_dir)
            derived_data_service = _build_derived_data_service(
                config=config,
                metadata=metadata,
                journal=journal,
                layout=layout,
            )
            seal_service = SealService(
                metadata_store=metadata,
                journal=journal,
                box_info_writer=BoxInfoParquetWriter(layout.staging_root(DatasetKind.BOX_INFO)),
                derived_data_service=derived_data_service,
                layout=layout,
                late_arrival_window_seconds=config.runtime.late_arrival_window_seconds,
            )
            recovery = _recover_orphan_files(metadata, layout)
            retention_service = RetentionService(config, metadata, journal)
            maintenance_service = MaintenanceService(seal_service, derived_data_service, retention_service)
            result = maintenance_service.run_cycle(
                schema_version_id=schema_version_id,
                now_ms=args.now_ms,
                low_watermark_bytes=args.low_watermark_bytes,
                high_watermark_bytes=args.high_watermark_bytes,
                query_wait_timeout_ms=int(args.query_wait_timeout_seconds * 1000),
                query_poll_interval_ms=int(args.query_poll_interval_seconds * 1000),
                seal_note=args.seal_note,
                retention_note=args.retention_note,
            )
            runtime_logger.append(
                "maintenance_cycle_completed",
                {
                    "schema_version_id": schema_version_id,
                    "seal_triggered": result.seal_result.triggered,
                    "seal_partition_count": result.seal_result.partition_count,
                    "seal_replaced_group_count": result.seal_result.replaced_group_count,
                    "seal_new_release_id": result.seal_result.new_release_id,
                    "trace_cooling_triggered": result.trace_index_cooling_result.triggered,
                    "trace_cooling_cooled_group_count": result.trace_index_cooling_result.cooled_group_count,
                    "trace_cooling_new_release_id": result.trace_index_cooling_result.new_release_id,
                    "retention_triggered": (
                        None if result.retention_result is None else result.retention_result.triggered
                    ),
                    "retention_new_release_id": (
                        None
                        if result.retention_result is None
                        else result.retention_result.new_release_id
                    ),
                },
            )
            print(f"seal_triggered={result.seal_result.triggered}")
            print(f"seal_reason={result.seal_result.reason}")
            print(f"seal_partition_count={result.seal_result.partition_count}")
            print(f"seal_replaced_group_count={result.seal_result.replaced_group_count}")
            print(f"seal_new_release_id={result.seal_result.new_release_id}")
            print(f"trace_cooling_triggered={result.trace_index_cooling_result.triggered}")
            print(f"trace_cooling_reason={result.trace_index_cooling_result.reason}")
            print(f"trace_cooling_cooled_group_count={result.trace_index_cooling_result.cooled_group_count}")
            print(f"trace_cooling_retained_hot_group_count={result.trace_index_cooling_result.retained_hot_group_count}")
            print(f"trace_cooling_new_release_id={result.trace_index_cooling_result.new_release_id}")
            print(f"detected_live_orphans={recovery['detected_live']}")
            print(f"recovered_staging_orphans={recovery['deleted_staging']}")
            print(f"recovered_stale_queries={recovered_stale_queries}")
            if result.retention_result is None:
                print("retention_triggered=None")
                print("retention_reason=skipped_without_watermarks")
            else:
                print(f"retention_triggered={result.retention_result.triggered}")
                print(f"retention_reason={result.retention_result.reason}")
                print(f"retention_deleted_partition_count={result.retention_result.deleted_partition_count}")
                print(f"retention_deleted_group_count={result.retention_result.deleted_group_count}")
                print(f"retention_new_release_id={result.retention_result.new_release_id}")
            return
    except ModuleNotFoundError as exc:
        if exc.name == "duckdb":
            parser.exit(2, "duckdb package is required for metadata and parquet commands\n")
        raise

    parser.exit(2, "unsupported command\n")


if __name__ == "__main__":
    main()
