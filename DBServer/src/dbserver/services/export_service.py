from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from uuid import uuid4

from dbserver.metadata.store import MetadataStore
from dbserver.models import QueryHandle, QueryKind, TimeRange
from dbserver.runtime_logging import RuntimeLogger
from dbserver.services.query_service import QueryPlan, QueryService
from dbserver.storage.layout import StorageLayout


def _now_ms() -> int:
    return time_ns() // 1_000_000


@dataclass(slots=True, frozen=True)
class ExportJobRecord:
    export_id: str
    status: str
    created_at_ms: int
    release_id: str
    query_kind: str
    format: str
    sql: str
    files: tuple[str, ...]
    parameters: dict[str, object]
    target_range: TimeRange | None
    output_path: str | None = None
    started_at_ms: int | None = None
    finished_at_ms: int | None = None
    row_count: int | None = None
    error: str | None = None

    def to_json(self) -> str:
        payload = {
            "export_id": self.export_id,
            "status": self.status,
            "created_at_ms": self.created_at_ms,
            "release_id": self.release_id,
            "query_kind": self.query_kind,
            "format": self.format,
            "sql": self.sql,
            "files": list(self.files),
            "parameters": self.parameters,
            "target_range_start_ms": None if self.target_range is None else self.target_range.start_ms,
            "target_range_end_ms": None if self.target_range is None else self.target_range.end_ms,
            "output_path": self.output_path,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "row_count": self.row_count,
            "error": self.error,
        }
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_path(cls, path: Path) -> "ExportJobRecord":
        payload = json.loads(path.read_text(encoding="utf-8"))
        target_range = None
        if (
            payload.get("target_range_start_ms") is not None
            and payload.get("target_range_end_ms") is not None
        ):
            target_range = TimeRange(
                start_ms=int(payload["target_range_start_ms"]),
                end_ms=int(payload["target_range_end_ms"]),
            )
        return cls(
            export_id=str(payload["export_id"]),
            status=str(payload["status"]),
            created_at_ms=int(payload["created_at_ms"]),
            release_id=str(payload["release_id"]),
            query_kind=str(payload["query_kind"]),
            format=str(payload["format"]),
            sql=str(payload["sql"]),
            files=tuple(str(item) for item in payload["files"]),
            parameters=dict(payload["parameters"]),
            target_range=target_range,
            output_path=None if payload.get("output_path") is None else str(payload["output_path"]),
            started_at_ms=payload.get("started_at_ms"),
            finished_at_ms=payload.get("finished_at_ms"),
            row_count=payload.get("row_count"),
            error=payload.get("error"),
        )


@dataclass(slots=True, frozen=True)
class ExportQueueResult:
    processed_jobs: int
    finished_jobs: int
    failed_jobs: int
    export_ids: tuple[str, ...]


class ExportService:
    def __init__(
        self,
        metadata_store: MetadataStore,
        layout: StorageLayout,
        runtime_logger: RuntimeLogger,
    ) -> None:
        self.metadata_store = metadata_store
        self.layout = layout
        self.runtime_logger = runtime_logger
        self.query_service = QueryService(metadata_store)

    def submit_detail_export(
        self,
        *,
        device_id: int,
        time_range: TimeRange,
    ) -> ExportJobRecord:
        record = self.build_detail_job_record(device_id=device_id, time_range=time_range)
        return self._persist_pending_job(record)

    def submit_trace_export(
        self,
        *,
        device_id: int,
        trace_id: int,
        time_range: TimeRange | None = None,
    ) -> ExportJobRecord:
        record = self.build_trace_job_record(
            device_id=device_id,
            trace_id=trace_id,
            time_range=time_range,
        )
        return self._persist_pending_job(record)

    def build_trace_job_record(
        self,
        *,
        device_id: int,
        trace_id: int,
        time_range: TimeRange | None = None,
        export_id: str | None = None,
    ) -> ExportJobRecord:
        plan = self.query_service.plan_device_trace_query(
            device_id=device_id,
            trace_id=trace_id,
            time_range=time_range,
        )
        parameters: dict[str, object] = {
            "kind": "trace",
            "device_id": device_id,
            "trace_id": trace_id,
        }
        if time_range is not None:
            parameters["start_ms"] = time_range.start_ms
            parameters["end_ms"] = time_range.end_ms
        return self._build_job_record(
            plan=plan,
            parameters=parameters,
            target_range=time_range,
            export_id=export_id,
        )

    def submit_rollup_export(
        self,
        *,
        device_id: int,
        time_range: TimeRange,
        lane_id: int | None = None,
        obj_type: int | None = None,
    ) -> ExportJobRecord:
        record = self.build_rollup_job_record(
            device_id=device_id,
            time_range=time_range,
            lane_id=lane_id,
            obj_type=obj_type,
        )
        return self._persist_pending_job(record)

    def build_detail_job_record(
        self,
        *,
        device_id: int,
        time_range: TimeRange,
        export_id: str | None = None,
    ) -> ExportJobRecord:
        plan = self.query_service.plan_device_time_range_query(
            device_id=device_id,
            time_range=time_range,
        )
        return self._build_job_record(
            plan=plan,
            parameters={
                "kind": "detail",
                "device_id": device_id,
                "start_ms": time_range.start_ms,
                "end_ms": time_range.end_ms,
            },
            target_range=time_range,
            export_id=export_id,
        )

    def build_rollup_job_record(
        self,
        *,
        device_id: int,
        time_range: TimeRange,
        lane_id: int | None = None,
        obj_type: int | None = None,
        export_id: str | None = None,
    ) -> ExportJobRecord:
        plan = self.query_service.plan_rollup_query(
            device_id=device_id,
            time_range=time_range,
            lane_id=lane_id,
            obj_type=obj_type,
        )
        parameters: dict[str, object] = {
            "kind": "rollup",
            "device_id": device_id,
            "start_ms": time_range.start_ms,
            "end_ms": time_range.end_ms,
        }
        if lane_id is not None:
            parameters["lane_id"] = lane_id
        if obj_type is not None:
            parameters["obj_type"] = obj_type
        return self._build_job_record(
            plan=plan,
            parameters=parameters,
            target_range=time_range,
            export_id=export_id,
        )

    def list_jobs(self) -> list[ExportJobRecord]:
        jobs: list[ExportJobRecord] = []
        for path in sorted(self.layout.paths.exports.joinpath("pending").glob("*.json")):
            record = ExportJobRecord.from_path(path)
            if path.name.endswith(".working.json"):
                record = ExportJobRecord(
                    export_id=record.export_id,
                    status="running",
                    created_at_ms=record.created_at_ms,
                    release_id=record.release_id,
                    query_kind=record.query_kind,
                    format=record.format,
                    sql=record.sql,
                    files=record.files,
                    parameters=record.parameters,
                    target_range=record.target_range,
                    output_path=record.output_path,
                    started_at_ms=record.started_at_ms,
                    finished_at_ms=record.finished_at_ms,
                    row_count=record.row_count,
                    error=record.error,
                )
            jobs.append(record)
        for path in sorted(self.layout.paths.exports.joinpath("finished").glob("*.json")):
            jobs.append(ExportJobRecord.from_path(path))
        jobs.sort(key=lambda item: (item.created_at_ms, item.export_id))
        return jobs

    def process_pending_jobs(self, *, max_jobs: int | None = None) -> ExportQueueResult:
        pending_paths = sorted(self.layout.paths.exports.joinpath("pending").glob("*.json"))
        if max_jobs is not None:
            pending_paths = pending_paths[:max_jobs]
        processed_jobs = 0
        finished_jobs = 0
        failed_jobs = 0
        export_ids: list[str] = []
        for pending_path in pending_paths:
            if pending_path.name.endswith(".working.json"):
                continue
            record = ExportJobRecord.from_path(pending_path)
            working_path = self.layout.export_working_request_path(record.export_id)
            try:
                pending_path.replace(working_path)
            except FileNotFoundError:
                continue
            processed_jobs += 1
            export_ids.append(record.export_id)
            terminal_record = self.execute_job_record(record)
            working_path.unlink(missing_ok=True)
            manifest_path = self.layout.export_manifest_path(record.export_id)
            manifest_path.write_text(terminal_record.to_json(), encoding="utf-8")
            if terminal_record.status == "finished":
                finished_jobs += 1
            else:
                failed_jobs += 1
        return ExportQueueResult(
            processed_jobs=processed_jobs,
            finished_jobs=finished_jobs,
            failed_jobs=failed_jobs,
            export_ids=tuple(export_ids),
        )

    def _build_job_record(
        self,
        *,
        plan: QueryPlan,
        parameters: dict[str, object],
        target_range: TimeRange | None,
        export_id: str | None = None,
    ) -> ExportJobRecord:
        selected_export_id = export_id or uuid4().hex
        return ExportJobRecord(
            export_id=selected_export_id,
            status="pending",
            created_at_ms=_now_ms(),
            release_id=plan.release_id,
            query_kind=plan.query_kind.value,
            format="parquet",
            sql=plan.sql,
            files=plan.files,
            parameters=parameters,
            target_range=target_range,
        )

    def _persist_pending_job(self, record: ExportJobRecord) -> ExportJobRecord:
        path = self.layout.export_pending_request_path(record.export_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(record.to_json(), encoding="utf-8")
        self.runtime_logger.append(
            "export_job_submitted",
            {
                "export_id": record.export_id,
                "release_id": record.release_id,
                "query_kind": record.query_kind,
                "file_count": len(record.files),
                "parameters": record.parameters,
            },
            ts_ms=record.created_at_ms,
        )
        return record

    def execute_job_record(self, record: ExportJobRecord) -> ExportJobRecord:
        started_at_ms = _now_ms()
        query_id = uuid4().hex
        handle = QueryHandle(
            query_id=query_id,
            release_id=record.release_id,
            query_kind=QueryKind.EXPORT,
            started_at_ms=started_at_ms,
            target_range=record.target_range,
        )
        self.metadata_store.start_query(handle)
        output_path = self.layout.export_result_path(record.export_id)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_output_path = output_path.with_suffix(".tmp.parquet")
        try:
            row_count = self._copy_query_to_parquet(record.sql, temp_output_path, output_path)
        except Exception as exc:
            self.metadata_store.finish_query(query_id)
            temp_output_path.unlink(missing_ok=True)
            terminal = ExportJobRecord(
                export_id=record.export_id,
                status="failed",
                created_at_ms=record.created_at_ms,
                release_id=record.release_id,
                query_kind=record.query_kind,
                format=record.format,
                sql=record.sql,
                files=record.files,
                parameters=record.parameters,
                target_range=record.target_range,
                output_path=None,
                started_at_ms=started_at_ms,
                finished_at_ms=_now_ms(),
                row_count=None,
                error=str(exc),
            )
            self.runtime_logger.append(
                "export_job_failed",
                {
                    "export_id": record.export_id,
                    "release_id": record.release_id,
                    "query_kind": record.query_kind,
                    "error": str(exc),
                },
                ts_ms=terminal.finished_at_ms,
            )
            return terminal
        self.metadata_store.finish_query(query_id)
        terminal = ExportJobRecord(
            export_id=record.export_id,
            status="finished",
            created_at_ms=record.created_at_ms,
            release_id=record.release_id,
            query_kind=record.query_kind,
            format=record.format,
            sql=record.sql,
            files=record.files,
            parameters=record.parameters,
            target_range=record.target_range,
            output_path=str(output_path),
            started_at_ms=started_at_ms,
            finished_at_ms=_now_ms(),
            row_count=row_count,
            error=None,
        )
        self.runtime_logger.append(
            "export_job_finished",
            {
                "export_id": record.export_id,
                "release_id": record.release_id,
                "query_kind": record.query_kind,
                "row_count": row_count,
                "output_path": str(output_path),
            },
            ts_ms=terminal.finished_at_ms,
        )
        return terminal

    def _copy_query_to_parquet(self, sql: str, temp_output_path: Path, final_output_path: Path) -> int:
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "duckdb package is required to execute export jobs"
            ) from exc
        output_literal = self._sql_literal(str(temp_output_path))
        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS export_source"
        copy_sql = f"COPY ({sql}) TO {output_literal} (FORMAT PARQUET, COMPRESSION ZSTD)"
        connection = duckdb.connect()
        try:
            row_count = int(connection.execute(count_sql).fetchone()[0])
            connection.execute(copy_sql)
        finally:
            connection.close()
        final_output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_output_path.replace(final_output_path)
        return row_count

    def _sql_literal(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
