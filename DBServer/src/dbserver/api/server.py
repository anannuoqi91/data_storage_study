from __future__ import annotations

import argparse
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from dbserver.app import ApplicationSettings, DbserverApplication
from dbserver.api.schemas import (
    DeleteJobRequest,
    ExportJobRequest,
    IngestFramesRequest,
    IngestFramesResponse,
    JobAcceptedResponse,
    JobStatusResponse,
)
from dbserver.services.job_service import CallbackSpec, JobRecord


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_device_ids(raw: str | None) -> tuple[int, ...]:
    if raw is None or not raw.strip():
        return (1, 2, 3, 4)
    values = tuple(
        int(part.strip())
        for part in raw.split(",")
        if part.strip()
    )
    if not values:
        raise ValueError("cyber device ids must not be empty")
    return values


@dataclass(slots=True, frozen=True)
class ApiServerSettings:
    root: str | Path = "storage"
    schema_version_id: str | None = None
    host: str = "0.0.0.0"
    port: int = 8080
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
    log_level: str = "info"


def _job_response(record: JobRecord) -> JobStatusResponse:
    payload = record.to_public_dict()
    return JobStatusResponse(**payload)


def _callback_spec(callback: object) -> CallbackSpec | None:
    if callback is None:
        return None
    payload = callback.dict()
    return CallbackSpec(
        url=str(payload["url"]),
        headers={str(key): str(value) for key, value in dict(payload.get("headers", {})).items()},
        secret=None if payload.get("secret") is None else str(payload["secret"]),
    )


def create_app(settings: ApiServerSettings | None = None) -> FastAPI:
    active_settings = settings or ApiServerSettings()
    application = DbserverApplication(
        ApplicationSettings(
            root=active_settings.root,
            schema_version_id=active_settings.schema_version_id,
            publish_interval_seconds=active_settings.publish_interval_seconds,
            run_maintenance=active_settings.run_maintenance,
            maintenance_interval_seconds=active_settings.maintenance_interval_seconds,
            enable_cyber_ingest=active_settings.enable_cyber_ingest,
            cyber_device_ids=active_settings.cyber_device_ids,
            cyber_channel_template=active_settings.cyber_channel_template,
            cyber_node_name=active_settings.cyber_node_name,
            cyber_queue_maxsize=active_settings.cyber_queue_maxsize,
            run_job_worker=active_settings.run_job_worker,
            max_jobs_per_cycle=active_settings.max_jobs_per_cycle,
            run_export_worker=active_settings.run_export_worker,
            max_export_jobs_per_cycle=active_settings.max_export_jobs_per_cycle,
            callback_timeout_seconds=active_settings.callback_timeout_seconds,
            callback_retry_seconds=active_settings.callback_retry_seconds,
            callback_max_attempts=active_settings.callback_max_attempts,
            low_watermark_bytes=active_settings.low_watermark_bytes,
            high_watermark_bytes=active_settings.high_watermark_bytes,
            query_wait_timeout_seconds=active_settings.query_wait_timeout_seconds,
            query_poll_interval_seconds=active_settings.query_poll_interval_seconds,
            mode="api",
        )
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        application.start()
        try:
            yield
        finally:
            application.stop()

    app = FastAPI(
        title="DBServer API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.dbserver_application = application

    def _application(request: Request) -> DbserverApplication:
        return request.app.state.dbserver_application

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        payload, status_code = _application(request).health_payload()
        return JSONResponse(payload, status_code=status_code)

    @app.get("/status")
    @app.get("/v1/status")
    async def status(request: Request) -> JSONResponse:
        return JSONResponse(_application(request).status_snapshot())

    @app.get("/metrics")
    @app.get("/v1/metrics")
    async def metrics(request: Request) -> PlainTextResponse:
        return PlainTextResponse(
            _application(request).metrics_text(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.post("/v1/ingest/box-frames", response_model=IngestFramesResponse)
    async def ingest_box_frames(request: Request, payload: IngestFramesRequest) -> IngestFramesResponse:
        if not payload.frames:
            raise HTTPException(status_code=400, detail="frames payload must not be empty")
        result = _application(request).ingest_frames(
            tuple(frame.to_frame_batch() for frame in payload.frames),
            source=payload.source,
        )
        return IngestFramesResponse(
            accepted=result.accepted,
            accepted_at_ms=result.accepted_at_ms,
            frames_total=result.frames_total,
            frames_accepted=result.frames_accepted,
            frames_rejected=result.frames_rejected,
            boxes_accepted=result.boxes_accepted,
            buffered_windows=result.buffered_windows,
            buffered_boxes=result.buffered_boxes,
        )

    @app.post("/v1/jobs/export", response_model=JobAcceptedResponse, status_code=202)
    async def submit_export_job(request: Request, payload: ExportJobRequest) -> JobAcceptedResponse:
        if payload.export_kind in {"detail", "rollup"}:
            if payload.start_ms is None or payload.end_ms is None:
                raise HTTPException(status_code=400, detail="start_ms and end_ms are required")
        if payload.export_kind == "trace" and payload.trace_id is None:
            raise HTTPException(status_code=400, detail="trace_id is required for trace export")
        result = _application(request).submit_export_job(
            export_kind=payload.export_kind,
            device_id=payload.device_id,
            start_ms=payload.start_ms,
            end_ms=payload.end_ms,
            trace_id=payload.trace_id,
            lane_id=payload.lane_id,
            obj_type=payload.obj_type,
            request_id=payload.request_id,
            callback=_callback_spec(payload.callback),
        )
        return JobAcceptedResponse(
            job_id=result.record.job_id,
            job_type=result.record.job_type.value,
            status=result.record.status.value,
            accepted_at_ms=result.record.created_at_ms,
            request_id=result.record.request_id,
            reused_existing=result.reused_existing,
        )

    @app.post("/v1/jobs/delete", response_model=JobAcceptedResponse, status_code=202)
    async def submit_delete_job(request: Request, payload: DeleteJobRequest) -> JobAcceptedResponse:
        result = _application(request).submit_delete_job(
            before_ms=payload.before_ms,
            request_id=payload.request_id,
            query_wait_timeout_seconds=payload.query_wait_timeout_seconds,
            query_poll_interval_seconds=payload.query_poll_interval_seconds,
            callback=_callback_spec(payload.callback),
        )
        return JobAcceptedResponse(
            job_id=result.record.job_id,
            job_type=result.record.job_type.value,
            status=result.record.status.value,
            accepted_at_ms=result.record.created_at_ms,
            request_id=result.record.request_id,
            reused_existing=result.reused_existing,
        )

    @app.get("/v1/jobs", response_model=list[JobStatusResponse])
    async def list_jobs(request: Request) -> list[JobStatusResponse]:
        return [_job_response(record) for record in _application(request).list_jobs()]

    @app.get("/v1/jobs/{job_id}", response_model=JobStatusResponse)
    async def get_job(request: Request, job_id: str) -> JobStatusResponse:
        record = _application(request).get_job(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_response(record)

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DBServer REST API server")
    parser.add_argument("--root", default=os.environ.get("DBSERVER_ROOT", "storage"))
    parser.add_argument("--schema-version-id", default=os.environ.get("DBSERVER_SCHEMA_VERSION_ID"))
    parser.add_argument("--host", default=os.environ.get("DBSERVER_API_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DBSERVER_API_PORT", "8080")))
    parser.add_argument(
        "--publish-interval-seconds",
        type=float,
        default=float(os.environ.get("DBSERVER_PUBLISH_INTERVAL_SECONDS", "1.0")),
    )
    parser.add_argument(
        "--maintenance-interval-seconds",
        type=float,
        default=float(os.environ.get("DBSERVER_MAINTENANCE_INTERVAL_SECONDS", "60.0")),
    )
    parser.add_argument(
        "--enable-cyber-ingest",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("DBSERVER_ENABLE_CYBER_INGEST", False),
    )
    parser.add_argument(
        "--cyber-device-ids",
        default=os.environ.get("DBSERVER_CYBER_DEVICE_IDS", "1,2,3,4"),
    )
    parser.add_argument(
        "--cyber-channel-template",
        default=os.environ.get(
            "DBSERVER_CYBER_CHANNEL_TEMPLATE",
            "omnisense/test/{device_id}/boxes",
        ),
    )
    parser.add_argument(
        "--cyber-node-name",
        default=os.environ.get("DBSERVER_CYBER_NODE_NAME", "dbserver_cyber_ingest"),
    )
    parser.add_argument(
        "--cyber-queue-maxsize",
        type=int,
        default=int(os.environ.get("DBSERVER_CYBER_QUEUE_MAXSIZE", "1024")),
    )
    parser.add_argument(
        "--run-job-worker",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("DBSERVER_ENABLE_JOB_WORKER", True),
    )
    parser.add_argument(
        "--max-jobs-per-cycle",
        type=int,
        default=int(os.environ.get("DBSERVER_MAX_JOBS_PER_CYCLE", "1")),
    )
    parser.add_argument(
        "--run-maintenance",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("DBSERVER_ENABLE_MAINTENANCE", True),
    )
    parser.add_argument(
        "--run-export-worker",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("DBSERVER_ENABLE_EXPORT_WORKER", False),
    )
    parser.add_argument(
        "--max-export-jobs-per-cycle",
        type=int,
        default=int(os.environ.get("DBSERVER_MAX_EXPORT_JOBS_PER_CYCLE", "1")),
    )
    parser.add_argument(
        "--callback-timeout-seconds",
        type=float,
        default=float(os.environ.get("DBSERVER_CALLBACK_TIMEOUT_SECONDS", "5.0")),
    )
    parser.add_argument(
        "--callback-retry-seconds",
        type=float,
        default=float(os.environ.get("DBSERVER_CALLBACK_RETRY_SECONDS", "30.0")),
    )
    parser.add_argument(
        "--callback-max-attempts",
        type=int,
        default=int(os.environ.get("DBSERVER_CALLBACK_MAX_ATTEMPTS", "5")),
    )
    parser.add_argument(
        "--low-watermark-bytes",
        type=int,
        default=(
            None
            if os.environ.get("DBSERVER_LOW_WATERMARK_BYTES") is None
            else int(os.environ["DBSERVER_LOW_WATERMARK_BYTES"])
        ),
    )
    parser.add_argument(
        "--high-watermark-bytes",
        type=int,
        default=(
            None
            if os.environ.get("DBSERVER_HIGH_WATERMARK_BYTES") is None
            else int(os.environ["DBSERVER_HIGH_WATERMARK_BYTES"])
        ),
    )
    parser.add_argument(
        "--query-wait-timeout-seconds",
        type=float,
        default=float(os.environ.get("DBSERVER_QUERY_WAIT_TIMEOUT_SECONDS", "0.0")),
    )
    parser.add_argument(
        "--query-poll-interval-seconds",
        type=float,
        default=float(os.environ.get("DBSERVER_QUERY_POLL_INTERVAL_SECONDS", "0.5")),
    )
    parser.add_argument("--log-level", default=os.environ.get("DBSERVER_LOG_LEVEL", "info"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = ApiServerSettings(
        root=args.root,
        schema_version_id=args.schema_version_id,
        host=args.host,
        port=args.port,
        publish_interval_seconds=args.publish_interval_seconds,
        run_maintenance=args.run_maintenance,
        maintenance_interval_seconds=args.maintenance_interval_seconds,
        enable_cyber_ingest=args.enable_cyber_ingest,
        cyber_device_ids=_parse_device_ids(args.cyber_device_ids),
        cyber_channel_template=args.cyber_channel_template,
        cyber_node_name=args.cyber_node_name,
        cyber_queue_maxsize=args.cyber_queue_maxsize,
        run_job_worker=args.run_job_worker,
        max_jobs_per_cycle=args.max_jobs_per_cycle,
        run_export_worker=args.run_export_worker,
        max_export_jobs_per_cycle=args.max_export_jobs_per_cycle,
        callback_timeout_seconds=args.callback_timeout_seconds,
        callback_retry_seconds=args.callback_retry_seconds,
        callback_max_attempts=args.callback_max_attempts,
        low_watermark_bytes=args.low_watermark_bytes,
        high_watermark_bytes=args.high_watermark_bytes,
        query_wait_timeout_seconds=args.query_wait_timeout_seconds,
        query_poll_interval_seconds=args.query_poll_interval_seconds,
        log_level=args.log_level,
    )
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
