from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from dbserver.domain.records import BoxRecord, FrameBatch


class BoxPayload(BaseModel):
    trace_id: int
    obj_type: int
    position_x_mm: int
    position_y_mm: int
    position_z_mm: int
    length_mm: int
    width_mm: int
    height_mm: int
    speed_centi_kmh_100: int
    spindle_centi_deg_100: int
    lane_id: int | None = None


class FramePayload(BaseModel):
    device_id: int
    frame_id: int
    event_time_ms: int
    boxes: list[BoxPayload] = Field(default_factory=list)

    def to_frame_batch(self) -> FrameBatch:
        records = tuple(
            BoxRecord(
                trace_id=box.trace_id,
                device_id=self.device_id,
                event_time_ms=self.event_time_ms,
                obj_type=box.obj_type,
                position_x_mm=box.position_x_mm,
                position_y_mm=box.position_y_mm,
                position_z_mm=box.position_z_mm,
                length_mm=box.length_mm,
                width_mm=box.width_mm,
                height_mm=box.height_mm,
                speed_centi_kmh_100=box.speed_centi_kmh_100,
                spindle_centi_deg_100=box.spindle_centi_deg_100,
                lane_id=box.lane_id,
                frame_id=self.frame_id,
            )
            for box in self.boxes
        )
        return FrameBatch(
            device_id=self.device_id,
            frame_id=self.frame_id,
            event_time_ms=self.event_time_ms,
            records=records,
        )


class IngestFramesRequest(BaseModel):
    source: str = "rest"
    frames: list[FramePayload] = Field(default_factory=list)


class IngestFramesResponse(BaseModel):
    accepted: bool
    accepted_at_ms: int
    frames_total: int
    frames_accepted: int
    frames_rejected: int
    boxes_accepted: int
    buffered_windows: int
    buffered_boxes: int


class CallbackRequest(BaseModel):
    url: str
    headers: dict[str, str] = Field(default_factory=dict)
    secret: str | None = None


class ExportJobRequest(BaseModel):
    request_id: str | None = None
    export_kind: Literal["detail", "trace", "rollup"]
    device_id: int
    start_ms: int | None = None
    end_ms: int | None = None
    trace_id: int | None = None
    lane_id: int | None = None
    obj_type: int | None = None
    callback: CallbackRequest | None = None


class DeleteJobRequest(BaseModel):
    request_id: str | None = None
    before_ms: int
    query_wait_timeout_seconds: float = 0.0
    query_poll_interval_seconds: float = 0.5
    callback: CallbackRequest | None = None


class JobAcceptedResponse(BaseModel):
    job_id: str
    job_type: str
    status: str
    accepted_at_ms: int
    request_id: str | None = None
    reused_existing: bool = False


class JobCallbackStatusResponse(BaseModel):
    configured: bool
    status: str
    attempt_count: int
    next_attempt_at_ms: int | None = None
    delivered_at_ms: int | None = None
    last_error: str | None = None
    last_status_code: int | None = None
    url: str | None = None


class JobStatusResponse(BaseModel):
    job_id: str
    job_type: str
    status: str
    created_at_ms: int
    request_id: str | None = None
    parameters: dict[str, object]
    result: dict[str, object] | None = None
    error: str | None = None
    started_at_ms: int | None = None
    finished_at_ms: int | None = None
    callback: JobCallbackStatusResponse
