from __future__ import annotations

from dataclasses import dataclass

from dbserver.services.derived_data_service import DerivedDataService, TraceIndexCoolingResult
from dbserver.services.retention_service import RetentionRunResult, RetentionService
from dbserver.services.seal_service import SealResult, SealService


@dataclass(slots=True, frozen=True)
class MaintenanceRunResult:
    seal_result: SealResult
    trace_index_cooling_result: TraceIndexCoolingResult
    retention_result: RetentionRunResult | None


class MaintenanceService:
    def __init__(
        self,
        seal_service: SealService,
        derived_data_service: DerivedDataService,
        retention_service: RetentionService,
    ) -> None:
        self.seal_service = seal_service
        self.derived_data_service = derived_data_service
        self.retention_service = retention_service

    def run_cycle(
        self,
        *,
        schema_version_id: str,
        now_ms: int | None = None,
        low_watermark_bytes: int | None = None,
        high_watermark_bytes: int | None = None,
        query_wait_timeout_ms: int = 0,
        query_poll_interval_ms: int = 500,
        seal_note: str = "maintenance seal ready hours",
        trace_cooling_note: str = "maintenance trace_index hot-to-cold",
        retention_note: str = "maintenance automatic retention cleanup",
    ) -> MaintenanceRunResult:
        seal_result = self.seal_service.seal_ready_partitions(
            schema_version_id=schema_version_id,
            now_ms=now_ms,
            note=seal_note,
        )
        retention_result: RetentionRunResult | None = None
        if low_watermark_bytes is not None and high_watermark_bytes is not None:
            retention_result = self.retention_service.enforce_watermarks(
                schema_version_id=schema_version_id,
                low_watermark_bytes=low_watermark_bytes,
                high_watermark_bytes=high_watermark_bytes,
                query_wait_timeout_ms=query_wait_timeout_ms,
                query_poll_interval_ms=query_poll_interval_ms,
                note=retention_note,
            )
        trace_index_cooling_result = self.derived_data_service.cool_ready_trace_indexes(
            schema_version_id=schema_version_id,
            now_ms=now_ms,
            note=trace_cooling_note,
        )
        return MaintenanceRunResult(
            seal_result=seal_result,
            trace_index_cooling_result=trace_index_cooling_result,
            retention_result=retention_result,
        )
