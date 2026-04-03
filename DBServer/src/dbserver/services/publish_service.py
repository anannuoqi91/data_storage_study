from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from time import time_ns
from uuid import uuid4

from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import DatasetKind, PartitionKey, PartitionState, ReleaseEventType, ReleaseRecord
from dbserver.services.derived_data_service import DerivedDataService
from dbserver.services.ingest_service import PublishPlan
from dbserver.storage.layout import StorageLayout
from dbserver.storage.parquet_writer import BoxInfoParquetWriter


def _now_ms() -> int:
    return time_ns() // 1_000_000


class PublishService:
    def __init__(
        self,
        metadata_store: MetadataStore,
        journal: ReleaseJournal,
        box_info_writer: BoxInfoParquetWriter,
        derived_data_service: DerivedDataService,
        layout: StorageLayout,
    ) -> None:
        self.metadata_store = metadata_store
        self.journal = journal
        self.box_info_writer = box_info_writer
        self.derived_data_service = derived_data_service
        self.layout = layout

    def publish_box_info_plan(
        self,
        plan: PublishPlan,
        *,
        schema_version_id: str,
        retention_floor_ms: int | None = None,
        note: str = "publish box_info batch",
    ) -> ReleaseRecord:
        if (
            plan.box_info_group.date_utc is None
            or plan.box_info_group.hour_utc is None
            or plan.box_info_group.device_id is None
        ):
            raise ValueError("publish plan is missing partition coordinates")

        partition = PartitionKey(
            date_utc=plan.box_info_group.date_utc,
            hour_utc=plan.box_info_group.hour_utc,
            device_id=plan.box_info_group.device_id,
        )
        live_path = Path(plan.box_info_group.path_list[0])
        staged_path = self.layout.staging_file_path(
            DatasetKind.BOX_INFO,
            partition,
            plan.box_info_group.file_group_id,
        )
        result = self.box_info_writer.write_records(output_path=staged_path, records=plan.records)
        try:
            self.box_info_writer.promote_staged_file(result.path, live_path)
        except Exception:
            staged_path.unlink(missing_ok=True)
            raise
        file_group = replace(
            plan.box_info_group,
            row_count=result.row_count,
            total_bytes=live_path.stat().st_size,
        )
        derived_artifacts = self.derived_data_service.build_from_records(
            source_group=file_group,
            partition=partition,
            records=plan.records,
            watermark_ms=plan.window.window_end_ms,
        )
        parent_release_id = self.metadata_store.get_current_release()
        existing_state = self.metadata_store.get_partition_state(partition)
        effective_retention_floor_ms = (
            self.metadata_store.get_current_retention_floor_ms()
            if retention_floor_ms is None
            else retention_floor_ms
        )
        release = ReleaseRecord(
            release_id=uuid4().hex,
            parent_release_id=parent_release_id,
            schema_version_id=schema_version_id,
            created_at_ms=_now_ms(),
            event_type=ReleaseEventType.PUBLISH,
            retention_floor_ms=effective_retention_floor_ms,
            note=note,
        )
        self.metadata_store.create_release_from_current(
            release,
            add_groups=(file_group,) + derived_artifacts.all_groups,
        )
        last_event_time_ms = file_group.max_event_time_ms
        if (
            existing_state is not None
            and existing_state.last_event_time_ms is not None
            and (last_event_time_ms is None or existing_state.last_event_time_ms > last_event_time_ms)
        ):
            last_event_time_ms = existing_state.last_event_time_ms
        watermark_ms = file_group.window_end_ms
        if (
            existing_state is not None
            and existing_state.watermark_ms is not None
            and (watermark_ms is None or existing_state.watermark_ms > watermark_ms)
        ):
            watermark_ms = existing_state.watermark_ms
        self.metadata_store.upsert_partition_state(
            partition,
            PartitionState.PUBLISHABLE,
            last_event_time_ms=last_event_time_ms,
            watermark_ms=watermark_ms,
            last_publish_release_id=release.release_id,
            last_seal_release_id=None if existing_state is None else existing_state.last_seal_release_id,
        )
        self.journal.append_event(
            "release_created",
            {
                "release_id": release.release_id,
                "parent_release_id": parent_release_id,
                "event_type": release.event_type.value,
                "schema_version_id": schema_version_id,
                "file_group_ids": [
                    file_group.file_group_id,
                    *[group.file_group_id for group in derived_artifacts.all_groups],
                ],
            },
            ts_ms=release.created_at_ms,
        )
        self.journal.append_event(
            "current_release_switched",
            {
                "release_id": release.release_id,
                "parent_release_id": parent_release_id,
                "event_type": release.event_type.value,
            },
            ts_ms=release.created_at_ms,
        )
        self.journal.write_checkpoint(
            {
                "current_release_id": release.release_id,
                "retention_floor_ms": release.retention_floor_ms,
            },
            ts_ms=release.created_at_ms,
        )
        return release
