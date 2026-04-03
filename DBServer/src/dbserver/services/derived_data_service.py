from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2
from time import time_ns
from uuid import uuid4

from dbserver.config import AppConfig
from dbserver.domain.records import BoxRecord
from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import (
    DatasetKind,
    FileGroupRecord,
    FileGroupState,
    PartitionKey,
    ReleaseEventType,
    ReleaseRecord,
)
from dbserver.storage.layout import StorageLayout
from dbserver.storage.structured_parquet_writer import StructuredParquetWriter

TRACE_SEGMENT_GAP_MS = 150

TRACE_INDEX_SCHEMA = (
    ("device_id", "USMALLINT"),
    ("trace_id", "INTEGER"),
    ("segment_id", "VARCHAR"),
    ("min_event_time_ms", "BIGINT"),
    ("max_event_time_ms", "BIGINT"),
    ("start_frame_id", "INTEGER"),
    ("end_frame_id", "INTEGER"),
    ("row_count", "INTEGER"),
    ("state", "VARCHAR"),
)

ROLLUP_SCHEMA = (
    ("device_id", "USMALLINT"),
    ("lane_id", "UTINYINT"),
    ("bucket_start_ms", "BIGINT"),
    ("obj_type", "UTINYINT"),
    ("box_count", "BIGINT"),
    ("trace_count", "BIGINT"),
)


def _now_ms() -> int:
    return time_ns() // 1_000_000


@dataclass(slots=True, frozen=True)
class DerivedArtifacts:
    trace_index_groups: tuple[FileGroupRecord, ...]
    rollup_groups: tuple[FileGroupRecord, ...]

    @property
    def all_groups(self) -> tuple[FileGroupRecord, ...]:
        return self.trace_index_groups + self.rollup_groups


@dataclass(slots=True, frozen=True)
class RebuildDerivedResult:
    triggered: bool
    rebuilt_trace_index_groups: int
    rebuilt_rollup_groups: int
    removed_group_count: int
    new_release_id: str | None
    reason: str


@dataclass(slots=True, frozen=True)
class TraceIndexCoolingResult:
    triggered: bool
    cooled_group_count: int
    retained_hot_group_count: int
    new_release_id: str | None
    reason: str


class DerivedDataService:
    def __init__(
        self,
        config: AppConfig,
        metadata_store: MetadataStore,
        journal: ReleaseJournal,
        layout: StorageLayout,
        writer: StructuredParquetWriter,
    ) -> None:
        self.config = config
        self.metadata_store = metadata_store
        self.journal = journal
        self.layout = layout
        self.writer = writer

    def build_from_records(
        self,
        *,
        source_group: FileGroupRecord,
        partition: PartitionKey,
        records: tuple[BoxRecord, ...],
        watermark_ms: int,
        now_ms: int | None = None,
    ) -> DerivedArtifacts:
        trace_rows = self._build_trace_index_rows(
            records=records,
            source_file_group_id=source_group.file_group_id,
            watermark_ms=watermark_ms,
        )
        rollup_rows = self._build_rollup_rows(records=records, source_file_group_id=source_group.file_group_id)

        trace_kind = self._trace_dataset_kind(
            max_event_time_ms=source_group.max_event_time_ms or source_group.window_end_ms or watermark_ms,
            now_ms=now_ms,
        )
        trace_group_id = uuid4().hex
        trace_output_path = self.layout.live_file_path(trace_kind, partition, trace_group_id)
        trace_staging_path = self.layout.staging_file_path(trace_kind, partition, trace_group_id)
        trace_result = self.writer.write_rows(
            trace_staging_path,
            rows=trace_rows,
            schema=TRACE_INDEX_SCHEMA,
            order_by=("device_id", "trace_id", "min_event_time_ms", "start_frame_id", "segment_id"),
        )
        trace_output_path.parent.mkdir(parents=True, exist_ok=True)
        trace_result.path.replace(trace_output_path)

        rollup_group_id = uuid4().hex
        rollup_output_path = self.layout.live_file_path(DatasetKind.OBJ_TYPE_ROLLUP, partition, rollup_group_id)
        rollup_staging_path = self.layout.staging_file_path(
            DatasetKind.OBJ_TYPE_ROLLUP,
            partition,
            rollup_group_id,
        )
        rollup_result = self.writer.write_rows(
            rollup_staging_path,
            rows=rollup_rows,
            schema=ROLLUP_SCHEMA,
            order_by=("bucket_start_ms", "lane_id", "obj_type"),
        )
        rollup_output_path.parent.mkdir(parents=True, exist_ok=True)
        rollup_result.path.replace(rollup_output_path)

        trace_group = FileGroupRecord(
            file_group_id=trace_group_id,
            dataset_kind=trace_kind,
            device_id=partition.device_id,
            date_utc=partition.date_utc,
            hour_utc=partition.hour_utc,
            window_start_ms=source_group.window_start_ms,
            window_end_ms=source_group.window_end_ms,
            min_event_time_ms=source_group.min_event_time_ms,
            max_event_time_ms=source_group.max_event_time_ms,
            row_count=trace_result.row_count,
            file_count=1,
            total_bytes=trace_output_path.stat().st_size,
            state=source_group.state,
            path_list=(str(trace_output_path),),
            created_at_ms=source_group.created_at_ms,
        )
        rollup_group = FileGroupRecord(
            file_group_id=rollup_group_id,
            dataset_kind=DatasetKind.OBJ_TYPE_ROLLUP,
            device_id=partition.device_id,
            date_utc=partition.date_utc,
            hour_utc=partition.hour_utc,
            window_start_ms=source_group.window_start_ms,
            window_end_ms=source_group.window_end_ms,
            min_event_time_ms=source_group.min_event_time_ms,
            max_event_time_ms=source_group.max_event_time_ms,
            row_count=rollup_result.row_count,
            file_count=1,
            total_bytes=rollup_output_path.stat().st_size,
            state=source_group.state,
            path_list=(str(rollup_output_path),),
            created_at_ms=source_group.created_at_ms,
        )
        return DerivedArtifacts(
            trace_index_groups=(trace_group,),
            rollup_groups=(rollup_group,),
        )

    def build_from_file_group(
        self,
        source_group: FileGroupRecord,
        *,
        now_ms: int | None = None,
        watermark_ms: int | None = None,
    ) -> DerivedArtifacts:
        if source_group.dataset_kind is not DatasetKind.BOX_INFO:
            raise ValueError("derived data can only be built from box_info file groups")
        if source_group.date_utc is None or source_group.hour_utc is None or source_group.device_id is None:
            raise ValueError("source group is missing partition coordinates")
        records = self._read_box_records(source_group.path_list)
        partition = PartitionKey(
            date_utc=source_group.date_utc,
            hour_utc=source_group.hour_utc,
            device_id=source_group.device_id,
        )
        effective_watermark_ms = watermark_ms
        if effective_watermark_ms is None:
            current_ms = _now_ms() if now_ms is None else now_ms
            effective_watermark_ms = (
                current_ms - self.config.runtime.late_arrival_window_seconds * 1000
            )
        return self.build_from_records(
            source_group=source_group,
            partition=partition,
            records=records,
            watermark_ms=effective_watermark_ms,
            now_ms=now_ms,
        )

    def rebuild_current_release(
        self,
        *,
        schema_version_id: str,
        rebuild_trace_index: bool,
        rebuild_rollup: bool,
        now_ms: int | None = None,
        note: str = "manual rebuild derived datasets",
    ) -> RebuildDerivedResult:
        current_release_id = self.metadata_store.get_current_release()
        if current_release_id is None:
            return RebuildDerivedResult(
                triggered=False,
                rebuilt_trace_index_groups=0,
                rebuilt_rollup_groups=0,
                removed_group_count=0,
                new_release_id=None,
                reason="no current release exists",
            )
        current_groups = self.metadata_store.list_release_file_groups(current_release_id)
        box_groups = [group for group in current_groups if group.dataset_kind is DatasetKind.BOX_INFO]
        if not box_groups:
            return RebuildDerivedResult(
                triggered=False,
                rebuilt_trace_index_groups=0,
                rebuilt_rollup_groups=0,
                removed_group_count=0,
                new_release_id=None,
                reason="no box_info groups found in current release",
            )

        target_kinds: set[DatasetKind] = set()
        if rebuild_trace_index:
            target_kinds.update({DatasetKind.TRACE_INDEX_HOT, DatasetKind.TRACE_INDEX_COLD})
        if rebuild_rollup:
            target_kinds.add(DatasetKind.OBJ_TYPE_ROLLUP)
        if not target_kinds:
            return RebuildDerivedResult(
                triggered=False,
                rebuilt_trace_index_groups=0,
                rebuilt_rollup_groups=0,
                removed_group_count=0,
                new_release_id=None,
                reason="no derived dataset kind selected for rebuild",
            )

        add_groups: list[FileGroupRecord] = []
        remove_groups = [group for group in current_groups if group.dataset_kind in target_kinds]
        for box_group in box_groups:
            artifacts = self.build_from_file_group(box_group, now_ms=now_ms)
            if rebuild_trace_index:
                add_groups.extend(artifacts.trace_index_groups)
            if rebuild_rollup:
                add_groups.extend(artifacts.rollup_groups)

        release = ReleaseRecord(
            release_id=uuid4().hex,
            parent_release_id=current_release_id,
            schema_version_id=schema_version_id,
            created_at_ms=_now_ms(),
            event_type=ReleaseEventType.REBUILD,
            retention_floor_ms=self.metadata_store.get_current_retention_floor_ms(),
            note=note,
        )
        remove_group_ids = [group.file_group_id for group in remove_groups]
        self.metadata_store.create_release_from_current(
            release,
            add_groups=tuple(add_groups),
            remove_file_group_ids=tuple(remove_group_ids),
        )
        invalidated_release_ids = self.metadata_store.delete_releases_referencing_file_groups(
            remove_group_ids,
            keep_release_ids=(release.release_id,),
        )
        self.metadata_store.delete_file_groups(remove_group_ids)
        deleted_bytes = self._delete_group_files(remove_groups)
        self.journal.append_event(
            "derived_datasets_rebuilt",
            {
                "release_id": release.release_id,
                "parent_release_id": current_release_id,
                "rebuilt_trace_index_groups": sum(
                    1
                    for group in add_groups
                    if group.dataset_kind in (DatasetKind.TRACE_INDEX_HOT, DatasetKind.TRACE_INDEX_COLD)
                ),
                "rebuilt_rollup_groups": sum(
                    1 for group in add_groups if group.dataset_kind is DatasetKind.OBJ_TYPE_ROLLUP
                ),
                "removed_group_ids": remove_group_ids,
                "invalidated_release_ids": invalidated_release_ids,
                "deleted_bytes": deleted_bytes,
            },
            ts_ms=release.created_at_ms,
        )
        self.journal.append_event(
            "current_release_switched",
            {
                "release_id": release.release_id,
                "parent_release_id": current_release_id,
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
        return RebuildDerivedResult(
            triggered=True,
            rebuilt_trace_index_groups=sum(
                1
                for group in add_groups
                if group.dataset_kind in (DatasetKind.TRACE_INDEX_HOT, DatasetKind.TRACE_INDEX_COLD)
            ),
            rebuilt_rollup_groups=sum(
                1 for group in add_groups if group.dataset_kind is DatasetKind.OBJ_TYPE_ROLLUP
            ),
            removed_group_count=len(remove_groups),
            new_release_id=release.release_id,
            reason="rebuilt derived datasets from current box_info release",
        )

    def cool_ready_trace_indexes(
        self,
        *,
        schema_version_id: str,
        now_ms: int | None = None,
        note: str = "automatic trace_index hot-to-cold",
    ) -> TraceIndexCoolingResult:
        current_release_id = self.metadata_store.get_current_release()
        if current_release_id is None:
            return TraceIndexCoolingResult(
                triggered=False,
                cooled_group_count=0,
                retained_hot_group_count=0,
                new_release_id=None,
                reason="no current release exists",
            )
        current_groups = self.metadata_store.list_release_file_groups(current_release_id)
        hot_groups = [group for group in current_groups if group.dataset_kind is DatasetKind.TRACE_INDEX_HOT]
        if not hot_groups:
            return TraceIndexCoolingResult(
                triggered=False,
                cooled_group_count=0,
                retained_hot_group_count=0,
                new_release_id=None,
                reason="no hot trace_index groups found in current release",
            )
        cutoff_ms = self._hot_cutoff_ms(now_ms)
        cooldown_groups = [
            group
            for group in hot_groups
            if group.max_event_time_ms is not None and group.max_event_time_ms < cutoff_ms
        ]
        if not cooldown_groups:
            return TraceIndexCoolingResult(
                triggered=False,
                cooled_group_count=0,
                retained_hot_group_count=len(hot_groups),
                new_release_id=None,
                reason="no trace_index hot groups are older than the cooling cutoff",
            )

        add_groups: list[FileGroupRecord] = []
        copied_paths: list[Path] = []
        created_at_ms = _now_ms()
        try:
            for group in cooldown_groups:
                partition = self._partition_from_group(group)
                target_group_id = uuid4().hex
                copied_group_paths: list[str] = []
                total_bytes = 0
                for index, raw_path in enumerate(group.path_list):
                    source_path = Path(raw_path)
                    target_path = self._trace_index_cold_target_path(
                        partition=partition,
                        file_group_id=target_group_id,
                        path_index=index,
                    )
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    copy2(source_path, target_path)
                    copied_paths.append(target_path)
                    copied_group_paths.append(str(target_path))
                    total_bytes += target_path.stat().st_size
                add_groups.append(
                    FileGroupRecord(
                        file_group_id=target_group_id,
                        dataset_kind=DatasetKind.TRACE_INDEX_COLD,
                        device_id=group.device_id,
                        date_utc=group.date_utc,
                        hour_utc=group.hour_utc,
                        window_start_ms=group.window_start_ms,
                        window_end_ms=group.window_end_ms,
                        min_event_time_ms=group.min_event_time_ms,
                        max_event_time_ms=group.max_event_time_ms,
                        row_count=group.row_count,
                        file_count=len(copied_group_paths),
                        total_bytes=total_bytes,
                        state=group.state,
                        path_list=tuple(copied_group_paths),
                        created_at_ms=created_at_ms,
                    )
                )
        except Exception:
            for path in copied_paths:
                path.unlink(missing_ok=True)
            raise

        release = ReleaseRecord(
            release_id=uuid4().hex,
            parent_release_id=current_release_id,
            schema_version_id=schema_version_id,
            created_at_ms=created_at_ms,
            event_type=ReleaseEventType.TRACE_INDEX_COOL,
            retention_floor_ms=self.metadata_store.get_current_retention_floor_ms(),
            note=note,
        )
        remove_group_ids = [group.file_group_id for group in cooldown_groups]
        self.metadata_store.create_release_from_current(
            release,
            add_groups=tuple(add_groups),
            remove_file_group_ids=tuple(remove_group_ids),
        )
        invalidated_release_ids = self.metadata_store.delete_releases_referencing_file_groups(
            remove_group_ids,
            keep_release_ids=(release.release_id,),
        )
        self.metadata_store.delete_file_groups(remove_group_ids)
        deleted_bytes = self._delete_group_files(cooldown_groups)
        self.journal.append_event(
            "trace_index_cooled",
            {
                "release_id": release.release_id,
                "parent_release_id": current_release_id,
                "cooling_cutoff_ms": cutoff_ms,
                "cooled_group_count": len(cooldown_groups),
                "retained_hot_group_count": len(hot_groups) - len(cooldown_groups),
                "removed_group_ids": remove_group_ids,
                "new_group_ids": [group.file_group_id for group in add_groups],
                "invalidated_release_ids": invalidated_release_ids,
                "deleted_bytes": deleted_bytes,
            },
            ts_ms=release.created_at_ms,
        )
        self.journal.append_event(
            "current_release_switched",
            {
                "release_id": release.release_id,
                "parent_release_id": current_release_id,
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
        return TraceIndexCoolingResult(
            triggered=True,
            cooled_group_count=len(cooldown_groups),
            retained_hot_group_count=len(hot_groups) - len(cooldown_groups),
            new_release_id=release.release_id,
            reason="cooled eligible trace_index hot groups into cold storage",
        )

    def seal_partition_groups(
        self,
        *,
        partition: PartitionKey,
        groups_by_kind: dict[DatasetKind, list[FileGroupRecord]],
        created_at_ms: int,
    ) -> tuple[FileGroupRecord, ...]:
        sealed_groups: list[FileGroupRecord] = []
        created_paths: list[Path] = []
        try:
            for dataset_kind in (
                DatasetKind.TRACE_INDEX_HOT,
                DatasetKind.TRACE_INDEX_COLD,
                DatasetKind.OBJ_TYPE_ROLLUP,
            ):
                members = groups_by_kind.get(dataset_kind, [])
                if not members:
                    continue
                sealed_group = self._compact_partition_groups(
                    partition=partition,
                    dataset_kind=dataset_kind,
                    members=members,
                    created_at_ms=created_at_ms,
                )
                sealed_groups.append(sealed_group)
                created_paths.extend(Path(path) for path in sealed_group.path_list)
        except Exception:
            for path in created_paths:
                path.unlink(missing_ok=True)
            raise
        return tuple(sealed_groups)

    def _build_trace_index_rows(
        self,
        *,
        records: tuple[BoxRecord, ...],
        source_file_group_id: str,
        watermark_ms: int,
    ) -> tuple[dict[str, object], ...]:
        by_trace: dict[int, list[BoxRecord]] = defaultdict(list)
        for record in records:
            by_trace[record.trace_id].append(record)

        rows: list[dict[str, object]] = []
        for trace_id in sorted(by_trace):
            ordered = sorted(
                by_trace[trace_id],
                key=lambda record: (record.event_time_ms, record.frame_id, record.trace_id),
            )
            segment_index = 1
            segment_records: list[BoxRecord] = []
            previous: BoxRecord | None = None
            for record in ordered:
                if previous is not None and (
                    record.frame_id < previous.frame_id
                    or record.event_time_ms - previous.event_time_ms > TRACE_SEGMENT_GAP_MS
                ):
                    rows.append(
                        self._trace_row_from_segment(
                            segment_records=tuple(segment_records),
                            source_file_group_id=source_file_group_id,
                            segment_index=segment_index,
                            watermark_ms=watermark_ms,
                        )
                    )
                    segment_index += 1
                    segment_records = []
                segment_records.append(record)
                previous = record
            if segment_records:
                rows.append(
                    self._trace_row_from_segment(
                        segment_records=tuple(segment_records),
                        source_file_group_id=source_file_group_id,
                        segment_index=segment_index,
                        watermark_ms=watermark_ms,
                    )
                )
        return tuple(rows)

    def _trace_row_from_segment(
        self,
        *,
        segment_records: tuple[BoxRecord, ...],
        source_file_group_id: str,
        segment_index: int,
        watermark_ms: int,
    ) -> dict[str, object]:
        first = segment_records[0]
        last = segment_records[-1]
        state = "closed" if watermark_ms > last.event_time_ms + TRACE_SEGMENT_GAP_MS else "open"
        return {
            "device_id": first.device_id,
            "trace_id": first.trace_id,
            "segment_id": f"{source_file_group_id}:{segment_index}",
            "min_event_time_ms": first.event_time_ms,
            "max_event_time_ms": last.event_time_ms,
            "start_frame_id": first.frame_id,
            "end_frame_id": last.frame_id,
            "row_count": len(segment_records),
            "state": state,
        }

    def _build_rollup_rows(
        self,
        *,
        records: tuple[BoxRecord, ...],
        source_file_group_id: str,
    ) -> tuple[dict[str, object], ...]:
        del source_file_group_id
        box_counts: dict[tuple[int, int, int, int], int] = defaultdict(int)
        trace_counts: dict[tuple[int, int, int, int], int] = defaultdict(int)
        by_trace: dict[int, list[BoxRecord]] = defaultdict(list)
        for record in records:
            bucket_start_ms = self._bucket_start_ms(record.event_time_ms)
            box_counts[(record.device_id, record.lane_id or 0, bucket_start_ms, record.obj_type)] += 1
            by_trace[record.trace_id].append(record)

        for trace_records in by_trace.values():
            ordered = sorted(
                trace_records,
                key=lambda record: (record.event_time_ms, record.frame_id, record.trace_id),
            )
            segment_records: list[BoxRecord] = []
            previous: BoxRecord | None = None
            for record in ordered:
                if previous is not None and (
                    record.frame_id < previous.frame_id
                    or record.event_time_ms - previous.event_time_ms > TRACE_SEGMENT_GAP_MS
                ):
                    self._mark_rollup_trace_presence(trace_counts, tuple(segment_records))
                    segment_records = []
                segment_records.append(record)
                previous = record
            if segment_records:
                self._mark_rollup_trace_presence(trace_counts, tuple(segment_records))

        rows = [
            {
                "device_id": device_id,
                "lane_id": lane_id,
                "bucket_start_ms": bucket_start_ms,
                "obj_type": obj_type,
                "box_count": box_count,
                "trace_count": trace_counts[(device_id, lane_id, bucket_start_ms, obj_type)],
            }
            for (device_id, lane_id, bucket_start_ms, obj_type), box_count in sorted(box_counts.items())
        ]
        return tuple(rows)

    def _mark_rollup_trace_presence(
        self,
        trace_counts: dict[tuple[int, int, int, int], int],
        segment_records: tuple[BoxRecord, ...],
    ) -> None:
        seen_keys: set[tuple[int, int, int, int]] = set()
        for record in segment_records:
            seen_keys.add(
                (
                    record.device_id,
                    record.lane_id or 0,
                    self._bucket_start_ms(record.event_time_ms),
                    record.obj_type,
                )
            )
        for key in seen_keys:
            trace_counts[key] += 1

    def _bucket_start_ms(self, event_time_ms: int) -> int:
        window_ms = self.config.runtime.publish_interval_minutes * 60 * 1000
        return event_time_ms - (event_time_ms % window_ms)

    def _trace_dataset_kind(self, *, max_event_time_ms: int, now_ms: int | None) -> DatasetKind:
        hot_cutoff_ms = self._hot_cutoff_ms(now_ms)
        if max_event_time_ms >= hot_cutoff_ms:
            return DatasetKind.TRACE_INDEX_HOT
        return DatasetKind.TRACE_INDEX_COLD

    def _hot_cutoff_ms(self, now_ms: int | None) -> int:
        current_ms = _now_ms() if now_ms is None else now_ms
        return current_ms - self.config.runtime.hot_trace_index_days * 24 * 60 * 60 * 1000

    def _partition_from_group(self, group: FileGroupRecord) -> PartitionKey:
        if group.date_utc is None or group.device_id is None:
            raise ValueError("trace_index group is missing partition coordinates")
        return PartitionKey(
            date_utc=group.date_utc,
            hour_utc=0 if group.hour_utc is None else group.hour_utc,
            device_id=group.device_id,
        )

    def _trace_index_cold_target_path(
        self,
        *,
        partition: PartitionKey,
        file_group_id: str,
        path_index: int,
    ) -> Path:
        if path_index == 0:
            return self.layout.live_file_path(DatasetKind.TRACE_INDEX_COLD, partition, file_group_id)
        parent = self.layout.trace_index_partition_dir(partition, hot=False)
        return parent / f"part-{file_group_id}-{path_index:03d}.parquet"

    def _compact_partition_groups(
        self,
        *,
        partition: PartitionKey,
        dataset_kind: DatasetKind,
        members: list[FileGroupRecord],
        created_at_ms: int,
    ) -> FileGroupRecord:
        file_group_id = uuid4().hex
        output_path = self.layout.live_file_path(dataset_kind, partition, file_group_id)
        staging_path = self.layout.staging_file_path(dataset_kind, partition, file_group_id)
        result = self.writer.compact_files(
            staging_path,
            input_paths=(path for group in members for path in group.path_list),
            schema=self._schema_for_dataset_kind(dataset_kind),
            order_by=self._order_by_for_dataset_kind(dataset_kind),
        )
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result.path.replace(output_path)
        except Exception:
            staging_path.unlink(missing_ok=True)
            raise

        partition_start_ms = self._partition_start_ms(partition)
        return FileGroupRecord(
            file_group_id=file_group_id,
            dataset_kind=dataset_kind,
            device_id=partition.device_id,
            date_utc=partition.date_utc,
            hour_utc=partition.hour_utc,
            window_start_ms=partition_start_ms,
            window_end_ms=partition_start_ms + 60 * 60 * 1000,
            min_event_time_ms=self._min_event_time_ms(members),
            max_event_time_ms=self._max_event_time_ms(members),
            row_count=result.row_count,
            file_count=1,
            total_bytes=output_path.stat().st_size,
            state=FileGroupState.SEALED,
            path_list=(str(output_path),),
            created_at_ms=created_at_ms,
        )

    def _schema_for_dataset_kind(self, dataset_kind: DatasetKind) -> tuple[tuple[str, str], ...]:
        if dataset_kind in (DatasetKind.TRACE_INDEX_HOT, DatasetKind.TRACE_INDEX_COLD):
            return TRACE_INDEX_SCHEMA
        if dataset_kind is DatasetKind.OBJ_TYPE_ROLLUP:
            return ROLLUP_SCHEMA
        raise ValueError(f"unsupported derived dataset kind for compaction: {dataset_kind}")

    def _order_by_for_dataset_kind(self, dataset_kind: DatasetKind) -> tuple[str, ...]:
        if dataset_kind in (DatasetKind.TRACE_INDEX_HOT, DatasetKind.TRACE_INDEX_COLD):
            return ("device_id", "trace_id", "min_event_time_ms", "start_frame_id", "segment_id")
        if dataset_kind is DatasetKind.OBJ_TYPE_ROLLUP:
            return ("bucket_start_ms", "lane_id", "obj_type")
        raise ValueError(f"unsupported derived dataset kind for compaction: {dataset_kind}")

    def _read_box_records(self, path_list: tuple[str, ...]) -> tuple[BoxRecord, ...]:
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "duckdb package is required to rebuild derived datasets"
            ) from exc

        input_literal = "[" + ", ".join(self._sql_literal(path) for path in path_list) + "]"
        sql = f"""
        SELECT
            trace_id,
            device_id,
            event_time_ms,
            obj_type,
            position_x_mm,
            position_y_mm,
            position_z_mm,
            length_mm,
            width_mm,
            height_mm,
            speed_centi_kmh_100,
            spindle_centi_deg_100,
            lane_id,
            frame_id
        FROM read_parquet({input_literal})
        ORDER BY event_time_ms, frame_id, trace_id
        """
        connection = duckdb.connect()
        try:
            rows = connection.execute(sql).fetchall()
        finally:
            connection.close()
        return tuple(
            BoxRecord(
                trace_id=row[0],
                device_id=row[1],
                event_time_ms=row[2],
                obj_type=row[3],
                position_x_mm=row[4],
                position_y_mm=row[5],
                position_z_mm=row[6],
                length_mm=row[7],
                width_mm=row[8],
                height_mm=row[9],
                speed_centi_kmh_100=row[10],
                spindle_centi_deg_100=row[11],
                lane_id=row[12],
                frame_id=row[13],
            )
            for row in rows
        )

    def _delete_group_files(self, groups: list[FileGroupRecord]) -> int:
        deleted_bytes = 0
        for group in groups:
            for raw_path in group.path_list:
                path = Path(raw_path)
                if path.exists():
                    deleted_bytes += path.stat().st_size
                path.unlink(missing_ok=True)
        return deleted_bytes

    def _partition_start_ms(self, partition: PartitionKey) -> int:
        return self._bucket_start_ms_from_fields(partition.date_utc, partition.hour_utc)

    def _bucket_start_ms_from_fields(self, date_utc: str, hour_utc: int) -> int:
        from datetime import UTC, datetime

        dt = datetime.fromisoformat(f"{date_utc}T{hour_utc:02d}:00:00+00:00")
        return int(dt.astimezone(UTC).timestamp() * 1000)

    def _min_event_time_ms(self, groups: list[FileGroupRecord]) -> int | None:
        values = [group.min_event_time_ms for group in groups if group.min_event_time_ms is not None]
        if not values:
            return None
        return min(values)

    def _max_event_time_ms(self, groups: list[FileGroupRecord]) -> int | None:
        values = [group.max_event_time_ms for group in groups if group.max_event_time_ms is not None]
        if not values:
            return None
        return max(values)

    def _sql_literal(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
