from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns
from uuid import uuid4

from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import (
    DatasetKind,
    FileGroupRecord,
    FileGroupState,
    PartitionKey,
    PartitionState,
    ReleaseEventType,
    ReleaseRecord,
)
from dbserver.services.derived_data_service import DerivedDataService
from dbserver.storage.layout import StorageLayout
from dbserver.storage.parquet_writer import BoxInfoParquetWriter


def _now_ms() -> int:
    return time_ns() // 1_000_000


@dataclass(slots=True, frozen=True)
class SealResult:
    triggered: bool
    partition_count: int
    replaced_group_count: int
    sealed_row_count: int
    new_release_id: str | None
    reason: str


class SealService:
    def __init__(
        self,
        metadata_store: MetadataStore,
        journal: ReleaseJournal,
        box_info_writer: BoxInfoParquetWriter,
        derived_data_service: DerivedDataService,
        layout: StorageLayout,
        *,
        late_arrival_window_seconds: int = 120,
    ) -> None:
        self.metadata_store = metadata_store
        self.journal = journal
        self.box_info_writer = box_info_writer
        self.derived_data_service = derived_data_service
        self.layout = layout
        self.late_arrival_window_ms = late_arrival_window_seconds * 1000

    def seal_ready_partitions(
        self,
        *,
        schema_version_id: str,
        now_ms: int | None = None,
        note: str = "seal ready hours",
    ) -> SealResult:
        current_release_id = self.metadata_store.get_current_release()
        if current_release_id is None:
            return SealResult(
                triggered=False,
                partition_count=0,
                replaced_group_count=0,
                sealed_row_count=0,
                new_release_id=None,
                reason="no current release exists",
            )
        current_groups = self.metadata_store.list_release_file_groups(current_release_id)
        groups = [group for group in current_groups if group.dataset_kind is DatasetKind.BOX_INFO]
        watermark_ms = (_now_ms() if now_ms is None else now_ms) - self.late_arrival_window_ms
        partition_groups = self._group_partitions(groups)
        derived_partition_groups = self._group_partitions(
            [group for group in current_groups if group.dataset_kind is not DatasetKind.BOX_INFO]
        )
        selected = {
            key: members
            for key, members in partition_groups.items()
            if self._partition_end_ms(key) <= watermark_ms
            and self._partition_needs_seal(
                partition=key,
                box_groups=members,
                derived_groups=derived_partition_groups.get(key, []),
            )
        }
        if not selected:
            return SealResult(
                triggered=False,
                partition_count=0,
                replaced_group_count=0,
                sealed_row_count=0,
                new_release_id=None,
                reason="no ready partitions require sealing",
            )
        return self._seal_groups(
            selected,
            derived_partition_groups=derived_partition_groups,
            schema_version_id=schema_version_id,
            note=note,
        )

    def seal_partition(
        self,
        *,
        date_utc: str,
        hour_utc: int,
        device_id: int,
        schema_version_id: str,
        note: str = "manual seal partition",
    ) -> SealResult:
        current_release_id = self.metadata_store.get_current_release()
        if current_release_id is None:
            return SealResult(
                triggered=False,
                partition_count=0,
                replaced_group_count=0,
                sealed_row_count=0,
                new_release_id=None,
                reason="no current release exists",
            )
        current_groups = self.metadata_store.list_release_file_groups(current_release_id)
        groups = [group for group in current_groups if group.dataset_kind is DatasetKind.BOX_INFO]
        partition_groups = self._group_partitions(groups)
        derived_partition_groups = self._group_partitions(
            [group for group in current_groups if group.dataset_kind is not DatasetKind.BOX_INFO]
        )
        key = PartitionKey(date_utc=date_utc, hour_utc=hour_utc, device_id=device_id)
        members = partition_groups.get(key)
        if not members:
            return SealResult(
                triggered=False,
                partition_count=0,
                replaced_group_count=0,
                sealed_row_count=0,
                new_release_id=None,
                reason="target partition not found in current release",
            )
        if not self._partition_needs_seal(
            partition=key,
            box_groups=members,
            derived_groups=derived_partition_groups.get(key, []),
        ):
            return SealResult(
                triggered=False,
                partition_count=0,
                replaced_group_count=0,
                sealed_row_count=0,
                new_release_id=None,
                reason="target partition is already sealed",
            )
        return self._seal_groups(
            {key: members},
            derived_partition_groups=derived_partition_groups,
            schema_version_id=schema_version_id,
            note=note,
        )

    def _seal_groups(
        self,
        partition_groups: dict[PartitionKey, list[FileGroupRecord]],
        *,
        derived_partition_groups: dict[PartitionKey, list[FileGroupRecord]],
        schema_version_id: str,
        note: str,
    ) -> SealResult:
        remove_groups: list[FileGroupRecord] = []
        sealed_groups: list[FileGroupRecord] = []
        sealed_row_count = 0
        replaced_group_count = 0
        created_at_ms = _now_ms()
        partition_max_event_time_ms: dict[PartitionKey, int | None] = {}
        created_paths: list[Path] = []
        try:
            for partition, members in sorted(
                partition_groups.items(),
                key=lambda item: (item[0].date_utc, item[0].hour_utc, item[0].device_id),
            ):
                existing_state = self.metadata_store.get_partition_state(partition)
                existing_last_event_time_ms = None if existing_state is None else existing_state.last_event_time_ms
                existing_watermark_ms = None if existing_state is None else existing_state.watermark_ms
                last_publish_release_id = None if existing_state is None else existing_state.last_publish_release_id
                last_seal_release_id = None if existing_state is None else existing_state.last_seal_release_id
                group_max_event_time_ms = max(
                    group.max_event_time_ms for group in members if group.max_event_time_ms is not None
                )
                partition_max_event_time_ms[partition] = group_max_event_time_ms
                self.metadata_store.upsert_partition_state(
                    partition,
                    PartitionState.SEALING,
                    last_event_time_ms=(
                        group_max_event_time_ms
                        if existing_last_event_time_ms is None or group_max_event_time_ms > existing_last_event_time_ms
                        else existing_last_event_time_ms
                    ),
                    watermark_ms=existing_watermark_ms,
                    last_publish_release_id=last_publish_release_id,
                    last_seal_release_id=last_seal_release_id,
                )
                if self._groups_need_seal(partition, members):
                    sealed_box_group = self._compact_box_partition(
                        partition=partition,
                        members=members,
                        created_at_ms=created_at_ms,
                    )
                    sealed_groups.append(sealed_box_group)
                    created_paths.extend(Path(path) for path in sealed_box_group.path_list)
                    remove_groups.extend(members)
                    sealed_row_count += sealed_box_group.row_count
                    replaced_group_count += len(members)

                derived_members = derived_partition_groups.get(partition, [])
                derived_groups_by_kind = {
                    dataset_kind: dataset_members
                    for dataset_kind, dataset_members in self._group_dataset_kinds(derived_members).items()
                    if self._groups_need_seal(partition, dataset_members)
                }
                if derived_groups_by_kind:
                    sealed_derived_groups = self.derived_data_service.seal_partition_groups(
                        partition=partition,
                        groups_by_kind=derived_groups_by_kind,
                        created_at_ms=created_at_ms,
                    )
                    sealed_groups.extend(sealed_derived_groups)
                    created_paths.extend(
                        Path(path)
                        for group in sealed_derived_groups
                        for path in group.path_list
                    )
                    remove_groups.extend(
                        group
                        for dataset_members in derived_groups_by_kind.values()
                        for group in dataset_members
                    )
                    replaced_group_count += sum(
                        len(dataset_members) for dataset_members in derived_groups_by_kind.values()
                    )
        except Exception:
            for path in created_paths:
                path.unlink(missing_ok=True)
            raise

        remove_group_ids = [group.file_group_id for group in remove_groups]

        parent_release_id = self.metadata_store.get_current_release()
        release = ReleaseRecord(
            release_id=uuid4().hex,
            parent_release_id=parent_release_id,
            schema_version_id=schema_version_id,
            created_at_ms=created_at_ms,
            event_type=ReleaseEventType.SEAL,
            retention_floor_ms=self.metadata_store.get_current_retention_floor_ms(),
            note=note,
        )
        self.metadata_store.create_release_from_current(
            release,
            add_groups=tuple(sealed_groups),
            remove_file_group_ids=tuple(remove_group_ids),
        )
        for partition in partition_groups:
            existing_state = self.metadata_store.get_partition_state(partition)
            self.metadata_store.upsert_partition_state(
                partition,
                PartitionState.SEALED,
                last_event_time_ms=partition_max_event_time_ms[partition],
                watermark_ms=self._partition_end_ms(partition),
                last_publish_release_id=(
                    None if existing_state is None else existing_state.last_publish_release_id
                ),
                last_seal_release_id=release.release_id,
            )
        invalidated_release_ids = self.metadata_store.delete_releases_referencing_file_groups(
            remove_group_ids,
            keep_release_ids=(release.release_id,),
        )
        self.metadata_store.delete_file_groups(remove_group_ids)
        deleted_bytes = self._delete_file_groups(remove_groups)
        self.journal.append_event(
            "release_created",
            {
                "release_id": release.release_id,
                "parent_release_id": parent_release_id,
                "event_type": release.event_type.value,
                "schema_version_id": schema_version_id,
                "file_group_ids": [group.file_group_id for group in sealed_groups],
                "replaced_file_group_ids": remove_group_ids,
                "sealed_file_groups_by_dataset": self._count_groups_by_dataset(sealed_groups),
                "replaced_file_groups_by_dataset": self._count_groups_by_dataset(remove_groups),
                "invalidated_release_ids": invalidated_release_ids,
                "deleted_bytes": deleted_bytes,
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
        return SealResult(
            triggered=True,
            partition_count=len(partition_groups),
            replaced_group_count=replaced_group_count,
            sealed_row_count=sealed_row_count,
            new_release_id=release.release_id,
            reason="sealed ready partitions",
        )

    def _group_partitions(
        self,
        groups: list[FileGroupRecord],
    ) -> dict[PartitionKey, list[FileGroupRecord]]:
        grouped: dict[PartitionKey, list[FileGroupRecord]] = {}
        for group in groups:
            if group.date_utc is None or group.hour_utc is None or group.device_id is None:
                continue
            key = PartitionKey(
                date_utc=group.date_utc,
                hour_utc=group.hour_utc,
                device_id=group.device_id,
            )
            grouped.setdefault(key, []).append(group)
        return grouped

    def _partition_needs_seal(
        self,
        *,
        partition: PartitionKey,
        box_groups: list[FileGroupRecord],
        derived_groups: list[FileGroupRecord],
    ) -> bool:
        if self._groups_need_seal(partition, box_groups):
            return True
        grouped_derived = self._group_dataset_kinds(derived_groups)
        return any(
            self._groups_need_seal(partition, members)
            for members in grouped_derived.values()
        )

    def _groups_need_seal(self, partition: PartitionKey, groups: list[FileGroupRecord]) -> bool:
        if not groups:
            return False
        partition_start_ms = self._partition_start_ms(partition)
        expected_end_ms = partition_start_ms + 60 * 60 * 1000
        if (
            len(groups) == 1
            and groups[0].state is FileGroupState.SEALED
            and groups[0].window_start_ms == partition_start_ms
            and groups[0].window_end_ms == expected_end_ms
            and groups[0].file_count == 1
            and len(groups[0].path_list) == 1
        ):
            return False
        return True

    def _delete_file_groups(self, groups: list[FileGroupRecord]) -> int:
        deleted_bytes = 0
        for group in groups:
            for raw_path in group.path_list:
                path = Path(raw_path)
                if path.exists():
                    deleted_bytes += path.stat().st_size
                path.unlink(missing_ok=True)
        return deleted_bytes

    def _compact_box_partition(
        self,
        *,
        partition: PartitionKey,
        members: list[FileGroupRecord],
        created_at_ms: int,
    ) -> FileGroupRecord:
        input_paths = [path for group in members for path in group.path_list]
        file_group_id = uuid4().hex
        output_path = self.layout.live_file_path(DatasetKind.BOX_INFO, partition, file_group_id)
        staged_path = self.layout.staging_file_path(DatasetKind.BOX_INFO, partition, file_group_id)
        result = self.box_info_writer.compact_files(staged_path, input_paths)
        try:
            self.box_info_writer.promote_staged_file(result.path, output_path)
        except Exception:
            staged_path.unlink(missing_ok=True)
            raise
        partition_start_ms = self._partition_start_ms(partition)
        return FileGroupRecord(
            file_group_id=file_group_id,
            dataset_kind=DatasetKind.BOX_INFO,
            device_id=partition.device_id,
            date_utc=partition.date_utc,
            hour_utc=partition.hour_utc,
            window_start_ms=partition_start_ms,
            window_end_ms=partition_start_ms + 60 * 60 * 1000,
            min_event_time_ms=min(
                group.min_event_time_ms for group in members if group.min_event_time_ms is not None
            ),
            max_event_time_ms=max(
                group.max_event_time_ms for group in members if group.max_event_time_ms is not None
            ),
            row_count=result.row_count,
            file_count=1,
            total_bytes=output_path.stat().st_size,
            state=FileGroupState.SEALED,
            path_list=(str(output_path),),
            created_at_ms=created_at_ms,
        )

    def _group_dataset_kinds(
        self,
        groups: list[FileGroupRecord],
    ) -> dict[DatasetKind, list[FileGroupRecord]]:
        grouped: dict[DatasetKind, list[FileGroupRecord]] = {}
        for group in groups:
            grouped.setdefault(group.dataset_kind, []).append(group)
        return grouped

    def _count_groups_by_dataset(self, groups: list[FileGroupRecord]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for group in groups:
            counts[group.dataset_kind.value] = counts.get(group.dataset_kind.value, 0) + 1
        return counts

    def _partition_start_ms(self, partition: PartitionKey) -> int:
        dt = datetime.fromisoformat(f"{partition.date_utc}T{partition.hour_utc:02d}:00:00+00:00")
        return int(dt.astimezone(UTC).timestamp() * 1000)

    def _partition_end_ms(self, partition: PartitionKey) -> int:
        return self._partition_start_ms(partition) + 60 * 60 * 1000
