from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import sleep
from time import time_ns
from uuid import uuid4

from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import (
    FileGroupRecord,
    FileGroupState,
    PartitionKey,
    PartitionState,
    PartitionStateRecord,
    ReleaseEventType,
    ReleaseRecord,
)


def _now_ms() -> int:
    return time_ns() // 1_000_000


class DeleteConflictError(RuntimeError):
    pass


@dataclass(slots=True, frozen=True)
class DeletePlan:
    source_release_id: str
    new_release: ReleaseRecord
    keep_groups: tuple[FileGroupRecord, ...]
    delete_groups: tuple[FileGroupRecord, ...]


class OpsService:
    def __init__(self, metadata_store: MetadataStore, journal: ReleaseJournal) -> None:
        self.metadata_store = metadata_store
        self.journal = journal

    def rollback_to_release(
        self,
        target_release_id: str,
        schema_version_id: str,
        retention_floor_ms: int,
        note: str = "manual rollback",
    ) -> ReleaseRecord:
        current_release_id = self.metadata_store.get_current_release()
        groups = self.metadata_store.list_release_file_groups(target_release_id)
        release = ReleaseRecord(
            release_id=uuid4().hex,
            parent_release_id=current_release_id,
            schema_version_id=schema_version_id,
            created_at_ms=_now_ms(),
            event_type=ReleaseEventType.ROLLBACK,
            retention_floor_ms=retention_floor_ms,
            note=note,
        )
        self.metadata_store.create_release_snapshot(release, groups)
        self.journal.append_event(
            "current_release_switched",
            {
                "event_type": release.event_type.value,
                "release_id": release.release_id,
                "target_release_id": target_release_id,
                "retention_floor_ms": retention_floor_ms,
            },
            ts_ms=release.created_at_ms,
        )
        self.journal.write_checkpoint(
            {
                "current_release_id": release.release_id,
                "retention_floor_ms": retention_floor_ms,
            },
            ts_ms=release.created_at_ms,
        )
        return release

    def build_delete_plan(
        self,
        retention_floor_ms: int,
        schema_version_id: str,
        note: str = "retention delete",
    ) -> DeletePlan:
        current_release_id = self.metadata_store.get_current_release()
        if current_release_id is None:
            raise ValueError("no current release exists")
        groups = self.metadata_store.list_release_file_groups(current_release_id)
        keep_groups: list[FileGroupRecord] = []
        delete_groups: list[FileGroupRecord] = []
        for group in groups:
            if self._group_is_deletable(group, retention_floor_ms=retention_floor_ms):
                delete_groups.append(group)
            else:
                keep_groups.append(group)
        new_release = ReleaseRecord(
            release_id=uuid4().hex,
            parent_release_id=current_release_id,
            schema_version_id=schema_version_id,
            created_at_ms=_now_ms(),
            event_type=ReleaseEventType.DELETE,
            retention_floor_ms=retention_floor_ms,
            note=note,
        )
        return DeletePlan(
            source_release_id=current_release_id,
            new_release=new_release,
            keep_groups=tuple(keep_groups),
            delete_groups=tuple(delete_groups),
        )

    def commit_delete_plan(self, plan: DeletePlan) -> ReleaseRecord:
        return self.commit_delete_plan_with_wait(plan)

    def commit_delete_plan_with_wait(
        self,
        plan: DeletePlan,
        *,
        query_wait_timeout_ms: int = 0,
        query_poll_interval_ms: int = 500,
    ) -> ReleaseRecord:
        partitions = self._deleted_partitions(plan.delete_groups)
        previous_states = {
            partition: self.metadata_store.get_partition_state(partition)
            for partition in partitions
        }
        self._mark_partitions_deleting(partitions)
        try:
            self._wait_for_queries_to_exit(
                retention_floor_ms=plan.new_release.retention_floor_ms,
                query_wait_timeout_ms=query_wait_timeout_ms,
                query_poll_interval_ms=query_poll_interval_ms,
            )
        except Exception:
            self._restore_partition_states(previous_states)
            raise

        self.metadata_store.create_release_snapshot(plan.new_release, plan.keep_groups)
        deleted_group_ids = [group.file_group_id for group in plan.delete_groups]
        invalidated_release_ids = self.metadata_store.delete_releases_referencing_file_groups(
            deleted_group_ids,
            keep_release_ids=(plan.new_release.release_id,),
        )
        self.metadata_store.delete_file_groups(deleted_group_ids)
        for partition in partitions:
            self.metadata_store.upsert_partition_state(
                partition,
                PartitionState.DELETED,
                last_event_time_ms=None,
                watermark_ms=plan.new_release.retention_floor_ms,
                last_publish_release_id=None,
                last_seal_release_id=None,
            )
        deleted_bytes = self._delete_group_files(plan.delete_groups)
        self.journal.append_event(
            "retention_floor_advanced",
            {
                "release_id": plan.new_release.release_id,
                "source_release_id": plan.source_release_id,
                "retention_floor_ms": plan.new_release.retention_floor_ms,
                "delete_file_group_ids": deleted_group_ids,
                "invalidated_release_ids": invalidated_release_ids,
                "deleted_bytes": deleted_bytes,
            },
            ts_ms=plan.new_release.created_at_ms,
        )
        self.journal.append_event(
            "current_release_switched",
            {
                "release_id": plan.new_release.release_id,
                "source_release_id": plan.source_release_id,
                "event_type": plan.new_release.event_type.value,
            },
            ts_ms=plan.new_release.created_at_ms,
        )
        self.journal.write_checkpoint(
            {
                "current_release_id": plan.new_release.release_id,
                "retention_floor_ms": plan.new_release.retention_floor_ms,
            },
            ts_ms=plan.new_release.created_at_ms,
        )
        return plan.new_release

    def _mark_partitions_deleting(self, partitions: list[PartitionKey]) -> None:
        for partition in partitions:
            existing_state = self.metadata_store.get_partition_state(partition)
            self.metadata_store.upsert_partition_state(
                partition,
                PartitionState.DELETING,
                last_event_time_ms=None if existing_state is None else existing_state.last_event_time_ms,
                watermark_ms=None if existing_state is None else existing_state.watermark_ms,
                last_publish_release_id=(
                    None if existing_state is None else existing_state.last_publish_release_id
                ),
                last_seal_release_id=None if existing_state is None else existing_state.last_seal_release_id,
            )

    def _restore_partition_states(
        self,
        previous_states: dict[PartitionKey, PartitionStateRecord | None],
    ) -> None:
        for partition, record in previous_states.items():
            if record is None:
                continue
            self.metadata_store.upsert_partition_state(
                partition,
                record.state,
                last_event_time_ms=record.last_event_time_ms,
                watermark_ms=record.watermark_ms,
                last_publish_release_id=record.last_publish_release_id,
                last_seal_release_id=record.last_seal_release_id,
            )

    def _wait_for_queries_to_exit(
        self,
        *,
        retention_floor_ms: int,
        query_wait_timeout_ms: int,
        query_poll_interval_ms: int,
    ) -> None:
        deadline_ms = _now_ms() + query_wait_timeout_ms
        while True:
            stale_handles = self.metadata_store.prune_stale_active_queries()
            if stale_handles:
                self.journal.append_event(
                    "stale_active_queries_recovered",
                    {
                        "query_ids": [handle.query_id for handle in stale_handles],
                        "owner_pids": [handle.owner_pid for handle in stale_handles],
                        "retention_floor_ms": retention_floor_ms,
                    },
                )
            conflicting_queries = [
                handle.query_id
                for handle in self.metadata_store.list_active_queries()
                if handle.target_range is None or handle.target_range.start_ms < retention_floor_ms
            ]
            if not conflicting_queries:
                return
            if _now_ms() >= deadline_ms:
                raise DeleteConflictError(
                    "delete is blocked by active queries: " + ",".join(conflicting_queries)
                )
            sleep(max(query_poll_interval_ms, 1) / 1000)

    def _delete_group_files(self, groups: tuple[FileGroupRecord, ...]) -> int:
        deleted_bytes = 0
        for group in groups:
            for raw_path in group.path_list:
                path = Path(raw_path)
                if path.exists():
                    deleted_bytes += path.stat().st_size
                path.unlink(missing_ok=True)
        return deleted_bytes

    def _deleted_partitions(self, groups: tuple[FileGroupRecord, ...]) -> list[PartitionKey]:
        seen: set[tuple[str, int, int]] = set()
        partitions: list[PartitionKey] = []
        for group in groups:
            if group.date_utc is None or group.hour_utc is None or group.device_id is None:
                continue
            key = (group.date_utc, group.hour_utc, group.device_id)
            if key in seen:
                continue
            seen.add(key)
            partitions.append(
                PartitionKey(
                    date_utc=group.date_utc,
                    hour_utc=group.hour_utc,
                    device_id=group.device_id,
                )
            )
        return partitions

    def _group_is_deletable(self, group: FileGroupRecord, *, retention_floor_ms: int) -> bool:
        if group.max_event_time_ms is None or group.max_event_time_ms >= retention_floor_ms:
            return False
        if group.date_utc is None or group.hour_utc is None or group.device_id is None:
            return False
        partition_state = self.metadata_store.get_partition_state(
            PartitionKey(
                date_utc=group.date_utc,
                hour_utc=group.hour_utc,
                device_id=group.device_id,
            )
        )
        if partition_state is None:
            return group.state is FileGroupState.SEALED
        return partition_state.state in (
            PartitionState.SEALED,
            PartitionState.DELETING,
            PartitionState.DELETED,
        )
