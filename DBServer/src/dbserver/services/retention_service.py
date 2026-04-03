from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns

from dbserver.config import AppConfig
from dbserver.metadata.journal import ReleaseJournal
from dbserver.metadata.store import MetadataStore
from dbserver.models import FileGroupRecord, FileGroupState
from dbserver.services.ops_service import DeleteConflictError, OpsService


@dataclass(slots=True, frozen=True)
class StorageUsage:
    root_bytes: int
    live_bytes: int
    staging_bytes: int
    metadata_bytes: int
    quarantine_bytes: int
    exports_bytes: int


@dataclass(slots=True, frozen=True)
class RetentionRunResult:
    triggered: bool
    low_watermark_bytes: int
    high_watermark_bytes: int
    usage_before_bytes: int
    usage_after_bytes: int
    deleted_partition_count: int
    deleted_group_count: int
    deleted_estimated_bytes: int
    new_release_id: str | None
    retention_floor_ms: int | None
    reason: str


class RetentionService:
    def __init__(
        self,
        config: AppConfig,
        metadata_store: MetadataStore,
        journal: ReleaseJournal,
    ) -> None:
        self.config = config
        self.metadata_store = metadata_store
        self.journal = journal
        self.ops_service = OpsService(metadata_store, journal)

    def collect_usage(self) -> StorageUsage:
        return StorageUsage(
            root_bytes=self._directory_size_bytes(self.config.paths.root),
            live_bytes=self._directory_size_bytes(self.config.paths.live),
            staging_bytes=self._directory_size_bytes(self.config.paths.staging),
            metadata_bytes=self._directory_size_bytes(self.config.paths.metadata),
            quarantine_bytes=self._directory_size_bytes(self.config.paths.quarantine),
            exports_bytes=self._directory_size_bytes(self.config.paths.exports),
        )

    def enforce_watermarks(
        self,
        *,
        schema_version_id: str,
        low_watermark_bytes: int | None = None,
        high_watermark_bytes: int | None = None,
        query_wait_timeout_ms: int = 0,
        query_poll_interval_ms: int = 500,
        note: str = "automatic retention cleanup",
    ) -> RetentionRunResult:
        low = self.config.runtime.low_watermark_bytes if low_watermark_bytes is None else low_watermark_bytes
        high = (
            self.config.runtime.high_watermark_bytes
            if high_watermark_bytes is None
            else high_watermark_bytes
        )
        if low is None or high is None:
            raise ValueError("both low_watermark_bytes and high_watermark_bytes are required")
        if high > low:
            raise ValueError("high_watermark_bytes must be <= low_watermark_bytes")

        usage_before = self.collect_usage()
        if usage_before.root_bytes < low:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=None,
                reason="usage below low watermark",
            )

        current_release_id = self.metadata_store.get_current_release()
        if current_release_id is None:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=None,
                reason="no current release exists",
            )

        groups = self.metadata_store.list_release_file_groups(current_release_id)
        partitions = self._collect_deletable_partitions(groups)
        if not partitions:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=None,
                reason="no deletable sealed partitions",
            )

        bytes_to_reclaim = usage_before.root_bytes - high
        if bytes_to_reclaim <= 0:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=None,
                reason="usage already below high watermark",
            )

        selected_partition_count = 0
        deleted_estimated_bytes = 0
        retention_floor_ms: int | None = None
        for partition in partitions:
            selected_partition_count += 1
            deleted_estimated_bytes += partition["total_bytes"]
            retention_floor_ms = partition["retention_floor_ms"]
            if deleted_estimated_bytes >= bytes_to_reclaim:
                break

        if retention_floor_ms is None:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=None,
                reason="failed to derive retention floor",
            )

        plan = self.ops_service.build_delete_plan(
            retention_floor_ms=retention_floor_ms,
            schema_version_id=schema_version_id,
            note=note,
        )
        if not plan.delete_groups:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=retention_floor_ms,
                reason="delete plan is empty",
            )

        try:
            release = self.ops_service.commit_delete_plan_with_wait(
                plan,
                query_wait_timeout_ms=query_wait_timeout_ms,
                query_poll_interval_ms=query_poll_interval_ms,
            )
        except DeleteConflictError as exc:
            return RetentionRunResult(
                triggered=False,
                low_watermark_bytes=low,
                high_watermark_bytes=high,
                usage_before_bytes=usage_before.root_bytes,
                usage_after_bytes=usage_before.root_bytes,
                deleted_partition_count=0,
                deleted_group_count=0,
                deleted_estimated_bytes=0,
                new_release_id=None,
                retention_floor_ms=retention_floor_ms,
                reason=str(exc),
            )
        usage_after = self.collect_usage()
        return RetentionRunResult(
            triggered=True,
            low_watermark_bytes=low,
            high_watermark_bytes=high,
            usage_before_bytes=usage_before.root_bytes,
            usage_after_bytes=usage_after.root_bytes,
            deleted_partition_count=selected_partition_count,
            deleted_group_count=len(plan.delete_groups),
            deleted_estimated_bytes=sum(group.total_bytes for group in plan.delete_groups),
            new_release_id=release.release_id,
            retention_floor_ms=release.retention_floor_ms,
            reason="deleted oldest partitions to reach target watermark",
        )

    def _collect_deletable_partitions(
        self,
        groups: list[FileGroupRecord],
    ) -> list[dict[str, int]]:
        current_hour_start_ms = self._current_hour_start_ms()
        aggregated: dict[int, int] = {}
        for group in groups:
            if group.date_utc is None or group.hour_utc is None:
                continue
            if group.state is not FileGroupState.SEALED:
                continue
            partition_start_ms = self._partition_start_ms(group.date_utc, group.hour_utc)
            if partition_start_ms >= current_hour_start_ms:
                continue
            aggregated.setdefault(partition_start_ms, 0)
            aggregated[partition_start_ms] += group.total_bytes
        ordered = []
        for partition_start_ms in sorted(aggregated):
            ordered.append(
                {
                    "partition_start_ms": partition_start_ms,
                    "retention_floor_ms": partition_start_ms + 60 * 60 * 1000,
                    "total_bytes": aggregated[partition_start_ms],
                }
            )
        return ordered

    def _directory_size_bytes(self, path: Path) -> int:
        if not path.exists():
            return 0
        total = 0
        for entry in path.rglob("*"):
            if entry.is_file():
                total += entry.stat().st_size
        return total

    def _partition_start_ms(self, date_utc: str, hour_utc: int) -> int:
        dt = datetime.fromisoformat(f"{date_utc}T{hour_utc:02d}:00:00+00:00").astimezone(UTC)
        return int(dt.timestamp() * 1000)

    def _current_hour_start_ms(self) -> int:
        now_ms = time_ns() // 1_000_000
        return now_ms - (now_ms % (60 * 60 * 1000))
