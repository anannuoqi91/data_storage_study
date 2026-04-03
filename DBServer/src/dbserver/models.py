from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class DatasetKind(StrEnum):
    BOX_INFO = "box_info"
    TRACE_INDEX_HOT = "trace_index_hot"
    TRACE_INDEX_COLD = "trace_index_cold"
    OBJ_TYPE_ROLLUP = "obj_type_rollup"


class ReleaseEventType(StrEnum):
    PUBLISH = "publish"
    SEAL = "seal"
    ROLLBACK = "rollback"
    DELETE = "delete"
    REBUILD = "rebuild"
    TRACE_INDEX_COOL = "trace_index_cool"


class FileGroupState(StrEnum):
    STAGING = "staging"
    PUBLISHED = "published"
    SEALED = "sealed"
    GARBAGE_PENDING = "garbage_pending"
    DELETED = "deleted"


class PartitionState(StrEnum):
    OPEN = "open"
    PUBLISHABLE = "publishable"
    SEALING = "sealing"
    SEALED = "sealed"
    DELETING = "deleting"
    DELETED = "deleted"


class QueryKind(StrEnum):
    DETAIL = "detail"
    TRACE = "trace"
    AGGREGATE = "aggregate"
    EXPORT = "export"


class JobType(StrEnum):
    EXPORT = "export"
    DELETE = "delete"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class CallbackStatus(StrEnum):
    NOT_CONFIGURED = "not_configured"
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass(slots=True, frozen=True)
class TimeRange:
    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if self.end_ms < self.start_ms:
            raise ValueError("end_ms must be >= start_ms")

    def overlaps(self, min_ms: int | None, max_ms: int | None) -> bool:
        if min_ms is None or max_ms is None:
            return True
        return not (self.end_ms < min_ms or self.start_ms > max_ms)


@dataclass(slots=True, frozen=True)
class WindowKey:
    device_id: int
    window_start_ms: int
    window_end_ms: int

    @classmethod
    def from_event_time(
        cls,
        device_id: int,
        event_time_ms: int,
        window_minutes: int = 10,
    ) -> "WindowKey":
        window_ms = window_minutes * 60 * 1000
        start_ms = event_time_ms - (event_time_ms % window_ms)
        return cls(
            device_id=device_id,
            window_start_ms=start_ms,
            window_end_ms=start_ms + window_ms,
        )


@dataclass(slots=True, frozen=True)
class PartitionKey:
    date_utc: str
    hour_utc: int
    device_id: int

    @classmethod
    def from_event_time(cls, device_id: int, event_time_ms: int) -> "PartitionKey":
        dt = datetime.fromtimestamp(event_time_ms / 1000, tz=UTC)
        return cls(date_utc=dt.strftime("%Y-%m-%d"), hour_utc=dt.hour, device_id=device_id)


@dataclass(slots=True, frozen=True)
class FileGroupRecord:
    file_group_id: str
    dataset_kind: DatasetKind
    device_id: int | None
    date_utc: str | None
    hour_utc: int | None
    window_start_ms: int | None
    window_end_ms: int | None
    min_event_time_ms: int | None
    max_event_time_ms: int | None
    row_count: int = 0
    file_count: int = 0
    total_bytes: int = 0
    state: FileGroupState = FileGroupState.STAGING
    path_list: tuple[str, ...] = field(default_factory=tuple)
    created_at_ms: int = 0


@dataclass(slots=True, frozen=True)
class ReleaseRecord:
    release_id: str
    parent_release_id: str | None
    schema_version_id: str
    created_at_ms: int
    event_type: ReleaseEventType
    retention_floor_ms: int
    note: str = ""


@dataclass(slots=True, frozen=True)
class QueryHandle:
    query_id: str
    release_id: str
    query_kind: QueryKind
    started_at_ms: int
    target_range: TimeRange | None = None
    owner_pid: int | None = None


@dataclass(slots=True, frozen=True)
class PartitionStateRecord:
    partition: PartitionKey
    state: PartitionState
    last_event_time_ms: int | None = None
    watermark_ms: int | None = None
    last_publish_release_id: str | None = None
    last_seal_release_id: str | None = None
