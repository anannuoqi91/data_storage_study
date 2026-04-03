from __future__ import annotations

from dataclasses import dataclass, field
from time import time_ns
from uuid import uuid4

from dbserver.config import AppConfig
from dbserver.domain.records import BoxRecord, FrameBatch
from dbserver.models import DatasetKind, FileGroupRecord, FileGroupState, PartitionKey, WindowKey
from dbserver.storage.layout import StorageLayout


def _now_ms() -> int:
    return time_ns() // 1_000_000


@dataclass(slots=True)
class BufferedWindow:
    key: WindowKey
    frames: list[FrameBatch] = field(default_factory=list)
    box_count: int = 0
    min_event_time_ms: int | None = None
    max_event_time_ms: int | None = None

    def add_frame(self, frame: FrameBatch) -> None:
        self.frames.append(frame)
        self.box_count += frame.box_count
        if self.min_event_time_ms is None or frame.event_time_ms < self.min_event_time_ms:
            self.min_event_time_ms = frame.event_time_ms
        if self.max_event_time_ms is None or frame.event_time_ms > self.max_event_time_ms:
            self.max_event_time_ms = frame.event_time_ms


@dataclass(slots=True, frozen=True)
class PublishPlan:
    window: WindowKey
    box_info_group: FileGroupRecord
    records: tuple[BoxRecord, ...]


class IngestService:
    """Buffered ingest service for normalized frame batches."""

    def __init__(self, config: AppConfig, layout: StorageLayout) -> None:
        self.config = config
        self.layout = layout
        self._buffers: dict[WindowKey, BufferedWindow] = {}

    def ingest_frame(self, frame: FrameBatch) -> None:
        normalized = frame.normalized(self.config.runtime.lane_unknown_value)
        window = WindowKey.from_event_time(
            device_id=normalized.device_id,
            event_time_ms=normalized.event_time_ms,
            window_minutes=self.config.runtime.publish_interval_minutes,
        )
        buffered = self._buffers.setdefault(window, BufferedWindow(key=window))
        buffered.add_frame(normalized)

    def build_publish_plans(self, now_ms: int | None = None) -> list[PublishPlan]:
        current_ms = _now_ms() if now_ms is None else now_ms
        watermark_ms = current_ms - self.config.runtime.late_arrival_window_seconds * 1000
        ready_windows = [
            buffered
            for key, buffered in self._buffers.items()
            if key.window_end_ms <= watermark_ms
        ]
        ready_windows.sort(key=lambda buffered: (buffered.key.window_start_ms, buffered.key.device_id))
        return [self._build_publish_plan(buffered) for buffered in ready_windows]

    def mark_window_published(self, window: WindowKey) -> None:
        self._buffers.pop(window, None)

    def buffered_window_count(self) -> int:
        return len(self._buffers)

    def buffered_box_count(self) -> int:
        return sum(buffered.box_count for buffered in self._buffers.values())

    def max_buffered_window_end_ms(self) -> int | None:
        if not self._buffers:
            return None
        return max(window.window_end_ms for window in self._buffers)

    def _build_publish_plan(self, buffered: BufferedWindow) -> PublishPlan:
        created_at_ms = _now_ms()
        partition = PartitionKey.from_event_time(
            device_id=buffered.key.device_id,
            event_time_ms=buffered.key.window_start_ms,
        )
        box_group_id = uuid4().hex
        box_path = self.layout.live_file_path(DatasetKind.BOX_INFO, partition, box_group_id)
        records = self._sorted_window_records(buffered)
        return PublishPlan(
            window=buffered.key,
            box_info_group=FileGroupRecord(
                file_group_id=box_group_id,
                dataset_kind=DatasetKind.BOX_INFO,
                device_id=buffered.key.device_id,
                date_utc=partition.date_utc,
                hour_utc=partition.hour_utc,
                window_start_ms=buffered.key.window_start_ms,
                window_end_ms=buffered.key.window_end_ms,
                min_event_time_ms=buffered.min_event_time_ms,
                max_event_time_ms=buffered.max_event_time_ms,
                row_count=buffered.box_count,
                file_count=1,
                total_bytes=0,
                state=FileGroupState.PUBLISHED,
                path_list=(str(box_path),),
                created_at_ms=created_at_ms,
            ),
            records=records,
        )

    def _sorted_window_records(self, buffered: BufferedWindow) -> tuple[BoxRecord, ...]:
        flattened = [record for frame in buffered.frames for record in frame.records]
        flattened.sort(key=lambda record: (record.event_time_ms, record.frame_id, record.trace_id))
        return tuple(flattened)
