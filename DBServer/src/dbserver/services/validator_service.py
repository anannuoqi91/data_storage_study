from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import time_ns

from dbserver.domain.records import FrameBatch


def _now_ms() -> int:
    return time_ns() // 1_000_000


@dataclass(slots=True, frozen=True)
class ValidationError(Exception):
    reason: str

    def __str__(self) -> str:
        return self.reason


class ValidatorService:
    def __init__(
        self,
        *,
        min_valid_year: int = 2000,
        future_skew_ms: int = 60 * 60 * 1000,
        late_arrival_window_ms: int | None = None,
        max_devices: int | None = None,
    ) -> None:
        self.min_valid_ms = int(datetime(min_valid_year, 1, 1, tzinfo=UTC).timestamp() * 1000)
        self.future_skew_ms = future_skew_ms
        self.late_arrival_window_ms = late_arrival_window_ms
        self.max_devices = max_devices
        self._active_devices: set[int] = set()
        self._device_max_event_time_ms: dict[int, int] = {}

    def prime_device_state(self, device_id: int, *, max_event_time_ms: int | None = None) -> None:
        self._active_devices.add(device_id)
        if max_event_time_ms is None:
            return
        previous_max_event_time_ms = self._device_max_event_time_ms.get(device_id)
        if previous_max_event_time_ms is None or max_event_time_ms > previous_max_event_time_ms:
            self._device_max_event_time_ms[device_id] = max_event_time_ms

    def validate_frame(self, frame: FrameBatch, now_ms: int | None = None) -> None:
        current_ms = _now_ms() if now_ms is None else now_ms
        if frame.event_time_ms < self.min_valid_ms:
            raise ValidationError("event_time_ms is earlier than the minimum valid timestamp")
        if frame.event_time_ms > current_ms + self.future_skew_ms:
            raise ValidationError("event_time_ms exceeds the allowed future skew")
        if frame.device_id < 0:
            raise ValidationError("device_id must be non-negative")
        if (
            self.max_devices is not None
            and frame.device_id not in self._active_devices
            and len(self._active_devices) >= self.max_devices
        ):
            raise ValidationError("device count exceeds configured max_devices")
        if frame.frame_id < 0:
            raise ValidationError("frame_id must be non-negative")
        if not frame.records:
            raise ValidationError("frame contains no box records")
        for record in frame.records:
            if record.event_time_ms != frame.event_time_ms:
                raise ValidationError("frame contains mixed event_time_ms values")
            if record.device_id != frame.device_id:
                raise ValidationError("frame contains mixed device_id values")
            if record.frame_id != frame.frame_id:
                raise ValidationError("frame contains mixed frame_id values")
        if self.late_arrival_window_ms is not None:
            max_seen_event_time_ms = self._device_max_event_time_ms.get(frame.device_id)
            if (
                max_seen_event_time_ms is not None
                and frame.event_time_ms < max_seen_event_time_ms - self.late_arrival_window_ms
            ):
                raise ValidationError("event_time_ms exceeds the allowed late arrival window")
        previous_max_event_time_ms = self._device_max_event_time_ms.get(frame.device_id)
        if previous_max_event_time_ms is None or frame.event_time_ms > previous_max_event_time_ms:
            self._device_max_event_time_ms[frame.device_id] = frame.event_time_ms
        self._active_devices.add(frame.device_id)
