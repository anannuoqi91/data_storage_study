from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(slots=True, frozen=True)
class BoxRecord:
    trace_id: int
    device_id: int
    event_time_ms: int
    obj_type: int
    position_x_mm: int
    position_y_mm: int
    position_z_mm: int
    length_mm: int
    width_mm: int
    height_mm: int
    speed_centi_kmh_100: int
    spindle_centi_deg_100: int
    lane_id: int | None
    frame_id: int

    def normalized(self, lane_unknown_value: int = 255) -> "BoxRecord":
        if self.lane_id is None:
            return replace(self, lane_id=lane_unknown_value)
        return self


@dataclass(slots=True, frozen=True)
class FrameBatch:
    device_id: int
    frame_id: int
    event_time_ms: int
    records: tuple[BoxRecord, ...]

    def normalized(self, lane_unknown_value: int = 255) -> "FrameBatch":
        normalized_records = tuple(
            record.normalized(lane_unknown_value=lane_unknown_value)
            for record in self.records
        )
        return FrameBatch(
            device_id=self.device_id,
            frame_id=self.frame_id,
            event_time_ms=self.event_time_ms,
            records=normalized_records,
        )

    @property
    def box_count(self) -> int:
        return len(self.records)
