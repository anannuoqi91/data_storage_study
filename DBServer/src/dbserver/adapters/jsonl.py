from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from dbserver.domain.records import BoxRecord, FrameBatch


class JsonlFrameSource:
    """Read frame batches from local JSONL input."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def iter_frames(self) -> Iterator[FrameBatch]:
        with self.path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    boxes = tuple(
                        self._parse_box(payload, box_payload) for box_payload in payload["boxes"]
                    )
                    yield FrameBatch(
                        device_id=int(payload["device_id"]),
                        frame_id=int(payload["frame_id"]),
                        event_time_ms=int(payload["event_time_ms"]),
                        records=boxes,
                    )
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError(f"invalid jsonl frame at line {line_no}: {exc}") from exc

    def _parse_box(self, frame_payload: dict[str, object], box_payload: dict[str, object]) -> BoxRecord:
        return BoxRecord(
            trace_id=int(box_payload["trace_id"]),
            device_id=int(frame_payload["device_id"]),
            event_time_ms=int(frame_payload["event_time_ms"]),
            obj_type=int(box_payload["obj_type"]),
            position_x_mm=int(box_payload["position_x_mm"]),
            position_y_mm=int(box_payload["position_y_mm"]),
            position_z_mm=int(box_payload["position_z_mm"]),
            length_mm=int(box_payload["length_mm"]),
            width_mm=int(box_payload["width_mm"]),
            height_mm=int(box_payload["height_mm"]),
            speed_centi_kmh_100=int(box_payload["speed_centi_kmh_100"]),
            spindle_centi_deg_100=int(box_payload["spindle_centi_deg_100"]),
            lane_id=None if box_payload.get("lane_id") is None else int(box_payload["lane_id"]),
            frame_id=int(frame_payload["frame_id"]),
        )
