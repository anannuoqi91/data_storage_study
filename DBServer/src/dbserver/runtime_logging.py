from __future__ import annotations

import json
from pathlib import Path
from time import time_ns
from typing import Any


def _now_ms() -> int:
    return time_ns() // 1_000_000


class RuntimeLogger:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, event_type: str, payload: dict[str, Any], *, ts_ms: int | None = None) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts_ms": _now_ms() if ts_ms is None else ts_ms,
            "event_type": event_type,
            "payload": payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")
