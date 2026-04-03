from __future__ import annotations

import json
from pathlib import Path
from time import time_ns
from typing import Any, Iterator


def _now_ms() -> int:
    return time_ns() // 1_000_000


class ReleaseJournal:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.events_path = root / "events.jsonl"
        self.checkpoint_path = root / "latest_checkpoint.json"

    def ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def append_event(self, event_type: str, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.ensure_root()
        record = {
            "ts_ms": _now_ms() if ts_ms is None else ts_ms,
            "event_type": event_type,
            "payload": payload,
        }
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")

    def write_checkpoint(self, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.ensure_root()
        checkpoint = {
            "ts_ms": _now_ms() if ts_ms is None else ts_ms,
            "payload": payload,
        }
        temp_path = self.checkpoint_path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(checkpoint, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.checkpoint_path)

    def load_checkpoint(self) -> dict[str, Any] | None:
        if not self.checkpoint_path.exists():
            return None
        return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))

    def iter_events(self) -> Iterator[dict[str, Any]]:
        if not self.events_path.exists():
            return
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
