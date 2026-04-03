from __future__ import annotations

from typing import Protocol

from dbserver.domain.records import FrameBatch


class FrameSourceAdapter(Protocol):
    """Abstract adapter boundary for DDS/cybernode or future protocols."""

    def receive(self) -> FrameBatch | None:
        """Return the next normalized frame if available."""
