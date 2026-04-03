from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class StoragePaths:
    root: Path = Path("storage")
    live: Path = field(init=False)
    staging: Path = field(init=False)
    metadata: Path = field(init=False)
    jobs: Path = field(init=False)
    quarantine: Path = field(init=False)
    exports: Path = field(init=False)

    def __post_init__(self) -> None:
        self.live = self.root / "live"
        self.staging = self.root / "staging"
        self.metadata = self.root / "metadata"
        self.jobs = self.metadata / "jobs"
        self.quarantine = self.root / "quarantine"
        self.exports = self.root / "exports"

    @property
    def metadata_db(self) -> Path:
        return self.metadata / "metadata.db"

    @property
    def journal_dir(self) -> Path:
        return self.metadata / "journal"

    @property
    def checkpoint_dir(self) -> Path:
        return self.metadata / "checkpoints"

    @property
    def runtime_log_path(self) -> Path:
        return self.metadata / "runtime_events.jsonl"

    def ensure(self) -> None:
        for path in (
            self.live,
            self.staging,
            self.metadata,
            self.jobs,
            self.quarantine,
            self.exports,
            self.journal_dir,
            self.checkpoint_dir,
            self.jobs / "pending",
            self.jobs / "running",
            self.jobs / "finished",
            self.exports / "pending",
            self.exports / "finished",
            self.quarantine / "bad_frames",
            self.quarantine / "bad_timestamps",
        ):
            path.mkdir(parents=True, exist_ok=True)


@dataclass(slots=True)
class RuntimeSettings:
    timezone: str = "UTC"
    publish_interval_minutes: int = 10
    late_arrival_window_seconds: int = 120
    memory_loss_budget_seconds: int = 5
    max_devices: int = 4
    hot_trace_index_days: int = 30
    retention_days: int = 180
    lane_unknown_value: int = 255
    low_watermark_bytes: int | None = None
    high_watermark_bytes: int | None = None


@dataclass(slots=True)
class AppConfig:
    paths: StoragePaths = field(default_factory=StoragePaths)
    runtime: RuntimeSettings = field(default_factory=RuntimeSettings)


def build_config(root: str | Path = "storage") -> AppConfig:
    return AppConfig(paths=StoragePaths(Path(root)))
