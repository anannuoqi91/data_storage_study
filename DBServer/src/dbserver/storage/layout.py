from __future__ import annotations

from pathlib import Path

from dbserver.config import StoragePaths
from dbserver.models import DatasetKind, PartitionKey


class StorageLayout:
    def __init__(self, paths: StoragePaths) -> None:
        self.paths = paths

    def dataset_root(self, dataset_kind: DatasetKind) -> Path:
        return self.paths.live / dataset_kind.value

    def staging_root(self, dataset_kind: DatasetKind) -> Path:
        return self.paths.staging / dataset_kind.value

    def box_info_partition_dir(self, partition: PartitionKey) -> Path:
        return (
            self.dataset_root(DatasetKind.BOX_INFO)
            / f"date={partition.date_utc}"
            / f"hour={partition.hour_utc:02d}"
            / f"device_id={partition.device_id}"
        )

    def trace_index_partition_dir(self, partition: PartitionKey, hot: bool) -> Path:
        kind = DatasetKind.TRACE_INDEX_HOT if hot else DatasetKind.TRACE_INDEX_COLD
        return (
            self.dataset_root(kind)
            / f"date={partition.date_utc}"
            / f"device_id={partition.device_id}"
        )

    def rollup_partition_dir(self, partition: PartitionKey) -> Path:
        return (
            self.dataset_root(DatasetKind.OBJ_TYPE_ROLLUP)
            / f"date={partition.date_utc}"
            / f"hour={partition.hour_utc:02d}"
            / f"device_id={partition.device_id}"
        )

    def live_file_path(
        self,
        dataset_kind: DatasetKind,
        partition: PartitionKey,
        file_group_id: str,
        suffix: str = "parquet",
    ) -> Path:
        if dataset_kind is DatasetKind.BOX_INFO:
            parent = self.box_info_partition_dir(partition)
        elif dataset_kind is DatasetKind.OBJ_TYPE_ROLLUP:
            parent = self.rollup_partition_dir(partition)
        elif dataset_kind is DatasetKind.TRACE_INDEX_HOT:
            parent = self.trace_index_partition_dir(partition, hot=True)
        elif dataset_kind is DatasetKind.TRACE_INDEX_COLD:
            parent = self.trace_index_partition_dir(partition, hot=False)
        else:
            raise ValueError(f"Unsupported dataset kind: {dataset_kind}")
        return parent / f"part-{file_group_id}.{suffix}"

    def staging_file_path(
        self,
        dataset_kind: DatasetKind,
        partition: PartitionKey,
        file_group_id: str,
        suffix: str = "parquet",
    ) -> Path:
        if dataset_kind is DatasetKind.BOX_INFO:
            parent = (
                self.staging_root(DatasetKind.BOX_INFO)
                / f"date={partition.date_utc}"
                / f"hour={partition.hour_utc:02d}"
                / f"device_id={partition.device_id}"
            )
        elif dataset_kind is DatasetKind.OBJ_TYPE_ROLLUP:
            parent = (
                self.staging_root(DatasetKind.OBJ_TYPE_ROLLUP)
                / f"date={partition.date_utc}"
                / f"hour={partition.hour_utc:02d}"
                / f"device_id={partition.device_id}"
            )
        elif dataset_kind is DatasetKind.TRACE_INDEX_HOT:
            parent = (
                self.staging_root(DatasetKind.TRACE_INDEX_HOT)
                / f"date={partition.date_utc}"
                / f"device_id={partition.device_id}"
            )
        elif dataset_kind is DatasetKind.TRACE_INDEX_COLD:
            parent = (
                self.staging_root(DatasetKind.TRACE_INDEX_COLD)
                / f"date={partition.date_utc}"
                / f"device_id={partition.device_id}"
            )
        else:
            raise ValueError(f"Unsupported dataset kind: {dataset_kind}")
        return parent / f"part-{file_group_id}.{suffix}"

    def iter_live_data_files(self) -> tuple[Path, ...]:
        if not self.paths.live.exists():
            return ()
        return tuple(path for path in self.paths.live.rglob("*.parquet") if path.is_file())

    def iter_staging_files(self) -> tuple[Path, ...]:
        if not self.paths.staging.exists():
            return ()
        return tuple(path for path in self.paths.staging.rglob("*") if path.is_file())

    def export_result_path(self, export_id: str) -> Path:
        return self.paths.exports / "finished" / f"{export_id}.parquet"

    def export_manifest_path(self, export_id: str) -> Path:
        return self.paths.exports / "finished" / f"{export_id}.json"

    def export_pending_request_path(self, export_id: str) -> Path:
        return self.paths.exports / "pending" / f"{export_id}.json"

    def export_working_request_path(self, export_id: str) -> Path:
        return self.paths.exports / "pending" / f"{export_id}.working.json"

    def job_pending_path(self, job_id: str) -> Path:
        return self.paths.jobs / "pending" / f"{job_id}.json"

    def job_running_path(self, job_id: str) -> Path:
        return self.paths.jobs / "running" / f"{job_id}.json"

    def job_finished_path(self, job_id: str) -> Path:
        return self.paths.jobs / "finished" / f"{job_id}.json"
