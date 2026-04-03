from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dbserver.metadata.store import MetadataStore
from dbserver.models import DatasetKind, PartitionKey, PartitionState, QueryKind, TimeRange


@dataclass(slots=True, frozen=True)
class QueryPlan:
    release_id: str
    query_kind: QueryKind
    sql: str
    files: tuple[str, ...]


class QueryService:
    def __init__(self, metadata_store: MetadataStore) -> None:
        self.metadata_store = metadata_store

    def plan_device_time_range_query(
        self,
        device_id: int,
        time_range: TimeRange,
        release_id: str | None = None,
    ) -> QueryPlan:
        selected_release = self._require_release(release_id)
        files = self._select_files(
            release_id=selected_release,
            dataset_kind=DatasetKind.BOX_INFO,
            device_id=device_id,
            time_range=time_range,
        )
        sql = (
            f"SELECT * FROM read_parquet({self._sql_list(files)}) "
            f"WHERE device_id = {device_id} "
            f"AND event_time_ms BETWEEN {time_range.start_ms} AND {time_range.end_ms} "
            "ORDER BY event_time_ms, frame_id, trace_id"
        )
        return QueryPlan(
            release_id=selected_release,
            query_kind=QueryKind.DETAIL,
            sql=sql,
            files=files,
        )

    def plan_trace_lookup_query(
        self,
        device_id: int,
        trace_id: int,
        time_range: TimeRange | None = None,
        release_id: str | None = None,
    ) -> QueryPlan:
        selected_release = self._require_release(release_id)
        hot_files = self._select_files(
            release_id=selected_release,
            dataset_kind=DatasetKind.TRACE_INDEX_HOT,
            device_id=device_id,
            time_range=time_range,
            allow_empty=True,
        )
        cold_files = self._select_files(
            release_id=selected_release,
            dataset_kind=DatasetKind.TRACE_INDEX_COLD,
            device_id=device_id,
            time_range=time_range,
            allow_empty=True,
        )
        files = hot_files + cold_files
        if not files:
            raise ValueError("no matching trace index files found for query")
        predicates = [
            f"device_id = {device_id}",
            f"trace_id = {trace_id}",
        ]
        if time_range is not None:
            predicates.append(f"max_event_time_ms >= {time_range.start_ms}")
            predicates.append(f"min_event_time_ms <= {time_range.end_ms}")
        filter_sql = " AND ".join(predicates)
        sql = (
            "WITH raw_segments AS ("
            f" SELECT device_id, trace_id, segment_id, min_event_time_ms, max_event_time_ms, "
            f"start_frame_id, end_frame_id, row_count, state FROM read_parquet({self._sql_list(files)}) "
            f"WHERE {filter_sql}"
            "), ordered_segments AS ("
            " SELECT *, CASE "
            "WHEN LAG(max_event_time_ms) OVER trace_window IS NULL THEN 1 "
            "WHEN start_frame_id < LAG(end_frame_id) OVER trace_window THEN 1 "
            "WHEN min_event_time_ms - LAG(max_event_time_ms) OVER trace_window > 150 THEN 1 "
            "ELSE 0 END AS new_segment_flag "
            "FROM raw_segments "
            "WINDOW trace_window AS ("
            "PARTITION BY device_id, trace_id "
            "ORDER BY min_event_time_ms, start_frame_id, segment_id"
            ")"
            "), merged_segments AS ("
            " SELECT *, "
            "SUM(new_segment_flag) OVER ("
            "PARTITION BY device_id, trace_id "
            "ORDER BY min_event_time_ms, start_frame_id, segment_id "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ") AS merged_segment_ordinal "
            "FROM ordered_segments"
            ") "
            "SELECT "
            "device_id, "
            "trace_id, "
            "merged_segment_ordinal AS segment_ordinal, "
            "MIN(min_event_time_ms) AS min_event_time_ms, "
            "MAX(max_event_time_ms) AS max_event_time_ms, "
            "arg_min(start_frame_id, min_event_time_ms) AS start_frame_id, "
            "arg_max(end_frame_id, max_event_time_ms) AS end_frame_id, "
            "SUM(row_count) AS row_count, "
            "arg_max(state, max_event_time_ms) AS state, "
            "COUNT(*) AS source_segment_count "
            "FROM merged_segments "
            "GROUP BY device_id, trace_id, merged_segment_ordinal "
            "ORDER BY min_event_time_ms, max_event_time_ms"
        )
        return QueryPlan(
            release_id=selected_release,
            query_kind=QueryKind.TRACE,
            sql=sql,
            files=files,
        )

    def plan_device_trace_query(
        self,
        device_id: int,
        trace_id: int,
        time_range: TimeRange | None = None,
        release_id: str | None = None,
    ) -> QueryPlan:
        selected_release = self._require_release(release_id)
        files = self._select_files(
            release_id=selected_release,
            dataset_kind=DatasetKind.BOX_INFO,
            device_id=device_id,
            time_range=time_range,
        )
        predicates = [
            f"device_id = {device_id}",
            f"trace_id = {trace_id}",
        ]
        if time_range is not None:
            predicates.append(
                f"event_time_ms BETWEEN {time_range.start_ms} AND {time_range.end_ms}"
            )
        sql = (
            f"SELECT * FROM read_parquet({self._sql_list(files)}) "
            f"WHERE {' AND '.join(predicates)} "
            "ORDER BY event_time_ms, frame_id, trace_id"
        )
        return QueryPlan(
            release_id=selected_release,
            query_kind=QueryKind.TRACE,
            sql=sql,
            files=files,
        )

    def plan_rollup_query(
        self,
        device_id: int,
        time_range: TimeRange,
        lane_id: int | None = None,
        obj_type: int | None = None,
        release_id: str | None = None,
    ) -> QueryPlan:
        selected_release = self._require_release(release_id)
        files = self._select_files(
            release_id=selected_release,
            dataset_kind=DatasetKind.OBJ_TYPE_ROLLUP,
            device_id=device_id,
            time_range=time_range,
        )
        predicates = [
            f"device_id = {device_id}",
            f"bucket_start_ms BETWEEN {time_range.start_ms} AND {time_range.end_ms}",
        ]
        if lane_id is not None:
            predicates.append(f"lane_id = {lane_id}")
        if obj_type is not None:
            predicates.append(f"obj_type = {obj_type}")
        sql = (
            f"SELECT * FROM read_parquet({self._sql_list(files)}) "
            f"WHERE {' AND '.join(predicates)} "
            "ORDER BY bucket_start_ms, lane_id, obj_type"
        )
        return QueryPlan(
            release_id=selected_release,
            query_kind=QueryKind.AGGREGATE,
            sql=sql,
            files=files,
        )

    def _require_release(self, release_id: str | None) -> str:
        selected_release = release_id or self.metadata_store.get_current_release()
        if selected_release is None:
            raise ValueError("no current release exists")
        return selected_release

    def _select_files(
        self,
        release_id: str,
        dataset_kind: DatasetKind,
        device_id: int,
        time_range: TimeRange | None,
        allow_empty: bool = False,
    ) -> tuple[str, ...]:
        groups = self.metadata_store.list_release_file_groups(release_id, dataset_kind=dataset_kind)
        files: list[str] = []
        for group in groups:
            if group.device_id is not None and group.device_id != device_id:
                continue
            if time_range is not None and not time_range.overlaps(
                group.min_event_time_ms,
                group.max_event_time_ms,
            ):
                continue
            if group.date_utc is not None and group.hour_utc is not None and group.device_id is not None:
                partition_state = self.metadata_store.get_partition_state(
                    PartitionKey(
                        date_utc=group.date_utc,
                        hour_utc=group.hour_utc,
                        device_id=group.device_id,
                    )
                )
                if partition_state is not None and partition_state.state is PartitionState.DELETING:
                    raise ValueError("target partition is currently deleting")
                if partition_state is not None and partition_state.state is PartitionState.DELETED:
                    raise ValueError("target partition is already deleted")
            files.extend(group.path_list)
        if not files and not allow_empty:
            raise ValueError("no matching files found for query")
        return tuple(str(Path(path)) for path in files)

    def _sql_list(self, files: tuple[str, ...]) -> str:
        quoted = ", ".join(repr(path) for path in files)
        return f"[{quoted}]"
