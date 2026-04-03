from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Iterator

from dbserver.metadata.schema import SCHEMA_STATEMENTS
from dbserver.models import (
    DatasetKind,
    FileGroupRecord,
    FileGroupState,
    PartitionKey,
    PartitionState,
    PartitionStateRecord,
    QueryHandle,
    QueryKind,
    ReleaseEventType,
    ReleaseRecord,
    TimeRange,
)


class MetadataStore:
    """DuckDB-backed metadata store.

    This class keeps the metadata model importable without requiring DuckDB at
    package import time. The actual DuckDB dependency is only loaded when the
    store opens a connection.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = RLock()

    @contextmanager
    def connect(self) -> Iterator[Any]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        import duckdb  # type: ignore

        connection = duckdb.connect(str(self.db_path))
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with self._lock:
            with self.connect() as connection:
                connection.execute("BEGIN TRANSACTION")
                try:
                    yield connection
                except Exception:
                    connection.execute("ROLLBACK")
                    raise
                else:
                    connection.execute("COMMIT")

    def init_schema(self) -> None:
        with self.connect() as connection:
            for statement in SCHEMA_STATEMENTS:
                connection.execute(statement)
            self._ensure_active_queries_schema(connection)
            row_count = connection.execute("SELECT COUNT(*) FROM current_release").fetchone()[0]
            if row_count == 0:
                connection.execute(
                    """
                    INSERT INTO current_release (singleton, release_id, updated_at_ms)
                    VALUES (TRUE, NULL, 0)
                    """
                )

    def get_release(self, release_id: str) -> ReleaseRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    release_id,
                    parent_release_id,
                    schema_version_id,
                    created_at_ms,
                    event_type,
                    retention_floor_ms,
                    note
                FROM database_releases
                WHERE release_id = ?
                """,
                [release_id],
            ).fetchone()
        if row is None:
            return None
        return ReleaseRecord(
            release_id=row[0],
            parent_release_id=row[1],
            schema_version_id=row[2],
            created_at_ms=row[3],
            event_type=ReleaseEventType(row[4]),
            retention_floor_ms=row[5],
            note=row[6],
        )

    def list_releases(self) -> list[ReleaseRecord]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    release_id,
                    parent_release_id,
                    schema_version_id,
                    created_at_ms,
                    event_type,
                    retention_floor_ms,
                    note
                FROM database_releases
                ORDER BY created_at_ms DESC, release_id DESC
                """
            ).fetchall()
        return [
            ReleaseRecord(
                release_id=row[0],
                parent_release_id=row[1],
                schema_version_id=row[2],
                created_at_ms=row[3],
                event_type=ReleaseEventType(row[4]),
                retention_floor_ms=row[5],
                note=row[6],
            )
            for row in rows
        ]

    def get_current_release(self) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT release_id FROM current_release WHERE singleton = TRUE"
            ).fetchone()
        if row is None:
            return None
        return row[0]

    def get_current_retention_floor_ms(self) -> int:
        current_release_id = self.get_current_release()
        if current_release_id is None:
            return 0
        release = self.get_release(current_release_id)
        if release is None:
            return 0
        return release.retention_floor_ms

    def upsert_schema_version(
        self,
        *,
        schema_version_id: str,
        created_at_ms: int,
        table_name: str,
        schema_json: str,
        partition_rule: str,
        sort_rule: str,
        compression_codec: str,
        is_current: bool,
    ) -> None:
        with self.transaction() as connection:
            if is_current:
                connection.execute("UPDATE schema_versions SET is_current = FALSE")
            connection.execute(
                "DELETE FROM schema_versions WHERE schema_version_id = ?",
                [schema_version_id],
            )
            connection.execute(
                """
                INSERT INTO schema_versions (
                    schema_version_id,
                    created_at_ms,
                    table_name,
                    schema_json,
                    partition_rule,
                    sort_rule,
                    compression_codec,
                    is_current
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    schema_version_id,
                    created_at_ms,
                    table_name,
                    schema_json,
                    partition_rule,
                    sort_rule,
                    compression_codec,
                    is_current,
                ],
            )

    def schema_version_exists(self, schema_version_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM schema_versions
                WHERE schema_version_id = ?
                """,
                [schema_version_id],
            ).fetchone()
        return bool(row[0])

    def create_release_snapshot(
        self,
        release: ReleaseRecord,
        file_groups: Iterable[FileGroupRecord],
    ) -> None:
        groups = tuple(file_groups)
        with self.transaction() as connection:
            self._insert_release_header(connection, release)
            for group in groups:
                self._upsert_file_group(connection, group)
                connection.execute(
                    """
                    INSERT INTO database_release_items (release_id, file_group_id)
                    VALUES (?, ?)
                    """,
                    [release.release_id, group.file_group_id],
                )
            connection.execute("DELETE FROM current_release WHERE singleton = TRUE")
            connection.execute(
                """
                INSERT INTO current_release (singleton, release_id, updated_at_ms)
                VALUES (TRUE, ?, ?)
                """,
                [release.release_id, release.created_at_ms],
            )

    def create_release_from_current(
        self,
        release: ReleaseRecord,
        *,
        add_groups: Iterable[FileGroupRecord] = (),
        remove_file_group_ids: Iterable[str] = (),
    ) -> None:
        add_groups_tuple = tuple(add_groups)
        remove_ids = set(remove_file_group_ids)
        current_release_id = self.get_current_release()
        inherited_ids: list[str] = []
        if current_release_id is not None:
            inherited_ids = self.get_release_file_group_ids(current_release_id)
        retained_ids = [file_group_id for file_group_id in inherited_ids if file_group_id not in remove_ids]

        with self.transaction() as connection:
            self._insert_release_header(connection, release)
            for file_group_id in retained_ids:
                connection.execute(
                    """
                    INSERT INTO database_release_items (release_id, file_group_id)
                    VALUES (?, ?)
                    """,
                    [release.release_id, file_group_id],
                )
            for group in add_groups_tuple:
                self._upsert_file_group(connection, group)
                connection.execute(
                    """
                    INSERT INTO database_release_items (release_id, file_group_id)
                    VALUES (?, ?)
                    """,
                    [release.release_id, group.file_group_id],
                )
            connection.execute("DELETE FROM current_release WHERE singleton = TRUE")
            connection.execute(
                """
                INSERT INTO current_release (singleton, release_id, updated_at_ms)
                VALUES (TRUE, ?, ?)
                """,
                [release.release_id, release.created_at_ms],
            )

    def list_release_file_groups(
        self,
        release_id: str,
        dataset_kind: DatasetKind | None = None,
    ) -> list[FileGroupRecord]:
        sql = """
            SELECT
                fg.file_group_id,
                fg.dataset_kind,
                fg.device_id,
                fg.date_utc,
                fg.hour_utc,
                fg.window_start_ms,
                fg.window_end_ms,
                fg.min_event_time_ms,
                fg.max_event_time_ms,
                fg.row_count,
                fg.file_count,
                fg.total_bytes,
                fg.state,
                fg.path_list_json,
                fg.created_at_ms
            FROM database_release_items dri
            JOIN file_groups fg
              ON dri.file_group_id = fg.file_group_id
            WHERE dri.release_id = ?
        """
        params: list[Any] = [release_id]
        if dataset_kind is not None:
            sql += " AND fg.dataset_kind = ?"
            params.append(dataset_kind.value)
        sql += " ORDER BY fg.min_event_time_ms NULLS LAST, fg.file_group_id"
        with self.connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._row_to_file_group(row) for row in rows]

    def get_release_file_group_ids(
        self,
        release_id: str,
        dataset_kind: DatasetKind | None = None,
    ) -> list[str]:
        return [group.file_group_id for group in self.list_release_file_groups(release_id, dataset_kind)]

    def upsert_partition_state(
        self,
        partition: PartitionKey,
        state: PartitionState,
        last_event_time_ms: int | None = None,
        watermark_ms: int | None = None,
        last_publish_release_id: str | None = None,
        last_seal_release_id: str | None = None,
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                DELETE FROM partition_states
                WHERE date_utc = ? AND hour_utc = ? AND device_id = ?
                """,
                [partition.date_utc, partition.hour_utc, partition.device_id],
            )
            connection.execute(
                """
                INSERT INTO partition_states (
                    date_utc,
                    hour_utc,
                    device_id,
                    state,
                    last_event_time_ms,
                    watermark_ms,
                    last_publish_release_id,
                    last_seal_release_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    partition.date_utc,
                    partition.hour_utc,
                    partition.device_id,
                    state.value,
                    last_event_time_ms,
                    watermark_ms,
                    last_publish_release_id,
                    last_seal_release_id,
                ],
            )

    def get_partition_state(self, partition: PartitionKey) -> PartitionStateRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    date_utc,
                    hour_utc,
                    device_id,
                    state,
                    last_event_time_ms,
                    watermark_ms,
                    last_publish_release_id,
                    last_seal_release_id
                FROM partition_states
                WHERE date_utc = ? AND hour_utc = ? AND device_id = ?
                """,
                [partition.date_utc, partition.hour_utc, partition.device_id],
            ).fetchone()
        if row is None:
            return None
        return self._row_to_partition_state(row)

    def list_partition_states(self) -> list[PartitionStateRecord]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    date_utc,
                    hour_utc,
                    device_id,
                    state,
                    last_event_time_ms,
                    watermark_ms,
                    last_publish_release_id,
                    last_seal_release_id
                FROM partition_states
                ORDER BY date_utc, hour_utc, device_id
                """
            ).fetchall()
        return [self._row_to_partition_state(row) for row in rows]

    def delete_partition_state(self, partition: PartitionKey) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                DELETE FROM partition_states
                WHERE date_utc = ? AND hour_utc = ? AND device_id = ?
                """,
                [partition.date_utc, partition.hour_utc, partition.device_id],
            )

    def start_query(self, handle: QueryHandle) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO active_queries (
                    query_id,
                    release_id,
                    query_kind,
                    started_at_ms,
                    status,
                    owner_pid,
                    target_range_start_ms,
                    target_range_end_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    handle.query_id,
                    handle.release_id,
                    handle.query_kind.value,
                    handle.started_at_ms,
                    "running",
                    os.getpid() if handle.owner_pid is None else handle.owner_pid,
                    None if handle.target_range is None else handle.target_range.start_ms,
                    None if handle.target_range is None else handle.target_range.end_ms,
                ],
            )

    def finish_query(self, query_id: str) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM active_queries WHERE query_id = ?", [query_id])

    def list_active_queries(self) -> list[QueryHandle]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    query_id,
                    release_id,
                    query_kind,
                    started_at_ms,
                    owner_pid,
                    target_range_start_ms,
                    target_range_end_ms
                FROM active_queries
                ORDER BY started_at_ms, query_id
                """
            ).fetchall()
        return [self._row_to_query_handle(row) for row in rows]

    def prune_stale_active_queries(self) -> list[QueryHandle]:
        handles = self.list_active_queries()
        stale_handles = [
            handle
            for handle in handles
            if handle.owner_pid is None or not self._pid_is_alive(handle.owner_pid)
        ]
        if not stale_handles:
            return []
        with self.transaction() as connection:
            for handle in stale_handles:
                connection.execute("DELETE FROM active_queries WHERE query_id = ?", [handle.query_id])
        return stale_handles

    def clear_active_queries(self) -> int:
        with self.transaction() as connection:
            deleted = connection.execute("DELETE FROM active_queries RETURNING query_id").fetchall()
        return len(deleted)

    def delete_releases_referencing_file_groups(
        self,
        file_group_ids: Iterable[str],
        *,
        keep_release_ids: Iterable[str] = (),
    ) -> list[str]:
        target_ids = tuple(dict.fromkeys(file_group_ids))
        if not target_ids:
            return []
        keep_ids = set(keep_release_ids)
        placeholders = ", ".join("?" for _ in target_ids)
        with self.transaction() as connection:
            rows = connection.execute(
                f"""
                SELECT DISTINCT release_id
                FROM database_release_items
                WHERE file_group_id IN ({placeholders})
                """,
                list(target_ids),
            ).fetchall()
            release_ids = [row[0] for row in rows if row[0] not in keep_ids]
            if not release_ids:
                return []
            release_placeholders = ", ".join("?" for _ in release_ids)
            connection.execute(
                f"DELETE FROM database_release_items WHERE release_id IN ({release_placeholders})",
                release_ids,
            )
            connection.execute(
                f"DELETE FROM database_releases WHERE release_id IN ({release_placeholders})",
                release_ids,
            )
        return release_ids

    def delete_file_groups(self, file_group_ids: Iterable[str]) -> None:
        target_ids = tuple(dict.fromkeys(file_group_ids))
        if not target_ids:
            return
        placeholders = ", ".join("?" for _ in target_ids)
        with self.transaction() as connection:
            connection.execute(
                f"DELETE FROM database_release_items WHERE file_group_id IN ({placeholders})",
                list(target_ids),
            )
            connection.execute(
                f"DELETE FROM file_groups WHERE file_group_id IN ({placeholders})",
                list(target_ids),
            )

    def list_all_file_group_paths(self) -> tuple[str, ...]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT path_list_json
                FROM file_groups
                ORDER BY file_group_id
                """
            ).fetchall()
        paths: list[str] = []
        for row in rows:
            paths.extend(json.loads(row[0]))
        return tuple(paths)

    def _insert_release_header(self, connection: Any, release: ReleaseRecord) -> None:
        connection.execute(
            """
            INSERT INTO database_releases (
                release_id,
                parent_release_id,
                schema_version_id,
                created_at_ms,
                event_type,
                retention_floor_ms,
                note
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                release.release_id,
                release.parent_release_id,
                release.schema_version_id,
                release.created_at_ms,
                release.event_type.value,
                release.retention_floor_ms,
                release.note,
            ],
        )

    def _upsert_file_group(self, connection: Any, group: FileGroupRecord) -> None:
        connection.execute("DELETE FROM file_groups WHERE file_group_id = ?", [group.file_group_id])
        connection.execute(
            """
            INSERT INTO file_groups (
                file_group_id,
                dataset_kind,
                device_id,
                date_utc,
                hour_utc,
                window_start_ms,
                window_end_ms,
                min_event_time_ms,
                max_event_time_ms,
                row_count,
                file_count,
                total_bytes,
                state,
                path_list_json,
                created_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                group.file_group_id,
                group.dataset_kind.value,
                group.device_id,
                group.date_utc,
                group.hour_utc,
                group.window_start_ms,
                group.window_end_ms,
                group.min_event_time_ms,
                group.max_event_time_ms,
                group.row_count,
                group.file_count,
                group.total_bytes,
                group.state.value,
                json.dumps(list(group.path_list)),
                group.created_at_ms,
            ],
        )

    def _row_to_file_group(self, row: tuple[Any, ...]) -> FileGroupRecord:
        return FileGroupRecord(
            file_group_id=row[0],
            dataset_kind=DatasetKind(row[1]),
            device_id=row[2],
            date_utc=row[3],
            hour_utc=row[4],
            window_start_ms=row[5],
            window_end_ms=row[6],
            min_event_time_ms=row[7],
            max_event_time_ms=row[8],
            row_count=row[9],
            file_count=row[10],
            total_bytes=row[11],
            state=FileGroupState(row[12]),
            path_list=tuple(json.loads(row[13])),
            created_at_ms=row[14],
        )

    def _row_to_partition_state(self, row: tuple[Any, ...]) -> PartitionStateRecord:
        return PartitionStateRecord(
            partition=PartitionKey(date_utc=row[0], hour_utc=row[1], device_id=row[2]),
            state=PartitionState(row[3]),
            last_event_time_ms=row[4],
            watermark_ms=row[5],
            last_publish_release_id=row[6],
            last_seal_release_id=row[7],
        )

    def _row_to_query_handle(self, row: tuple[Any, ...]) -> QueryHandle:
        target_range = None
        if row[5] is not None and row[6] is not None:
            target_range = TimeRange(start_ms=row[5], end_ms=row[6])
        return QueryHandle(
            query_id=row[0],
            release_id=row[1],
            query_kind=QueryKind(row[2]),
            started_at_ms=row[3],
            target_range=target_range,
            owner_pid=row[4],
        )

    def _ensure_active_queries_schema(self, connection: Any) -> None:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('active_queries')").fetchall()
        }
        if "owner_pid" not in columns:
            connection.execute("ALTER TABLE active_queries ADD COLUMN owner_pid INTEGER")

    def _pid_is_alive(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True
