from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable

from dbserver.domain.records import BoxRecord


BOX_INFO_COLUMNS = (
    "trace_id",
    "device_id",
    "event_time_ms",
    "obj_type",
    "position_x_mm",
    "position_y_mm",
    "position_z_mm",
    "length_mm",
    "width_mm",
    "height_mm",
    "speed_centi_kmh_100",
    "spindle_centi_deg_100",
    "lane_id",
    "frame_id",
)


@dataclass(slots=True, frozen=True)
class WriteResult:
    path: Path
    row_count: int
    total_bytes: int


class BoxInfoParquetWriter:
    """Write normalized box_info records to Parquet using DuckDB."""

    def __init__(self, staging_root: Path) -> None:
        self.staging_root = staging_root

    def write_records(self, output_path: Path | str, records: Iterable[BoxRecord]) -> WriteResult:
        output_path = Path(output_path)
        rows = tuple(records)
        if not rows:
            raise ValueError("cannot write an empty parquet file")

        self.staging_root.mkdir(parents=True, exist_ok=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv",
            dir=self.staging_root,
            delete=False,
        ) as handle:
            temp_csv_path = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=BOX_INFO_COLUMNS)
            writer.writeheader()
            for record in rows:
                writer.writerow(
                    {
                        "trace_id": record.trace_id,
                        "device_id": record.device_id,
                        "event_time_ms": record.event_time_ms,
                        "obj_type": record.obj_type,
                        "position_x_mm": record.position_x_mm,
                        "position_y_mm": record.position_y_mm,
                        "position_z_mm": record.position_z_mm,
                        "length_mm": record.length_mm,
                        "width_mm": record.width_mm,
                        "height_mm": record.height_mm,
                        "speed_centi_kmh_100": record.speed_centi_kmh_100,
                        "spindle_centi_deg_100": record.spindle_centi_deg_100,
                        "lane_id": record.lane_id,
                        "frame_id": record.frame_id,
                    }
                )

        try:
            self._materialize_parquet(temp_csv_path, output_path)
        finally:
            temp_csv_path.unlink(missing_ok=True)

        return WriteResult(
            path=output_path,
            row_count=len(rows),
            total_bytes=output_path.stat().st_size,
        )

    def compact_files(self, output_path: Path | str, input_paths: Iterable[Path | str]) -> WriteResult:
        output_path = Path(output_path)
        normalized_inputs = tuple(str(Path(path)) for path in input_paths)
        if not normalized_inputs:
            raise ValueError("cannot compact an empty parquet file list")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "duckdb package is required to compact parquet files"
            ) from exc

        input_literal = self._sql_list_literal(normalized_inputs)
        output_literal = self._sql_literal(str(output_path))
        row_count_sql = f"SELECT COUNT(*) FROM read_parquet({input_literal})"
        copy_sql = f"""
        COPY (
            SELECT
                trace_id,
                device_id,
                event_time_ms,
                obj_type,
                position_x_mm,
                position_y_mm,
                position_z_mm,
                length_mm,
                width_mm,
                height_mm,
                speed_centi_kmh_100,
                spindle_centi_deg_100,
                lane_id,
                frame_id
            FROM read_parquet({input_literal})
            ORDER BY event_time_ms, frame_id, trace_id
        ) TO {output_literal} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        connection = duckdb.connect()
        try:
            row_count = int(connection.execute(row_count_sql).fetchone()[0])
            connection.execute(copy_sql)
        finally:
            connection.close()

        return WriteResult(
            path=output_path,
            row_count=row_count,
            total_bytes=output_path.stat().st_size,
        )

    def promote_staged_file(self, staged_path: Path | str, output_path: Path | str) -> WriteResult:
        staged_path = Path(staged_path)
        output_path = Path(output_path)
        if not staged_path.exists():
            raise FileNotFoundError(f"staged parquet file does not exist: {staged_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.replace(output_path)
        return WriteResult(
            path=output_path,
            row_count=0,
            total_bytes=output_path.stat().st_size,
        )

    def _materialize_parquet(self, temp_csv_path: Path, output_path: Path) -> None:
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "duckdb package is required to write parquet output"
            ) from exc

        input_literal = self._sql_literal(str(temp_csv_path))
        output_literal = self._sql_literal(str(output_path))
        sql = f"""
        COPY (
            SELECT
                CAST(trace_id AS INTEGER) AS trace_id,
                CAST(device_id AS USMALLINT) AS device_id,
                CAST(event_time_ms AS BIGINT) AS event_time_ms,
                CAST(obj_type AS UTINYINT) AS obj_type,
                CAST(position_x_mm AS INTEGER) AS position_x_mm,
                CAST(position_y_mm AS INTEGER) AS position_y_mm,
                CAST(position_z_mm AS INTEGER) AS position_z_mm,
                CAST(length_mm AS USMALLINT) AS length_mm,
                CAST(width_mm AS USMALLINT) AS width_mm,
                CAST(height_mm AS USMALLINT) AS height_mm,
                CAST(speed_centi_kmh_100 AS USMALLINT) AS speed_centi_kmh_100,
                CAST(spindle_centi_deg_100 AS USMALLINT) AS spindle_centi_deg_100,
                CAST(lane_id AS UTINYINT) AS lane_id,
                CAST(frame_id AS INTEGER) AS frame_id
            FROM read_csv_auto({input_literal})
            ORDER BY event_time_ms, frame_id, trace_id
        ) TO {output_literal} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        connection = duckdb.connect()
        try:
            connection.execute(sql)
        finally:
            connection.close()

    def _sql_literal(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"

    def _sql_list_literal(self, values: Iterable[str]) -> str:
        return "[" + ", ".join(self._sql_literal(value) for value in values) + "]"
