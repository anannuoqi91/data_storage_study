from __future__ import annotations

import csv
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable, Mapping, Sequence

from dbserver.storage.parquet_writer import WriteResult


class StructuredParquetWriter:
    """Materialize typed row dictionaries into Parquet via DuckDB."""

    def __init__(self, staging_root: Path) -> None:
        self.staging_root = staging_root

    def write_rows(
        self,
        output_path: Path | str,
        *,
        rows: Iterable[Mapping[str, object]],
        schema: Sequence[tuple[str, str]],
        order_by: Sequence[str] = (),
    ) -> WriteResult:
        output_path = Path(output_path)
        materialized_rows = tuple(rows)
        if not materialized_rows:
            raise ValueError("cannot write an empty parquet file")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)

        columns = tuple(column for column, _ in schema)
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv",
            dir=self.staging_root,
            delete=False,
        ) as handle:
            temp_csv_path = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in materialized_rows:
                writer.writerow({column: row[column] for column in columns})

        try:
            self._materialize_parquet(
                temp_csv_path=temp_csv_path,
                output_path=output_path,
                schema=schema,
                order_by=order_by,
            )
        finally:
            temp_csv_path.unlink(missing_ok=True)

        return WriteResult(
            path=output_path,
            row_count=len(materialized_rows),
            total_bytes=output_path.stat().st_size,
        )

    def compact_files(
        self,
        output_path: Path | str,
        *,
        input_paths: Iterable[Path | str],
        schema: Sequence[tuple[str, str]],
        order_by: Sequence[str] = (),
    ) -> WriteResult:
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
        select_columns = ",\n                ".join(
            f"CAST({column} AS {duckdb_type}) AS {column}"
            for column, duckdb_type in schema
        )
        order_clause = ""
        if order_by:
            order_clause = "\n            ORDER BY " + ", ".join(order_by)
        row_count_sql = f"SELECT COUNT(*) FROM read_parquet({input_literal})"
        copy_sql = f"""
        COPY (
            SELECT
                {select_columns}
            FROM read_parquet({input_literal}){order_clause}
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

    def _materialize_parquet(
        self,
        *,
        temp_csv_path: Path,
        output_path: Path,
        schema: Sequence[tuple[str, str]],
        order_by: Sequence[str],
    ) -> None:
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "duckdb package is required to write parquet output"
            ) from exc

        input_literal = self._sql_literal(str(temp_csv_path))
        output_literal = self._sql_literal(str(output_path))
        select_columns = ",\n                ".join(
            f"CAST({column} AS {duckdb_type}) AS {column}"
            for column, duckdb_type in schema
        )
        order_clause = ""
        if order_by:
            order_clause = "\n            ORDER BY " + ", ".join(order_by)
        sql = f"""
        COPY (
            SELECT
                {select_columns}
            FROM read_csv_auto({input_literal}){order_clause}
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
