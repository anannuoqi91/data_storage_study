from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def escape_tsv_field(value: object) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    return text.replace("\\", r"\\").replace("\t", r"\t").replace("\n", r"\n")


@dataclass(frozen=True)
class ClickHouseClient:
    host: str = "127.0.0.1"
    port: int = 8123
    database: str = "traj_store_clickhose"
    user: str = "default"
    password: str = ""
    timeout_seconds: float = 30.0

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def _request(self, *, query: str | None = None, data: bytes | None = None, database: str | None = None) -> bytes:
        params: dict[str, str] = {}
        target_database = self.database if database is None else database
        if target_database:
            params["database"] = target_database
        if query is not None:
            params["query"] = query
        url = self.base_url
        if params:
            url = f"{url}?{urlencode(params)}"

        headers = {"X-ClickHouse-User": self.user}
        if self.password:
            headers["X-ClickHouse-Key"] = self.password

        request = Request(url, data=b"" if data is None else data, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return response.read()
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace").strip()
            message = body or exc.reason
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {message}") from exc
        except URLError as exc:
            raise RuntimeError(f"ClickHouse connection failed: {exc}") from exc
        except OSError as exc:
            raise RuntimeError(f"ClickHouse socket failed: {exc}") from exc

    def execute(self, query: str, *, database: str | None = None) -> None:
        self._request(query=query, database=database)

    def query_text(self, query: str, *, database: str | None = None) -> str:
        return self._request(query=query, database=database).decode("utf-8")

    def query_tsv_row(self, query: str, *, database: str | None = None) -> list[str]:
        text = self.query_text(query, database=database).strip()
        if not text:
            return []
        return text.split("\t")

    def query_single_value(self, query: str, *, database: str | None = None) -> str:
        row = self.query_tsv_row(query, database=database)
        return row[0] if row else ""

    def insert_tsv(self, table_name: str, columns: Sequence[str], rows: Iterable[Sequence[object]]) -> int:
        rendered_rows = []
        row_count = 0
        for row in rows:
            rendered_rows.append("\t".join(escape_tsv_field(value) for value in row))
            row_count += 1
        if row_count == 0:
            return 0
        payload = ("\n".join(rendered_rows) + "\n").encode("utf-8")
        columns_sql = ", ".join(columns)
        self._request(query=f"INSERT INTO {table_name} ({columns_sql}) FORMAT TSV", data=payload)
        return row_count

    def wait_until_ready(self, timeout_seconds: float = 30.0, *, database: str | None = None) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error = ""
        while time.monotonic() < deadline:
            try:
                value = self.query_single_value("SELECT 1", database=database)
                if value.strip() == "1":
                    return
            except Exception as exc:
                last_error = str(exc)
            time.sleep(0.5)
        raise TimeoutError(f"ClickHouse was not ready within {timeout_seconds} seconds. {last_error}".strip())
