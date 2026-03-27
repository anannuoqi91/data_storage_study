from __future__ import annotations

import base64
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class TDengineRestClient:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        timeout_seconds: float = 10.0,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds

    def _endpoint(self, database: str | None = None) -> str:
        base = f"http://{self.host}:{self.port}/rest/sql"
        return f"{base}/{database}" if database else base

    def _headers(self) -> dict[str, str]:
        token = base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("ascii")
        return {
            "Authorization": f"Basic {token}",
            "Content-Type": "text/plain; charset=utf-8",
        }

    def _request(self, sql: str, *, database: str | None = None) -> dict:
        request = Request(
            self._endpoint(database),
            data=sql.encode("utf-8"),
            headers=self._headers(),
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"TDengine HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"TDengine request failed: {exc}") from exc

        payload = json.loads(body)
        if isinstance(payload, dict) and payload.get("code") not in (None, 0):
            raise RuntimeError(f"TDengine error {payload.get('code')}: {payload.get('desc')}")
        return payload

    def execute(self, sql: str, *, database: str | None = None) -> dict:
        return self._request(sql, database=database)

    def query_rows(self, sql: str, *, database: str | None = None) -> list[list]:
        payload = self._request(sql, database=database)
        data = payload.get("data", [])
        return data if isinstance(data, list) else []

    def query_single_value(self, sql: str, *, database: str | None = None):
        rows = self.query_rows(sql, database=database)
        if not rows or not rows[0]:
            return None
        return rows[0][0]

    def wait_until_ready(self, *, timeout_seconds: float = 30.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self.query_rows("SHOW DATABASES")
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(0.5)
        raise TimeoutError(f"TDengine did not become ready within {timeout_seconds} seconds: {last_error}")
