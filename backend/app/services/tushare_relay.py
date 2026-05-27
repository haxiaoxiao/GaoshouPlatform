from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from loguru import logger

from app.core.config import settings


RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}


@dataclass
class TushareRelayMeta:
    api_name: str
    base_url: str
    status_code: int
    elapsed_ms: int
    cache: str | None = None
    cache_bucket: str | None = None
    request_id: str | None = None
    attempt: int = 1


@dataclass
class TushareRelayResult:
    rows: list[dict[str, Any]]
    payload: dict[str, Any]
    meta: TushareRelayMeta


class TushareRelayError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        api_name: str,
        base_url: str | None = None,
        status_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.api_name = api_name
        self.base_url = base_url
        self.status_code = status_code
        self.retryable = retryable


def parse_relay_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse native Tushare envelopes and relay list-style responses."""
    data = payload.get("data")
    if isinstance(data, dict):
        fields = data.get("fields")
        items = data.get("items")
        if isinstance(fields, list) and isinstance(items, list):
            rows: list[dict[str, Any]] = []
            for item in items:
                if isinstance(item, dict):
                    rows.append(item)
                elif isinstance(item, (list, tuple)):
                    rows.append(dict(zip(fields, item)))
            return rows
        rows_value = data.get("rows") or data.get("items")
        if isinstance(rows_value, list):
            return [row for row in rows_value if isinstance(row, dict)]
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(payload.get("items"), list):
        return [row for row in payload["items"] if isinstance(row, dict)]
    if isinstance(payload.get("rows"), list):
        return [row for row in payload["rows"] if isinstance(row, dict)]
    return []


class TushareRelayClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_urls: list[str] | None = None,
        rps: float | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.indevs_tushare_api_key
        self.base_urls = base_urls or self._settings_base_urls()
        self.rps = max(float(rps if rps is not None else settings.indevs_tushare_rps or 1.0), 0.1)
        self.timeout_seconds = int(timeout_seconds if timeout_seconds is not None else settings.indevs_tushare_timeout_seconds or 30)
        self._last_request_at = 0.0

    @staticmethod
    def _settings_base_urls() -> list[str]:
        urls = [
            item.strip().rstrip("/")
            for item in str(settings.indevs_tushare_base_urls or "").split(",")
            if item.strip()
        ]
        return urls or ["https://ai-tool.indevs.in/tushare/pro"]

    def _throttle(self) -> None:
        interval = 1.0 / self.rps
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_request_at = time.monotonic()

    def request(
        self,
        api_name: str,
        params: dict[str, Any] | None = None,
        *,
        retries: int = 2,
    ) -> TushareRelayResult:
        if not self.api_key:
            raise TushareRelayError(
                "INDEVS_TUSHARE_API_KEY is not configured",
                api_name=api_name,
                retryable=False,
            )

        params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        last_error: Exception | None = None
        for attempt in range(1, retries + 2):
            for base_url in self.base_urls:
                self._throttle()
                try:
                    return self._request_once(api_name, params, base_url=base_url, attempt=attempt)
                except TushareRelayError as exc:
                    last_error = exc
                    if not exc.retryable:
                        logger.debug(
                            "Tushare relay request failed without retry: api={} base={} status={} err={}",
                            api_name,
                            exc.base_url,
                            exc.status_code,
                            exc,
                        )
                    if attempt > retries or not exc.retryable:
                        continue
                except Exception as exc:
                    last_error = exc
                    logger.debug("Tushare relay request error: api={} base={} err={}", api_name, base_url, exc)
            if attempt <= retries:
                time.sleep(min(2.0 * (2 ** (attempt - 1)), 10.0))

        message = str(last_error) if last_error else f"Tushare relay request failed: {api_name}"
        raise TushareRelayError(message, api_name=api_name, retryable=False) from last_error

    def _request_once(
        self,
        api_name: str,
        params: dict[str, Any],
        *,
        base_url: str,
        attempt: int,
    ) -> TushareRelayResult:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{base_url}/{urllib.parse.quote(api_name)}"
        if query:
            url = f"{url}?{query}"

        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "GaoshouPlatform/relay-sync",
                "X-API-Key": self.api_key,
            },
            method="GET",
        )
        started = time.perf_counter()
        status_code = 0
        headers: dict[str, str] = {}
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                status_code = int(response.status)
                headers = {k.lower(): v for k, v in response.headers.items()}
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            status_code = int(exc.code)
            headers = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
            body = exc.read().decode("utf-8", errors="replace")
            raise TushareRelayError(
                self._error_message(api_name, status_code, body),
                api_name=api_name,
                base_url=base_url,
                status_code=status_code,
                retryable=status_code in RETRYABLE_HTTP_STATUS or status_code == 404,
            ) from exc
        except TimeoutError as exc:
            raise TushareRelayError(
                f"Tushare relay timeout after {self.timeout_seconds}s",
                api_name=api_name,
                base_url=base_url,
                retryable=True,
            ) from exc
        except OSError as exc:
            raise TushareRelayError(
                f"Tushare relay network error: {exc}",
                api_name=api_name,
                base_url=base_url,
                retryable=True,
            ) from exc

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise TushareRelayError(
                self._error_message(api_name, status_code, body),
                api_name=api_name,
                base_url=base_url,
                status_code=status_code,
                retryable=status_code in RETRYABLE_HTTP_STATUS,
            ) from exc

        code = payload.get("code")
        message = str(payload.get("msg") or payload.get("message") or "")
        if code not in (None, 0, "0"):
            retryable = "relay_pending" in message.lower() or "timeout" in message.lower()
            raise TushareRelayError(
                f"Tushare relay returned code={code}: {message}",
                api_name=api_name,
                base_url=base_url,
                status_code=status_code,
                retryable=retryable,
            )

        meta = TushareRelayMeta(
            api_name=api_name,
            base_url=base_url,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            cache=headers.get("x-api-relay-cache"),
            cache_bucket=headers.get("x-tushare-cache-bucket"),
            request_id=headers.get("x-request-id"),
            attempt=attempt,
        )
        return TushareRelayResult(rows=parse_relay_rows(payload), payload=payload, meta=meta)

    @staticmethod
    def _error_message(api_name: str, status_code: int, body: str) -> str:
        snippet = " ".join(body[:300].split())
        return f"Tushare relay {api_name} HTTP {status_code}: {snippet}"
