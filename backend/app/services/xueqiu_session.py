from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, Callable


@dataclass(frozen=True)
class XueqiuLoginResult:
    status: str
    auth: dict[str, Any]


class XueqiuSession:
    def __init__(
        self,
        *,
        crawler_factory: Callable[[], Any] | None = None,
        login_verifier: Callable[[Any], dict[str, Any]] | None = None,
        collector: Callable[..., Any],
        disconnector: Callable[[Any], None] | None = None,
        poll_interval: float = 2.0,
        login_timeout: float = 900.0,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._crawler_factory = crawler_factory or self._default_crawler_factory
        self._login_verifier = login_verifier or self._default_login_verifier
        self._collector = collector
        self._disconnector = disconnector or self._default_disconnector
        self._poll_interval = max(0.0, poll_interval)
        self._login_timeout = max(0.0, login_timeout)
        self._progress_callback = progress_callback
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="xueqiu")
        self._crawler: Any | None = None
        self._disconnected = False

    @staticmethod
    def _default_crawler_factory() -> Any:
        from app.services.sentiment import _BuiltinXueqiuCrawler

        return _BuiltinXueqiuCrawler()

    @staticmethod
    def _default_login_verifier(crawler: Any) -> dict[str, Any]:
        from app.services.sentiment import _inject_xueqiu_cookie, _verify_xueqiu_login

        return _verify_xueqiu_login(crawler, _inject_xueqiu_cookie(crawler))

    @staticmethod
    def _default_disconnector(crawler: Any) -> None:
        disconnect = getattr(crawler, "disconnect", None) or getattr(crawler, "close")
        disconnect()

    async def _run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, partial(func, *args, **kwargs))

    def _emit(self, stage: str, **details: Any) -> None:
        if self._progress_callback is not None:
            self._progress_callback(
                {
                    "stage": stage,
                    "source": "xueqiu_spyder",
                    "updated_at": datetime.now().isoformat(),
                    **details,
                }
            )

    async def start(self) -> None:
        if self._crawler is None:
            self._crawler = await self._run(self._crawler_factory)

    async def wait_for_login(self) -> XueqiuLoginResult:
        if self._crawler is None:
            raise RuntimeError("Xueqiu session has not been started")

        started_at = datetime.now().isoformat()
        deadline = time.monotonic() + self._login_timeout
        waiting_emitted = False
        while True:
            auth = await self._run(self._login_verifier, self._crawler)
            if bool(auth.get("server_verified")):
                if waiting_emitted:
                    self._emit("xueqiu_spyder.login_succeeded")
                return XueqiuLoginResult(status="authenticated", auth=auth)

            if not waiting_emitted:
                self._emit(
                    "xueqiu_spyder.waiting_for_login",
                    login_wait_started_at=started_at,
                    login_wait_timeout_seconds=self._login_timeout,
                    login_url="https://xueqiu.com/",
                )
                waiting_emitted = True

            if time.monotonic() >= deadline:
                return XueqiuLoginResult(status="login_timeout", auth=auth)
            await asyncio.sleep(self._poll_interval)

    async def collect(self, symbol: str, **kwargs: Any) -> Any:
        if self._crawler is None:
            raise RuntimeError("Xueqiu session has not been started")
        return await self._run(self._collector, self._crawler, symbol, **kwargs)

    async def disconnect(self) -> None:
        if self._disconnected:
            return
        self._disconnected = True
        try:
            if self._crawler is not None:
                await self._run(self._disconnector, self._crawler)
        finally:
            self._executor.shutdown(wait=True, cancel_futures=True)
