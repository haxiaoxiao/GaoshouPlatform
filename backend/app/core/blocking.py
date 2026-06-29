"""Shared executor for blocking I/O work.

Most platform blocking calls are I/O-heavy adapters around local files, DuckDB,
xtquant, or third-party SDKs.  Installing this executor as asyncio's default
keeps existing ``asyncio.to_thread`` and ``loop.run_in_executor(None, ...)``
call sites on one configurable pool instead of relying on Python's implicit
default size.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Lock
from typing import Any, Callable, TypeVar

from loguru import logger

from app.core.config import settings

_T = TypeVar("_T")

_executor: ThreadPoolExecutor | None = None
_executor_lock = Lock()


def _configured_workers() -> int:
    return max(1, int(settings.blocking_thread_pool_workers or 1))


def get_blocking_executor() -> ThreadPoolExecutor:
    """Return the process-wide executor for ordinary blocking work."""
    global _executor
    with _executor_lock:
        if _executor is None:
            _executor = ThreadPoolExecutor(
                max_workers=_configured_workers(),
                thread_name_prefix="gaoshou-blocking",
            )
        return _executor


def install_default_executor() -> None:
    """Make the shared executor the default for the current event loop."""
    loop = asyncio.get_running_loop()
    loop.set_default_executor(get_blocking_executor())
    logger.info("Installed shared blocking executor with {} workers", _configured_workers())


async def run_blocking(func: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:
    """Run a synchronous callable on the shared blocking executor."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_blocking_executor(), partial(func, *args, **kwargs))


async def run_detached_blocking_with_timeout(
    func: Callable[[], _T],
    /,
    *,
    timeout: int | float,
    thread_name_prefix: str = "gaoshou-timeout",
) -> _T:
    """Run one blocking callable with an asyncio timeout without occupying the shared pool.

    Python cannot forcibly stop a thread already inside a third-party SDK call.
    On timeout we detach that worker and return control to asyncio immediately,
    so the platform's shared executor remains available for unrelated work.
    """
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=thread_name_prefix)
    future = executor.submit(func)
    shutdown_now = False
    try:
        return await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout)
    except asyncio.TimeoutError:
        shutdown_now = True
        future.cancel()
        raise
    except asyncio.CancelledError:
        shutdown_now = True
        future.cancel()
        raise
    finally:
        if shutdown_now or future.done():
            executor.shutdown(wait=False, cancel_futures=True)


def shutdown_default_executor() -> None:
    """Stop the shared executor during application shutdown."""
    global _executor
    with _executor_lock:
        executor = _executor
        _executor = None
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)
        logger.info("Shared blocking executor stopped")
