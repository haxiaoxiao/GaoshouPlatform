"""Exponential backoff retry utility for async operations."""
import asyncio
from collections.abc import Callable, Awaitable
from typing import Any, TypeVar

from loguru import logger

T = TypeVar("T")

DEFAULT_RETRYABLE = (
    ConnectionError,
    TimeoutError,
    OSError,
)


async def async_retry(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff: float = 2.0,
    retryable_exceptions: tuple[type[Exception], ...] = DEFAULT_RETRYABLE,
    **kwargs: Any,
) -> T:
    """对异步函数执行指数退避重试。

    Args:
        func: 要重试的异步函数
        *args: 传递给 func 的位置参数
        max_retries: 最大重试次数（不含首次执行，共执行 max_retries+1 次）
        base_delay: 首次重试等待秒数
        backoff: 退避倍数（每次重试延迟 = base_delay * backoff^(attempt-1)）
        retryable_exceptions: 可重试的异常类型元组
        **kwargs: 传递给 func 的关键字参数

    Returns:
        func 的返回值
    """
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except retryable_exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = base_delay * (backoff ** attempt)
                logger.warning(
                    "Retry %d/%d for %s: %s: %s. Waiting %.1fs...",
                    attempt + 1, max_retries, func.__name__,
                    e.__class__.__name__, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "All %d retries exhausted for %s: %s: %s",
                    max_retries, func.__name__,
                    e.__class__.__name__, e,
                )
        except Exception:
            raise

    raise last_exception  # type: ignore[misc]
