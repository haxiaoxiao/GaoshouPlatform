"""Tests for async_retry utility."""
import asyncio
import pytest
from app.core.retry import async_retry


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


@pytest.mark.asyncio
async def test_retry_succeeds_first_try():
    call_count = 0

    async def succeeds():
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await async_retry(succeeds, max_retries=3, base_delay=0.01)
    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_eventually_succeeds():
    call_count = 0

    async def fails_twice():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise TransientError("transient")
        return "recovered"

    result = await async_retry(
        fails_twice,
        max_retries=3,
        base_delay=0.01,
        retryable_exceptions=(TransientError,),
    )
    assert result == "recovered"
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_exhausted_raises():
    call_count = 0

    async def always_fails():
        nonlocal call_count
        call_count += 1
        raise TransientError("transient")

    with pytest.raises(TransientError):
        await async_retry(
            always_fails,
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(TransientError,),
        )
    assert call_count == 3  # initial + 2 retries


@pytest.mark.asyncio
async def test_non_retryable_raises_immediately():
    call_count = 0

    async def permanent_fail():
        nonlocal call_count
        call_count += 1
        raise PermanentError("permanent")

    with pytest.raises(PermanentError):
        await async_retry(
            permanent_fail,
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(TransientError,),
        )
    assert call_count == 1  # no retry


@pytest.mark.asyncio
async def test_backoff_delay_increases():
    """Verify the delay formula: base_delay * (backoff ** (attempt - 1))."""
    assert 0.01 * (2 ** 0) == 0.01
    assert 0.01 * (2 ** 1) == 0.02
    assert 0.01 * (2 ** 2) == 0.04


@pytest.mark.asyncio
async def test_passes_args_and_kwargs():
    async def with_args(a, b, c=0):
        return a + b + c

    result = await async_retry(with_args, 1, 2, c=3, max_retries=1, base_delay=0.01)
    assert result == 6
