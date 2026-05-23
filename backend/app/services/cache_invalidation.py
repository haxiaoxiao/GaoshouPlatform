"""Cache invalidation helpers after data syncs."""

from __future__ import annotations

from fnmatch import fnmatch

from loguru import logger

from app.compute.cache import get_compute_cache
from app.services.backtest_redis_cache import get_backtest_cache


def invalidate_after_sync(sync_type: str) -> dict[str, object]:
    """Invalidate coarse caches that can be stale after a data sync."""
    patterns_by_sync = {
        "stock_info": ["*:index_components:*", "*:daily_basic_mv:*"],
        "stock_full": ["*:index_components:*", "*:daily_basic_mv:*"],
        "financial_data": ["*:daily_basic_mv:*"],
        "realtime_mv": ["*:daily_basic_mv:*"],
        "kline_daily": ["*:daily_window:*", "*:daily_basic_mv:*", "*:timer_coverage:*"],
        "index_daily": ["*:daily_window:*", "*:timer_coverage:*"],
        "kline_minute": ["*:timer_coverage:*"],
        "kline_weekly": [],
        "dividends": [],
        "factor_dependency": ["*:daily_window:*", "*:daily_basic_mv:*", "*:timer_coverage:*", "*:index_components:*"],
    }
    patterns = patterns_by_sync.get(sync_type, [])
    get_compute_cache().clear_l1()
    deleted = _delete_backtest_cache_patterns(patterns)
    return {
        "sync_type": sync_type,
        "compute_l1_cleared": True,
        "redis_patterns": patterns,
        "redis_deleted": deleted,
    }


def _delete_backtest_cache_patterns(patterns: list[str]) -> int:
    if not patterns:
        return 0
    cache = get_backtest_cache()
    if not cache.available:
        return 0
    client = cache._binary_client()
    if client is None:
        return 0
    deleted = 0
    try:
        for raw_key in client.scan_iter(match=f"{cache.namespace}:*", count=1000):
            key = raw_key.decode("utf-8", errors="ignore") if isinstance(raw_key, bytes) else str(raw_key)
            if any(fnmatch(key, pattern) for pattern in patterns):
                deleted += int(client.delete(raw_key) or 0)
    except Exception as exc:
        logger.debug("Redis cache invalidation failed: {}", exc)
    return deleted
