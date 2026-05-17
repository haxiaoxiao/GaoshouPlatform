"""行情数据存储工厂"""
from __future__ import annotations

from app.core.config import settings
from app.data_stores.base import MarketDataStore


def get_market_data_store() -> MarketDataStore:
    """根据配置返回行情数据存储实例"""
    if settings.market_data_backend == "clickhouse":
        from app.data_stores.clickhouse_store import ClickHouseMarketDataStore

        return ClickHouseMarketDataStore()
    from app.data_stores.parquet_store import ParquetMarketDataStore

    return ParquetMarketDataStore(settings.parquet_data_dir)


def get_indicator_store():
    """根据配置返回指标存储实例"""
    if settings.market_data_backend == "clickhouse":
        from app.data_stores.clickhouse_indicator_store import ClickHouseIndicatorStore

        return ClickHouseIndicatorStore()
    from app.data_stores.parquet_indicator_store import ParquetIndicatorStore

    return ParquetIndicatorStore(settings.parquet_data_dir)
