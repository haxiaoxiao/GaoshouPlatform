"""行情数据存储工厂"""
from __future__ import annotations

from app.core.config import settings
from app.core.dev_data_mode import apply_dev_data_mode_to_settings
from app.data_stores.base import MarketDataStore


def get_market_data_store() -> MarketDataStore:
    """根据配置返回行情数据存储实例"""
    from app.data_stores.parquet_store import ParquetMarketDataStore

    apply_dev_data_mode_to_settings()
    return ParquetMarketDataStore(settings.parquet_data_dir)


def get_indicator_store():
    """根据配置返回指标存储实例"""
    from app.data_stores.parquet_indicator_store import ParquetIndicatorStore

    apply_dev_data_mode_to_settings()
    return ParquetIndicatorStore(settings.parquet_data_dir)
