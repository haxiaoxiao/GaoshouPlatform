# backend/app/db/clickhouse.py
"""ClickHouse 数据库连接"""
from clickhouse_driver import Client

from app.core.config import settings


def get_clickhouse_client() -> Client:
    """获取 ClickHouse 客户端"""
    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def init_clickhouse_tables():
    """初始化 ClickHouse 表"""
    client = get_clickhouse_client()

    # 创建数据库（如果不存在）
    client.execute(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")

    # 创建日K线表
    client.execute("""
        CREATE TABLE IF NOT EXISTS klines_daily
        (
            symbol LowCardinality(String),
            trade_date Date,
            open Decimal(10, 4),
            high Decimal(10, 4),
            low Decimal(10, 4),
            close Decimal(10, 4),
            volume UInt64,
            amount Decimal(18, 4),
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(trade_date)
        ORDER BY (symbol, trade_date)
    """)

    # 创建分钟K线表
    client.execute("""
        CREATE TABLE IF NOT EXISTS klines_minute
        (
            symbol LowCardinality(String),
            datetime DateTime,
            open Decimal(10, 4),
            high Decimal(10, 4),
            low Decimal(10, 4),
            close Decimal(10, 4),
            volume UInt64,
            amount Decimal(18, 4),
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(datetime)
        ORDER BY (symbol, datetime)
    """)


# 全局客户端
_clickhouse_client: Client | None = None


def get_ch_client() -> Client:
    """获取全局 ClickHouse 客户端（单例）"""
    global _clickhouse_client
    if _clickhouse_client is None:
        _clickhouse_client = get_clickhouse_client()
    return _clickhouse_client
