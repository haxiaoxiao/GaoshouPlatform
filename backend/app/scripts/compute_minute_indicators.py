"""
分钟K线指标计算脚本
基于分钟K线数据计算日内时序指标，存入 ClickHouse
"""
import asyncio
import sys
import io
from datetime import date, datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent.parent))

from clickhouse_driver import Client
from app.core.config import settings


def compute_minute_indicators():
    """计算分钟K线指标"""
    ch = Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
    )

    print("=== 计算分钟K线指标 ===")

    # 获取今日有数据的股票
    today = date.today()
    stocks = ch.execute(f"""
        SELECT DISTINCT symbol
        FROM klines_minute
        WHERE toDate(datetime) = '{today}'
        LIMIT 1000
    """)
    symbols = [s[0] for s in stocks]
    print(f"今日有数据的股票: {len(symbols)}")

    if not symbols:
        print("没有找到今日的分钟K线数据")
        return

    # 指标SQL模板
    indicators = [
        # 日内涨跌幅 (当日最新价 vs 开盘价)
        ("intraday_return", """
            (last(close) - first(open)) / first(open) * 100
        """, "%"),

        # 当前价格
        ("last_price", "last(close)", "元"),

        # 开盘价
        ("open_price", "first(open)", "元"),

        # 最高价
        ("high_price", "max(high)", "元"),

        # 最低价
        ("low_price", "min(low)", "元"),

        # 成交量
        ("total_volume", "sum(volume)", "股"),

        # 成交额
        ("total_amount", "sum(amount)", "万元"),

        # 均价 (VWAP)
        ("vwap", "sum(amount) / NULLIF(sum(volume), 0)", "元"),

        # 均价（简单）
        ("avg_price", "avg(close)", "元"),

        # 涨跌幅 (收盘 vs 昨日收盘) - 需要昨日收盘价
        # 这里简化为日内波动
        ("intraday_volatility", """
            (max(close) - min(close)) / first(open) * 100
        """, "%"),

        # 换手率 (需要流通股本，这里用成交量/100万估算)
        ("volume_ratio", """
            sum(volume) / 1000000
        """, "万手"),
    ]

    for symbol in symbols[:100]:  # 先测试100只
        try:
            # 获取该股票今日的分钟数据
            rows = ch.execute(f"""
                SELECT
                    toDateTime('{today}') as datetime,
                    '{symbol}' as symbol,
                    last(close) as last_price,
                    first(open) as open_price,
                    max(high) as high_price,
                    min(low) as low_price,
                    sum(volume) as total_volume,
                    sum(amount) as total_amount,
                    sum(amount) / NULLIF(sum(volume), 0) as vwap,
                    avg(close) as avg_price,
                    (max(close) - min(close)) / first(open) * 100 as intraday_volatility,
                    (last(close) - first(open)) / first(open) * 100 as intraday_return
                FROM klines_minute
                WHERE symbol = '{symbol}' AND toDate(datetime) = '{today}'
            """)

            if rows and rows[0]:
                # 插入指标数据
                for indicator_name, value in [
                    ("last_price", rows[0][1]),
                    ("open_price", rows[0][2]),
                    ("high_price", rows[0][3]),
                    ("low_price", rows[0][4]),
                    ("total_volume", rows[0][5]),
                    ("total_amount", rows[0][6]),
                    ("vwap", rows[0][7]),
                    ("avg_price", rows[0][8]),
                    ("intraday_volatility", rows[0][9]),
                    ("intraday_return", rows[0][10]),
                ]:
                    if value is not None:
                        ch.execute(
                            """INSERT INTO indicator_timeseries
                               (symbol, indicator_name, datetime, value, updated_at) VALUES""",
                            [(symbol, indicator_name, rows[0][0], float(value), datetime.now())]
                        )

            print(f"\r[{symbols.index(symbol)+1}/{len(symbols[:100])}] {symbol}", end="")

        except Exception as e:
            print(f"\n[X] {symbol}: {e}")

    # 统计
    count = ch.execute("SELECT count() FROM indicator_timeseries")[0][0]
    print(f"\n\n指标计算完成!")
    print(f"indicator_timeseries 总行数: {count:,}")


if __name__ == "__main__":
    compute_minute_indicators()