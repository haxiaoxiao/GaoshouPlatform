"""
数据同步脚本 - 同步股票行情数据到 ClickHouse
"""
import asyncio
import sys
import io
from datetime import date, datetime
from pathlib import Path

# 修复 Windows 控制台编码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from clickhouse_driver import Client
from app.engines.qmt_gateway import qmt_gateway


class DataSyncer:
    """数据同步器"""

    def __init__(self):
        self.client = Client(
            host='localhost',
            port=19000,
            database='gaoshou'
        )
        self.total_stocks = 0
        self.synced_stocks = 0
        self.failed_stocks = []
        self.total_rows = 0

    def create_tables(self):
        """创建数据表"""
        self.client.execute("""
            CREATE TABLE IF NOT EXISTS klines_daily (
                symbol String,
                datetime Date,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Float64,
                amount Float64
            ) ENGINE = MergeTree()
            ORDER BY (symbol, datetime)
        """)
        print("[OK] 数据表已创建")

    async def sync_stock_list(self) -> list:
        """获取并同步股票列表"""
        print("\n=== 获取股票列表 ===")
        stocks = await qmt_gateway.get_stock_list()
        self.total_stocks = len(stocks)
        print(f"[OK] 获取到 {self.total_stocks} 只股票")
        return [s.symbol for s in stocks]

    async def sync_klines(self, symbols: list[str], start_date: date, end_date: date):
        """同步K线数据"""
        print(f"\n=== 同步K线数据 ({start_date} ~ {end_date}) ===")
        print(f"共 {len(symbols)} 只股票需要同步\n")

        start_time = datetime.now()

        for i, symbol in enumerate(symbols, 1):
            try:
                # 获取K线数据
                klines = await qmt_gateway.get_kline_daily(symbol, start_date, end_date)

                if klines:
                    # 转换为 ClickHouse 格式
                    rows = [
                        {
                            'symbol': k.symbol,
                            'trade_date': k.datetime,
                            'open': k.open,
                            'high': k.high,
                            'low': k.low,
                            'close': k.close,
                            'volume': k.volume,
                            'amount': k.amount
                        }
                        for k in klines
                    ]

                    # 批量插入
                    self.client.execute(
                        "INSERT INTO klines_daily (symbol, trade_date, open, high, low, close, volume, amount) VALUES",
                        rows
                    )

                    self.total_rows += len(rows)
                    self.synced_stocks += 1

                # 显示进度
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = i / len(symbols) * 100
                eta = elapsed / i * (len(symbols) - i) if i > 0 else 0

                print(f"\r[{i}/{len(symbols)}] {rate:5.1f}% | "
                      f"{symbol:8s} {len(klines):4d}条 | "
                      f"累计: {self.total_rows:8d}行 | "
                      f"ETA: {int(eta//60):2d}:{int(eta%60):02d}", end="")

            except Exception as e:
                self.failed_stocks.append((symbol, str(e)))
                print(f"\n[X] {symbol} 同步失败: {e}")

        print(f"\n\n=== 同步完成 ===")
        print(f"成功: {self.synced_stocks}/{self.total_stocks}")
        print(f"总行数: {self.total_rows}")
        if self.failed_stocks:
            print(f"失败: {len(self.failed_stocks)} 只")
            for sym, err in self.failed_stocks[:10]:
                print(f"  - {sym}: {err}")

    def get_stats(self):
        """获取数据库统计"""
        result = self.client.execute("SELECT count() FROM klines_daily")
        count = result[0][0] if result else 0
        return count


async def main():
    print("=" * 60)
    print("GaoshouPlatform 数据同步工具")
    print("=" * 60)

    syncer = DataSyncer()

    # 检查 QMT 连接
    print("\n检查 QMT 连接...")
    connected = await qmt_gateway.check_connection()
    if not connected:
        print("[X] 无法连接 QMT，请确保 QMT 已登录")
        return
    print("[OK] QMT 连接正常")

    # 创建表
    syncer.create_tables()

    # 获取股票列表
    symbols = await syncer.sync_stock_list()

    # 同步最近10年数据
    end_date = date.today()
    start_date = date(end_date.year - 10, 1, 1)

    await syncer.sync_klines(symbols, start_date, end_date)

    # 显示最终统计
    print(f"\n数据库总记录: {syncer.get_stats()}")


if __name__ == "__main__":
    asyncio.run(main())
