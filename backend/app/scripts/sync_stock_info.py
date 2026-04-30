"""
股票基础信息同步脚本
"""
import asyncio
import sys
import io
from datetime import datetime
from pathlib import Path

# 修复 Windows 控制台编码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from clickhouse_driver import Client
from app.engines.qmt_gateway import qmt_gateway


class StockInfoSyncer:
    """股票信息同步器"""

    def __init__(self):
        self.client = Client(
            host='localhost',
            port=19000,
            database='gaoshou'
        )
        self.total_stocks = 0
        self.synced_stocks = 0
        self.failed_stocks = []

    def create_table(self):
        """创建数据表"""
        self.client.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol String,
                name String,
                exchange String,
                industry String,
                list_date Date,
                is_st UInt8 DEFAULT 0,
                total_shares Float64,
                float_shares Float64,
                total_mv Float64,
                circ_mv Float64,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY symbol
        """)
        print("[OK] stock_info 表已创建")

    async def sync_stock_info(self):
        """同步股票信息"""
        print("\n=== 获取股票列表 ===")
        stocks = await qmt_gateway.get_stock_list()
        self.total_stocks = len(stocks)
        print(f"[OK] 获取到 {self.total_stocks} 只股票\n")

        print("=== 同步股票信息 ===")
        start_time = datetime.now()

        for i, stock in enumerate(stocks, 1):
            try:
                row = {
                    'symbol': stock.symbol,
                    'name': stock.name or '',
                    'exchange': stock.exchange or '',
                    'industry': stock.industry or '',
                    'list_date': stock.list_date,
                    'is_st': 1 if stock.is_st else 0,
                    'total_shares': stock.total_shares or 0.0,
                    'float_shares': stock.float_shares or 0.0,
                    'total_mv': stock.total_mv or 0.0,
                    'circ_mv': stock.circ_mv or 0.0,
                }

                self.client.execute(
                    "INSERT INTO stock_info (symbol, name, exchange, industry, list_date, is_st, total_shares, float_shares, total_mv, circ_mv) VALUES",
                    [row]
                )

                self.synced_stocks += 1

                # 显示进度
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = i / len(stocks) * 100

                st_count = sum(1 for s in stocks[:i] if s.is_st)
                print(f"\r[{i}/{len(stocks)}] {rate:5.1f}% | {stock.symbol:8s} {stock.name:10s} | ST: {st_count}", end="")

            except Exception as e:
                self.failed_stocks.append((stock.symbol, str(e)))
                print(f"\n[X] {stock.symbol} 失败: {e}")

        print(f"\n\n=== 同步完成 ===")
        print(f"成功: {self.synced_stocks}/{self.total_stocks}")
        if self.failed_stocks:
            print(f"失败: {len(self.failed_stocks)} 只")

    def get_stats(self):
        """获取统计信息"""
        result = self.client.execute("SELECT count() FROM stock_info")
        count = result[0][0] if result else 0

        st_result = self.client.execute("SELECT count() FROM stock_info WHERE is_st = 1")
        st_count = st_result[0][0] if st_result else 0

        return count, st_count


async def main():
    print("=" * 60)
    print("GaoshouPlatform 股票信息同步")
    print("=" * 60)

    syncer = StockInfoSyncer()

    # 检查 QMT 连接
    print("\n检查 QMT 连接...")
    connected = await qmt_gateway.check_connection()
    if not connected:
        print("[X] 无法连接 QMT，请确保 QMT 已登录")
        return
    print("[OK] QMT 连接正常")

    # 创建表
    syncer.create_table()

    # 同步股票信息
    await syncer.sync_stock_info()

    # 显示统计
    count, st_count = syncer.get_stats()
    print(f"\n股票总数: {count}")
    print(f"ST股票: {st_count}")


if __name__ == "__main__":
    asyncio.run(main())
