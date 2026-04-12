import asyncio
from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class StockInfo:
    """股票信息"""

    symbol: str
    name: str
    exchange: str | None = None
    industry: str | None = None
    list_date: date | None = None


@dataclass
class KlineData:
    """K线数据"""

    symbol: str
    datetime: datetime | date
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float


class QMTGateway:
    """miniQMT 数据网关 (xtquant 封装)"""

    def __init__(self):
        self._connected = False
        self._xt = None

    def _get_xt(self):
        """延迟导入 xtquant"""
        if self._xt is None:
            try:
                import xtquant.xtdata as xt

                self._xt = xt
            except ImportError as e:
                raise RuntimeError("xtquant 未安装，请运行: pip install xtquant") from e
        return self._xt

    async def check_connection(self) -> bool:
        """检查连接状态"""
        try:
            xt = self._get_xt()
            # 尝试获取股票列表来验证连接
            stocks = xt.get_stock_list_in_sector("沪深A股")
            self._connected = len(stocks) > 0
            return self._connected
        except Exception:
            self._connected = False
            return False

    async def get_stock_list(self) -> list[StockInfo]:
        """获取股票列表"""
        xt = self._get_xt()

        # 在线程池中运行同步代码
        loop = asyncio.get_event_loop()
        stock_codes = await loop.run_in_executor(
            None, xt.get_stock_list_in_sector, "沪深A股"
        )

        results = []
        for code in stock_codes:
            try:
                # 获取股票详情
                info = await loop.run_in_executor(
                    None, xt.get_instrument_detail, code
                )
                if info:
                    stock_info = StockInfo(
                        symbol=code,
                        name=info.get("InstrumentName", ""),
                        exchange=info.get("ExchangeCode"),
                        industry=info.get("ProductClass"),
                        list_date=self._parse_date(info.get("OpenDate")),
                    )
                    results.append(stock_info)
            except Exception:
                # 单只股票获取失败不影响整体
                continue

        return results

    async def get_kline_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[KlineData]:
        """获取日K线数据"""
        xt = self._get_xt()

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            lambda: xt.get_market_data_ex(
                field_list=[],
                stock_list=[symbol],
                period="1d",
                start_time=start_str,
                end_time=end_str,
            ),
        )

        results = []
        if symbol in data:
            df = data[symbol]
            for idx, row in df.iterrows():
                try:
                    kline = KlineData(
                        symbol=symbol,
                        datetime=idx.date() if hasattr(idx, "date") else idx,
                        open=float(row.get("open", 0)),
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=float(row.get("close", 0)),
                        volume=int(row.get("volume", 0)),
                        amount=float(row.get("amount", 0)),
                    )
                    results.append(kline)
                except Exception:
                    continue

        return results

    async def get_kline_minute(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[KlineData]:
        """获取分钟K线数据"""
        xt = self._get_xt()

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            lambda: xt.get_market_data_ex(
                field_list=[],
                stock_list=[symbol],
                period="1m",
                start_time=start_str,
                end_time=end_str,
            ),
        )

        results = []
        if symbol in data:
            df = data[symbol]
            for idx, row in df.iterrows():
                try:
                    kline = KlineData(
                        symbol=symbol,
                        datetime=idx,
                        open=float(row.get("open", 0)),
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=float(row.get("close", 0)),
                        volume=int(row.get("volume", 0)),
                        amount=float(row.get("amount", 0)),
                    )
                    results.append(kline)
                except Exception:
                    continue

        return results

    def _parse_date(self, date_str: str | None) -> date | None:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            return datetime.strptime(str(date_str), "%Y%m%d").date()
        except ValueError:
            return None


# 全局单例
qmt_gateway = QMTGateway()
