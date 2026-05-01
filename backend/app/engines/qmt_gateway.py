import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from loguru import logger


@dataclass
class StockInfo:
    """股票完整信息 - 涵盖QMT所有可用字段"""

    # === 基础信息 ===
    symbol: str
    name: str
    exchange: str | None = None

    # === 分类信息 ===
    industry: str | None = None
    industry2: str | None = None
    industry3: str | None = None
    sector: str | None = None
    concept: str | None = None

    # === 上市信息 ===
    list_date: date | None = None
    delist_date: date | None = None
    is_st: int = 0
    is_delist: int = 0
    is_suspend: int = 0

    # === 股本结构(单位: 万股) ===
    total_shares: float | None = None
    float_shares: float | None = None
    a_float_shares: float | None = None
    limit_sell_shares: float | None = None

    # === 市值信息(单位: 万元) ===
    total_mv: float | None = None
    circ_mv: float | None = None

    # === 公司基本信息 ===
    company_name: str | None = None
    company_name_en: str | None = None
    province: str | None = None
    city: str | None = None
    office_addr: str | None = None
    business_scope: str | None = None
    main_business: str | None = None
    website: str | None = None
    employees: int | None = None

    # === 财务指标(最新) ===
    eps: float | None = None
    bvps: float | None = None
    roe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    total_assets: float | None = None
    total_liability: float | None = None
    total_equity: float | None = None
    net_profit: float | None = None
    revenue: float | None = None

    # === 其他属性 ===
    security_type: str | None = None
    product_class: str | None = None

    # === 原始数据(用于调试) ===
    raw_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class FinancialQuarter:
    """单季财务数据"""

    symbol: str
    report_date: date
    report_type: str | None = None
    eps: float | None = None
    bvps: float | None = None
    roe: float | None = None
    revenue: float | None = None
    net_profit: float | None = None
    revenue_yoy: float | None = None
    profit_yoy: float | None = None
    gross_margin: float | None = None
    total_assets: float | None = None
    total_liability: float | None = None
    total_equity: float | None = None
    total_shares: float | None = None
    float_shares: float | None = None
    a_float_shares: float | None = None
    limit_sell_shares: float | None = None
    total_mv: float | None = None
    circ_mv: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    raw_data: str | None = None


@dataclass
class KlineData:
    """K线数据 - 扩展字段"""

    symbol: str
    datetime: datetime | date
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    # 扩展字段
    turnover: float | None = None  # 换手率
    amplitude: float | None = None  # 振幅
    change_pct: float | None = None  # 涨跌幅
    change_amt: float | None = None  # 涨跌额


class QMTGateway:
    """miniQMT 数据网关 (xtquant 封装) - 扩展版"""

    def __init__(self):
        self._connected = False
        self._xt = None
        self._industry_map: dict[str, str] | None = None

    def _get_xt(self):
        """延迟导入 xtquant"""
        if self._xt is None:
            try:
                import xtquant.xtdata as xt

                self._xt = xt
            except ImportError as e:
                raise RuntimeError("xtquant 未安装，请运行: pip install xtquant") from e
        return self._xt

    def _build_industry_map(self) -> dict[str, str]:
        """构建 symbol -> 申万一级行业 映射"""
        if self._industry_map is not None:
            return self._industry_map

        xt = self._get_xt()
        industry_map: dict[str, str] = {}

        try:
            all_sectors = xt.get_sector_list()
            sw1_sectors = [
                s for s in all_sectors
                if s.startswith("SW1") and "权" not in s
            ]
            for sector_name in sw1_sectors:
                try:
                    stocks = xt.get_stock_list_in_sector(sector_name)
                    industry_name = sector_name.replace("SW1", "").strip()
                    for symbol in stocks:
                        industry_map[symbol] = industry_name
                except Exception:
                    continue
        except Exception:
            pass

        self._industry_map = industry_map
        return self._industry_map

    async def check_connection(self) -> bool:
        """检查连接状态"""
        try:
            xt = self._get_xt()
            info = xt.get_instrument_detail("600051.SH", iscomplete=False)
            self._connected = info is not None
            return self._connected
        except Exception:
            self._connected = False
            return False

    def _scan_all_stocks(self) -> list[str]:
        """当板块数据不可用时，通过申万一级行业板块扫描A股"""
        xt = self._get_xt()
        a_stocks: set[str] = set()

        try:
            sector_list = xt.get_sector_list()
            for s in sector_list:
                if s.startswith("SW1") and "\u6743" not in s:
                    try:
                        codes = xt.get_stock_list_in_sector(s)
                        for code in codes:
                            if code.endswith((".SH", ".SZ")):
                                num = code[:6]
                                if num.startswith(("60", "00", "30", "68")):
                                    a_stocks.add(code)
                    except Exception:
                        continue
        except Exception:
            pass

        return sorted(list(a_stocks))

    def _determine_sector(self, symbol: str, exchange: str | None) -> str | None:
        """判断板块类型"""
        if not symbol:
            return None
        if symbol.startswith("688"):
            return "科创板"
        elif symbol.startswith("300"):
            return "创业板"
        elif symbol.startswith("8") or symbol.startswith("4"):
            return "北交所"
        elif exchange == "SH":
            return "沪市主板"
        elif exchange == "SZ":
            return "深市主板"
        return "主板"

    def _determine_st_status(self, name: str) -> int:
        """判断ST状态"""
        if not name:
            return 0
        if "*ST" in name:
            return 2
        if "ST" in name:
            return 1
        return 0

    @staticmethod
    def _extract_exchange(symbol: str) -> str:
        """从代码后缀提取交易所标识，如 600051.SH -> SH"""
        if "." in symbol:
            return symbol.rsplit(".", 1)[-1]
        return ""

    async def get_stock_list(self) -> list[StockInfo]:
        """获取股票列表 - 含股本和计算市值"""
        xt = self._get_xt()
        loop = asyncio.get_running_loop()

        stock_codes = await loop.run_in_executor(
            None, xt.get_stock_list_in_sector, "沪深A股"
        )

        if not stock_codes:
            logger.info("Sector data empty, trying download...")
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, xt.download_sector_data),
                    timeout=30,
                )
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"download_sector_data failed: {e}")

            stock_codes = await loop.run_in_executor(
                None, xt.get_stock_list_in_sector, "沪深A股"
            )

        if not stock_codes:
            logger.info("Sector still empty, falling back to SW-sector scan")
            stock_codes = await loop.run_in_executor(None, self._scan_all_stocks)

        if not stock_codes:
            raise RuntimeError("QMT返回股票列表为空，请确认QMT客户端是否在线")

        industry_map = await loop.run_in_executor(
            None, self._build_industry_map
        )

        results = []
        for code in stock_codes:
            try:
                info = await loop.run_in_executor(
                    None, lambda c=code: xt.get_instrument_detail(c, iscomplete=True)
                )
                if info:
                    name = info.get("InstrumentName", "")
                    exchange = info.get("ExchangeID") or self._extract_exchange(code)

                    total_vol = info.get("TotalVolume")
                    float_vol = info.get("FloatVolume") or info.get("FloatVolumn")
                    pre_close = info.get("PreClose")

                    total_mv = None
                    circ_mv = None
                    if total_vol and pre_close and pre_close > 0:
                        total_mv = round(total_vol * pre_close / 10000, 2)
                    if float_vol and pre_close and pre_close > 0:
                        circ_mv = round(float_vol * pre_close / 10000, 2)

                    stock_info = StockInfo(
                        symbol=code,
                        name=name,
                        exchange=exchange,
                        industry=industry_map.get(code) or info.get("ProductClass"),
                        sector=self._determine_sector(code, exchange),
                        list_date=self._parse_date(info.get("OpenDate")),
                        is_st=self._determine_st_status(name),
                        product_class=info.get("ProductClass"),
                        security_type=info.get("ProductType"),
                        total_shares=total_vol / 10000 if total_vol else None,
                        float_shares=float_vol / 10000 if float_vol else None,
                        total_mv=total_mv,
                        circ_mv=circ_mv,
                        raw_data=dict(info),
                    )
                    results.append(stock_info)
            except Exception:
                continue

        return results

    async def get_stock_full_info(self, symbol: str) -> StockInfo | None:
        """获取股票完整信息 - 含财务数据(按最新季度)"""
        xt = self._get_xt()
        loop = asyncio.get_running_loop()

        try:
            info = await loop.run_in_executor(
                None, lambda: xt.get_instrument_detail(symbol, iscomplete=True)
            )
            if not info:
                return None

            name = info.get("InstrumentName", "")
            exchange = info.get("ExchangeID") or self._extract_exchange(symbol)

            quote_data = {}
            try:
                quote = await loop.run_in_executor(
                    None, lambda: xt.get_full_tick([symbol])
                )
                quote_data = quote.get(symbol, {}) if quote else {}
            except Exception:
                pass

            total_mv = quote_data.get("totalValue")
            circ_mv = quote_data.get("floatValue")

            financial_data = await self._fetch_financial_data(symbol)

            eps = bvps = roe = revenue = net_profit = None
            revenue_yoy = profit_yoy = gross_margin = None
            total_assets = total_liability = total_equity = None
            total_shares = float_shares = a_float_shares = limit_sell_shares = None
            pe_ttm = pb = None

            if financial_data:
                fq = financial_data[0]
                eps = fq.eps
                bvps = fq.bvps
                roe = fq.roe
                revenue = fq.revenue
                net_profit = fq.net_profit
                revenue_yoy = fq.revenue_yoy
                profit_yoy = fq.profit_yoy
                gross_margin = fq.gross_margin
                total_assets = fq.total_assets
                total_liability = fq.total_liability
                total_equity = fq.total_equity
                total_shares = fq.total_shares
                float_shares = fq.float_shares
                a_float_shares = fq.a_float_shares
                limit_sell_shares = fq.limit_sell_shares
                if not total_mv:
                    total_mv = fq.total_mv
                if not circ_mv:
                    circ_mv = fq.circ_mv

            if total_mv and net_profit and net_profit != 0:
                pe_ttm = round(total_mv / net_profit, 4)
            if total_mv and total_equity and total_equity != 0:
                pb = round(total_mv / total_equity, 4)

            stock_info = StockInfo(
                symbol=symbol,
                name=name,
                exchange=exchange,
                security_type=info.get("ProductType"),
                product_class=info.get("ProductClass"),
                industry=self._build_industry_map().get(symbol) or info.get("ProductClass"),
                sector=self._determine_sector(symbol, exchange),
                list_date=self._parse_date(info.get("OpenDate")),
                delist_date=self._parse_date(info.get("ExpireDate")),
                is_st=self._determine_st_status(name),
                is_suspend=1 if info.get("InstrumentStatus") == "Suspend" else 0,
                total_shares=total_shares or info.get("TotalVolume"),
                float_shares=float_shares or info.get("FloatVolume"),
                a_float_shares=a_float_shares,
                limit_sell_shares=limit_sell_shares,
                total_mv=total_mv,
                circ_mv=circ_mv,
                eps=eps,
                bvps=bvps,
                roe=roe,
                pe_ttm=pe_ttm,
                pb=pb,
                total_assets=total_assets,
                total_liability=total_liability,
                total_equity=total_equity,
                net_profit=net_profit,
                revenue=revenue,
                raw_data=dict(info),
            )

            return stock_info
        except Exception:
            return None

    async def _fetch_financial_data(
        self, symbol: str, report_count: int = 1
    ) -> list[FinancialQuarter]:
        """从QMT获取财务数据(按季度) - 读取本地缓存，不触发下载"""
        xt = self._get_xt()
        loop = asyncio.get_running_loop()

        results: list[FinancialQuarter] = []

        tables = ["PershareIndex", "Balance", "Income", "Capital"]
        raw: dict[str, Any] = {}

        try:
            data = await loop.run_in_executor(
                None,
                lambda: xt.get_financial_data(
                    [symbol], tables, start_time="20200101"
                ),
            )
            if symbol in data:
                for tbl in tables:
                    if tbl in data[symbol]:
                        raw[tbl] = data[symbol][tbl]
        except Exception:
            pass

        if not raw:
            return results

        date_set: set = set()
        for table_data in raw.values():
            if hasattr(table_data, "columns") and "m_timetag" in getattr(table_data, "columns", []):
                for v in table_data["m_timetag"].dropna().astype(str).tolist():
                    date_set.add(v)
            elif isinstance(table_data, dict) and "m_timetag" in table_data:
                for v in table_data.get("m_timetag", []):
                    date_set.add(str(v))

        if not date_set:
            return results

        sorted_dates = sorted(date_set, reverse=True)[:report_count]

        for rd_str in sorted_dates:
            try:
                rd = date(int(rd_str[:4]), int(rd_str[4:6]), int(rd_str[6:8]))
            except (ValueError, IndexError):
                continue

            fq = FinancialQuarter(symbol=symbol, report_date=rd, report_type=self._infer_report_type(rd))

            for table_name, table_data in raw.items():
                if hasattr(table_data, "columns") and "m_timetag" in table_data.columns:
                    row_df = table_data[table_data["m_timetag"].astype(str) == rd_str]
                    if row_df.empty:
                        continue
                    row = row_df.iloc[0]
                else:
                    row = self._extract_row_legacy(table_data, int(rd_str))
                    if row is None:
                        continue

                if table_name == "PershareIndex":
                    fq.eps = self._safe_float(row.get("s_fa_eps_basic"))
                    fq.bvps = self._safe_float(row.get("s_fa_bps"))
                    fq.roe = self._safe_float(row.get("du_return_on_equity")) or self._safe_float(row.get("equity_roe"))
                elif table_name == "Balance":
                    fq.total_assets = self._safe_float(row.get("tot_assets"))
                    fq.total_liability = self._safe_float(row.get("tot_liab"))
                    fq.total_equity = self._safe_float(row.get("total_equity"))
                elif table_name == "Income":
                    fq.revenue = self._safe_float(row.get("revenue_inc")) or self._safe_float(row.get("revenue"))
                    fq.net_profit = self._safe_float(row.get("net_profit_incl_min_int_inc"))
                    fq.revenue_yoy = self._safe_float(row.get("inc_revenue_rate"))
                    fq.profit_yoy = self._safe_float(row.get("du_profit_rate"))
                    fq.gross_margin = self._safe_float(row.get("sales_gross_profit"))
                elif table_name == "Capital":
                    fq.total_shares = self._safe_float(row.get("total_capital"))
                    fq.float_shares = self._safe_float(row.get("circulating_capital"))
                    fq.a_float_shares = self._safe_float(row.get("circulating_a"))
                    fq.limit_sell_shares = self._safe_float(row.get("restrict_circulating_capital"))

            results.append(fq)

        return results

    @staticmethod
    def _parse_financial_dataframes(
        symbol: str,
        tables_raw: dict[str, Any],
        report_count: int = 8,
    ) -> list[FinancialQuarter]:
        """解析get_financial_data返回的DataFrame格式财务数据"""
        import pandas as pd

        results: list[FinancialQuarter] = []

        tables = ["PershareIndex", "Balance", "Income", "Capital"]
        parsed: dict[str, pd.DataFrame] = {}
        for tbl in tables:
            if tbl not in tables_raw:
                continue
            df = tables_raw[tbl]
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            if "m_timetag" in df.columns:
                df["m_timetag"] = df["m_timetag"].astype(str)
            parsed[tbl] = df

        if not parsed:
            return results

        date_set: set = set()
        for df in parsed.values():
            if "m_timetag" in df.columns:
                date_set.update(df["m_timetag"].dropna().astype(str).tolist())

        if not date_set:
            return results

        sorted_dates = sorted(date_set, reverse=True)[:report_count]

        for rd_str in sorted_dates:
            try:
                rd = date(int(rd_str[:4]), int(rd_str[4:6]), int(rd_str[6:8]))
            except (ValueError, IndexError):
                continue

            fq = FinancialQuarter(symbol=symbol, report_date=rd, report_type=QMTGateway._infer_report_type(rd))

            for table_name, df in parsed.items():
                if "m_timetag" not in df.columns:
                    continue
                row_df = df[df["m_timetag"] == rd_str]
                if row_df.empty:
                    continue
                row = row_df.iloc[0]

                if table_name == "PershareIndex":
                    fq.eps = QMTGateway._safe_float(row.get("s_fa_eps_basic"))
                    fq.bvps = QMTGateway._safe_float(row.get("s_fa_bps"))
                    fq.roe = QMTGateway._safe_float(row.get("du_return_on_equity")) or QMTGateway._safe_float(row.get("equity_roe"))
                elif table_name == "Balance":
                    fq.total_assets = QMTGateway._safe_float(row.get("tot_assets"))
                    fq.total_liability = QMTGateway._safe_float(row.get("tot_liab"))
                    fq.total_equity = QMTGateway._safe_float(row.get("total_equity"))
                elif table_name == "Income":
                    fq.revenue = QMTGateway._safe_float(row.get("revenue_inc")) or QMTGateway._safe_float(row.get("revenue"))
                    fq.net_profit = QMTGateway._safe_float(row.get("net_profit_incl_min_int_inc"))
                    fq.revenue_yoy = QMTGateway._safe_float(row.get("inc_revenue_rate"))
                    fq.profit_yoy = QMTGateway._safe_float(row.get("du_profit_rate"))
                    fq.gross_margin = QMTGateway._safe_float(row.get("sales_gross_profit"))
                elif table_name == "Capital":
                    fq.total_shares = QMTGateway._safe_float(row.get("total_capital"))
                    fq.float_shares = QMTGateway._safe_float(row.get("circulating_capital"))
                    fq.a_float_shares = QMTGateway._safe_float(row.get("circulating_a"))
                    fq.limit_sell_shares = QMTGateway._safe_float(row.get("restrict_circulating_capital"))

            results.append(fq)

        return results

    @staticmethod
    def _extract_row_legacy(table_data: dict, target_date: int) -> dict[str, Any] | None:
        """旧版xtquant返回的嵌套dict格式解析"""
        if "field" not in table_data or "date" not in table_data or "value" not in table_data:
            return None
        fields = table_data["field"]
        dates = table_data["date"]
        values = table_data["value"]

        if not dates or not fields:
            return None

        row_idx = None
        for i, d in enumerate(dates):
            if d == target_date:
                row_idx = i
                break
        if row_idx is None:
            return None

        row: dict[str, Any] = {}
        for j, fname in enumerate(fields):
            if row_idx < len(values) and j < len(values[row_idx]):
                row[fname] = values[row_idx][j]
        return row

    @staticmethod
    def _safe_float(val: Any) -> float | None:
        if val is None:
            return None
        try:
            v = float(val)
            return v if v != 0 or val == 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _infer_report_type(rd: date) -> str:
        m = rd.month
        if m == 3:
            return "Q1"
        elif m == 6:
            return "H1"
        elif m == 9:
            return "Q3"
        elif m == 12:
            return "Annual"
        return "Unknown"

    async def get_financial_quarters(
        self, symbol: str, report_count: int = 8
    ) -> list[FinancialQuarter]:
        """获取多季度财务数据"""
        return await self._fetch_financial_data(symbol, report_count)

    async def get_stock_batch_info(self, symbols: list[str]) -> list[StockInfo]:
        """批量获取股票信息"""
        results = []
        for symbol in symbols:
            info = await self.get_stock_full_info(symbol)
            if info:
                results.append(info)
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

        loop = asyncio.get_running_loop()

        # 先下载历史数据（miniQMT需要）
        try:
            await loop.run_in_executor(
                None,
                lambda: xt.download_history_data(symbol, period="1d", start_time=start_str, end_time=end_str),
            )
        except Exception:
            pass

        # 获取数据
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
                    if isinstance(idx, str):
                        trade_date = datetime.strptime(idx, "%Y%m%d").date()
                    elif hasattr(idx, "date"):
                        trade_date = idx.date()
                    else:
                        trade_date = idx

                    open_price = float(row.get("open", 0))
                    close_price = float(row.get("close", 0))

                    kline = KlineData(
                        symbol=symbol,
                        datetime=trade_date,
                        open=open_price,
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=close_price,
                        volume=int(row.get("volume", 0)),
                        amount=float(row.get("amount", 0)),
                        turnover=float(row.get("turnover", 0)) if row.get("turnover") else None,
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

        loop = asyncio.get_running_loop()

        # 先下载历史数据
        try:
            await loop.run_in_executor(
                None,
                lambda: xt.download_history_data(symbol, period="1m", start_time=start_str, end_time=end_str),
            )
        except Exception:
            pass

        # 获取数据
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
                    # 索引可能是时间戳(毫秒)或字符串格式 20250423093000
                    if isinstance(idx, str):
                        # 格式: YYYYMMDDHHMMSS
                        idx = idx.strip()
                        if len(idx) >= 12:
                            trade_datetime = datetime.strptime(idx[:14], "%Y%m%d%H%M%S")
                        else:
                            continue
                    elif isinstance(idx, (int, float)):
                        # 毫秒时间戳
                        trade_datetime = datetime.fromtimestamp(idx / 1000)
                    elif hasattr(idx, "to_pydatetime"):
                        trade_datetime = idx.to_pydatetime()
                    elif hasattr(idx, "date"):
                        trade_datetime = idx
                    else:
                        trade_datetime = idx

                    kline = KlineData(
                        symbol=symbol,
                        datetime=trade_datetime,
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

    async def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        """获取实时行情"""
        xt = self._get_xt()
        loop = asyncio.get_running_loop()

        try:
            ticks = await loop.run_in_executor(
                None, lambda: xt.get_full_tick(symbols)
            )

            # 获取股本数据用于计算市值
            details = {}
            for symbol in symbols:
                try:
                    detail = await loop.run_in_executor(
                        None, lambda s: xt.get_instrument_detail(s), symbol
                    )
                    if detail:
                        details[symbol] = detail
                except Exception:
                    pass

            results = []
            for symbol, tick in ticks.items():
                if tick:
                    # 计算市值
                    total_value = None
                    float_value = None
                    detail = details.get(symbol, {})
                    if detail:
                        total_vol = detail.get("TotalVolume", 0) or 0
                        float_vol = detail.get("FloatVolume", 0) or 0
                        price = tick.get("lastPrice") or tick.get("open") or detail.get("PreClose")
                        if total_vol and price:
                            total_value = total_vol * price / 10000  # 万元
                        if float_vol and price:
                            float_value = float_vol * price / 10000  # 万元

                    results.append({
                        "symbol": symbol,
                        "price": tick.get("lastPrice"),
                        "open": tick.get("open"),
                        "high": tick.get("high"),
                        "low": tick.get("low"),
                        "volume": tick.get("volume"),
                        "amount": tick.get("amount"),
                        "bid_price": tick.get("bidPrice", []),
                        "ask_price": tick.get("askPrice", []),
                        "bid_volume": tick.get("bidVol", []),
                        "ask_volume": tick.get("askVol", []),
                        "total_value": total_value,
                        "float_value": float_value,
                    })
            return results
        except Exception:
            return []

    def _parse_date(self, date_str: str | None) -> date | None:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            return datetime.strptime(str(date_str), "%Y%m%d").date()
        except ValueError:
            return None

    def clean_local_cache(
        self,
        symbols: list[str] | None = None,
        data_type: str = "kline",
    ) -> dict[str, int]:
        """
        清理QMT本地缓存文件，数据已写入ClickHouse后调用。

        Args:
            symbols: 要清理的股票列表，None则清理全部
            data_type: 数据类型 - "kline"(行情), "financial"(财务), "all"(全部)

        Returns:
            {"deleted": 删除文件数, "freed_mb": 释放MB}
        """
        import glob as glob_mod

        data_dir = self._get_data_dir()
        if not data_dir or not os.path.isdir(data_dir):
            return {"deleted": 0, "freed_mb": 0}

        deleted = 0
        freed_bytes = 0

        type_dirs: list[str] = []
        if data_type == "kline" or data_type == "all":
            for exchange in ("SH", "SZ"):
                for period in ("60", "86400"):
                    type_dirs.append(os.path.join(data_dir, exchange, period))
            type_dirs.append(os.path.join(data_dir, "DividData"))
            type_dirs.append(os.path.join(data_dir, "Weight"))

        if data_type == "financial" or data_type == "all":
            for table in ("Balance", "Income", "CashFlow", "Capital",
                          "Top10FlowHolder", "Top10Holder", "HolderNum",
                          "PershareIndex"):
                for exchange in ("SH", "SZ"):
                    p = os.path.join(data_dir, exchange, table)
                    if os.path.isdir(p):
                        type_dirs.append(p)

        for dir_path in type_dirs:
            if not os.path.isdir(dir_path):
                continue

            if symbols:
                for sym in symbols:
                    code = sym.split(".")[0]
                    for ext in (".DAT", ".dat", ".bin", ".csv"):
                        fp = os.path.join(dir_path, code + ext)
                        if os.path.isfile(fp):
                            freed_bytes += os.path.getsize(fp)
                            os.remove(fp)
                            deleted += 1
            else:
                for fn in os.listdir(dir_path):
                    fp = os.path.join(dir_path, fn)
                    if os.path.isfile(fp):
                        freed_bytes += os.path.getsize(fp)
                        os.remove(fp)
                        deleted += 1

        return {"deleted": deleted, "freed_mb": round(freed_bytes / 1024 / 1024, 1)}

    def _get_data_dir(self) -> str | None:
        xt = self._get_xt()
        try:
            return xt.get_data_dir()
        except Exception:
            return None


# 全局单例
qmt_gateway = QMTGateway()
