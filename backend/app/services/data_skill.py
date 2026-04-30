# backend/app/services/data_skill.py
"""数据技能模块 — 为策略模块提供统一数据访问接口

设计原则:
1. 策略只需调用 DataSkill 的方法，无需关心数据来自 QMT/SQLite/ClickHouse
2. 优先从本地数据库读取（已同步的数据），无数据时才实时请求 QMT
3. 所有方法均为 async，QMT 调用自动包装 run_in_executor
4. 返回值使用纯 dataclass 或 dict，不暴露 ORM 模型或 QMT 内部结构
5. 货币单位统一为万元，股数单位统一为万股
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.clickhouse import get_ch_client
from app.db.models import Stock
from app.db.models.financial import FinancialData
from app.engines.qmt_gateway import qmt_gateway


# ═══════════════════════════════════════════════════════════════
# 数据类 — 统一返回格式
# ═══════════════════════════════════════════════════════════════

@dataclass
class StockSnapshot:
    """股票截面快照 — 合并了基础信息 + 最新财务 + 实时市值"""
    symbol: str
    name: str
    exchange: str | None = None
    industry: str | None = None
    sector: str | None = None
    list_date: date | None = None
    is_st: int = 0
    is_suspend: int = 0

    total_shares: float | None = None
    float_shares: float | None = None
    a_float_shares: float | None = None

    total_mv: float | None = None
    circ_mv: float | None = None

    eps: float | None = None
    bvps: float | None = None
    roe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None

    revenue: float | None = None
    net_profit: float | None = None

    total_assets: float | None = None
    total_liability: float | None = None
    total_equity: float | None = None

    report_date: date | None = None
    report_type: str | None = None


@dataclass
class KlineBar:
    """单根K线"""
    symbol: str
    datetime: date | datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    turnover: float | None = None
    change_pct: float | None = None


@dataclass
class FinancialReport:
    """单季度财务报告"""
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
    pe_ttm: float | None = None
    pb: float | None = None


@dataclass
class ScreenResult:
    """选股筛选结果"""
    stocks: list[StockSnapshot]
    total: int


@dataclass
class IndustryInfo:
    """行业统计"""
    name: str
    stock_count: int
    avg_mv: float | None = None
    avg_pe: float | None = None
    avg_roe: float | None = None


# ═══════════════════════════════════════════════════════════════
# DataSkill — 统一数据接口
# ═══════════════════════════════════════════════════════════════

class DataSkill:
    """数据技能 — 策略模块的数据访问层

    提供:
    - 股票截面快照 (基础信息 + 最新财务 + 市值)
    - K线时序数据 (日K / 分钟K)
    - 多季度财务报告
    - 行情快照 (实时)
    - 选股筛选 (条件过滤)
    - 行业统计
    - 指标查询 (ClickHouse)
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    # ─── 股票信息 ─────────────────────────────────────────

    async def get_stock(self, symbol: str) -> StockSnapshot | None:
        """获取单只股票快照（本地数据库优先，无数据时从 QMT 获取）

        Args:
            symbol: 股票代码，如 '600051.SH'

        Returns:
            StockSnapshot 或 None
        """
        db_stock = await self._get_db_stock(symbol)
        if db_stock and db_stock.total_mv:
            return self._stock_to_snapshot(db_stock)

        snapshot = self._stock_to_snapshot(db_stock) if db_stock else None
        if snapshot:
            return snapshot

        return await self._get_stock_from_qmt(symbol)

    async def get_stocks(self, symbols: list[str]) -> dict[str, StockSnapshot]:
        """批量获取股票快照

        Returns:
            {symbol: StockSnapshot} 字典，找不到的不会包含
        """
        result: dict[str, StockSnapshot] = {}

        if not symbols:
            return result

        stmt = select(Stock).where(Stock.symbol.in_(symbols))
        rows = (await self.session.execute(stmt)).scalars().all()
        db_map = {s.symbol: s for s in rows}

        missing = [s for s in symbols if s not in db_map]
        for symbol, stock in db_map.items():
            result[symbol] = self._stock_to_snapshot(stock)

        if missing:
            try:
                qmt_stocks = await qmt_gateway.get_stock_batch_info(missing)
                for qs in qmt_stocks:
                    snapshot = self._qmt_stock_info_to_snapshot(qs)
                    result[qs.symbol] = snapshot
            except Exception:
                pass

        return result

    async def screen_stocks(
        self,
        industry: str | None = None,
        exchange: str | None = None,
        is_st: int | None = None,
        min_mv: float | None = None,
        max_mv: float | None = None,
        min_pe: float | None = None,
        max_pe: float | None = None,
        min_roe: float | None = None,
        limit: int = 500,
    ) -> ScreenResult:
        """条件选股筛选

        Args:
            industry: 行业筛选
            exchange: 交易所筛选 (SH/SZ)
            is_st: ST状态 (0=正常, 1=ST, 2=*ST)
            min_mv/max_mv: 总市值范围（万元）
            min_pe/max_pe: PE_TTM范围
            min_roe: 最低ROE(%)
            min_revenue_yoy: 最低营收同比增长率(%)
            limit: 最大返回数量
        """
        stmt = select(Stock)

        if industry:
            stmt = stmt.where(Stock.industry == industry)
        if exchange:
            stmt = stmt.where(Stock.exchange == exchange)
        if is_st is not None:
            stmt = stmt.where(Stock.is_st == is_st)
        if min_mv is not None:
            stmt = stmt.where(Stock.total_mv >= min_mv)
        if max_mv is not None:
            stmt = stmt.where(Stock.total_mv <= max_mv)
        if min_pe is not None:
            stmt = stmt.where(Stock.pe_ttm >= min_pe)
        if max_pe is not None:
            stmt = stmt.where((Stock.pe_ttm <= max_pe) | (Stock.pe_ttm.is_(None)))
        if min_roe is not None:
            stmt = stmt.where(Stock.roe >= min_roe)

        count_stmt = select(func.count()).select_from(stmt.subquery())
        total = (await self.session.execute(count_stmt)).scalar() or 0

        stmt = stmt.order_by(Stock.total_mv.desc()).limit(limit)
        rows = (await self.session.execute(stmt)).scalars().all()

        return ScreenResult(
            stocks=[self._stock_to_snapshot(s) for s in rows],
            total=total,
        )

    # ─── K线数据 ──────────────────────────────────────────

    async def get_kline_daily(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[KlineBar]:
        """获取日K线数据（ClickHouse 优先，无数据时从 QMT 请求）

        Args:
            symbol: 股票代码
            start_date: 起始日期
            end_date: 结束日期
            limit: 最大条数
        """
        bars = self._query_ch_klines("klines_daily", symbol, start_date, end_date, limit)
        if bars:
            return bars

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=limit)

        raw = await qmt_gateway.get_kline_daily(symbol, start_date, end_date)
        return [self._qmt_kline_to_bar(k) for k in raw]

    async def get_kline_minute(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[KlineBar]:
        """获取分钟K线数据（同上，ClickHouse 优先）"""
        bars = self._query_ch_klines_minute(symbol, start_date, end_date, limit)
        if bars:
            return bars

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date

        raw = await qmt_gateway.get_kline_minute(symbol, start_date, end_date)
        return [self._qmt_kline_to_bar(k) for k in raw]

    # ─── 财务数据 ─────────────────────────────────────────

    async def get_financial(
        self,
        symbol: str,
        report_count: int = 8,
    ) -> list[FinancialReport]:
        """获取多季度财务数据（SQLite 优先，无数据时从 QMT 请求）

        Args:
            symbol: 股票代码
            report_count: 获取最近 N 个季度
        """
        stmt = (
            select(FinancialData)
            .where(FinancialData.symbol == symbol)
            .order_by(FinancialData.report_date.desc())
            .limit(report_count)
        )
        rows = (await self.session.execute(stmt)).scalars().all()

        if rows:
            return [self._financial_model_to_report(r) for r in rows]

        quarters = await qmt_gateway._fetch_financial_data(symbol, report_count)
        return [self._financial_quarter_to_report(q) for q in quarters]

    async def get_financial_batch(
        self,
        symbols: list[str],
        report_count: int = 1,
    ) -> dict[str, list[FinancialReport]]:
        """批量获取多只股票的财务数据

        Returns:
            {symbol: [FinancialReport, ...]}
        """
        result: dict[str, list[FinancialReport]] = {}
        if not symbols:
            return result

        stmt = (
            select(FinancialData)
            .where(FinancialData.symbol.in_(symbols))
            .order_by(FinancialData.symbol, FinancialData.report_date.desc())
        )
        rows = (await self.session.execute(stmt)).scalars().all()

        found_symbols: set[str] = set()
        current_symbol: str | None = None
        count_per_symbol: int = 0

        for row in rows:
            if row.symbol != current_symbol:
                current_symbol = row.symbol
                count_per_symbol = 0
            count_per_symbol += 1
            if count_per_symbol <= report_count:
                result.setdefault(row.symbol, []).append(self._financial_model_to_report(row))
                found_symbols.add(row.symbol)

        missing = [s for s in symbols if s not in found_symbols]
        for symbol in missing:
            try:
                quarters = await qmt_gateway._fetch_financial_data(symbol, report_count)
                if quarters:
                    result[symbol] = [self._financial_quarter_to_report(q) for q in quarters]
            except Exception:
                pass

        return result

    # ─── 实时行情 ──────────────────────────────────────────

    async def get_realtime_quote(self, symbol: str) -> dict[str, Any] | None:
        """获取单只股票实时行情"""
        quotes = await qmt_gateway.get_realtime_quotes([symbol])
        return quotes[0] if quotes else None

    async def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        """批量获取实时行情"""
        return await qmt_gateway.get_realtime_quotes(symbols)

    # ─── 行业统计 ──────────────────────────────────────────

    async def get_industries(self) -> list[IndustryInfo]:
        """获取所有行业及其股票数量"""
        stmt = (
            select(Stock.industry, func.count(Stock.symbol))
            .where(Stock.industry.isnot(None))
            .group_by(Stock.industry)
            .order_by(func.count(Stock.symbol).desc())
        )
        rows = (await self.session.execute(stmt)).all()
        return [IndustryInfo(name=r[0], stock_count=r[1]) for r in rows if r[0]]

    # ─── 指标查询 ──────────────────────────────────────────

    def get_indicator(
        self,
        symbol: str,
        indicator_name: str,
        trade_date: date | None = None,
    ) -> float | None:
        """从 ClickHouse 查询单只股票的指标值

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            trade_date: 交易日期，None 则取最新
        """
        ch = get_ch_client()
        if trade_date:
            rows = ch.execute(
                "SELECT value FROM stock_indicators "
                "WHERE symbol = %(sym)s AND indicator_name = %(name)s "
                "AND trade_date = %(dt)s ORDER BY updated_at DESC LIMIT 1",
                {"sym": symbol, "name": indicator_name, "dt": trade_date},
            )
        else:
            rows = ch.execute(
                "SELECT value FROM stock_indicators "
                "WHERE symbol = %(sym)s AND indicator_name = %(name)s "
                "ORDER BY trade_date DESC LIMIT 1",
                {"sym": symbol, "name": indicator_name},
            )
        if rows and rows[0][0] is not None:
            return float(rows[0][0])
        return None

    def get_indicators_batch(
        self,
        symbols: list[str],
        trade_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """批量查询截面指标

        Returns:
            [{symbol, indicator_name, value, trade_date}, ...]
        """
        if not symbols:
            return []
        ch = get_ch_client()
        if trade_date:
            rows = ch.execute(
                "SELECT symbol, indicator_name, value, trade_date "
                "FROM stock_indicators "
                "WHERE symbol IN %(syms)s AND trade_date = %(dt)s "
                "ORDER BY symbol, indicator_name",
                {"syms": symbols, "dt": trade_date},
            )
        else:
            rows = ch.execute(
                "SELECT symbol, indicator_name, value, trade_date "
                "FROM stock_indicators "
                "WHERE symbol IN %(syms)s "
                "ORDER BY symbol, indicator_name, trade_date DESC",
                {"syms": symbols},
            )
        return [
            {"symbol": r[0], "indicator_name": r[1], "value": float(r[2]) if r[2] is not None else None, "trade_date": r[3]}
            for r in rows
        ]

    # ─── 股票列表 ──────────────────────────────────────────

    async def get_all_symbols(self) -> list[str]:
        """获取所有股票代码列表"""
        stmt = select(Stock.symbol).order_by(Stock.symbol)
        rows = (await self.session.execute(stmt)).scalars().all()
        if rows:
            return list(rows)

        qmt_stocks = await qmt_gateway.get_stock_list()
        return [s.symbol for s in qmt_stocks]

    async def get_symbols_by_industry(self, industry: str) -> list[str]:
        """获取指定行业的所有股票代码"""
        stmt = select(Stock.symbol).where(Stock.industry == industry)
        rows = (await self.session.execute(stmt)).scalars().all()
        return list(rows)

    # ─── 辅助方法 ──────────────────────────────────────────

    async def _get_db_stock(self, symbol: str) -> Stock | None:
        stmt = select(Stock).where(Stock.symbol == symbol)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def _get_stock_from_qmt(self, symbol: str) -> StockSnapshot | None:
        try:
            info = await qmt_gateway.get_stock_full_info(symbol)
            if info:
                return self._qmt_stock_info_to_snapshot(info)
        except Exception:
            pass
        return None

    @staticmethod
    def _stock_to_snapshot(s: Stock) -> StockSnapshot:
        return StockSnapshot(
            symbol=s.symbol,
            name=s.name or "",
            exchange=s.exchange,
            industry=s.industry,
            sector=s.sector,
            list_date=s.list_date,
            is_st=s.is_st or 0,
            is_suspend=s.is_suspend or 0,
            total_shares=s.total_shares,
            float_shares=s.float_shares,
            a_float_shares=s.a_float_shares,
            total_mv=s.total_mv,
            circ_mv=s.circ_mv,
            eps=s.eps,
            bvps=s.bvps,
            roe=s.roe,
            pe_ttm=s.pe_ttm,
            pb=s.pb,
            revenue=s.revenue,
net_profit=s.net_profit,
        )

    @staticmethod
    def _qmt_stock_info_to_snapshot(info) -> StockSnapshot:
        return StockSnapshot(
            symbol=info.symbol,
            name=info.name or "",
            exchange=info.exchange,
            industry=info.industry,
            sector=info.sector,
            list_date=info.list_date,
            is_st=info.is_st or 0,
            is_suspend=info.is_suspend or 0,
            total_shares=info.total_shares,
            float_shares=info.float_shares,
            a_float_shares=info.a_float_shares,
            total_mv=info.total_mv,
            circ_mv=info.circ_mv,
            eps=info.eps,
            bvps=info.bvps,
            roe=info.roe,
            pe_ttm=info.pe_ttm,
            pb=info.pb,
            revenue=info.revenue,
            net_profit=info.net_profit,
            revenue_yoy=info.revenue_yoy,
            profit_yoy=info.profit_yoy,
            gross_margin=info.gross_margin,
            total_assets=info.total_assets,
            total_liability=info.total_liability,
            total_equity=info.total_equity,
        )

    @staticmethod
    def _qmt_kline_to_bar(k) -> KlineBar:
        dt = k.datetime
        if isinstance(dt, str):
            dt = datetime.strptime(dt, "%Y%m%d").date() if len(dt) == 8 else datetime.strptime(dt, "%Y%m%d%H%M%S")
        elif hasattr(dt, "date") and not isinstance(dt, date):
            dt = dt.date() if hasattr(dt, "date") else dt
        return KlineBar(
            symbol=k.symbol,
            datetime=dt,
            open=float(k.open),
            high=float(k.high),
            low=float(k.low),
            close=float(k.close),
            volume=k.volume,
            amount=float(k.amount),
            turnover=float(k.turnover) if k.turnover else None,
            change_pct=float(k.change_pct) if k.change_pct else None,
        )

    @staticmethod
    def _financial_model_to_report(f: FinancialData) -> FinancialReport:
        return FinancialReport(
            symbol=f.symbol,
            report_date=f.report_date,
            report_type=f.report_type,
            eps=f.eps,
            bvps=f.bvps,
            roe=f.roe,
            revenue=f.revenue,
            net_profit=f.net_profit,
            revenue_yoy=f.revenue_yoy,
            profit_yoy=f.profit_yoy,
            gross_margin=f.gross_margin,
            total_assets=f.total_assets,
            total_liability=f.total_liability,
            total_equity=f.total_equity,
            total_shares=f.total_shares,
            float_shares=f.float_shares,
            pe_ttm=f.pe_ttm,
            pb=f.pb,
        )

    @staticmethod
    def _financial_quarter_to_report(q) -> FinancialReport:
        return FinancialReport(
            symbol=q.symbol,
            report_date=q.report_date,
            report_type=q.report_type,
            eps=q.eps,
            bvps=q.bvps,
            roe=q.roe,
            revenue=q.revenue,
            net_profit=q.net_profit,
            revenue_yoy=q.revenue_yoy,
            profit_yoy=q.profit_yoy,
            gross_margin=q.gross_margin,
            total_assets=q.total_assets,
            total_liability=q.total_liability,
            total_equity=q.total_equity,
            total_shares=q.total_shares,
            float_shares=q.float_shares,
            pe_ttm=q.pe_ttm,
            pb=q.pb,
        )

    @staticmethod
    def _query_ch_klines(
        table: str,
        symbol: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
    ) -> list[KlineBar]:
        ch = get_ch_client()
        conditions = [f"symbol = %(sym)s"]
        params: dict[str, Any] = {"sym": symbol}
        if start_date:
            conditions.append("trade_date >= %(sd)s")
            params["sd"] = start_date
        if end_date:
            conditions.append("trade_date <= %(ed)s")
            params["ed"] = end_date
        where = " AND ".join(conditions)
        sql = f"SELECT symbol, trade_date, open, high, low, close, volume, amount FROM {table} WHERE {where} ORDER BY trade_date DESC LIMIT {limit}"
        try:
            rows = ch.execute(sql, params)
            return [
                KlineBar(
                    symbol=r[0], datetime=r[1],
                    open=float(r[2]), high=float(r[3]),
                    low=float(r[4]), close=float(r[5]),
                    volume=int(r[6]), amount=float(r[7]),
                )
                for r in rows
            ]
        except Exception:
            return []

    @staticmethod
    def _query_ch_klines_minute(
        symbol: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
    ) -> list[KlineBar]:
        ch = get_ch_client()
        conditions = [f"symbol = %(sym)s"]
        params: dict[str, Any] = {"sym": symbol}
        if start_date:
            conditions.append("toDate(datetime) >= %(sd)s")
            params["sd"] = start_date
        if end_date:
            conditions.append("toDate(datetime) <= %(ed)s")
            params["ed"] = end_date
        where = " AND ".join(conditions)
        sql = f"SELECT symbol, datetime, open, high, low, close, volume, amount FROM klines_minute WHERE {where} ORDER BY datetime DESC LIMIT {limit}"
        try:
            rows = ch.execute(sql, params)
            return [
                KlineBar(
                    symbol=r[0], datetime=r[1],
                    open=float(r[2]), high=float(r[3]),
                    low=float(r[4]), close=float(r[5]),
                    volume=int(r[6]), amount=float(r[7]),
                )
                for r in rows
            ]
        except Exception:
            return []