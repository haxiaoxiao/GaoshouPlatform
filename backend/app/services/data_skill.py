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

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.clickhouse import get_ch_client
from app.data_stores import get_market_data_store
from app.db.models import Stock
from app.db.models.financial import FinancialData
from app.engines.qmt_gateway import qmt_gateway
from app.services.security_symbols import normalize_security_symbol

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
    industry2: str | None = None
    industry3: str | None = None
    sector: str | None = None
    concept: str | None = None
    list_date: date | None = None
    delist_date: date | None = None
    is_st: int = 0
    is_delist: int = 0
    is_suspend: int = 0

    total_shares: float | None = None
    float_shares: float | None = None
    a_float_shares: float | None = None
    limit_sell_shares: float | None = None

    total_mv: float | None = None
    circ_mv: float | None = None

    eps: float | None = None
    bvps: float | None = None
    roe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None

    revenue: float | None = None
    net_profit: float | None = None
    revenue_yoy: float | None = None
    profit_yoy: float | None = None
    gross_margin: float | None = None

    total_assets: float | None = None
    total_liability: float | None = None
    total_equity: float | None = None

    report_date: date | None = None
    report_type: str | None = None

    company_name: str | None = None
    province: str | None = None
    city: str | None = None
    business_scope: str | None = None
    main_business: str | None = None
    website: str | None = None
    employees: int | None = None
    security_type: str | None = None
    product_class: str | None = None


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
        symbol = normalize_security_symbol(symbol) or symbol
        db_stock = await self._get_db_stock(symbol)
        if db_stock is not None:
            latest_financial = await self._get_latest_financial(symbol)
            return self._stock_to_snapshot(db_stock, latest_financial)

        return await self._get_stock_from_qmt(symbol)

    async def get_stocks(self, symbols: list[str]) -> dict[str, StockSnapshot]:
        """批量获取股票快照

        Returns:
            {symbol: StockSnapshot} 字典，找不到的不会包含
        """
        result: dict[str, StockSnapshot] = {}
        symbols = [normalize_security_symbol(symbol) or str(symbol).strip().upper() for symbol in symbols]

        if not symbols:
            return result

        stmt = select(Stock).where(Stock.symbol.in_(symbols))
        rows = (await self.session.execute(stmt)).scalars().all()
        db_map = {s.symbol: s for s in rows}
        latest_financial_map = await self._get_latest_financial_map(list(db_map.keys()))

        missing = [s for s in symbols if s not in db_map]
        for symbol, stock in db_map.items():
            result[symbol] = self._stock_to_snapshot(stock, latest_financial_map.get(symbol))

        if missing:
            try:
                qmt_stocks = await qmt_gateway.get_stock_batch_info(missing)
                for qs in qmt_stocks:
                    snapshot = self._qmt_stock_info_to_snapshot(qs)
                    result[qs.symbol] = snapshot
            except Exception as exc:
                logger.warning(
                    "DataSkill.get_stocks: QMT batch fetch failed ({} symbols): {}",
                    len(missing),
                    exc,
                )

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
        symbol = normalize_security_symbol(symbol) or symbol
        if limit <= 0:
            return []
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            # Why: 避免 start_date=None 时从 2000 年全量扫描；用 limit 估算一个合理的回看窗口。
            start_date = end_date - timedelta(days=max(limit * 2, 30))
        if start_date > end_date:
            return []
        # MarketDataStore queries are synchronous (DuckDB/ClickHouse). Run in a worker thread to avoid
        # blocking the FastAPI event loop under load.
        bars = await asyncio.to_thread(self._query_market_klines, symbol, start_date, end_date, limit)
        if bars:
            return bars

        raw = await qmt_gateway.get_kline_daily(symbol, start_date, end_date)
        bars_qmt = [self._qmt_kline_to_bar(k) for k in raw]
        bars_qmt.sort(key=lambda b: b.datetime, reverse=True)
        return bars_qmt[:limit]

    async def get_kline_minute(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
        timer_times: list[str] | None = None,
    ) -> list[KlineBar]:
        """获取分钟K线数据（Parquet/ClickHouse 优先，无数据时从 QMT 请求）

        Args:
            timer_times: 可选的分钟时间点过滤（如 ["10:00", "10:30", "14:50"]）。
                传入时会优先尝试读取稀疏分钟线（minute_timer）数据集；若稀疏数据缺失再回退到完整分钟线。
        """
        symbol = normalize_security_symbol(symbol) or symbol
        if limit <= 0:
            return []
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            # Why: minute 默认只取最近若干个 bar；按每交易日约 240 分钟估算需要回看多少天。
            minutes_per_day = 240
            days_needed = max(1, (limit + minutes_per_day - 1) // minutes_per_day)
            # Buffer 一天，避免因停牌/缺数导致返回不足 limit。
            start_date = end_date - timedelta(days=days_needed)
        if start_date > end_date:
            return []

        # Why: FastAPI list query params通常用重复参数传递，但前端/脚本也常用逗号拼接。
        # 为避免“10:00,10:30”被当作一个无效时间点，这里做一次扁平化拆分。
        normalized_times: list[str] = []
        for item in timer_times or []:
            for part in str(item).split(","):
                part = part.strip()
                if part:
                    normalized_times.append(part)
        timer_times = normalized_times or None
        bars = await asyncio.to_thread(
            self._query_market_klines_minute,
            symbol,
            start_date,
            end_date,
            limit,
            timer_times,
        )
        if bars:
            return bars

        raw = await qmt_gateway.get_kline_minute(symbol, start_date, end_date)
        bars_qmt = [self._qmt_kline_to_bar(k) for k in raw]
        if timer_times:
            wanted: set[int] = set()
            for text in timer_times:
                try:
                    h, m, *_ = str(text).split(":")
                    wanted.add(int(h) * 60 + int(m))
                except Exception:
                    continue
            if wanted:
                filtered: list[KlineBar] = []
                for bar in bars_qmt:
                    if isinstance(bar.datetime, datetime):
                        minute = bar.datetime.hour * 60 + bar.datetime.minute
                        if minute in wanted:
                            filtered.append(bar)
                bars_qmt = filtered
        bars_qmt.sort(key=lambda b: b.datetime, reverse=True)
        return bars_qmt[:limit]

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
        symbol = normalize_security_symbol(symbol) or symbol
        stmt = (
            select(FinancialData)
            .where(FinancialData.symbol == symbol)
            .order_by(FinancialData.report_date.desc())
            .limit(report_count)
        )
        rows = (await self.session.execute(stmt)).scalars().all()

        if rows:
            return [self._financial_model_to_report(r) for r in rows]

        quarters = await qmt_gateway.get_financial_quarters(symbol, report_count)
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
        symbols = [normalize_security_symbol(symbol) or str(symbol).strip().upper() for symbol in symbols]
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
                quarters = await qmt_gateway.get_financial_quarters(symbol, report_count)
                if quarters:
                    result[symbol] = [self._financial_quarter_to_report(q) for q in quarters]
            except Exception as exc:
                logger.warning("DataSkill.get_financial_batch: QMT fetch failed for {}: {}", symbol, exc)

        return result

    # ─── 实时行情 ──────────────────────────────────────────

    async def get_realtime_quote(self, symbol: str) -> dict[str, Any] | None:
        """获取单只股票实时行情"""
        symbol = normalize_security_symbol(symbol) or symbol
        quotes = await qmt_gateway.get_realtime_quotes([symbol])
        return quotes[0] if quotes else None

    async def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        """批量获取实时行情"""
        symbols = [normalize_security_symbol(symbol) or str(symbol).strip().upper() for symbol in symbols]
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

    async def get_indicator(
        self,
        symbol: str,
        indicator_name: str,
        trade_date: date | None = None,
    ) -> float | None:
        """查询单只股票的指标值

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            trade_date: 交易日期，None 则取最新
        """
        symbol = normalize_security_symbol(symbol) or symbol

        from app.data_stores import get_indicator_store

        store = get_indicator_store()

        def _load_from_store() -> float | None:
            try:
                if trade_date:
                    df = store.load_cross_section([indicator_name], trade_date, [symbol])
                else:
                    latest_date = store.latest_trade_date([indicator_name], [symbol])
                    if latest_date is None:
                        return None
                    df = store.load_cross_section([indicator_name], latest_date, [symbol])
                if df.empty:
                    return None
                val = df["value"].iloc[0]
                return float(val) if val is not None else None
            except Exception as exc:
                logger.warning("DataSkill.get_indicator: store query failed: {}", exc)
                return None

        value = await asyncio.to_thread(_load_from_store)
        if value is not None:
            return value

        # Parquet-first mode should not require ClickHouse; only attempt CH when explicitly enabled.
        if not settings.clickhouse_enabled:
            return None

        def _load_from_clickhouse() -> float | None:
            ch = get_ch_client()
            try:
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
            except Exception:
                return None
            if rows and rows[0] and rows[0][0] is not None:
                return float(rows[0][0])
            return None

        return await asyncio.to_thread(_load_from_clickhouse)

    async def get_indicators_batch(
        self,
        symbols: list[str],
        trade_date: date | None = None,
        names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """批量查询截面指标

        Returns:
            [{symbol, indicator_name, value, trade_date}, ...]
        """
        symbols = [normalize_security_symbol(symbol) or str(symbol).strip().upper() for symbol in symbols]
        if not symbols:
            return []
        from datetime import date as dt_date

        from app.data_stores import get_indicator_store

        store = get_indicator_store()

        def _load_df_from_store():
            try:
                target = trade_date or store.latest_trade_date(names, symbols) or dt_date.today()
                df_local = store.load_cross_section(names or [], target, symbols)
                return target, df_local
            except Exception as exc:
                logger.warning("DataSkill.get_indicators_batch: store query failed: {}", exc)
                return trade_date or dt_date.today(), None

        target_date, df = await asyncio.to_thread(_load_df_from_store)
        if df is not None and not df.empty:
            return [
                {
                    "symbol": r["symbol"],
                    "indicator_name": r["indicator_name"],
                    "value": float(r["value"]) if r["value"] is not None else None,
                    "trade_date": r.get("trade_date", target_date),
                }
                for _, r in df.iterrows()
            ]

        if not settings.clickhouse_enabled:
            return []

        def _load_rows_from_clickhouse():
            ch = get_ch_client()
            try:
                if trade_date:
                    return ch.execute(
                        "SELECT symbol, indicator_name, value, trade_date "
                        "FROM stock_indicators WHERE symbol IN %(syms)s AND trade_date = %(dt)s "
                        "ORDER BY symbol, indicator_name",
                        {"syms": symbols, "dt": trade_date},
                    )
                return ch.execute(
                    "SELECT symbol, indicator_name, value, trade_date "
                    "FROM stock_indicators WHERE symbol IN %(syms)s "
                    "ORDER BY symbol, indicator_name, trade_date DESC",
                    {"syms": symbols},
                )
            except Exception:
                return []

        rows = await asyncio.to_thread(_load_rows_from_clickhouse)
        return [
            {
                "symbol": r[0],
                "indicator_name": r[1],
                "value": float(r[2]) if r[2] is not None else None,
                "trade_date": r[3],
            }
            for r in (rows or [])
        ]

    async def get_indicator_timeseries(
        self,
        symbol: str,
        names: list[str],
        start_date: date,
        end_date: date,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        """查询单只股票的指标时序数据（Parquet/ClickHouse 指标库优先）"""
        symbol = normalize_security_symbol(symbol) or symbol
        return await self.get_indicators_timeseries_batch(
            symbols=[symbol],
            names=names,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    async def get_indicators_timeseries_batch(
        self,
        symbols: list[str],
        names: list[str],
        start_date: date,
        end_date: date,
        limit: int = 200_000,
    ) -> list[dict[str, Any]]:
        """批量查询指标时序数据

        Returns:
            [{symbol, indicator_name, datetime, value, updated_at?}, ...]
        """
        symbols = [normalize_security_symbol(symbol) or str(symbol).strip().upper() for symbol in (symbols or [])]
        symbols = [s for s in symbols if s]
        if not symbols or not names or limit <= 0:
            return []
        if start_date > end_date:
            return []

        # Why: 同 get_kline_minute(timer_times)，兼容逗号拼接的 names 参数。
        normalized_names: list[str] = []
        for item in names:
            for part in str(item).split(","):
                part = part.strip()
                if part:
                    normalized_names.append(part)
        names = normalized_names
        if not names:
            return []

        from app.data_stores import get_indicator_store

        store = get_indicator_store()

        def _load_from_store():
            try:
                return store.load_timeseries(names, start_date, end_date, symbols)
            except Exception as exc:
                logger.warning("DataSkill.get_indicators_timeseries_batch: store query failed: {}", exc)
                return None

        df = await asyncio.to_thread(_load_from_store)
        if df is not None and not df.empty:
            df = df.sort_values(["symbol", "indicator_name", "datetime"])
            if len(df) > limit:
                df = df.tail(limit)
            result: list[dict[str, Any]] = []
            for _, r in df.iterrows():
                result.append(
                    {
                        "symbol": r.get("symbol"),
                        "indicator_name": r.get("indicator_name"),
                        "datetime": r.get("datetime"),
                        "value": float(r["value"]) if r.get("value") is not None else None,
                        "updated_at": r.get("updated_at"),
                    }
                )
            return result

        if not settings.clickhouse_enabled:
            return []

        def _load_rows_from_clickhouse():
            ch = get_ch_client()
            try:
                return ch.execute(
                    "SELECT symbol, indicator_name, datetime, value, updated_at "
                    "FROM indicator_timeseries "
                    "WHERE symbol IN %(syms)s "
                    "AND indicator_name IN %(names)s "
                    "AND datetime >= %(start)s AND datetime < %(end_plus)s "
                    "ORDER BY symbol, indicator_name, datetime",
                    {
                        "syms": tuple(symbols),
                        "names": tuple(names),
                        "start": datetime.combine(start_date, datetime.min.time()),
                        "end_plus": datetime.combine(end_date, datetime.min.time()) + timedelta(days=1),
                    },
                )
            except Exception as exc:
                logger.warning("DataSkill.get_indicators_timeseries_batch: ClickHouse query failed: {}", exc)
                return []

        rows = await asyncio.to_thread(_load_rows_from_clickhouse)
        if not rows:
            return []
        if len(rows) > limit:
            rows = rows[-limit:]
        return [
            {
                "symbol": r[0],
                "indicator_name": r[1],
                "datetime": r[2],
                "value": float(r[3]) if r[3] is not None else None,
                "updated_at": r[4] if len(r) > 4 else None,
            }
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

    async def _get_latest_financial(self, symbol: str) -> FinancialData | None:
        stmt = (
            select(FinancialData)
            .where(FinancialData.symbol == symbol)
            .order_by(FinancialData.report_date.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def _get_latest_financial_map(self, symbols: list[str]) -> dict[str, FinancialData]:
        if not symbols:
            return {}
        stmt = (
            select(FinancialData)
            .where(FinancialData.symbol.in_(symbols))
            .order_by(FinancialData.symbol, FinancialData.report_date.desc())
        )
        rows = (await self.session.execute(stmt)).scalars().all()
        latest: dict[str, FinancialData] = {}
        for row in rows:
            if row.symbol not in latest:
                latest[row.symbol] = row
        return latest

    async def _get_stock_from_qmt(self, symbol: str) -> StockSnapshot | None:
        try:
            info = await qmt_gateway.get_stock_full_info(symbol)
            if info:
                return self._qmt_stock_info_to_snapshot(info)
        except Exception as exc:
            logger.warning("DataSkill._get_stock_from_qmt failed for {}: {}", symbol, exc)
        return None

    @staticmethod
    def _stock_to_snapshot(s: Stock, financial: FinancialData | None = None) -> StockSnapshot:
        snapshot = StockSnapshot(
            symbol=s.symbol,
            name=s.name or "",
            exchange=s.exchange,
            industry=s.industry,
            industry2=s.industry2,
            industry3=s.industry3,
            sector=s.sector,
            concept=s.concept,
            list_date=s.list_date,
            delist_date=s.delist_date,
            is_st=s.is_st or 0,
            is_delist=s.is_delist or 0,
            is_suspend=s.is_suspend or 0,
            total_shares=s.total_shares,
            float_shares=s.float_shares,
            a_float_shares=s.a_float_shares,
            limit_sell_shares=s.limit_sell_shares,
            total_mv=s.total_mv,
            circ_mv=s.circ_mv,
            eps=s.eps,
            bvps=s.bvps,
            roe=s.roe,
            pe_ttm=s.pe_ttm,
            pb=s.pb,
            revenue=s.revenue,
            net_profit=s.net_profit,
            total_assets=s.total_assets,
            total_liability=s.total_liability,
            total_equity=s.total_equity,
            company_name=s.company_name,
            province=s.province,
            city=s.city,
            business_scope=s.business_scope,
            main_business=s.main_business,
            website=s.website,
            employees=s.employees,
            security_type=s.security_type,
            product_class=s.product_class,
        )
        # Why: stocks 表以“快照”为主，financial_data 以“季度”为主。策略侧通常需要“尽量新”的财务字段，
        # 但 stocks 未必包含同比/毛利率等列，因此在存在财务季度数据时，用其补齐/覆盖。
        if financial is not None:
            snapshot.report_date = financial.report_date
            snapshot.report_type = financial.report_type
            snapshot.eps = financial.eps if financial.eps is not None else snapshot.eps
            snapshot.bvps = financial.bvps if financial.bvps is not None else snapshot.bvps
            snapshot.roe = financial.roe if financial.roe is not None else snapshot.roe
            snapshot.pe_ttm = financial.pe_ttm if financial.pe_ttm is not None else snapshot.pe_ttm
            snapshot.pb = financial.pb if financial.pb is not None else snapshot.pb
            snapshot.revenue = financial.revenue if financial.revenue is not None else snapshot.revenue
            snapshot.net_profit = financial.net_profit if financial.net_profit is not None else snapshot.net_profit
            snapshot.revenue_yoy = financial.revenue_yoy
            snapshot.profit_yoy = financial.profit_yoy
            snapshot.gross_margin = financial.gross_margin
            snapshot.total_assets = financial.total_assets if financial.total_assets is not None else snapshot.total_assets
            snapshot.total_liability = (
                financial.total_liability if financial.total_liability is not None else snapshot.total_liability
            )
            snapshot.total_equity = financial.total_equity if financial.total_equity is not None else snapshot.total_equity
            snapshot.total_shares = financial.total_shares if financial.total_shares is not None else snapshot.total_shares
            snapshot.float_shares = financial.float_shares if financial.float_shares is not None else snapshot.float_shares
            snapshot.a_float_shares = (
                financial.a_float_shares if financial.a_float_shares is not None else snapshot.a_float_shares
            )
            snapshot.limit_sell_shares = (
                financial.limit_sell_shares if financial.limit_sell_shares is not None else snapshot.limit_sell_shares
            )
            snapshot.total_mv = financial.total_mv if financial.total_mv is not None else snapshot.total_mv
            snapshot.circ_mv = financial.circ_mv if financial.circ_mv is not None else snapshot.circ_mv
        return snapshot

    @staticmethod
    def _qmt_stock_info_to_snapshot(info) -> StockSnapshot:
        return StockSnapshot(
            symbol=info.symbol,
            name=info.name or "",
            exchange=info.exchange,
            industry=info.industry,
            sector=getattr(info, "sector", None),
            list_date=getattr(info, "list_date", None),
            is_st=getattr(info, "is_st", 0) or 0,
            is_suspend=getattr(info, "is_suspend", 0) or 0,
            total_shares=getattr(info, "total_shares", None),
            float_shares=getattr(info, "float_shares", None),
            a_float_shares=getattr(info, "a_float_shares", None),
            total_mv=getattr(info, "total_mv", None),
            circ_mv=getattr(info, "circ_mv", None),
            eps=getattr(info, "eps", None),
            bvps=getattr(info, "bvps", None),
            roe=getattr(info, "roe", None),
            pe_ttm=getattr(info, "pe_ttm", None),
            pb=getattr(info, "pb", None),
            revenue=getattr(info, "revenue", None),
            net_profit=getattr(info, "net_profit", None),
            revenue_yoy=getattr(info, "revenue_yoy", None),
            profit_yoy=getattr(info, "profit_yoy", None),
            gross_margin=getattr(info, "gross_margin", None),
            total_assets=getattr(info, "total_assets", None),
            total_liability=getattr(info, "total_liability", None),
            total_equity=getattr(info, "total_equity", None),
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
    def _query_market_klines(
        symbol: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
    ) -> list[KlineBar]:
        store = get_market_data_store()
        sd = start_date or date(2000, 1, 1)
        ed = end_date or date.today()
        df = store.load_daily([symbol], sd, ed)
        if df.empty:
            return []
        df = df.sort_index(ascending=False).head(limit)
        return [
            KlineBar(
                symbol=r["symbol"], datetime=r.name,
                open=float(r["open"]), high=float(r["high"]),
                low=float(r["low"]), close=float(r["close"]),
                volume=int(r["volume"]), amount=float(r["amount"]),
            )
            for _, r in df.iterrows()
        ]

    @staticmethod
    def _query_market_klines_minute(
        symbol: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        timer_times: list[str] | None = None,
    ) -> list[KlineBar]:
        store = get_market_data_store()
        sd = start_date or date(2000, 1, 1)
        ed = end_date or date.today()
        dt_start = datetime.combine(sd, datetime.min.time())
        dt_end = datetime.combine(ed, datetime.min.time()) + timedelta(days=1)
        df = store.load_minute([symbol], dt_start, dt_end, timer_times=timer_times)
        if df.empty:
            return []
        df = df.sort_index(ascending=False).head(limit)
        return [
            KlineBar(
                symbol=r["symbol"], datetime=r.name,
                open=float(r["open"]), high=float(r["high"]),
                low=float(r["low"]), close=float(r["close"]),
                volume=int(r["volume"]), amount=float(r["amount"]),
            )
            for _, r in df.iterrows()
        ]
