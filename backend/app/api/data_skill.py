# backend/app/api/data_skill.py
"""数据技能 API — 为策略模块提供统一数据查询接口"""
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Stock
from app.db.sqlite import get_async_session
from app.services.data_skill import DataSkill
from app.services.security_symbols import normalize_security_symbol

router = APIRouter()


def _serialize_snapshot(s) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for f in s.__dataclass_fields__:
        v = getattr(s, f)
        if isinstance(v, date):
            v = v.isoformat()
        elif isinstance(v, datetime):
            v = v.isoformat()
        d[f] = v
    return d


def _serialize_bar(b) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for f in b.__dataclass_fields__:
        v = getattr(b, f)
        if isinstance(v, (date, datetime)):
            v = v.isoformat()
        d[f] = v
    return d


def _serialize_report(r) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for f in r.__dataclass_fields__:
        v = getattr(r, f)
        if isinstance(v, date):
            v = v.isoformat()
        elif isinstance(v, datetime):
            v = v.isoformat()
        d[f] = v
    return d


@router.get("/stock/{symbol}", summary="获取股票快照")
async def get_stock_snapshot(
    symbol: str = Path(description="股票代码，如 600051.SH"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    snapshot = await skill.get_stock(symbol)
    if snapshot is None:
        return {"code": 1, "message": f"未找到股票 {symbol}"}
    return {"code": 0, "data": _serialize_snapshot(snapshot)}


def _serialize_sql_row(row) -> dict[str, Any] | None:
    if row is None:
        return None
    d: dict[str, Any] = {}
    for key, value in row._mapping.items():
        if isinstance(value, date):
            value = value.isoformat()
        elif isinstance(value, datetime):
            value = value.isoformat()
        d[key] = value
    return d


def _serialize_stock_model(stock: Stock) -> dict[str, Any]:
    fields = [
        "symbol", "name", "exchange", "industry", "industry2", "industry3",
        "sector", "concept", "list_date", "delist_date", "is_st",
        "is_delist", "is_suspend", "total_shares", "float_shares",
        "a_float_shares", "limit_sell_shares", "total_mv", "circ_mv",
        "company_name", "province", "city", "business_scope",
        "main_business", "website", "employees", "eps", "bvps", "roe",
        "pe_ttm", "pb", "total_assets", "total_liability", "total_equity",
        "net_profit", "revenue", "security_type", "product_class",
    ]
    d: dict[str, Any] = {}
    for field in fields:
        value = getattr(stock, field, None)
        if isinstance(value, date):
            value = value.isoformat()
        elif isinstance(value, datetime):
            value = value.isoformat()
        d[field] = value
    return d


@router.get("/review/{symbol}", summary="A-share review context")
async def get_stock_review_context(
    symbol: str = Path(description="Stock symbol, e.g. 600051.SH"),
    as_of_date: date | None = Query(None, description="Review date YYYY-MM-DD"),
    lookback_days: int = Query(60, ge=1, le=365, description="Status lookback days"),
    session: AsyncSession = Depends(get_async_session),
):
    normalized = normalize_security_symbol(symbol) or symbol.strip().upper()
    stock = (
        await session.execute(select(Stock).where(Stock.symbol == normalized))
    ).scalar_one_or_none()
    if stock is None:
        return {"code": 1, "message": f"未找到股票 {symbol}"}

    end_date = as_of_date or date.today()
    start_date = end_date - timedelta(days=lookback_days)
    params = {
        "symbol": normalized,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    daily_basic = (
        await session.execute(
            text(
                """
                SELECT symbol, trade_date, total_share, float_share, total_mv,
                       circ_mv, turnover_rate, pe_ttm, pb, source, updated_at
                FROM stock_daily_basic
                WHERE symbol = :symbol AND trade_date <= :end_date
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ),
            params,
        )
    ).first()
    limit_prices = (
        await session.execute(
            text(
                """
                SELECT symbol, trade_date, up_limit, down_limit, source, updated_at
                FROM stock_limit_prices
                WHERE symbol = :symbol
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                ORDER BY trade_date DESC
                LIMIT 10
                """
            ),
            params,
        )
    ).all()
    name_changes = (
        await session.execute(
            text(
                """
                SELECT symbol, name, start_date, end_date, change_reason, source, updated_at
                FROM stock_name_changes
                WHERE symbol = :symbol
                  AND start_date <= :end_date
                  AND (end_date IS NULL OR end_date >= :start_date)
                ORDER BY start_date DESC
                LIMIT 10
                """
            ),
            params,
        )
    ).all()
    name_text = str(stock.name or "").upper()
    status = {
        "is_st": int(stock.is_st or 0),
        "is_delist": int(stock.is_delist or 0),
        "is_suspend": int(stock.is_suspend or 0),
        "name_has_st_marker": "ST" in name_text or "*" in name_text,
        "risk_warning_flag": bool(stock.is_st or "ST" in name_text or "*" in name_text),
        "suspension_flag": bool(stock.is_suspend),
    }
    return {
        "code": 0,
        "message": "success",
        "data": {
            "symbol": normalized,
            "as_of_date": end_date.isoformat(),
            "lookback_start": start_date.isoformat(),
            "stock": _serialize_stock_model(stock),
            "status": status,
            "daily_basic_latest": _serialize_sql_row(daily_basic),
            "limit_prices_recent": [_serialize_sql_row(row) for row in limit_prices],
            "name_changes": [_serialize_sql_row(row) for row in name_changes],
            "data_quality": {
                "daily_basic_present": daily_basic is not None,
                "limit_price_rows": len(limit_prices),
                "name_change_rows": len(name_changes),
            },
        },
    }


@router.post("/stocks/batch", summary="批量获取股票快照")
async def get_stocks_batch(
    symbols: list[str] = Query(description="股票代码列表"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    result = await skill.get_stocks(symbols)
    return {"code": 0, "data": {s: _serialize_snapshot(sn) for s, sn in result.items()}}


@router.get("/screen", summary="条件选股筛选")
async def screen_stocks(
    industry: str | None = Query(None, description="行业筛选"),
    exchange: str | None = Query(None, description="交易所 SH/SZ"),
    is_st: int | None = Query(None, description="ST状态 0=正常 1=ST 2=*ST"),
    min_mv: float | None = Query(None, description="最小总市值(万元)"),
    max_mv: float | None = Query(None, description="最大总市值(万元)"),
    min_pe: float | None = Query(None, description="最小PE_TTM"),
    max_pe: float | None = Query(None, description="最大PE_TTM"),
    min_roe: float | None = Query(None, description="最小ROE(%)"),
    limit: int = Query(500, ge=1, le=2000, description="最大返回数"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    result = await skill.screen_stocks(
        industry=industry, exchange=exchange, is_st=is_st,
        min_mv=min_mv, max_mv=max_mv, min_pe=min_pe, max_pe=max_pe,
        min_roe=min_roe, limit=limit,
    )
    return {
        "code": 0,
        "data": {
            "total": result.total,
            "stocks": [_serialize_snapshot(s) for s in result.stocks],
        },
    }


@router.get("/kline/daily/{symbol}", summary="获取日K线数据")
async def get_kline_daily(
    symbol: str = Path(description="股票代码"),
    start_date: date | None = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: date | None = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(500, ge=1, le=5000, description="最大条数"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    bars = await skill.get_kline_daily(symbol, start_date, end_date, limit)
    return {"code": 0, "data": [_serialize_bar(b) for b in bars]}


@router.get("/kline/minute/{symbol}", summary="获取分钟K线数据")
async def get_kline_minute(
    symbol: str = Path(description="股票代码"),
    start_date: date | None = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: date | None = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(500, ge=1, le=5000, description="最大条数"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    bars = await skill.get_kline_minute(symbol, start_date, end_date, limit)
    return {"code": 0, "data": [_serialize_bar(b) for b in bars]}


@router.get("/financial/{symbol}", summary="获取财务数据")
async def get_financial(
    symbol: str = Path(description="股票代码"),
    report_count: int = Query(8, ge=1, le=20, description="季度数"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    reports = await skill.get_financial(symbol, report_count)
    return {"code": 0, "data": [_serialize_report(r) for r in reports]}


@router.post("/financial/batch", summary="批量获取财务数据")
async def get_financial_batch(
    symbols: list[str] = Query(description="股票代码列表"),
    report_count: int = Query(1, ge=1, le=8, description="每只股票季度数"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    result = await skill.get_financial_batch(symbols, report_count)
    return {
        "code": 0,
        "data": {
            s: [_serialize_report(r) for r in reports]
            for s, reports in result.items()
        },
    }


@router.get("/quote/{symbol}", summary="获取实时行情")
async def get_realtime_quote(
    symbol: str = Path(description="股票代码"),
):
    from app.engines.qmt_gateway import qmt_gateway
    quotes = await qmt_gateway.get_realtime_quotes([symbol])
    if not quotes:
        return {"code": 1, "message": f"未获取到 {symbol} 的行情"}
    return {"code": 0, "data": quotes[0]}


@router.post("/quote/batch", summary="批量获取实时行情")
async def get_realtime_quotes(
    symbols: list[str] = Query(description="股票代码列表"),
):
    from app.engines.qmt_gateway import qmt_gateway
    quotes = await qmt_gateway.get_realtime_quotes(symbols)
    return {"code": 0, "data": quotes}


@router.get("/industries", summary="获取行业列表及统计")
async def get_industries(
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    industries = await skill.get_industries()
    return {
        "code": 0,
        "data": [
            {"name": i.name, "stock_count": i.stock_count}
            for i in industries
        ],
    }


@router.get("/symbols", summary="获取所有股票代码")
async def get_all_symbols(
    industry: str | None = Query(None, description="按行业筛选"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    if industry:
        symbols = await skill.get_symbols_by_industry(industry)
    else:
        symbols = await skill.get_all_symbols()
    return {"code": 0, "data": symbols}


@router.get("/indicator/{symbol}", summary="查询股票指标值")
async def get_indicator(
    symbol: str = Path(description="股票代码"),
    name: str = Query(..., description="指标名称"),
    trade_date: date | None = Query(None, description="交易日期"),
    session: AsyncSession = Depends(get_async_session),
):
    skill = DataSkill(session)
    value = skill.get_indicator(symbol, name, trade_date)
    return {"code": 0, "data": {"symbol": symbol, "indicator": name, "value": value, "trade_date": trade_date.isoformat() if trade_date else None}}
