# backend/app/api/data_skill.py
"""数据技能 API — 为策略模块提供统一数据查询接口"""
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services.data_skill import DataSkill

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