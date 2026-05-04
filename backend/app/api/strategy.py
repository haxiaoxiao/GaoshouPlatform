# backend/app/api/strategy.py
"""策略 API — 趋势资金事件驱动策略 + 深度价值策略"""
import asyncio
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.models.strategy import Strategy, Backtest
from app.db.models.watchlist import WatchlistGroup, WatchlistStock
from app.db.sqlite import get_async_session
from app.strategies.trend_capital import TrendCapitalStrategy

router = APIRouter()

# "A股精选" 分组名
WATCHLIST_GROUP_NAME = "A股精选"


async def _get_watchlist_symbols(session: AsyncSession) -> list[str] | None:
    """获取自选股"A股精选"分组中的股票代码"""
    result = await session.execute(
        select(WatchlistGroup).where(WatchlistGroup.name == WATCHLIST_GROUP_NAME)
    )
    group = result.scalar_one_or_none()
    if not group:
        return None

    result = await session.execute(
        select(WatchlistStock.symbol).where(WatchlistStock.group_id == group.id)
    )
    symbols = [row[0] for row in result.all()]
    return symbols if symbols else None


class SignalResponse(BaseModel):
    symbol: str
    trade_date: str
    signal_a_value: float | None = None
    signal_b_value: float | None = None
    signal_c_value: float | None = None
    signal_a_triggered: bool = False
    signal_b_triggered: bool = False
    signal_c_triggered: bool = False
    composite_triggered: bool = False
    trend_minute_count: int = 0
    total_minute_count: int = 0


class DailySignalsResponse(BaseModel):
    trade_date: str
    total_stocks: int
    triggered_count: int
    composite_count: int
    signals: list[SignalResponse]


class BacktestResponse(BaseModel):
    total_trades: int
    win_count: int
    win_rate: float
    avg_return: float
    total_return: float
    trades: list[dict]


def _get_strategy() -> TrendCapitalStrategy:
    return TrendCapitalStrategy(sample_space="CSI800")


@router.get("/signals/daily", response_model=list[DailySignalsResponse])
async def get_daily_signals(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    symbols: str | None = Query(None, description="股票代码, 逗号分隔"),
    use_watchlist: bool = Query(False, description="使用A股精选自选股"),
    session: AsyncSession = Depends(get_async_session),
):
    """获取指定日期范围的日度信号及综合信号"""
    sd = date.fromisoformat(start_date)
    ed = date.fromisoformat(end_date)

    if use_watchlist and not symbols:
        sym_list = await _get_watchlist_symbols(session)
    elif symbols:
        sym_list = [s.strip() for s in symbols.split(",")]
    else:
        sym_list = None

    strategy = _get_strategy()
    all_signals = await asyncio.to_thread(
        strategy.compute_composite_signals, sd, ed, sym_list
    )

    result = []
    for td in sorted(all_signals.keys()):
        day_sigs = all_signals[td]
        sig_list = []
        triggered = 0
        composite = 0
        for sym, sig in day_sigs.items():
            if sig.signal_a_triggered or sig.signal_b_triggered or sig.signal_c_triggered:
                triggered += 1
            if sig.composite_triggered:
                composite += 1
            sig_list.append(SignalResponse(
                symbol=sig.symbol,
                trade_date=sig.trade_date.isoformat(),
                signal_a_value=sig.signal_a_value,
                signal_b_value=sig.signal_b_value,
                signal_c_value=sig.signal_c_value,
                signal_a_triggered=sig.signal_a_triggered,
                signal_b_triggered=sig.signal_b_triggered,
                signal_c_triggered=sig.signal_c_triggered,
                composite_triggered=sig.composite_triggered,
                trend_minute_count=sig.trend_minute_count,
                total_minute_count=sig.total_minute_count,
            ))

        result.append(DailySignalsResponse(
            trade_date=td.isoformat(),
            total_stocks=len(sig_list),
            triggered_count=triggered,
            composite_count=composite,
            signals=sig_list,
        ))

    return result


@router.get("/signals/summary")
async def get_signals_summary(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    symbols: str | None = Query(None, description="股票代码, 逗号分隔"),
    use_watchlist: bool = Query(False, description="使用A股精选自选股"),
    session: AsyncSession = Depends(get_async_session),
):
    """获取信号汇总统计 — 每日复合信号触发的股票列表"""
    sd = date.fromisoformat(start_date)
    ed = date.fromisoformat(end_date)

    if use_watchlist and not symbols:
        sym_list = await _get_watchlist_symbols(session)
    elif symbols:
        sym_list = [s.strip() for s in symbols.split(",")]
    else:
        sym_list = None

    strategy = _get_strategy()
    all_signals = await asyncio.to_thread(
        strategy.compute_composite_signals, sd, ed, sym_list
    )

    summary = []
    for td in sorted(all_signals.keys()):
        day_sigs = all_signals[td]
        composite_stocks = [
            {
                "symbol": sym,
                "a": sig.signal_a_triggered,
                "b": sig.signal_b_triggered,
                "c": sig.signal_c_triggered,
                "a_val": round(sig.signal_a_value, 2) if sig.signal_a_value else None,
                "b_val": round(sig.signal_b_value, 4) if sig.signal_b_value else None,
                "c_val": round(sig.signal_c_value, 0) if sig.signal_c_value else None,
            }
            for sym, sig in day_sigs.items()
            if sig.composite_triggered
        ]
        if composite_stocks:
            summary.append({
                "trade_date": td.isoformat(),
                "count": len(composite_stocks),
                "stocks": composite_stocks[:50],  # 限制前50只
            })

    return {
        "start_date": start_date,
        "end_date": end_date,
        "trading_days_with_signals": len(summary),
        "total_composite_triggers": sum(s["count"] for s in summary),
        "avg_daily_triggers": round(sum(s["count"] for s in summary) / len(summary), 1) if summary else 0,
        "daily_summary": summary,
    }


@router.get("/backtest", response_model=BacktestResponse)
async def run_backtest(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    symbols: str | None = Query(None, description="股票代码, 逗号分隔"),
    use_watchlist: bool = Query(False, description="使用A股精选自选股"),
    session: AsyncSession = Depends(get_async_session),
):
    """运行组合策略回测 — 一个组合，评分选前5"""
    sd = date.fromisoformat(start_date)
    ed = date.fromisoformat(end_date)

    if use_watchlist and not symbols:
        sym_list = await _get_watchlist_symbols(session)
    elif symbols:
        sym_list = [s.strip() for s in symbols.split(",")]
    else:
        sym_list = None

    strategy = _get_strategy()
    result = await asyncio.to_thread(
        strategy.run_basket_strategy, sd, ed, sym_list
    )
    return result


class DeepValueBacktestRequest(BaseModel):
    start_date: date = "2015-01-01"
    end_date: date = "2025-12-31"
    initial_capital: float = 1_000_000
    pool: str = "all"  # all / top100 / top300 / top500


@router.post("/deep-value/backtest", summary="深度价值策略回测（独立引擎，秒级）")
async def run_deep_value_backtest(
    req: DeepValueBacktestRequest,
    session: AsyncSession = Depends(get_async_session),
):
    """深度价值策略独立回测 — 每年5月调仓，直接查ClickHouse筛选，结果自动保存"""
    from app.strategies.deep_value import DeepValueStrategy

    # Resolve pool
    if req.pool == "all":
        pool_symbols = None
        pool_label = "全量A股"
    else:
        await _ensure_pool_cache()
        pool_symbols = _POOL_CACHE.get(req.pool)
        pool_label = req.pool

    strategy = DeepValueStrategy(pool_symbols=pool_symbols)
    result = await asyncio.to_thread(
        strategy.run, req.start_date, req.end_date, req.initial_capital
    )

    # Save to backtests table
    try:
        # Find or create the strategy record
        stmt = select(Strategy).where(Strategy.name == "深度价值策略")
        exec_result = await session.execute(stmt)
        strategy_record = exec_result.scalar_one_or_none()
        if not strategy_record:
            strategy_record = Strategy(
                name="深度价值策略",
                code="standalone",
                description="低估值+高股息+深度折价：每年5月调仓，持有1年",
            )
            session.add(strategy_record)
            await session.flush()

        pool_count = len(pool_symbols) if pool_symbols else 4968
        backtest = Backtest(
            strategy_id=strategy_record.id,
            status="completed",
            start_date=req.start_date,
            end_date=req.end_date,
            initial_capital=Decimal(str(req.initial_capital)),
            parameters={
                "mode": "deep_value_standalone",
                "pool": req.pool,
                "pool_label": pool_label,
                "symbol_count": pool_count,
                "max_positions": 10,
                "single_pct": 0.10,
            },
            result=result,
        )
        session.add(backtest)
        await session.commit()
        result["backtest_id"] = backtest.id
    except Exception as e:
        logger.warning(f"Failed to save backtest: {e}")
        await session.rollback()

    return {"code": 0, "message": "success", "data": result}


# Shared pool cache (matching backtest/api.py)
_POOL_CACHE: dict[str, list[str]] = {}

async def _ensure_pool_cache():
    global _POOL_CACHE
    if _POOL_CACHE:
        return
    from app.db.clickhouse import get_ch_client
    ch = get_ch_client()
    for pool_name, limit in [("top100", 100), ("top300", 300), ("top500", 500)]:
        rows = ch.execute(
            "SELECT symbol FROM klines_daily "
            "WHERE trade_date >= %(s)s AND amount > 0 AND close > 0 "
            "GROUP BY symbol ORDER BY avg(amount) DESC LIMIT %(n)s",
            {"s": (date.today() - timedelta(days=365)).isoformat(), "n": limit},
        )
        _POOL_CACHE[pool_name] = [r[0] for r in rows]
