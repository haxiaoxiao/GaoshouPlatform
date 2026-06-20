# backend/app/api/strategy.py
"""策略 API — 趋势资金事件驱动策略 + 深度价值策略 + 研报转策略"""
import asyncio
import tempfile
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, Depends, File, Query, UploadFile
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.strategy import Backtest, Strategy
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
        for _sym, sig in day_sigs.items():
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
    pool: str = "all"
    pe_min: float = 0
    pe_max: float = 40
    dividend_yield_min: float = 3.5
    price_to_ma_max: float = 0.9
    max_positions: int = 10
    single_pct: float = 0.10


@router.post("/deep-value/backtest", summary="深度价值策略回测（独立引擎，秒级）")
async def run_deep_value_backtest(
    req: DeepValueBacktestRequest,
    session: AsyncSession = Depends(get_async_session),
):
    """深度价值策略独立回测 — 每年5月调仓，直接查本地数据筛选，结果自动保存"""
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
    strategy.PE_MIN = req.pe_min
    strategy.PE_MAX = req.pe_max
    strategy.DIVIDEND_YIELD_MIN = req.dividend_yield_min
    strategy.PRICE_TO_MA_MAX = req.price_to_ma_max
    strategy.MAX_POSITIONS = req.max_positions
    strategy.SINGLE_PCT = req.single_pct
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
                "mode": "deep_value",
                "pool": req.pool,
                "pool_label": pool_label,
                "symbol_count": pool_count,
                "pe_min": req.pe_min,
                "pe_max": req.pe_max,
                "dividend_yield_min": req.dividend_yield_min,
                "price_to_ma_max": req.price_to_ma_max,
                "max_positions": req.max_positions,
                "single_pct": req.single_pct,
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
    from app.data_stores import get_market_data_store

    store = get_market_data_store()
    start_date = date.today() - timedelta(days=365)
    end_date = date.today()
    symbols = await asyncio.to_thread(store.top_by_avg_amount, start_date, end_date, 500)
    for pool_name, limit in [("top100", 100), ("top300", 300), ("top500", 500)]:
        _POOL_CACHE[pool_name] = symbols[:limit]


@router.post("/from-report", summary="研报转策略 — 上传 PDF/TXT 自动生成策略代码")
async def strategy_from_report(file: UploadFile = File(...)):
    """上传研报文件, LLM 解析并生成策略代码"""
    suffix = Path(file.filename or "report.txt").suffix.lower()
    if suffix not in ('.pdf', '.txt', '.md'):
        return {"code": 1, "message": "仅支持 PDF/TXT/MD 文件", "data": None}

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        from app.services.report_to_strategy import generate_strategy, parse_report
        text = await asyncio.to_thread(parse_report, tmp_path)
        if not text or text.startswith("PDF"):
            return {"code": 1, "message": f"文本提取失败: {text}", "data": None}

        result = await asyncio.to_thread(generate_strategy, text)
        return {"code": 0, "message": "success", "data": result}
    except Exception as e:
        logger.error(f"from-report error: {e}")
        return {"code": 1, "message": f"生成失败: {e}", "data": None}
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ── LLM 策略生成（akquant 引擎）──

class ConvertRequest(BaseModel):
    source_code: str


class ChatMessageRequest(BaseModel):
    message: str


@router.post("/convert-to-akquant", summary="将任意策略代码转换为 AKQuant 格式")
async def convert_to_akquant(req: ConvertRequest):
    """单次调用：LLM 将 RQAlpha/Backtrader/VNPY 等代码转为 akquant Strategy"""
    if not req.source_code.strip():
        return {"code": 1, "message": "代码不能为空", "data": None}
    try:
        from app.services.llm_strategy import convert_to_akquant as do_convert
        code = await asyncio.to_thread(do_convert, req.source_code)
        return {"code": 0, "message": "success", "data": {"code": code}}
    except Exception as e:
        logger.error(f"convert-to-akquant error: {e}")
        return {"code": 1, "message": f"转换失败: {e}", "data": None}


@router.post("/chat-session", summary="创建研报对话会话")
async def create_chat_session(file: UploadFile = File(...)):
    """上传研报文件，创建对话会话，返回首次 LLM 回复"""
    suffix = Path(file.filename or "report.txt").suffix.lower()
    if suffix not in ('.pdf', '.txt', '.md'):
        return {"code": 1, "message": "仅支持 PDF/TXT/MD 文件", "data": None}

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        from app.services.llm_strategy import create_chat_session as do_create
        from app.services.report_to_strategy import parse_report

        text = await asyncio.to_thread(parse_report, tmp_path)
        if not text or text.startswith("PDF 解析失败"):
            return {"code": 1, "message": f"文本提取失败: {text}", "data": None}

        result = await asyncio.to_thread(do_create, text, file.filename or "report")
        return {"code": 0, "message": "success", "data": result}
    except Exception as e:
        logger.error(f"chat-session error: {e}")
        return {"code": 1, "message": f"创建会话失败: {e}", "data": None}
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@router.post("/chat-session/{session_id}/send", summary="发送消息到对话会话")
async def send_chat_message(session_id: str, req: ChatMessageRequest):
    """向已有会话发送消息，返回 LLM 回复和可能的代码"""
    if not req.message.strip():
        return {"code": 1, "message": "消息不能为空", "data": None}
    try:
        from app.services.llm_strategy import send_chat_message as do_send
        result = await asyncio.to_thread(do_send, session_id, req.message)
        return {"code": 0, "message": "success", "data": result}
    except ValueError as e:
        return {"code": 1, "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"chat-session/send error: {e}")
        return {"code": 1, "message": f"发送失败: {e}", "data": None}
