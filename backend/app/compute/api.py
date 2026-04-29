"""计算层 API — /api/v2/compute"""
import time
from datetime import date

import pandas as pd
from fastapi import APIRouter, Body
from pydantic import BaseModel, Field

from app.compute.cache import ComputeCache, get_compute_cache
from app.compute.expression import evaluate_expression, validate_expression
from app.compute.operators import auto_discover
from app.compute.operators.registry import OperatorRegistry

router = APIRouter(prefix="/v2/compute")

# 启动时自动发现所有算子
auto_discover()


class EvaluateRequest(BaseModel):
    expression: str = Field(..., description="因子表达式，如 Mean($close, 5)")
    symbols: list[str] = Field(..., description="股票代码列表")
    start_date: date = Field(..., description="开始日期")
    end_date: date = Field(..., description="结束日期")
    use_cache: bool = Field(default=True, description="是否使用缓存")


class ScreenRequest(BaseModel):
    condition: str = Field(..., description="筛选条件表达式，如 RSI($close, 14) < 30")
    universe: str = Field(default="all", description="股票池: all 或 自选股分组名")
    trade_date: date = Field(..., description="筛选日期")
    limit: int = Field(default=50, description="返回数量上限")


@router.get("/operators")
async def list_operators():
    """返回所有可用算子"""
    return {
        "code": 0,
        "message": "success",
        "data": OperatorRegistry.to_api_list(),
    }


@router.post("/evaluate")
async def evaluate(req: EvaluateRequest):
    """计算表达式"""
    t0 = time.time()
    cache_hit = False

    cache: ComputeCache = get_compute_cache()

    result = None
    if req.use_cache:
        result = cache.get(req.expression)

    if result is None:
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        rows = ch.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY symbol, trade_date
            """,
            {"syms": req.symbols, "start": req.start_date, "end": req.end_date},
        )

        if not rows:
            return {"code": 0, "message": "success", "data": {}, "meta": {"rows": 0}}

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close",
                     "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        result = evaluate_expression(req.expression, data)

        if req.use_cache:
            cache.set(req.expression, result)
    else:
        cache_hit = True

    out: dict[str, list[dict]] = {}
    if isinstance(result, dict):
        for sym, series in result.items():
            if not isinstance(series, pd.Series):
                continue
            out[sym] = [
                {"trade_date": str(idx.date()), "value": float(v) if pd.notna(v) else None}
                for idx, v in series.items()
                if req.start_date <= idx.date() <= req.end_date
            ]
    elif isinstance(result, pd.Series):
        out["result"] = [
            {"trade_date": str(idx.date()), "value": float(v) if pd.notna(v) else None}
            for idx, v in result.items()
            if req.start_date <= idx.date() <= req.end_date
        ]

    elapsed = (time.time() - t0) * 1000

    return {
        "code": 0,
        "message": "success",
        "data": out,
        "meta": {
            "expression": req.expression,
            "cache_hit": cache_hit,
            "compute_time_ms": round(elapsed, 1),
        },
    }


@router.post("/screen")
async def screen(req: ScreenRequest):
    """基于条件筛选股票"""
    valid, err = validate_expression(req.condition)
    if not valid:
        return {"code": 1, "message": f"Invalid condition: {err}", "data": None}

    from app.db.clickhouse import get_ch_client
    ch = get_ch_client()

    rows = ch.execute("SELECT DISTINCT symbol FROM klines_daily WHERE trade_date = %(d)s",
                      {"d": req.trade_date})
    symbols = [r[0] for r in rows]

    if not symbols:
        return {"code": 0, "message": "success", "data": {"symbols": [], "count": 0}}

    rows = ch.execute(
        """
        SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
        FROM klines_daily
        WHERE symbol IN %(syms)s
          AND trade_date >= %(start)s
        ORDER BY symbol, trade_date
        """,
        {"syms": symbols, "start": date(2020, 1, 1)},
    )

    df = pd.DataFrame(
        rows,
        columns=["symbol", "trade_date", "open", "high", "low", "close",
                 "volume", "amount", "turnover_rate"],
    )
    for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
        df[col] = df[col].astype(float)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    data = {}
    for sym, grp in df.groupby("symbol"):
        data[sym] = grp.set_index("trade_date")

    result = evaluate_expression(req.condition, data)

    matching = []
    if isinstance(result, dict):
        for sym, series in result.items():
            if isinstance(series, pd.Series) and req.trade_date in series.index:
                val = series.loc[req.trade_date]
                if isinstance(val, (bool, pd.Series)):
                    cond = bool(val.iloc[0] if isinstance(val, pd.Series) else val)
                elif pd.isna(val):
                    continue
                else:
                    cond = bool(val)
                if cond:
                    matching.append(sym)
        matching = matching[:req.limit]

    return {
        "code": 0,
        "message": "success",
        "data": {"symbols": matching, "count": len(matching)},
    }


@router.post("/validate")
async def validate_expr(expression: str = Body(..., description="要校验的表达式", embed=True)):
    """校验表达式语法"""
    valid, err = validate_expression(expression)
    return {
        "code": 0,
        "message": "success",
        "data": {"valid": valid, "error": err},
    }
