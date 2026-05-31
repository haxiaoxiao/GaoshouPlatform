"""AKShare 数据 API — 替代/补充 QMT 数据源

所有端点在线程池中执行（akshare 是同步 HTTP 调用）。
"""

import asyncio
from typing import Any

import pandas as pd
from fastapi import APIRouter, Query
from loguru import logger
from pydantic import BaseModel, Field

router = APIRouter(prefix="/akshare", tags=["AKShare"])


# ── Models ──

class StockDailyRequest(BaseModel):
    symbol: str = Field(..., description="股票代码，如 sh600000, sz000001")
    start_date: str = Field(..., description="开始日期 YYYYMMDD")
    end_date: str = Field(..., description="结束日期 YYYYMMDD")
    adjust: str = Field(default="qfq", description="复权类型: qfq/hfq/None")


class BatchDailyRequest(BaseModel):
    symbols: list[str] = Field(..., description="股票代码列表")
    start_date: str = Field(..., description="开始日期 YYYYMMDD")
    end_date: str = Field(..., description="结束日期 YYYYMMDD")
    adjust: str = Field(default="qfq", description="复权类型")


# ── Helpers ──

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame 转为 JSON 安全列表"""
    if df.empty:
        return []
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df.to_dict("records")


def _to_ak_symbol(platform_symbol: str) -> str:
    """平台格式 (000001.SZ) → akshare 格式 (sz000001)"""
    if "." in platform_symbol:
        code, market = platform_symbol.split(".")
        return f"{market.lower()}{code}"
    return platform_symbol


def _from_ak_symbol(ak_symbol: str) -> str:
    """akshare 格式 (sz000001) → 平台格式 (000001.SZ)"""
    if ak_symbol.startswith(("sh", "sz", "bj")):
        return f"{ak_symbol[2:]}.{ak_symbol[:2].upper()}"
    return ak_symbol


# ── Endpoints ──

@router.get("/stock/daily/{symbol}")
async def get_stock_daily(
    symbol: str,
    start_date: str = Query(..., description="开始日期 YYYYMMDD"),
    end_date: str = Query(..., description="结束日期 YYYYMMDD"),
    adjust: str = Query(default="qfq", description="复权: qfq/hfq/none"),
):
    """获取 A 股日线 OHLCV 数据（单只）"""
    def _fetch():
        import akshare as ak
        ak_sym = _to_ak_symbol(symbol)
        df = ak.stock_zh_a_daily(
            symbol=ak_sym,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
        if df.empty:
            return []
        # 标准化列名
        df = df.rename(columns={
            "date": "trade_date", "成交额": "amount",
            "涨跌幅": "pct_change", "换手率": "turnover_rate",
        })
        return _df_to_records(df)

    try:
        records = await asyncio.to_thread(_fetch)
        return {"code": 0, "data": {"symbol": symbol, "count": len(records), "records": records}}
    except Exception as e:
        logger.error("akshare daily {} failed: {}", symbol, e)
        return {"code": 1, "message": str(e), "data": None}


@router.post("/stock/daily/batch")
async def get_stock_daily_batch(req: BatchDailyRequest):
    """批量获取 A 股日线数据"""
    async def _fetch_one(sym: str):
        try:
            import akshare as ak
            ak_sym = _to_ak_symbol(sym)
            df = ak.stock_zh_a_daily(
                symbol=ak_sym,
                start_date=req.start_date,
                end_date=req.end_date,
                adjust=req.adjust,
            )
            if df.empty:
                return None
            df = df.rename(columns={
                "date": "trade_date", "成交额": "amount",
                "涨跌幅": "pct_change", "换手率": "turnover_rate",
            })
            df["symbol"] = sym
            return df
        except Exception as e:
            logger.warning("akshare daily {} failed: {}", sym, e)
            return None

    tasks = [_fetch_one(s) for s in req.symbols]
    results = await asyncio.gather(*tasks)
    all_frames = [r for r in results if r is not None]
    if not all_frames:
        return {"code": 1, "message": "All symbols failed", "data": None}
    combined = pd.concat(all_frames, ignore_index=True)
    return {"code": 0, "data": {"symbols": req.symbols, "count": len(combined), "records": _df_to_records(combined)}}


@router.get("/stock/spot")
async def get_stock_spot():
    """获取 A 股实时行情快照"""
    def _fetch():
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        if df.empty:
            return []
        return _df_to_records(df)

    try:
        records = await asyncio.to_thread(_fetch)
        return {"code": 0, "data": {"count": len(records), "records": records}}
    except Exception as e:
        logger.error("akshare spot failed: {}", e)
        return {"code": 1, "message": str(e), "data": None}


@router.get("/stock/list")
async def get_stock_list():
    """获取 A 股股票列表（代码+名称）— 优先 akshare，兜底 SQLite"""
    def _fetch_akshare():
        import akshare as ak
        df = ak.stock_info_a_code_name()
        if df.empty:
            return []
        result = []
        for _, row in df.iterrows():
            code = str(row.get("code", ""))
            name = str(row.get("name", ""))
            # 推断市场和平台格式
            if code.startswith("6"):
                symbol = f"{code}.SH"
            elif code.startswith(("0", "3")):
                symbol = f"{code}.SZ"
            elif code.startswith(("8", "4")):
                symbol = f"{code}.BJ"
            else:
                symbol = code
            result.append({"symbol": symbol, "code": code, "name": name})
        return result

    def _fetch_sqlite():
        from sqlalchemy import create_engine, text

        from app.core.config import settings
        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT symbol, name FROM stocks WHERE is_st=0 AND is_delist=0 ORDER BY symbol")
            ).all()
        engine.dispose()
        return [{"symbol": r[0], "code": r[0].split(".")[0] if "." in r[0] else r[0], "name": r[1]} for r in rows]

    try:
        records = await asyncio.to_thread(_fetch_akshare)
        if records:
            return {"code": 0, "data": {"source": "akshare", "count": len(records), "records": records}}
    except Exception as e:
        logger.warning("akshare stock list failed, falling back to SQLite: {}", e)

    try:
        records = await asyncio.to_thread(_fetch_sqlite)
        return {"code": 0, "data": {"source": "sqlite", "count": len(records), "records": records}}
    except Exception as e:
        logger.error("SQLite stock list fallback also failed: {}", e)
        return {"code": 1, "message": str(e), "data": None}


@router.get("/stock/info/{symbol}")
async def get_stock_info(symbol: str):
    """获取个股基本信息（含财务指标）"""
    def _fetch():
        import akshare as ak
        ak_sym = _to_ak_symbol(symbol)
        info = ak.stock_individual_info_em(symbol=ak_sym[2:])  # 纯数字，如 600000
        if info.empty:
            return {}
        return dict(zip(info["item"], info["value"], strict=False))

    try:
        info = await asyncio.to_thread(_fetch)
        return {"code": 0, "data": {"symbol": symbol, "info": info}}
    except Exception as e:
        logger.error("akshare info {} failed: {}", symbol, e)
        return {"code": 1, "message": str(e), "data": None}


@router.get("/stock/hist")
async def get_stock_hist_for_backtest(
    symbols: str = Query(..., description="逗号分隔，平台格式如 000001.SZ,600000.SH"),
    start_date: str = Query(..., description="开始日期 YYYYMMDD"),
    end_date: str = Query(..., description="结束日期 YYYYMMDD"),
    adjust: str = Query(default="qfq", description="复权: qfq/hfq/none"),
):
    """获取回测用历史数据 — akquant 兼容格式

    返回 format 适合直接作为 akquant.run_backtest(data=...) 的输入。
    每个 symbol 一个 DataFrame (DatetimeIndex + open/high/low/close/volume/symbol)。
    """
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]

    async def _fetch_one(sym: str) -> dict[str, Any] | None:
        try:
            import akshare as ak
            ak_sym = _to_ak_symbol(sym)
            df = ak.stock_zh_a_daily(
                symbol=ak_sym,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
            if df.empty:
                return None
            df = df.rename(columns={"date": "trade_date"})
            # 确保有 OHLCV 列
            df = df.rename(columns={
                "开盘": "open", "收盘": "close", "最高": "high",
                "最低": "low", "成交量": "volume", "成交额": "amount",
            })
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            df["symbol"] = sym
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date").sort_index()
            # 只保留 akquant 需要的列
            wanted = ["open", "high", "low", "close", "volume", "symbol"]
            available = [c for c in wanted if c in df.columns]
            return {"symbol": sym, "data": _df_to_records(df[available].reset_index())}
        except Exception as e:
            logger.warning("akshare hist {} failed: {}", sym, e)
            return None

    tasks = [_fetch_one(s) for s in sym_list]
    results = await asyncio.gather(*tasks)
    valid = [r for r in results if r is not None]
    return {
        "code": 0,
        "data": {
            "symbols": [r["symbol"] for r in valid],
            "records": {r["symbol"]: r["data"] for r in valid},
        },
    }
