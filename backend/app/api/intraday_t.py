"""API contract for intraday T backtesting and persistent paper simulation."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from math import isfinite
from typing import Annotated, Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.blocking import run_blocking
from app.data_stores import get_market_data_store
from app.db.sqlite import get_async_session
from app.services.intraday_t_backtest import BacktestConfig, IntradayTBacktester
from app.services.intraday_t_paper import IntradayTPaperService, intraday_t_paper_service
from app.services.intraday_t_strategy import SUPPORTED_SYMBOLS, CostModel, StrategyParams

router = APIRouter()


class StrategyParamsPayload(BaseModel):
    warmup_bars: int = Field(default=30, ge=10, le=120)
    volatility_window: int = Field(default=30, ge=10, le=120)
    fast_ema_span: int = Field(default=10, ge=2, le=60)
    slow_ema_span: int = Field(default=30, ge=5, le=180)
    vwap_slope_bars: int = Field(default=5, ge=1, le=30)
    entry_z: float = Field(default=1.75, ge=0.5, le=4.0)
    max_entry_z: float = Field(default=2.4, ge=0.5, le=6.0)
    exit_z: float = Field(default=0.25, ge=0.0, le=1.5)
    stop_z: float = Field(default=3.0, ge=1.5, le=6.0)
    realized_vol_window: int = Field(default=10, ge=2, le=120)
    min_realized_vol_bps: float = Field(default=0.0, ge=0.0, le=500.0)
    max_adverse_day_move_bps: float | None = Field(default=None, gt=0.0, le=5_000.0)
    max_trade_fraction: float = Field(default=0.25, gt=0.0, le=0.3)
    max_pairs_per_day: int = Field(default=1, ge=1, le=6)
    cooldown_minutes: int = Field(default=20, ge=0, le=120)
    edge_buffer_bps: float = Field(default=12.0, ge=0.0, le=100.0)
    max_daily_loss_bps: float = Field(default=45.0, gt=0.0, le=500.0)

    @model_validator(mode="after")
    def validate_windows(self) -> StrategyParamsPayload:
        if self.warmup_bars > self.volatility_window:
            raise ValueError("warmup_bars must not exceed volatility_window")
        if self.fast_ema_span >= self.slow_ema_span:
            raise ValueError("fast_ema_span must be below slow_ema_span")
        if self.exit_z >= self.entry_z:
            raise ValueError("exit_z must be below entry_z")
        if not self.entry_z < self.max_entry_z < self.stop_z:
            raise ValueError("entry_z must be below max_entry_z and max_entry_z below stop_z")
        return self


class CostModelPayload(BaseModel):
    commission_rate: float = Field(default=0.0003, ge=0.0, le=0.01)
    min_commission: float = Field(default=5.0, ge=0.0, le=100.0)
    stamp_duty_rate: float = Field(default=0.0005, ge=0.0, le=0.01)
    transfer_fee_rate: float = Field(default=0.00001, ge=0.0, le=0.001)
    slippage_bps: float = Field(default=2.0, ge=0.0, le=100.0)


class BacktestRequest(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: list(SUPPORTED_SYMBOLS), min_length=1)
    start_date: date
    end_date: date
    initial_capital: float = Field(default=1_000_000.0, gt=0)
    base_quantities: dict[str, int] = Field(default_factory=dict)
    cash_buffer_fraction: float = Field(default=0.30, ge=0.1, le=0.8)
    max_bar_volume_fraction: float = Field(default=0.05, gt=0.0, le=1.0)
    strategy: StrategyParamsPayload = Field(default_factory=StrategyParamsPayload)
    cost: CostModelPayload = Field(default_factory=CostModelPayload)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, values: list[str]) -> list[str]:
        normalized = list(dict.fromkeys(value.upper() for value in values))
        unknown = set(normalized) - set(SUPPORTED_SYMBOLS)
        if unknown:
            raise ValueError(f"unsupported symbols: {sorted(unknown)}")
        return normalized

    @field_validator("base_quantities")
    @classmethod
    def validate_base_quantities(cls, values: dict[str, int]) -> dict[str, int]:
        unknown = set(values) - set(SUPPORTED_SYMBOLS)
        if unknown:
            raise ValueError(f"unsupported base-quantity symbols: {sorted(unknown)}")
        if any(quantity <= 0 for quantity in values.values()):
            raise ValueError("base quantities must be positive")
        return values

    @model_validator(mode="after")
    def validate_period(self) -> BacktestRequest:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not exceed end_date")
        if (self.end_date - self.start_date).days > 365 * 8:
            raise ValueError("backtest period cannot exceed eight years")
        return self


class PositionPayload(BaseModel):
    quantity: int = Field(ge=0)
    available: int = Field(ge=0)
    avg_cost: float = Field(default=0.0, ge=0.0)

    @model_validator(mode="after")
    def validate_available(self) -> PositionPayload:
        if self.available > self.quantity:
            raise ValueError("available cannot exceed quantity")
        return self


class ManualAccountPayload(BaseModel):
    cash: float = Field(ge=0.0)
    positions: dict[str, PositionPayload]

    @field_validator("positions")
    @classmethod
    def validate_positions(cls, values: dict[str, PositionPayload]) -> dict[str, PositionPayload]:
        unknown = set(values) - set(SUPPORTED_SYMBOLS)
        if unknown:
            raise ValueError(f"unsupported position symbols: {sorted(unknown)}")
        return values


class PaperStartRequest(BaseModel):
    manual_account: ManualAccountPayload | None = None
    strategy: StrategyParamsPayload = Field(default_factory=StrategyParamsPayload)


class PaperRunnerRequest(BaseModel):
    interval_seconds: int = Field(default=30, ge=5, le=3_600)


def get_intraday_t_market_store():
    return get_market_data_store()


def get_intraday_t_paper_service() -> IntradayTPaperService:
    return intraday_t_paper_service


class IntradayTLimitPriceLoader:
    """Load exact exchange-supplied daily limits for a bounded symbol/date request."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def load(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[str, float]]:
        if not symbols:
            return {}
        statement = text(
            "SELECT symbol, trade_date, up_limit, down_limit "
            "FROM stock_limit_prices "
            "WHERE symbol IN :symbols "
            "AND trade_date >= :start_date "
            "AND trade_date <= :end_date "
            "ORDER BY symbol, trade_date"
        ).bindparams(bindparam("symbols", expanding=True))
        rows = (
            await self._session.execute(
                statement,
                {
                    "symbols": symbols,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
        ).all()
        limits: dict[str, dict[str, float]] = {}
        for symbol, raw_trade_date, raw_up, raw_down in rows:
            if raw_up is None or raw_down is None:
                continue
            up = float(raw_up)
            down = float(raw_down)
            if not isfinite(up) or not isfinite(down) or up <= 0 or down <= 0:
                continue
            if isinstance(raw_trade_date, datetime):
                trade_date = raw_trade_date.date()
            elif isinstance(raw_trade_date, date):
                trade_date = raw_trade_date
            else:
                trade_date = date.fromisoformat(str(raw_trade_date))
            limits[f"{symbol}|{trade_date.isoformat()}"] = {"up": up, "down": down}
        return limits


async def get_intraday_t_limit_price_loader(
    session: Annotated[AsyncSession, Depends(get_async_session)],
) -> IntradayTLimitPriceLoader:
    return IntradayTLimitPriceLoader(session)


def _ok(data: Any) -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": data}


def _date_bounds(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    return datetime.combine(start_date, time(9, 0)), datetime.combine(end_date, time(15, 0))


async def _service_call(awaitable):
    try:
        return await awaitable
    except ValueError as exc:
        message = str(exc)
        status_code = (
            404 if "not found" in message else 409 if "already running" in message else 400
        )
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.get("/capabilities")
async def capabilities():
    defaults = StrategyParamsPayload().model_dump()
    return _ok(
        {
            "symbols": [
                {
                    "symbol": symbol,
                    "name": name,
                    "board": "STAR" if symbol.startswith("688") else "MAIN",
                }
                for symbol, name in SUPPORTED_SYMBOLS.items()
            ],
            "modes": ["backtest", "paper"],
            "real_order_submit_enabled": False,
            "defaults": {
                "strategy": defaults,
                "cost": CostModelPayload().model_dump(),
                "initial_capital": 1_000_000,
                "cash_buffer_fraction": 0.30,
                "max_bar_volume_fraction": 0.05,
            },
            "risk_controls": {
                "next_bar_fill": True,
                "t_plus_one_sellable_inventory": True,
                "lunch_restore_time": "11:29",
                "force_restore_time": "14:49",
                "max_daily_loss_bps": defaults["max_daily_loss_bps"],
                "max_trade_fraction": 0.30,
                "entry_window": {
                    "start": "10:00",
                    "end": "10:30",
                    "end_exclusive": True,
                    "afternoon_entries": False,
                },
                "max_entry_z": defaults["max_entry_z"],
                "realized_vol_window": defaults["realized_vol_window"],
                "min_realized_vol_bps": defaults["min_realized_vol_bps"],
                "exact_limit_price_filter": True,
                "missing_limit_price_entry_policy": "block_entry",
                "simulated_only": True,
            },
        }
    )


@router.get("/coverage")
async def coverage(
    store: Annotated[Any, Depends(get_intraday_t_market_store)],
    symbols: str = Query(default=",".join(SUPPORTED_SYMBOLS)),
    start_date: date | None = None,
    end_date: date | None = None,
):
    requested = list(
        dict.fromkeys(value.strip().upper() for value in symbols.split(",") if value.strip())
    )
    unknown = set(requested) - set(SUPPORTED_SYMBOLS)
    if unknown or not requested:
        raise HTTPException(status_code=422, detail=f"unsupported symbols: {sorted(unknown)}")
    end_date = end_date or date.today()
    start_date = start_date or end_date - timedelta(days=90)
    if start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must not exceed end_date")
    start, end = _date_bounds(start_date, end_date)
    frame = await run_blocking(store.load_minute, requested, start, end)
    items = []
    for symbol in requested:
        group = frame.loc[frame["symbol"] == symbol] if not frame.empty else pd.DataFrame()
        items.append(
            {
                "symbol": symbol,
                "name": SUPPORTED_SYMBOLS[symbol],
                "bars": len(group),
                "trade_days": len({value.date() for value in group.index})
                if not group.empty
                else 0,
                "start": group.index.min().isoformat() if not group.empty else None,
                "end": group.index.max().isoformat() if not group.empty else None,
            }
        )
    return _ok(
        {
            "requested": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            "coverage": items,
        }
    )


@router.post("/backtest")
async def run_backtest(
    request: BacktestRequest,
    store: Annotated[Any, Depends(get_intraday_t_market_store)],
    limit_price_loader: Annotated[
        IntradayTLimitPriceLoader,
        Depends(get_intraday_t_limit_price_loader),
    ],
):
    start, end = _date_bounds(request.start_date, request.end_date)
    limit_prices = await limit_price_loader.load(
        request.symbols,
        request.start_date,
        request.end_date,
    )

    def _run() -> dict[str, Any]:
        frame = store.load_minute(request.symbols, start, end)
        if frame.empty:
            raise ValueError("no minute data in requested period")
        return IntradayTBacktester().run(
            frame,
            BacktestConfig(
                initial_capital=request.initial_capital,
                base_quantities=request.base_quantities,
                params=StrategyParams(**request.strategy.model_dump()),
                cost=CostModel(**request.cost.model_dump()),
                max_bar_volume_fraction=request.max_bar_volume_fraction,
                cash_buffer_fraction=request.cash_buffer_fraction,
                limit_prices=limit_prices,
                require_exact_limit_prices=True,
            ),
        )

    try:
        result = await run_blocking(_run)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _ok(result)


@router.post("/paper/start")
async def start_paper(
    request: PaperStartRequest,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    manual = request.manual_account.model_dump() if request.manual_account else None
    result = await _service_call(
        service.start(manual_account=manual, params=request.strategy.model_dump())
    )
    return _ok(result)


@router.get("/paper/status")
async def paper_status(
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
    session_id: str | None = None,
):
    return _ok(await _service_call(service.status(session_id)))


@router.post("/paper/{session_id}/evaluate")
async def evaluate_paper(
    session_id: str,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    return _ok(await _service_call(service.evaluate(session_id)))


@router.post("/paper/{session_id}/stop")
async def stop_paper(
    session_id: str,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    return _ok(await _service_call(service.stop(session_id)))


@router.post("/paper/{session_id}/runner/start")
async def start_paper_runner(
    session_id: str,
    request: PaperRunnerRequest,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    return _ok(
        await _service_call(
            service.start_runner(session_id, interval_seconds=request.interval_seconds)
        )
    )


@router.post("/paper/{session_id}/runner/stop")
async def stop_paper_runner(
    session_id: str,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    return _ok(await _service_call(service.stop_runner(session_id)))


@router.post("/paper/{session_id}/reset")
async def reset_paper(
    session_id: str,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    return _ok(await _service_call(service.reset(session_id)))


@router.get("/paper/{session_id}/trades")
async def paper_trades(
    session_id: str,
    service: Annotated[IntradayTPaperService, Depends(get_intraday_t_paper_service)],
):
    return _ok(await _service_call(service.trades(session_id)))
