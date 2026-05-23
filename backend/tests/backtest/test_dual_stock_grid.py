from __future__ import annotations

import pytest

from app.backtest.strategies.dual_stock_grid_akquant import (
    DEFAULT_DUAL_STOCK_GRID_PARAM_GRID,
    DUAL_STOCK_GRID_STRATEGY_CODE,
    DUAL_STOCK_GRID_SYMBOLS,
)
from app.services.grid_trading import GridTradingService


def test_dual_stock_grid_strategy_code_shape():
    assert "class DualStockGridStrategy" in DUAL_STOCK_GRID_STRATEGY_CODE
    assert "aq.Strategy" in DUAL_STOCK_GRID_STRATEGY_CODE
    assert "def on_bar" in DUAL_STOCK_GRID_STRATEGY_CODE
    assert DUAL_STOCK_GRID_SYMBOLS == ["002624.SZ", "300418.SZ"]
    assert DEFAULT_DUAL_STOCK_GRID_PARAM_GRID["grid_pct"]


def test_grid_signal_buy_sell_hold_logic():
    service = GridTradingService()
    params = service._normalize_params({"initial_cash": 100000, "grid_pct": 0.02})
    account = type(
        "A",
        (),
        {
            "cash": 100000.0,
            "positions": {
                "002624.SZ": {"quantity": 3000, "available": 3000, "avg_cost": 10.0}
            },
            "source": "manual",
            "error": None,
        },
    )()
    import datetime as dt

    now = dt.datetime(2026, 5, 20, 10, 30)
    service._state["002624.SZ"] = {"last_grid_price": 10.0, "grid_level": 0}
    buy = service._build_signal("002624.SZ", 9.7, 10.0, "minute_vwap", params, account, now)
    assert buy["action"] == "BUY"
    assert buy["quantity"] % 100 == 0

    service._state["002624.SZ"] = {"last_grid_price": 10.0, "grid_level": 1}
    sell = service._build_signal("002624.SZ", 10.3, 10.0, "minute_vwap", params, account, now)
    assert sell["action"] == "SELL"
    assert sell["quantity"] % 100 == 0

    hold = service._build_signal("002624.SZ", 10.01, 10.0, "minute_vwap", params, account, now)
    assert hold["action"] == "HOLD"


@pytest.mark.asyncio
async def test_order_preview_disabled_by_default():
    service = GridTradingService()
    result = await service.submit_order_preview({"symbol": "002624.SZ", "side": "BUY"})
    assert result["submitted"] is False
    assert result["enabled"] is False
