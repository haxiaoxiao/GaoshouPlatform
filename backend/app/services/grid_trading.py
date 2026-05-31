"""Dual-stock grid trading realtime signal service."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.engines.qmt_gateway import qmt_gateway
from app.services.qmt_trading import QmtAccountSnapshot, qmt_trading_service

GRID_SYMBOLS = ["002624.SZ", "300418.SZ"]
GRID_NAMES = {"002624.SZ": "完美世界", "300418.SZ": "昆仑万维"}


@dataclass
class GridAccountSnapshot:
    cash: float
    positions: dict[str, dict[str, Any]]
    source: str
    total_asset: float = 0.0
    market_value: float = 0.0
    error: str | None = None

    @classmethod
    def from_qmt(cls, snapshot: QmtAccountSnapshot) -> GridAccountSnapshot:
        return cls(
            cash=snapshot.cash,
            total_asset=snapshot.total_asset,
            market_value=snapshot.market_value,
            positions={k: v.as_dict() for k, v in snapshot.positions.items()},
            source=snapshot.source,
            error=snapshot.error,
        )


class GridTradingService:
    """Generate manual-execution grid signals from quote + account state."""

    def __init__(self) -> None:
        self._state: dict[str, dict[str, Any]] = {}

    async def status(self) -> dict[str, Any]:
        return await qmt_trading_service.status()

    async def account(self) -> GridAccountSnapshot:
        return await self._account_snapshot(None)

    async def signals(
        self,
        *,
        params: dict[str, Any] | None = None,
        manual_account: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params = self._normalize_params(params or {})
        quotes = await qmt_gateway.get_realtime_quotes(GRID_SYMBOLS)
        quote_map = {str(q.get("symbol") or q.get("code") or q.get("stock_code")): q for q in quotes}
        account = await self._account_snapshot(manual_account)
        now = datetime.now()
        signals: list[dict[str, Any]] = []

        for symbol in GRID_SYMBOLS:
            quote = quote_map.get(symbol) or {}
            price = self._quote_price(quote)
            if price <= 0:
                signals.append(self._empty_signal(symbol, "NO_QUOTE", "未获取到有效实时价格", now))
                continue
            anchor, anchor_source = self._anchor_price(symbol, params["anchor_window_minutes"], price, quote)
            signals.append(self._build_signal(symbol, price, anchor, anchor_source, params, account, now))

        return {
            "timestamp": now.isoformat(timespec="seconds"),
            "symbols": GRID_SYMBOLS,
            "account": {
                "cash": account.cash,
                "total_asset": account.total_asset,
                "market_value": account.market_value,
                "source": account.source,
                "error": account.error,
            },
            "order_submit_enabled": bool(settings.grid_trading_enable_order_submit),
            "signals": signals,
        }

    async def submit_order_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not settings.grid_trading_enable_order_submit:
            return {
                "enabled": False,
                "submitted": False,
                "message": "GRID_TRADING_ENABLE_ORDER_SUBMIT=false，当前仅生成手动执行信号。",
                "order_preview": payload,
            }
        return {
            "enabled": True,
            "submitted": False,
            "message": "真实委托已配置为可用，但预览接口不会下单；需要调用 submit 并显式 confirm。",
            "order_preview": payload,
        }

    async def submit_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await qmt_trading_service.submit_order(payload)

    def _normalize_params(self, raw: dict[str, Any]) -> dict[str, Any]:
        symbol_weights = raw.get("symbol_weights") or {"002624.SZ": 0.5, "300418.SZ": 0.5}
        return {
            "grid_pct": float(raw.get("grid_pct", 0.025)),
            "anchor_window_minutes": int(raw.get("anchor_window_minutes", 240)),
            "max_grid_levels": int(raw.get("max_grid_levels", 6)),
            "base_position_pct": float(raw.get("base_position_pct", 0.60)),
            "grid_sleeve_pct": float(raw.get("grid_sleeve_pct", 0.40)),
            "anchor_reset_pct": float(raw.get("anchor_reset_pct", 0.08)),
            "cash_buffer_pct": float(raw.get("cash_buffer_pct", 0.05)),
            "initial_cash": float(raw.get("initial_cash", 1_000_000.0)),
            "lot_size": int(raw.get("lot_size", 100)),
            "symbol_weights": {str(k): float(v) for k, v in dict(symbol_weights).items()},
        }

    async def _account_snapshot(self, manual: dict[str, Any] | None) -> GridAccountSnapshot:
        if manual:
            positions = manual.get("positions") or {}
            return GridAccountSnapshot(
                cash=float(manual.get("cash", 0.0) or 0.0),
                total_asset=float(manual.get("total_asset", 0.0) or 0.0),
                market_value=float(manual.get("market_value", 0.0) or 0.0),
                positions={str(k): dict(v or {}) for k, v in dict(positions).items()},
                source="manual",
            )
        try:
            return GridAccountSnapshot.from_qmt(await qmt_trading_service.account_snapshot())
        except Exception as exc:
            return GridAccountSnapshot(
                cash=0.0,
                positions={},
                source="qmt",
                error=f"{type(exc).__name__}: {exc}",
            )

    def _quote_price(self, quote: dict[str, Any]) -> float:
        for key in ("lastPrice", "last_price", "price", "close"):
            value = quote.get(key)
            if value:
                try:
                    return float(value)
                except Exception:
                    pass
        return 0.0

    def _anchor_price(
        self,
        symbol: str,
        window_minutes: int,
        fallback_price: float,
        quote: dict[str, Any] | None = None,
    ) -> tuple[float, str]:
        end = datetime.now()
        start = end - timedelta(minutes=max(1, window_minutes) * 2)
        try:
            df = get_market_data_store().load_minute([symbol], start, end)
            if not df.empty:
                frame = df.reset_index().tail(max(1, window_minutes))
                volume = pd.to_numeric(frame.get("volume"), errors="coerce").fillna(0.0)
                close = pd.to_numeric(frame.get("close"), errors="coerce").fillna(0.0)
                denom = float(volume.sum())
                if denom > 0:
                    return float((close * volume).sum() / denom), "minute_vwap"
                if (close > 0).any():
                    return float(close[close > 0].tail(1).iloc[0]), "minute_last"
        except Exception:
            pass

        tick_vwap = self._tick_vwap(quote or {}, fallback_price)
        if tick_vwap > 0:
            return tick_vwap, "tick_vwap"
        return fallback_price, "last_price_fallback"

    def _tick_vwap(self, quote: dict[str, Any], fallback_price: float) -> float:
        amount = self._safe_float(quote.get("amount"))
        volume = self._safe_float(quote.get("volume"))
        if amount <= 0 or volume <= 0:
            return 0.0

        low = self._safe_float(quote.get("low"))
        high = self._safe_float(quote.get("high"))
        lower = low * 0.8 if low > 0 else fallback_price * 0.8
        upper = high * 1.2 if high > 0 else fallback_price * 1.2
        candidates = [amount / volume, amount / (volume * 100.0)]
        for candidate in candidates:
            if lower <= candidate <= upper:
                return float(candidate)
        return 0.0

    def _build_signal(
        self,
        symbol: str,
        price: float,
        anchor: float,
        anchor_source: str,
        params: dict[str, Any],
        account: GridAccountSnapshot,
        now: datetime,
    ) -> dict[str, Any]:
        state = self._state.setdefault(symbol, {"last_grid_price": anchor or price, "grid_level": 0})
        last_grid_price = float(state.get("last_grid_price") or anchor or price)
        reset = False
        if anchor > 0 and abs(price / anchor - 1.0) > params["anchor_reset_pct"]:
            last_grid_price = anchor
            state["last_grid_price"] = anchor
            state["grid_level"] = 0
            reset = True

        position = account.positions.get(symbol, {})
        qty = float(position.get("quantity", 0.0) or 0.0)
        available = float(position.get("available", qty) or 0.0)
        budget = params["initial_cash"] * float(params["symbol_weights"].get(symbol, 0.5))
        base_qty = self._round_lot((budget * params["base_position_pct"]) / price, params["lot_size"])
        grid_lot = self._round_lot(
            (budget * params["grid_sleeve_pct"]) / max(1, params["max_grid_levels"]) / price,
            params["lot_size"],
        )
        buy_price = last_grid_price * (1.0 - params["grid_pct"])
        sell_price = last_grid_price * (1.0 + params["grid_pct"])

        action = "HOLD"
        quantity = 0
        trigger_price = None
        reason = "未触发网格"
        if reset:
            reason = "价格偏离滚动中枢，已重置网格基准"
        if grid_lot > 0 and price <= buy_price and account.cash * (1.0 - params["cash_buffer_pct"]) >= grid_lot * price:
            action = "BUY"
            quantity = int(grid_lot)
            trigger_price = buy_price
            reason = f"价格低于下一买入网格 {buy_price:.3f}"
        elif grid_lot > 0 and price >= sell_price and available > max(0, base_qty):
            action = "SELL"
            quantity = int(min(grid_lot, max(0, available - base_qty)))
            trigger_price = sell_price
            reason = f"价格高于下一卖出网格 {sell_price:.3f}"

        if action != "HOLD" and quantity <= 0:
            action = "HOLD"
            reason = "触发价格但可交易数量不足"
        if action != "HOLD":
            state["last_signal"] = {"action": action, "quantity": quantity, "price": price, "timestamp": now.isoformat()}

        signal_key = f"{symbol}:{action}:{quantity}:{round(price, 3)}:{now.strftime('%Y%m%d%H%M')}"
        return {
            "symbol": symbol,
            "name": GRID_NAMES.get(symbol, symbol),
            "action": action,
            "quantity": quantity,
            "current_price": round(price, 4),
            "anchor_price": round(anchor, 4),
            "anchor_source": anchor_source,
            "last_grid_price": round(last_grid_price, 4),
            "trigger_price": round(float(trigger_price), 4) if trigger_price else None,
            "next_buy_price": round(buy_price, 4),
            "next_sell_price": round(sell_price, 4),
            "grid_pct": params["grid_pct"],
            "grid_level": int(state.get("grid_level", 0) or 0),
            "position_qty": qty,
            "available_qty": available,
            "base_position_qty": base_qty,
            "reason": reason,
            "timestamp": now.isoformat(timespec="seconds"),
            "signal_key": signal_key,
            "order_preview": {
                "symbol": symbol,
                "side": action,
                "quantity": quantity,
                "price_type": "manual_limit_reference",
                "reference_price": round(price, 4),
                "strategy_name": "DualStockGridStrategy",
                "remark": reason,
            } if action in {"BUY", "SELL"} else None,
        }

    def _empty_signal(self, symbol: str, action: str, reason: str, now: datetime) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "name": GRID_NAMES.get(symbol, symbol),
            "action": action,
            "quantity": 0,
            "current_price": None,
            "anchor_price": None,
            "anchor_source": None,
            "last_grid_price": None,
            "trigger_price": None,
            "grid_pct": None,
            "reason": reason,
            "timestamp": now.isoformat(timespec="seconds"),
            "signal_key": f"{symbol}:{action}:{now.strftime('%Y%m%d%H%M')}",
            "order_preview": None,
        }

    def _round_lot(self, qty: float, lot_size: int) -> int:
        lot = max(1, int(lot_size or 100))
        return int(float(qty or 0.0) // lot * lot)

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return 0.0


grid_trading_service = GridTradingService()
