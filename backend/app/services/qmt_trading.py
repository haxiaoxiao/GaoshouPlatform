"""miniQMT trading/account integration helpers.

The service intentionally keeps order submission disabled unless the operator
explicitly enables it in config and sends a confirmation flag with the request.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.core.config import settings
from app.engines.qmt_gateway import qmt_gateway
from app.services.security_symbols import normalize_security_symbol


@dataclass
class QmtPosition:
    symbol: str
    quantity: float
    available: float
    avg_cost: float
    market_value: float = 0.0
    name: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "quantity": self.quantity,
            "available": self.available,
            "avg_cost": self.avg_cost,
            "market_value": self.market_value,
        }


@dataclass
class QmtAccountSnapshot:
    cash: float
    total_asset: float
    market_value: float
    positions: dict[str, QmtPosition]
    source: str = "qmt"
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "cash": self.cash,
            "total_asset": self.total_asset,
            "market_value": self.market_value,
            "positions": {k: v.as_dict() for k, v in self.positions.items()},
            "source": self.source,
            "error": self.error,
        }


class QmtTradingService:
    """Thin async wrapper around xtquant trader and quote APIs."""

    async def status(self) -> dict[str, Any]:
        data_dir = None
        xtdata_available = False
        xttrader_available = False
        quote_connected = False
        error = None

        try:
            xt = qmt_gateway._get_xt()
            xtdata_available = True
            try:
                data_dir = xt.get_data_dir()
            except Exception:
                data_dir = None
            quote_connected = await qmt_gateway.check_connection()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        try:
            __import__("xtquant.xttrader")
            __import__("xtquant.xttype")
            xttrader_available = True
        except Exception:
            xttrader_available = False

        configured = bool(settings.qmt_account_id and settings.qmt_trader_path)
        return {
            "xtdata_available": xtdata_available,
            "xttrader_available": xttrader_available,
            "quote_connected": quote_connected,
            "account_configured": configured,
            "account_id": self._mask_account(settings.qmt_account_id),
            "account_type": settings.qmt_account_type,
            "trader_path": settings.qmt_trader_path,
            "data_dir": data_dir,
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "error": error,
        }

    async def quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        return await qmt_gateway.get_realtime_quotes(symbols)

    async def account_snapshot(self) -> QmtAccountSnapshot:
        def _read() -> QmtAccountSnapshot:
            try:
                from xtquant.xttrader import XtQuantTrader
                from xtquant.xttype import StockAccount
            except Exception as exc:
                raise RuntimeError("xtquant trader module is unavailable") from exc

            if not settings.qmt_account_id:
                raise RuntimeError("QMT_ACCOUNT_ID is not configured")
            if not settings.qmt_trader_path:
                raise RuntimeError("QMT_TRADER_PATH is not configured")

            trader = XtQuantTrader(
                settings.qmt_trader_path,
                int(datetime.now().timestamp() * 1000) % 2_000_000_000,
            )
            trader.start()
            connect_result = trader.connect()
            if connect_result not in (0, None):
                raise RuntimeError(f"miniQMT trader connect failed: {connect_result}")

            account = StockAccount(settings.qmt_account_id, settings.qmt_account_type)
            try:
                trader.subscribe(account)
            except Exception:
                pass

            asset = trader.query_stock_asset(account)
            raw_positions = trader.query_stock_positions(account) or []
            cash = self._extract_float(asset, ("cash", "m_dCash", "available_cash"))
            total_asset = self._extract_float(asset, ("total_asset", "m_dBalance", "asset"))
            market_value = self._extract_float(asset, ("market_value", "m_dMarketValue"))

            positions: dict[str, QmtPosition] = {}
            for pos in raw_positions:
                symbol = self._normalize_symbol(
                    str(
                        getattr(pos, "stock_code", "")
                        or getattr(pos, "m_strInstrumentID", "")
                        or getattr(pos, "instrument_id", "")
                    ),
                    str(getattr(pos, "market", "") or getattr(pos, "m_strExchangeID", "")),
                )
                if not symbol:
                    continue
                positions[symbol] = QmtPosition(
                    symbol=symbol,
                    name=getattr(pos, "stock_name", None) or getattr(pos, "m_strInstrumentName", None),
                    quantity=self._extract_float(pos, ("volume", "m_nVolume", "total_volume")),
                    available=self._extract_float(pos, ("can_use_volume", "m_nCanUseVolume", "available_volume")),
                    avg_cost=self._extract_float(pos, ("open_price", "m_dOpenPrice", "avg_price", "cost_price")),
                    market_value=self._extract_float(pos, ("market_value", "m_dMarketValue")),
                )

            return QmtAccountSnapshot(
                cash=cash,
                total_asset=total_asset,
                market_value=market_value,
                positions=positions,
            )

        return await asyncio.to_thread(_read)

    async def submit_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not settings.live_trading_enable_order_submit:
            return {
                "enabled": False,
                "submitted": False,
                "message": "LIVE_TRADING_ENABLE_ORDER_SUBMIT=false，当前仅生成手动执行信号。",
                "order": payload,
            }
        if not payload.get("confirm"):
            return {
                "enabled": True,
                "submitted": False,
                "message": "真实委托需要 confirm=true。",
                "order": payload,
            }

        def _submit() -> dict[str, Any]:
            from xtquant import xtconstant
            from xtquant.xttrader import XtQuantTrader
            from xtquant.xttype import StockAccount

            side = str(payload.get("side") or payload.get("action") or "").upper()
            symbol = str(payload.get("symbol") or "")
            quantity = int(float(payload.get("quantity") or 0))
            price = float(payload.get("price") or payload.get("reference_price") or 0.0)
            if side not in {"BUY", "SELL"}:
                raise ValueError("side must be BUY or SELL")
            if not symbol or quantity <= 0:
                raise ValueError("symbol and positive quantity are required")

            order_type = xtconstant.STOCK_BUY if side == "BUY" else xtconstant.STOCK_SELL
            price_type = getattr(xtconstant, "FIX_PRICE", 11)
            trader = XtQuantTrader(
                settings.qmt_trader_path,
                int(datetime.now().timestamp() * 1000) % 2_000_000_000,
            )
            trader.start()
            connect_result = trader.connect()
            if connect_result not in (0, None):
                raise RuntimeError(f"miniQMT trader connect failed: {connect_result}")
            account = StockAccount(settings.qmt_account_id, settings.qmt_account_type)
            try:
                trader.subscribe(account)
            except Exception:
                pass
            order_id = trader.order_stock(
                account,
                symbol,
                order_type,
                quantity,
                price_type,
                price,
                str(payload.get("strategy_name") or "GaoshouPlatform"),
                str(payload.get("remark") or "manual live-trading signal"),
            )
            return {
                "enabled": True,
                "submitted": True,
                "order_id": order_id,
                "order": payload,
            }

        try:
            return await asyncio.to_thread(_submit)
        except Exception as exc:
            return {
                "enabled": True,
                "submitted": False,
                "message": f"{type(exc).__name__}: {exc}",
                "order": payload,
            }

    @staticmethod
    def _extract_float(obj: Any, keys: tuple[str, ...]) -> float:
        for key in keys:
            value = getattr(obj, key, None)
            if value is not None:
                try:
                    return float(value)
                except Exception:
                    continue
        return 0.0

    @staticmethod
    def _normalize_symbol(symbol: str, market: str = "") -> str:
        return normalize_security_symbol(symbol, market) or ""

    @staticmethod
    def _mask_account(account_id: str) -> str:
        if not account_id:
            return ""
        if len(account_id) <= 4:
            return "*" * len(account_id)
        return f"{account_id[:2]}***{account_id[-2:]}"


qmt_trading_service = QmtTradingService()
