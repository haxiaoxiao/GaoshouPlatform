"""miniQMT trading/account integration helpers.

The service intentionally keeps order submission disabled unless the operator
explicitly enables it in config and sends a confirmation flag with the request.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Any, Sequence

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


@dataclass(frozen=True)
class QmtRuntimeConfig:
    account_id: str
    account_type: str
    trader_path: str
    source: str


class QmtTradingService:
    """Thin async wrapper around xtquant trader and quote APIs."""

    def __init__(self) -> None:
        self._account_snapshot_lock = asyncio.Lock()
        self._account_snapshot_cache: QmtAccountSnapshot | None = None
        self._account_snapshot_cached_at: datetime | None = None
        self._account_snapshot_ttl_seconds = 3.0
        self._account_snapshot_timeout_seconds = 8.0
        self._account_snapshot_retry_seconds = (0.35, 0.75)
        self._order_query_timeout_seconds = 8.0

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

        config = self._runtime_config()
        configured = bool(config.account_id and config.trader_path)
        return {
            "xtdata_available": xtdata_available,
            "xttrader_available": xttrader_available,
            "quote_connected": quote_connected,
            "account_configured": configured,
            "account_id": self._mask_account(config.account_id),
            "account_type": config.account_type,
            "trader_path": config.trader_path,
            "account_config_source": config.source,
            "data_dir": data_dir,
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "error": error,
        }

    async def quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        return await qmt_gateway.get_realtime_quotes(symbols)

    async def account_snapshot(self) -> QmtAccountSnapshot:
        cached = self._cached_account_snapshot(max_age_seconds=self._account_snapshot_ttl_seconds)
        if cached is not None:
            return cached

        config = self._runtime_config()

        def _read() -> QmtAccountSnapshot:
            trader = None
            try:
                from xtquant.xttrader import XtQuantTrader
                from xtquant.xttype import StockAccount
            except Exception as exc:
                raise RuntimeError("xtquant trader module is unavailable") from exc

            self._ensure_runtime_config(config)

            try:
                trader = XtQuantTrader(
                    config.trader_path,
                    int(datetime.now().timestamp() * 1000) % 2_000_000_000,
                )
                trader.start()
                connect_result = trader.connect()
                if connect_result not in (0, None):
                    raise RuntimeError(f"miniQMT trader connect failed: {connect_result}")

                account = StockAccount(config.account_id, config.account_type)
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
            finally:
                self._stop_trader(trader)

        async with self._account_snapshot_lock:
            cached = self._cached_account_snapshot(max_age_seconds=self._account_snapshot_ttl_seconds)
            if cached is not None:
                return cached
            last_exc: Exception | None = None
            for attempt, wait_seconds in enumerate((0.0, *self._account_snapshot_retry_seconds)):
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                try:
                    snapshot = await asyncio.wait_for(
                        asyncio.to_thread(_read),
                        timeout=self._account_snapshot_timeout_seconds,
                    )
                    self._account_snapshot_cache = self._copy_account_snapshot(snapshot)
                    self._account_snapshot_cached_at = datetime.now()
                    return snapshot
                except asyncio.TimeoutError:
                    last_exc = RuntimeError(
                        f"QMT account snapshot timed out after {self._account_snapshot_timeout_seconds:.0f}s"
                    )
                    break
                except Exception as exc:
                    last_exc = exc
                    if "WaitingFreeWriter" not in str(exc) and attempt > 0:
                        break
            fallback = self._cached_account_snapshot(max_age_seconds=15.0)
            if fallback is not None and last_exc is not None and (
                "WaitingFreeWriter" in str(last_exc) or "timed out" in str(last_exc)
            ):
                return fallback
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("QMT account snapshot failed")

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
            trader = None
            config = self._runtime_config()
            self._ensure_runtime_config(config)

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
            try:
                trader = XtQuantTrader(
                    config.trader_path,
                    int(datetime.now().timestamp() * 1000) % 2_000_000_000,
                )
                trader.start()
                connect_result = trader.connect()
                if connect_result not in (0, None):
                    raise RuntimeError(f"miniQMT trader connect failed: {connect_result}")
                account = StockAccount(config.account_id, config.account_type)
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
            finally:
                self._stop_trader(trader)

        try:
            async with self._account_snapshot_lock:
                return await asyncio.to_thread(_submit)
        except Exception as exc:
            return {
                "enabled": True,
                "submitted": False,
                "message": f"{type(exc).__name__}: {exc}",
                "order": payload,
            }

    async def query_order_updates(self, order_ids: Sequence[str | int] | None = None) -> dict[str, Any]:
        """Read miniQMT same-day orders/trades and aggregate fill state by order_id."""

        wanted = {self._normalize_order_id(order_id) for order_id in (order_ids or []) if self._normalize_order_id(order_id)}

        def _query() -> dict[str, Any]:
            from xtquant.xttrader import XtQuantTrader
            from xtquant.xttype import StockAccount

            config = self._runtime_config()
            self._ensure_runtime_config(config)

            trader = None
            try:
                trader = XtQuantTrader(
                    config.trader_path,
                    int(datetime.now().timestamp() * 1000) % 2_000_000_000,
                )
                trader.start()
                connect_result = trader.connect()
                if connect_result not in (0, None):
                    raise RuntimeError(f"miniQMT trader connect failed: {connect_result}")
                account = StockAccount(config.account_id, config.account_type)
                try:
                    trader.subscribe(account)
                except Exception:
                    pass

                orders = [
                    self._qmt_order_dict(order)
                    for order in (trader.query_stock_orders(account) or [])
                ]
                trades = [
                    self._qmt_trade_dict(trade)
                    for trade in (trader.query_stock_trades(account) or [])
                ]
                if wanted:
                    orders = [order for order in orders if str(order.get("order_id") or "") in wanted]
                    trades = [trade for trade in trades if str(trade.get("order_id") or "") in wanted]

                by_order_id: dict[str, dict[str, Any]] = {}
                for order in orders:
                    order_id = str(order.get("order_id") or "")
                    if not order_id:
                        continue
                    by_order_id.setdefault(order_id, {"order_id": order_id, "order": order, "trades": []})
                    by_order_id[order_id]["order"] = order

                for trade in trades:
                    order_id = str(trade.get("order_id") or "")
                    if not order_id:
                        continue
                    entry = by_order_id.setdefault(order_id, {"order_id": order_id, "order": None, "trades": []})
                    entry["trades"].append(trade)

                for entry in by_order_id.values():
                    order = dict(entry.get("order") or {})
                    trade_rows = list(entry.get("trades") or [])
                    traded_volume = sum(float(row.get("traded_volume") or 0.0) for row in trade_rows)
                    traded_amount = sum(float(row.get("traded_amount") or 0.0) for row in trade_rows)
                    if traded_volume <= 0:
                        traded_volume = float(order.get("traded_volume") or 0.0)
                        order_price = float(order.get("traded_price") or 0.0)
                        traded_amount = traded_volume * order_price if traded_volume and order_price else 0.0
                    avg_price = (traded_amount / traded_volume) if traded_volume > 0 and traded_amount > 0 else float(order.get("traded_price") or 0.0)
                    order_volume = float(order.get("order_volume") or 0.0)
                    status = self._qmt_order_status(
                        int(float(order.get("order_status") or 0)) if order else None,
                        order_volume=order_volume,
                        traded_volume=traded_volume,
                    )
                    entry.update(
                        {
                            "status": status,
                            "filled_quantity": traded_volume,
                            "filled_price": avg_price,
                            "filled_value": traded_amount if traded_amount > 0 else traded_volume * avg_price,
                            "remaining_quantity": max(0.0, order_volume - traded_volume) if order_volume > 0 else 0.0,
                            "order_status": order.get("order_status"),
                            "status_msg": order.get("status_msg"),
                            "symbol": order.get("symbol") or (trade_rows[0].get("symbol") if trade_rows else None),
                            "stock_name": order.get("stock_name") or (trade_rows[0].get("stock_name") if trade_rows else None),
                            "side": order.get("side") or (trade_rows[0].get("side") if trade_rows else None),
                            "last_trade_time": max([str(row.get("traded_time") or "") for row in trade_rows] or [""]) or None,
                        }
                    )
                return {"orders": orders, "trades": trades, "by_order_id": by_order_id}
            finally:
                self._stop_trader(trader)

        async with self._account_snapshot_lock:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(_query),
                    timeout=self._order_query_timeout_seconds,
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"QMT order update query timed out after {self._order_query_timeout_seconds:.0f}s"
                ) from exc

    async def cancel_order(self, order_id: str | int) -> dict[str, Any]:
        normalized_order_id = self._normalize_order_id(order_id)
        if not normalized_order_id:
            return {"cancelled": False, "message": "order_id is required", "order_id": order_id}

        def _cancel() -> dict[str, Any]:
            from xtquant.xttrader import XtQuantTrader
            from xtquant.xttype import StockAccount

            config = self._runtime_config()
            self._ensure_runtime_config(config)

            trader = XtQuantTrader(
                config.trader_path,
                int(datetime.now().timestamp() * 1000) % 2_000_000_000,
            )
            trader.start()
            connect_result = trader.connect()
            if connect_result not in (0, None):
                raise RuntimeError(f"miniQMT trader connect failed: {connect_result}")
            account = StockAccount(config.account_id, config.account_type)
            try:
                trader.subscribe(account)
            except Exception:
                pass
            result_code = trader.cancel_order_stock(account, int(normalized_order_id))
            return {
                "cancelled": result_code == 0,
                "cancel_result": result_code,
                "order_id": normalized_order_id,
                "message": "撤单请求已发送" if result_code == 0 else f"撤单失败: {result_code}",
            }

        try:
            async with self._account_snapshot_lock:
                return await asyncio.to_thread(_cancel)
        except Exception as exc:
            return {
                "cancelled": False,
                "order_id": normalized_order_id,
                "message": f"{type(exc).__name__}: {exc}",
            }

    def _runtime_config(self) -> QmtRuntimeConfig:
        account_id = str(settings.qmt_account_id or "").strip()
        account_type = str(settings.qmt_account_type or "STOCK").strip() or "STOCK"
        trader_path = str(settings.qmt_trader_path or "").strip()
        sources: list[str] = []
        if account_id:
            sources.append("env_account")
        if trader_path:
            sources.append("env_trader_path")

        if not trader_path:
            trader_path = self._discover_trader_path()
            if trader_path:
                sources.append("auto_trader_path")

        if not account_id and trader_path:
            accounts = self._discover_account_ids(trader_path)
            if len(accounts) == 1:
                account_id = accounts[0]
                sources.append("auto_account")

        return QmtRuntimeConfig(
            account_id=account_id,
            account_type=account_type,
            trader_path=trader_path,
            source=",".join(sources) if sources else "unconfigured",
        )

    @staticmethod
    def _ensure_runtime_config(config: QmtRuntimeConfig) -> None:
        if not config.account_id:
            raise RuntimeError("QMT_ACCOUNT_ID is not configured and no unique miniQMT account could be auto-detected")
        if not config.trader_path:
            raise RuntimeError("QMT_TRADER_PATH is not configured and miniQMT userdata_mini could not be auto-detected")

    def _discover_trader_path(self) -> str:
        candidates: list[Path] = []
        try:
            xt = qmt_gateway._get_xt()
            data_dir = xt.get_data_dir()
        except Exception:
            data_dir = None
        if data_dir:
            path = Path(str(data_dir))
            candidates.append(path.parent if path.name.lower() == "datadir" else path)
        candidates.extend(
            [
                Path(r"E:\Program Files (x86)\huataiQMT\userdata_mini"),
                Path(r"C:\Program Files (x86)\huataiQMT\userdata_mini"),
                Path(r"C:\Program Files\huataiQMT\userdata_mini"),
            ]
        )
        for candidate in candidates:
            if (candidate / "users").exists():
                return str(candidate)
        return ""

    @staticmethod
    def _discover_account_ids(trader_path: str) -> list[str]:
        users_dir = Path(trader_path) / "users"
        if not users_dir.exists():
            return []
        accounts: set[str] = set()
        for config_file in users_dir.glob("*/authAndConfig.xml"):
            try:
                text = config_file.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for key in re.findall(r'AccountAuth\s+key="([^"]+)"', text):
                parts = [part for part in key.split("____") if part]
                numeric_parts = [part for part in parts if part.isdigit() and len(part) >= 6]
                if numeric_parts:
                    accounts.add(max(numeric_parts, key=len))
        return sorted(accounts)

    def _cached_account_snapshot(self, *, max_age_seconds: float) -> QmtAccountSnapshot | None:
        cached_at = self._account_snapshot_cached_at
        cached = self._account_snapshot_cache
        if cached is None or cached_at is None:
            return None
        if (datetime.now() - cached_at).total_seconds() > max_age_seconds:
            return None
        return self._copy_account_snapshot(cached)

    @staticmethod
    def _copy_account_snapshot(snapshot: QmtAccountSnapshot) -> QmtAccountSnapshot:
        return QmtAccountSnapshot(
            cash=float(snapshot.cash or 0.0),
            total_asset=float(snapshot.total_asset or 0.0),
            market_value=float(snapshot.market_value or 0.0),
            positions={
                symbol: QmtPosition(
                    symbol=position.symbol,
                    quantity=float(position.quantity or 0.0),
                    available=float(position.available or 0.0),
                    avg_cost=float(position.avg_cost or 0.0),
                    market_value=float(position.market_value or 0.0),
                    name=position.name,
                )
                for symbol, position in snapshot.positions.items()
            },
            source=snapshot.source,
            error=snapshot.error,
        )

    @staticmethod
    def _stop_trader(trader: Any) -> None:
        if trader is None:
            return
        for method_name in ("stop", "disconnect"):
            method = getattr(trader, method_name, None)
            if not callable(method):
                continue
            try:
                method()
                return
            except Exception:
                continue

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
    def _normalize_order_id(order_id: str | int | float | None) -> str:
        raw = str(order_id or "").strip()
        if not raw:
            return ""
        try:
            numeric = float(raw)
            if numeric.is_integer():
                return str(int(numeric))
        except Exception:
            pass
        return raw

    def _qmt_order_dict(self, order: Any) -> dict[str, Any]:
        symbol = self._normalize_symbol(
            str(getattr(order, "stock_code", "") or getattr(order, "m_strInstrumentID", "")),
            str(getattr(order, "market", "") or getattr(order, "m_strExchangeID", "")),
        )
        order_type = int(float(getattr(order, "order_type", 0) or 0))
        return {
            "order_id": self._normalize_order_id(getattr(order, "order_id", None)),
            "order_sysid": str(getattr(order, "order_sysid", "") or ""),
            "order_time": self._format_qmt_time(getattr(order, "order_time", None)),
            "symbol": symbol,
            "stock_name": getattr(order, "instrument_name", None) or getattr(order, "stock_name", None),
            "side": self._side_from_order_type(order_type),
            "order_type": order_type,
            "order_volume": float(getattr(order, "order_volume", 0.0) or 0.0),
            "price_type": getattr(order, "price_type", None),
            "price": float(getattr(order, "price", 0.0) or 0.0),
            "traded_volume": float(getattr(order, "traded_volume", 0.0) or 0.0),
            "traded_price": float(getattr(order, "traded_price", 0.0) or 0.0),
            "order_status": getattr(order, "order_status", None),
            "status_msg": str(getattr(order, "status_msg", "") or ""),
            "strategy_name": getattr(order, "strategy_name", None),
            "order_remark": getattr(order, "order_remark", None),
        }

    def _qmt_trade_dict(self, trade: Any) -> dict[str, Any]:
        symbol = self._normalize_symbol(
            str(getattr(trade, "stock_code", "") or getattr(trade, "m_strInstrumentID", "")),
            str(getattr(trade, "market", "") or getattr(trade, "m_strExchangeID", "")),
        )
        order_type = int(float(getattr(trade, "order_type", 0) or 0))
        return {
            "trade_id": str(getattr(trade, "traded_id", "") or ""),
            "traded_time": self._format_qmt_time(getattr(trade, "traded_time", None)),
            "traded_price": float(getattr(trade, "traded_price", 0.0) or 0.0),
            "traded_volume": float(getattr(trade, "traded_volume", 0.0) or 0.0),
            "traded_amount": float(getattr(trade, "traded_amount", 0.0) or 0.0),
            "order_id": self._normalize_order_id(getattr(trade, "order_id", None)),
            "order_sysid": str(getattr(trade, "order_sysid", "") or ""),
            "symbol": symbol,
            "stock_name": getattr(trade, "instrument_name", None) or getattr(trade, "stock_name", None),
            "side": self._side_from_order_type(order_type),
            "order_type": order_type,
            "commission": float(getattr(trade, "commission", 0.0) or 0.0),
            "strategy_name": getattr(trade, "strategy_name", None),
            "order_remark": getattr(trade, "order_remark", None),
        }

    @staticmethod
    def _side_from_order_type(order_type: int) -> str | None:
        if order_type == 23:
            return "BUY"
        if order_type == 24:
            return "SELL"
        return None

    @staticmethod
    def _qmt_order_status(order_status: int | None, *, order_volume: float, traded_volume: float) -> str:
        if order_status == 56 or (order_volume > 0 and traded_volume >= order_volume):
            return "live_filled"
        if order_status in {55, 52} or traded_volume > 0:
            return "partially_filled"
        if order_status == 53:
            return "partially_cancelled" if traded_volume > 0 else "cancelled"
        if order_status == 54:
            return "cancelled"
        if order_status == 57:
            return "failed"
        if order_status == 51:
            return "cancel_requested"
        if order_status in {48, 49, 50, 255, None}:
            return "live_pending"
        return "live_pending"

    @staticmethod
    def _format_qmt_time(value: Any) -> str | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw or raw in {"0", "0.0"}:
            return None
        try:
            number = int(float(raw))
        except Exception:
            return raw
        text = str(number)
        try:
            if len(text) == 14:
                return datetime.strptime(text, "%Y%m%d%H%M%S").isoformat(timespec="seconds")
            if len(text) == 13:
                return datetime.fromtimestamp(number / 1000).isoformat(timespec="seconds")
            if len(text) == 10:
                return datetime.fromtimestamp(number).isoformat(timespec="seconds")
            if len(text) == 6:
                today = datetime.now().strftime("%Y%m%d")
                return datetime.strptime(f"{today}{text}", "%Y%m%d%H%M%S").isoformat(timespec="seconds")
        except Exception:
            return raw
        return raw

    @staticmethod
    def _mask_account(account_id: str) -> str:
        if not account_id:
            return ""
        if len(account_id) <= 4:
            return "*" * len(account_id)
        return f"{account_id[:2]}***{account_id[-2:]}"


qmt_trading_service = QmtTradingService()
