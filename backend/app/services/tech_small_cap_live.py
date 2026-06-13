"""Live target generation for the TSMF technology small-cap strategy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.backtest.strategies.tech_small_cap_akquant import (
    TECH_SMALL_CAP_FACTOR_CONFIGS,
    TECH_SMALL_CAP_FILTER_FACTORS,
    TECH_SMALL_CAP_VARIANTS,
    get_tech_small_cap_params,
    get_tech_small_cap_variant,
)
from app.core.config import settings
from app.engines.qmt_gateway import qmt_gateway
from app.services.factor_pipeline import FactorPipeline, LinearFactorScorer
from app.services.index_components import load_index_symbols
from app.services.qmt_trading import QmtAccountSnapshot, qmt_trading_service
from app.services.us_market import apply_entry_filter_to_target_weights, us_overnight_entry_filter_state


@dataclass
class LiveAccount:
    cash: float
    total_asset: float
    market_value: float
    positions: dict[str, dict[str, Any]]
    source: str
    error: str | None = None

    @classmethod
    def from_qmt(cls, snapshot: QmtAccountSnapshot) -> "LiveAccount":
        return cls(
            cash=snapshot.cash,
            total_asset=snapshot.total_asset,
            market_value=snapshot.market_value,
            positions={symbol: position.as_dict() for symbol, position in snapshot.positions.items()},
            source=snapshot.source,
            error=snapshot.error,
        )


class TechSmallCapLiveService:
    """Build QMT-ready basket orders from the same factor/parameter contract as backtests."""

    async def status(self) -> dict[str, Any]:
        data = await qmt_trading_service.status()
        data["variants"] = self.variants()
        return data

    def variants(self) -> list[dict[str, Any]]:
        return [
            {
                "key": key,
                "name": str(item["name"]),
                "description": str(item["description"]),
                "params": get_tech_small_cap_params(key),
            }
            for key, item in TECH_SMALL_CAP_VARIANTS.items()
        ]

    async def signals(
        self,
        *,
        params: dict[str, Any] | None = None,
        manual_account: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_params(params or {})
        trade_date = self._parse_date(normalized.get("trade_date")) or date.today()
        account = await self._account_snapshot(manual_account)
        positions = {
            symbol: float(position.get("quantity", 0.0) or 0.0)
            for symbol, position in account.positions.items()
        }

        symbols = await self._resolve_symbols(normalized, trade_date)
        if not symbols:
            return self._empty_response(normalized, trade_date, account, "未解析到股票池")

        pipeline = FactorPipeline()
        result = await asyncio.to_thread(
            pipeline.build_cross_section,
            factor_specs=TECH_SMALL_CAP_FACTOR_CONFIGS,
            trade_date=trade_date,
            symbols=symbols,
            filters=TECH_SMALL_CAP_FILTER_FACTORS,
            min_factor_coverage=float(normalized.get("min_factor_coverage", 0.55) or 0.55),
            scorer=LinearFactorScorer(),
        )
        frame = self._apply_theme_filter(result.frame, normalized)
        if frame.empty:
            return self._empty_response(normalized, trade_date, account, "因子截面为空或主题过滤后无候选")

        target_symbols = self._rank_buffer_targets(frame, positions, normalized)
        quote_symbols = sorted(set(target_symbols) | set(positions))
        price_map, quote_error = await self._quote_prices(quote_symbols)
        price_map.update(self._position_price_fallbacks(account))

        target_weight = self._target_weight(target_symbols, normalized)
        target_weights = {symbol: target_weight for symbol in target_symbols if target_weight > 0}
        entry_state = us_overnight_entry_filter_state(
            trade_date,
            mode=str(normalized.get("us_overnight_entry_filter", "none") or "none"),
            data_path=str(normalized.get("us_overnight_data_path", "") or ""),
            max_lag_days=int(normalized.get("us_overnight_max_lag_days", 5) or 5),
            caution_exposure=float(normalized.get("us_overnight_caution_exposure", 0.85) or 0.85),
            defensive_exposure=float(normalized.get("us_overnight_defensive_exposure", 0.70) or 0.70),
            qqq_caution_ret=float(normalized.get("us_overnight_qqq_caution_ret", -0.01) or -0.01),
            qqq_defensive_ret=float(normalized.get("us_overnight_qqq_defensive_ret", -0.02) or -0.02),
            semi_caution_ret=float(normalized.get("us_overnight_semi_caution_ret", -0.02) or -0.02),
            semi_defensive_ret=float(normalized.get("us_overnight_semi_defensive_ret", -0.03) or -0.03),
            nvda_caution_ret=float(normalized.get("us_overnight_nvda_caution_ret", -0.03) or -0.03),
            nvda_defensive_ret=float(normalized.get("us_overnight_nvda_defensive_ret", -0.04) or -0.04),
        )
        portfolio_value = account.total_asset or account.cash or float(normalized.get("initial_capital", 1_000_000) or 1_000_000)
        filtered_weights, entry_state = apply_entry_filter_to_target_weights(
            target_weights,
            current_positions=positions,
            price_map=price_map,
            portfolio_value=portfolio_value,
            entry_filter_state=entry_state,
        )
        orders = self._build_orders(filtered_weights, positions, price_map, account, normalized, portfolio_value)

        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "strategy": "tech_small_cap_tsmf",
            "variant": normalized.get("strategy_variant"),
            "trade_date": trade_date.isoformat(),
            "account": {
                "cash": account.cash,
                "total_asset": account.total_asset,
                "market_value": account.market_value,
                "source": account.source,
                "error": account.error,
            },
            "universe_size": len(symbols),
            "candidate_count": int(len(frame)),
            "target_symbols": target_symbols,
            "target_weights": filtered_weights,
            "entry_filter": entry_state,
            "quote_error": quote_error,
            "order_submit_enabled": bool(settings.grid_trading_enable_order_submit),
            "orders": orders,
            "top_candidates": self._top_candidates(frame, limit=30),
            "excluded_symbol_count": int(len(result.excluded_symbols)),
        }

    async def submit_orders(self, orders: list[dict[str, Any]], *, confirm: bool = False) -> dict[str, Any]:
        if not settings.grid_trading_enable_order_submit:
            return {
                "enabled": False,
                "submitted": False,
                "message": "GRID_TRADING_ENABLE_ORDER_SUBMIT=false，当前仅生成手动执行信号。",
                "orders": orders,
            }
        if not confirm:
            return {
                "enabled": True,
                "submitted": False,
                "message": "真实委托需要 confirm=true。",
                "orders": orders,
            }
        results = []
        for order in orders:
            if str(order.get("side") or "").upper() not in {"BUY", "SELL"}:
                continue
            results.append(await qmt_trading_service.submit_order({**order, "confirm": True}))
        return {
            "enabled": True,
            "submitted": all(bool(item.get("submitted")) for item in results) if results else False,
            "results": results,
        }

    def _normalize_params(self, raw: dict[str, Any]) -> dict[str, Any]:
        variant = get_tech_small_cap_variant(str(raw.get("strategy_variant") or raw.get("variant") or ""))
        params = {
            **get_tech_small_cap_params(str(variant["key"])),
            **raw,
            "strategy_variant": str(variant["key"]),
        }
        return params

    async def _resolve_symbols(self, params: dict[str, Any], trade_date: date) -> list[str]:
        raw_symbols = params.get("symbols")
        if isinstance(raw_symbols, list) and raw_symbols:
            return [str(symbol) for symbol in raw_symbols if str(symbol).strip()]
        index_symbol = str(params.get("index_symbol") or "399101.SZ")
        return await load_index_symbols(index_symbol, trade_date, trade_date)

    async def _account_snapshot(self, manual: dict[str, Any] | None) -> LiveAccount:
        if manual:
            positions = manual.get("positions") or {}
            return LiveAccount(
                cash=float(manual.get("cash", 0.0) or 0.0),
                total_asset=float(manual.get("total_asset", 0.0) or 0.0),
                market_value=float(manual.get("market_value", 0.0) or 0.0),
                positions={str(key): dict(value or {}) for key, value in dict(positions).items()},
                source="manual",
            )
        try:
            return LiveAccount.from_qmt(await qmt_trading_service.account_snapshot())
        except Exception as exc:
            return LiveAccount(cash=0.0, total_asset=0.0, market_value=0.0, positions={}, source="qmt", error=f"{type(exc).__name__}: {exc}")

    async def _quote_prices(self, symbols: list[str]) -> tuple[dict[str, float], str | None]:
        if not symbols:
            return {}, None
        try:
            quotes = await qmt_gateway.get_realtime_quotes(symbols)
        except Exception as exc:
            return {}, f"{type(exc).__name__}: {exc}"
        price_map: dict[str, float] = {}
        for quote in quotes:
            symbol = str(quote.get("symbol") or quote.get("code") or quote.get("stock_code") or "")
            price = self._quote_price(quote)
            if symbol and price > 0:
                price_map[symbol] = price
        return price_map, None

    def _build_orders(
        self,
        target_weights: dict[str, float],
        positions: dict[str, float],
        price_map: dict[str, float],
        account: LiveAccount,
        params: dict[str, Any],
        portfolio_value: float,
    ) -> list[dict[str, Any]]:
        lot_size = int(params.get("lot_size", 100) or 100)
        cash_buffer = float(params.get("cash_buffer_pct", 0.08) or 0.08)
        tolerance = float(params.get("rebalance_tolerance_pct", 0.01) or 0.01)
        available_cash = max(0.0, account.cash * (1.0 - cash_buffer))
        orders: list[dict[str, Any]] = []

        for symbol, qty in positions.items():
            if qty > 0 and symbol not in target_weights:
                available = float(account.positions.get(symbol, {}).get("available", qty) or qty)
                sell_qty = self._round_lot(min(qty, available), lot_size)
                price = float(price_map.get(symbol, 0.0) or 0.0)
                if sell_qty > 0:
                    orders.append(self._order(symbol, "SELL", sell_qty, price, "TSMF rank exit"))

        for symbol, weight in target_weights.items():
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0:
                continue
            target_qty = self._round_lot((portfolio_value * float(weight or 0.0)) / price, lot_size)
            current_qty = float(positions.get(symbol, 0.0) or 0.0)
            delta = target_qty - current_qty
            if abs(delta) * price < portfolio_value * tolerance:
                continue
            if delta > 0:
                buy_qty = self._round_lot(delta, lot_size)
                buy_value = buy_qty * price
                if buy_qty > 0 and buy_value <= available_cash:
                    orders.append(self._order(symbol, "BUY", buy_qty, price, "TSMF target weight buy/add"))
                    available_cash -= buy_value
            else:
                available = float(account.positions.get(symbol, {}).get("available", current_qty) or current_qty)
                sell_qty = self._round_lot(min(-delta, available), lot_size)
                if sell_qty > 0:
                    orders.append(self._order(symbol, "SELL", sell_qty, price, "TSMF target weight reduce"))
        return orders

    def _order(self, symbol: str, side: str, quantity: int, reference_price: float, remark: str) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "side": side,
            "quantity": int(quantity),
            "price_type": "latest_reference",
            "reference_price": round(float(reference_price or 0.0), 4),
            "strategy_name": "TSMFTechSmallCap",
            "remark": remark,
        }

    def _apply_theme_filter(self, frame, params: dict[str, Any]):
        include_terms = self._as_list(params.get("theme_include_industries")) + self._as_list(params.get("theme_include_keywords"))
        exclude_terms = self._as_list(params.get("theme_exclude_industries")) + self._as_list(params.get("theme_exclude_keywords"))
        if frame.empty or (not include_terms and not exclude_terms):
            return frame
        filtered = frame
        if exclude_terms:
            filtered = filtered.loc[[not self._row_matches_terms(row, exclude_terms) for _, row in filtered.iterrows()]]
        if include_terms:
            themed = filtered.loc[[self._row_matches_terms(row, include_terms) for _, row in filtered.iterrows()]]
            if bool(params.get("strict_theme_filter", False)) or len(themed) >= int(params.get("theme_min_candidates", 0) or 0):
                filtered = themed
        return filtered

    def _rank_buffer_targets(self, frame, positions: dict[str, float], params: dict[str, Any]) -> list[str]:
        top_n = max(1, int(params.get("top_n", 20) or 20))
        buffer_n = max(0, int(params.get("hold_rank_buffer", 0) or 0))
        ranked = list(frame.index.astype(str))
        if buffer_n <= top_n:
            return ranked[:top_n]
        rank_buffer = set(ranked[:buffer_n])
        current = {symbol for symbol, qty in positions.items() if qty > 0}
        keep = [symbol for symbol in ranked if symbol in current and symbol in rank_buffer]
        additions = [symbol for symbol in ranked if symbol not in keep]
        return (keep + additions)[:top_n]

    def _target_weight(self, target_symbols: list[str], params: dict[str, Any]) -> float:
        if not target_symbols:
            return 0.0
        return min(
            float(params.get("max_position_pct", 0.06) or 0.06),
            (1.0 - float(params.get("cash_buffer_pct", 0.08) or 0.08)) / float(len(target_symbols)),
        )

    def _position_price_fallbacks(self, account: LiveAccount) -> dict[str, float]:
        result: dict[str, float] = {}
        for symbol, position in account.positions.items():
            qty = float(position.get("quantity", 0.0) or 0.0)
            market_value = float(position.get("market_value", 0.0) or 0.0)
            avg_cost = float(position.get("avg_cost", 0.0) or 0.0)
            if qty > 0 and market_value > 0:
                result[symbol] = market_value / qty
            elif avg_cost > 0:
                result[symbol] = avg_cost
        return result

    def _top_candidates(self, frame, *, limit: int) -> list[dict[str, Any]]:
        preview = frame.head(limit).copy()
        preview = preview.where(preview.notna(), None)
        return [{"symbol": str(index), **dict(row)} for index, row in preview.iterrows()]

    def _empty_response(self, params: dict[str, Any], trade_date: date, account: LiveAccount, reason: str) -> dict[str, Any]:
        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "strategy": "tech_small_cap_tsmf",
            "variant": params.get("strategy_variant"),
            "trade_date": trade_date.isoformat(),
            "account": {
                "cash": account.cash,
                "total_asset": account.total_asset,
                "market_value": account.market_value,
                "source": account.source,
                "error": account.error,
            },
            "universe_size": 0,
            "candidate_count": 0,
            "target_symbols": [],
            "target_weights": {},
            "entry_filter": {},
            "quote_error": reason,
            "order_submit_enabled": bool(settings.grid_trading_enable_order_submit),
            "orders": [],
            "top_candidates": [],
            "excluded_symbol_count": 0,
        }

    def _row_matches_terms(self, row: Any, terms: list[str]) -> bool:
        text = " ".join(
            self._normalize_text(row.get(column, ""))
            for column in ("industry", "industry2", "industry3", "sector", "concept")
        )
        return bool(text) and any(self._normalize_text(term) in text for term in terms if self._normalize_text(term))

    def _quote_price(self, quote: dict[str, Any]) -> float:
        for key in ("lastPrice", "last_price", "price", "close"):
            try:
                value = float(quote.get(key) or 0.0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        return 0.0

    def _round_lot(self, qty: float, lot_size: int) -> int:
        lot = max(1, int(lot_size or 100))
        return int(float(qty or 0.0) // lot * lot)

    def _as_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        try:
            return [str(item).strip() for item in value if str(item).strip()]
        except Exception:
            return [str(value).strip()] if str(value).strip() else []

    def _normalize_text(self, value: Any) -> str:
        return str(value or "").strip().lower()

    def _parse_date(self, value: Any) -> date | None:
        if isinstance(value, date):
            return value
        if not value:
            return None
        try:
            return date.fromisoformat(str(value)[:10])
        except Exception:
            return None


tech_small_cap_live_service = TechSmallCapLiveService()
