from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.indicators.base import IndicatorContext, IndicatorRegistry


class IndicatorScheduler:
    """指标计算调度器"""

    def run_after_sync(
        self,
        sync_type: str,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> None:
        try:
            if sync_type in ("stock_info", "stock_full", "realtime_mv"):
                self._compute_by_data_type("截面", symbols, trade_date)
            elif sync_type in ("kline_daily", "kline_minute"):
                self._compute_by_data_type("时序", symbols, trade_date)
        except Exception:
            # ClickHouse may not be running; indicators are optional post-sync
            pass

    def compute_indicators(
        self,
        indicator_names: list[str] | None = None,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
        full_compute: bool = False,
    ) -> dict[str, int]:
        if indicator_names:
            indicators = [IndicatorRegistry.get(n) for n in indicator_names]
            indicators = [i for i in indicators if i is not None]
        else:
            indicators = IndicatorRegistry.all()

        if not indicators:
            return {}

        target_symbols = symbols or self._get_all_symbols()
        target_date = trade_date or date.today()

        cross_section = [i for i in indicators if i.data_type == "截面"]
        time_series = [i for i in indicators if i.data_type == "时序"]

        results: dict[str, int] = {}

        if cross_section:
            stock_info_map = self._load_stock_info_map(target_symbols)
            ordered = self._topo_sort(cross_section)
            for indicator_cls in ordered:
                indicator = indicator_cls()
                computed = self._compute_cross_section(
                    indicator, target_symbols, target_date, stock_info_map
                )
                self._save_results(indicator, computed, target_date)
                results[indicator.name] = len([v for v in computed.values() if v is not None])

        if time_series:
            kline_map = self._load_kline_map(target_symbols, limit=120)
            ordered = self._topo_sort(time_series)
            for indicator_cls in ordered:
                indicator = indicator_cls()
                computed = self._compute_time_series(
                    indicator, target_symbols, target_date, kline_map
                )
                self._save_results(indicator, computed, target_date)
                results[indicator.name] = len([v for v in computed.values() if v is not None])

        return results

    def _compute_by_data_type(
        self,
        data_type: str,
        symbols: list[str] | None,
        trade_date: date | None,
    ) -> None:
        indicators = IndicatorRegistry.by_data_type(data_type)
        if not indicators:
            return
        target_symbols = symbols or self._get_all_symbols()
        target_date = trade_date or date.today()
        ordered = self._topo_sort(indicators)

        if data_type == "截面":
            stock_info_map = self._load_stock_info_map(target_symbols)
            for indicator_cls in ordered:
                try:
                    indicator = indicator_cls()
                    computed = self._compute_cross_section(
                        indicator, target_symbols, target_date, stock_info_map
                    )
                    self._save_results(indicator, computed, target_date)
                except Exception as e:
                    print(f"Indicator {indicator_cls.name} compute failed: {e}")
        else:
            kline_map = self._load_kline_map(target_symbols, limit=120)
            for indicator_cls in ordered:
                try:
                    indicator = indicator_cls()
                    computed = self._compute_time_series(
                        indicator, target_symbols, target_date, kline_map
                    )
                    self._save_results(indicator, computed, target_date)
                except Exception as e:
                    print(f"Indicator {indicator_cls.name} compute failed: {e}")

    def _compute_cross_section(
        self,
        indicator: Any,
        symbols: list[str],
        trade_date: date,
        stock_info_map: dict[str, dict[str, Any]],
    ) -> dict[str, float | None]:
        results: dict[str, float | None] = {}
        for symbol in symbols:
            stock_info = stock_info_map.get(symbol)
            ctx = IndicatorContext(
                symbol=symbol,
                trade_date=trade_date,
                stock_info=stock_info,
            )
            try:
                results[symbol] = indicator.compute(ctx)
            except Exception:
                results[symbol] = None
        return results

    def _compute_time_series(
        self,
        indicator: Any,
        symbols: list[str],
        trade_date: date,
        kline_map: dict[str, list[dict[str, Any]]],
    ) -> dict[str, float | None]:
        results: dict[str, float | None] = {}
        for symbol in symbols:
            ctx = IndicatorContext(
                symbol=symbol,
                trade_date=trade_date,
                kline_data=kline_map.get(symbol, []),
            )
            try:
                results[symbol] = indicator.compute(ctx)
            except Exception:
                results[symbol] = None
        return results

    def _topo_sort(self, indicators: list[type]) -> list[type]:
        name_set = {i.name for i in indicators}
        remaining = list(indicators)
        ordered: list[type] = []

        while remaining:
            ready = [
                i for i in remaining
                if not any(d in name_set and d != i.name for d in i.dependencies)
                or all(d not in name_set for d in i.dependencies)
            ]
            if not ready:
                ordered.extend(remaining)
                break
            ordered.extend(ready)
            remaining = [i for i in remaining if i not in ready]

        return ordered

    def _load_stock_info_map(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        from sqlalchemy import create_engine, func, select

        from app.core.config import settings
        from app.db.models.financial import FinancialData
        from app.db.models.stock import Stock

        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        symbol_set = set(symbols)
        info_map: dict[str, dict[str, Any]] = {}

        with engine.connect() as conn:
            stock_rows = conn.execute(select(Stock).where(Stock.symbol.in_(symbol_set)))
            stock_map: dict[str, dict] = {}
            for row in stock_rows.mappings().all():
                d = dict(row)
                stock_map[d["symbol"]] = d

            latest_fin_subq = (
                select(
                    FinancialData.symbol,
                    func.max(FinancialData.report_date).label("max_date"),
                )
                .where(FinancialData.symbol.in_(symbol_set))
                .group_by(FinancialData.symbol)
                .subquery()
            )

            fin_rows = conn.execute(
                select(FinancialData).join(
                    latest_fin_subq,
                    (FinancialData.symbol == latest_fin_subq.c.symbol)
                    & (FinancialData.report_date == latest_fin_subq.c.max_date),
                )
            )
            fin_map: dict[str, dict] = {}
            for row in fin_rows.mappings().all():
                d = dict(row)
                fin_map[d["symbol"]] = d

        engine.dispose()

        for symbol in symbols:
            base = stock_map.get(symbol, {"symbol": symbol})
            fin = fin_map.get(symbol, {})
            merged: dict[str, Any] = {**base, **{k: v for k, v in fin.items() if v is not None}}
            info_map[symbol] = merged

        return info_map

    def _load_kline_map(self, symbols: list[str], limit: int = 120) -> dict[str, list[dict[str, Any]]]:
        from datetime import date as dt_date
        from datetime import timedelta

        from app.data_stores import get_market_data_store
        store = get_market_data_store()
        end = dt_date.today()
        start = end - timedelta(days=limit * 2)
        try:
            df = store.load_daily(symbols, start, end, columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"])
        except Exception:
            return {}

        if df.empty:
            return {}

        kline_map: dict[str, list[dict[str, Any]]] = {}
        for symbol, group in df.sort_values("trade_date", ascending=False).groupby("symbol"):
            kline_map[symbol] = group.head(limit).to_dict("records")
        return kline_map

    def _get_all_symbols(self) -> list[str]:
        from sqlalchemy import create_engine, select

        from app.core.config import settings
        from app.db.models.stock import Stock

        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        with engine.connect() as conn:
            result = conn.execute(select(Stock.symbol))
            symbols = [row[0] for row in result.all()]
        engine.dispose()
        return symbols

    def _save_results(
        self,
        indicator: Any,
        results: dict[str, float | None],
        trade_date: date | None = None,
    ) -> None:
        target_date = trade_date or date.today()

        rows = [
            (
                symbol,
                indicator.name,
                target_date,
                value,
                datetime.now(),
            )
            for symbol, value in results.items()
            if value is not None
        ]

        if rows:
            dataset = "stock_indicators" if indicator.data_type == "截面" else "indicator_timeseries"
            self._write_indicator_parquet(
                rows, indicator.name, target_date, dataset=dataset
            )


    @staticmethod
    def _write_indicator_parquet(
        rows: list[tuple], indicator_name: str, target_date: date, *, dataset: str
    ) -> None:
        try:
            import pandas as pd

            from app.data_stores import get_indicator_store

            df = pd.DataFrame(
                rows,
                columns=["symbol", "indicator_name",
                         "trade_date" if dataset == "stock_indicators" else "datetime",
                         "value", "updated_at"],
            )
            store = get_indicator_store()
            if dataset == "stock_indicators":
                store.write_cross_section(df)
            else:
                store.write_timeseries(df)
        except Exception:
            pass


indicator_scheduler = IndicatorScheduler()
