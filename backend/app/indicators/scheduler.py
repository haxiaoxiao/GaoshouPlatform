from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.db.clickhouse import get_ch_client
from app.indicators.base import IndicatorContext, IndicatorRegistry


class IndicatorScheduler:
    """指标计算调度器"""

    def run_after_sync(
        self,
        sync_type: str,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> None:
        """数据同步后自动触发相关指标计算"""
        if sync_type in ("stock_info", "stock_full", "realtime_mv"):
            self._compute_by_data_type("截面", symbols, trade_date)
        elif sync_type in ("kline_daily", "kline_minute"):
            self._compute_by_data_type("时序", symbols, trade_date)

    def compute_indicators(
        self,
        indicator_names: list[str] | None = None,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
        full_compute: bool = False,
    ) -> dict[str, int]:
        """手动触发指标计算，返回 {indicator_name: computed_count}"""
        if indicator_names:
            indicators = [
                IndicatorRegistry.get(n) for n in indicator_names
            ]
            indicators = [i for i in indicators if i is not None]
        else:
            indicators = IndicatorRegistry.all()

        if not indicators:
            return {}

        results: dict[str, int] = {}
        ordered = self._topo_sort(indicators)

        for indicator_cls in ordered:
            indicator = indicator_cls()
            context = self._build_context(indicator, symbols, trade_date)
            computed = indicator.compute_batch(
                symbols or self._get_all_symbols(), context
            )
            self._save_results(indicator, computed, trade_date)
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
        ordered = self._topo_sort(indicators)
        target_symbols = symbols or self._get_all_symbols()
        target_date = trade_date or date.today()

        for indicator_cls in ordered:
            try:
                indicator = indicator_cls()
                context = self._build_context(indicator, target_symbols, target_date)
                computed = indicator.compute_batch(target_symbols, context)
                self._save_results(indicator, computed, target_date)
            except Exception as e:
                print(f"Indicator {indicator_cls.name} compute failed: {e}")

    def _topo_sort(
        self, indicators: list[type]
    ) -> list[type]:
        """简单拓扑排序：无依赖的先算，有依赖的后算"""
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

    def _build_context(
        self,
        indicator_cls: type,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> IndicatorContext:
        """构建计算上下文"""
        return IndicatorContext(trade_date=trade_date or date.today())

    def _get_all_symbols(self) -> list[str]:
        """从SQLite获取所有股票代码"""
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings

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
        """保存计算结果到ClickHouse"""
        ch = get_ch_client()
        table = "stock_indicators" if indicator.data_type == "截面" else "indicator_timeseries"
        target_date = trade_date or date.today()

        rows = [
            {
                "symbol": symbol,
                "indicator_name": indicator.name,
                "trade_date": target_date,
                "value": value,
                "updated_at": datetime.now(),
            }
            for symbol, value in results.items()
            if value is not None
        ]

        if rows:
            ch.insert(table, rows)


indicator_scheduler = IndicatorScheduler()
