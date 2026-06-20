from __future__ import annotations

from datetime import date

from app.indicators.base import IndicatorRegistry
from app.indicators.scheduler import IndicatorScheduler


class _FakeCrossSectionPrecomputed:
    name = "fake_precomputed"
    display_name = "fake_precomputed"
    category = "quality"
    tags = []
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = ""
    unit = ""

    def compute(self, context):
        return 1.0


class _FakeCrossSectionDeferred:
    name = "fake_deferred"
    display_name = "fake_deferred"
    category = "technical"
    tags = []
    data_type = "截面"
    is_precomputed = False
    dependencies = []
    description = ""
    unit = ""

    def compute(self, context):
        return 2.0


def test_compute_indicators_skips_non_precomputed_by_default(monkeypatch):
    registry_backup = dict(IndicatorRegistry._registry)
    try:
        IndicatorRegistry._registry = {
            _FakeCrossSectionPrecomputed.name: _FakeCrossSectionPrecomputed,
            _FakeCrossSectionDeferred.name: _FakeCrossSectionDeferred,
        }
        scheduler = IndicatorScheduler()

        monkeypatch.setattr(scheduler, "_get_all_symbols", lambda: ["000001.SZ"])
        monkeypatch.setattr(scheduler, "_load_stock_info_map", lambda symbols: {"000001.SZ": {"symbol": "000001.SZ"}})
        monkeypatch.setattr(scheduler, "_save_results", lambda *args, **kwargs: None)

        results = scheduler.compute_indicators(symbols=["000001.SZ"], trade_date=date(2026, 6, 19))

        assert results == {"fake_precomputed": 1}
    finally:
        IndicatorRegistry._registry = registry_backup
