"""Lightweight AKQuant integration tests."""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from app.backtest.api import (
    RunBacktestRequest,
    _prepare_backtest_config,
    _resolve_strategy_code,
    router,
)
from app.backtest.config import BacktestConfig
from app.backtest.engine.akquant.engine import AkquantEngine
from app.backtest.engine.akquant.capabilities import get_akquant_capabilities
from app.backtest.engine.data_provider import ClickHouseDataProvider, StoreDataProvider
from app.services.akquant_optimize import _load_optimization_data
from app.services.optimization_result_store import save_optimization_result


def test_akquant_capabilities_shape():
    caps = get_akquant_capabilities()

    assert "available" in caps
    assert "features" in caps
    assert isinstance(caps["features"], dict)
    for name in ["backtest", "grid_search", "walk_forward", "talib_compat"]:
        assert name in caps["features"]


def test_akquant_routes_registered():
    paths = {getattr(route, "path", "") for route in router.routes}

    assert "/capabilities" in paths
    assert "/optimize/grid" in paths
    assert "/optimize/walk-forward" in paths
    assert "/strategy-params/schema" in paths
    assert "/strategy-params/validate" in paths
    assert "/presets/dual-stock-grid" in paths


@pytest.mark.asyncio
async def test_prepare_backtest_config_resolves_index_and_strategy(monkeypatch):
    async def fake_load_index_symbols(index_symbol, start_date, end_date):
        assert index_symbol == "399101.SZ"
        assert start_date == date(2025, 1, 1)
        assert end_date == date(2025, 1, 31)
        return ["000001.SZ", "000002.SZ"]

    class FakeScalarResult:
        def first(self):
            return type("StrategyRow", (), {"code": "class S: pass"})()

    class FakeExecuteResult:
        def scalars(self):
            return FakeScalarResult()

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt):
            return FakeExecuteResult()

    monkeypatch.setattr("app.backtest.api.load_index_symbols", fake_load_index_symbols)
    monkeypatch.setattr("app.backtest.api.async_session_factory", lambda: FakeSession())

    req = RunBacktestRequest(
        engine="akquant",
        mode="event_driven",
        symbols=[],
        index_symbol="399101.SZ",
        start_date="2025-01-01",
        end_date="2025-01-31",
        strategy_id=43,
        timer_times=["10:30"],
        benchmark_symbol="zz500",
        warm_start={"mode": "always", "chunk_days": 10, "keep_checkpoints": True},
    )

    config, error = await _prepare_backtest_config(
        req,
        date(2025, 1, 1),
        date(2025, 1, 31),
        task_id="task-x",
    )

    assert error is None
    assert config is not None
    assert config.symbols == ["000001.SZ", "000002.SZ"]
    assert config.index_symbol == "399101.SZ"
    assert config.universe_mode == "index"
    assert config.strategy_code == "class S: pass"
    assert config.timer_times == ["10:30"]
    assert config.benchmark_symbol == "000905.SH"
    assert config.warm_start == {"mode": "always", "chunk_days": 10, "keep_checkpoints": True}
    assert config._task_id == "task-x"


def test_akquant_warm_start_options_control_chunking():
    engine = AkquantEngine()
    start = date(2025, 1, 1)
    end = date(2025, 1, 20)
    config = BacktestConfig(
        symbols=["000001.SZ"],
        start_date=start,
        end_date=end,
        bar_type="minute",
        warm_start={"mode": "off", "chunk_days": 7},
    )

    assert engine._should_run_chunked(config, start, end) is False

    config.warm_start = {"mode": "always", "chunk_days": 7}
    assert engine._should_run_chunked(config, start, end) is True
    assert engine._chunk_dates(start, end, config)[0] == (start, date(2025, 1, 7))

    config.bar_type = "daily"
    assert engine._should_run_chunked(config, start, end) is False


@pytest.mark.asyncio
async def test_resolve_strategy_code_prefers_inline_code(monkeypatch):
    code, error = await _resolve_strategy_code("class Inline: pass", 43)

    assert error is None
    assert code == "class Inline: pass"


@pytest.mark.asyncio
async def test_load_minute_timer_filters_requested_times(monkeypatch):
    captured: dict = {}

    def fake_execute(self, query, params=None):
        captured["query"] = query
        captured["params"] = params
        return [
            (
                "000001.SZ",
                datetime(2025, 1, 2, 10, 30),
                10.0,
                10.2,
                9.9,
                10.1,
                1000,
                10100.0,
            )
        ]

    monkeypatch.setattr(ClickHouseDataProvider, "_execute", fake_execute)

    provider = ClickHouseDataProvider()
    df = await provider.load_minute(
        ["000001.SZ"],
        date(2025, 1, 1),
        date(2025, 1, 31),
        timer_times=("10:30:00", "14:50:00"),
    )

    assert not df.empty
    assert "toHour(datetime)" in captured["query"]
    assert captured["params"]["timer_minutes"] == (630, 890)


def test_optimization_data_uses_timer_minute_loader(monkeypatch):
    captured: dict = {}

    async def fake_load_minute(self, symbols, start_date, end_date, timer_times=None):
        captured["symbols"] = symbols
        captured["timer_times"] = timer_times
        return pd.DataFrame(
            [
                {
                    "symbol": "000001.SZ",
                    "datetime": pd.Timestamp("2025-01-02 10:30:00"),
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 1000,
                    "amount": 10100.0,
                }
            ]
        ).set_index("datetime")

    monkeypatch.setattr(StoreDataProvider, "load_minute", fake_load_minute)

    from app.backtest.config import BacktestConfig

    config = BacktestConfig(
        symbols=["000001.SZ"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        bar_type="minute_timer",
        timer_times=["10:30", "14:50"],
    )

    data = _load_optimization_data(config, config.start_date, config.end_date)

    assert list(data) == ["000001.SZ"]
    assert captured["symbols"] == ["000001.SZ"]
    assert captured["timer_times"] == ("10:30:00", "14:50:00")


def test_strategy_param_helpers_delegate_to_akquant(monkeypatch):
    import app.services.akquant_params as params_service

    class FakeAQ:
        @staticmethod
        def get_strategy_param_schema(strategy_cls):
            return {"title": strategy_cls.__name__, "properties": {"n": {"type": "integer"}}}

        @staticmethod
        def validate_strategy_params(strategy_cls, payload):
            return {"n": int(payload["n"])}

    class FakeStrategy:
        __name__ = "FakeStrategy"

    monkeypatch.setattr(params_service, "AKQUANT_AVAILABLE", True)
    monkeypatch.setattr(params_service, "_load_strategy_class", lambda code: FakeStrategy)
    monkeypatch.setitem(__import__("sys").modules, "akquant", FakeAQ)

    schema = params_service.get_strategy_param_schema("class FakeStrategy: pass")
    validated = params_service.validate_strategy_params(
        "class FakeStrategy: pass",
        {"n": "3"},
    )

    assert schema["properties"]["n"]["type"] == "integer"
    assert validated == {"n": 3}


@pytest.mark.asyncio
async def test_save_optimization_result_persists_as_backtest(monkeypatch):
    from app.backtest.config import BacktestConfig

    saved: dict = {}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt):
            class Result:
                def scalars(self):
                    class Scalars:
                        def first(self):
                            return None

                    return Scalars()

            return Result()

        def add(self, obj):
            cls_name = obj.__class__.__name__
            if cls_name == "Strategy":
                obj.id = 101
                saved["strategy"] = obj
            elif cls_name == "Backtest":
                obj.id = 202
                saved["backtest"] = obj

        async def flush(self):
            return None

        async def commit(self):
            saved["committed"] = True

    monkeypatch.setattr("app.services.optimization_result_store.async_session_factory", lambda: FakeSession())

    config = BacktestConfig(
        engine="akquant",
        mode="event_driven",
        symbols=["000001.SZ"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        initial_capital=123456.0,
        bar_type="daily",
        strategy_code="class MyStrategy: pass",
    )
    record_id = await save_optimization_result(
        task_id="task-a",
        optimization_type="grid_search",
        config=config,
        request_params={"param_grid": {"n": [1, 2]}},
        result={"rows": [{"n": 1, "sharpe_ratio": 1.2}], "count": 1},
        success=True,
    )

    backtest = saved["backtest"]
    assert record_id == 202
    assert saved["committed"] is True
    assert backtest.status == "completed"
    assert backtest.initial_capital == Decimal("123456.0")
    assert backtest.parameters["record_type"] == "optimization"
    assert backtest.parameters["optimization_type"] == "grid_search"
    assert backtest.result["count"] == 1
