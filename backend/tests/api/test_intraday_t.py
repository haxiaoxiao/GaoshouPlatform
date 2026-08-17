from datetime import date

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import intraday_t as intraday_t_api
from app.api.intraday_t import (
    get_intraday_t_market_store,
    get_intraday_t_paper_service,
    router,
)


class _Store:
    def load_minute(self, symbols, start, end):
        index = pd.date_range("2026-07-14 09:31", periods=40, freq="min")
        rows = []
        for symbol in symbols:
            for offset, timestamp in enumerate(index):
                price = 20 + offset * 0.01
                rows.append(
                    {
                        "datetime": timestamp,
                        "symbol": symbol,
                        "open": price,
                        "high": price + 0.02,
                        "low": price - 0.02,
                        "close": price,
                        "volume": 10_000,
                        "amount": price * 10_000,
                    }
                )
        return pd.DataFrame(rows).set_index("datetime")


class _PaperService:
    def __init__(self):
        self.calls = []

    async def start(self, **kwargs):
        self.calls.append(("start", kwargs))
        return {"session_id": "it-test", "status": "RUNNING", "real_order_submit_enabled": False}

    async def status(self, session_id=None):
        self.calls.append(("status", session_id))
        return {"session_id": session_id or "it-test", "status": "RUNNING"}

    async def evaluate(self, session_id, **kwargs):
        self.calls.append(("evaluate", session_id))
        return {"session_id": session_id, "duplicate": False, "signals": [], "fills": []}

    async def stop(self, session_id, **kwargs):
        self.calls.append(("stop", session_id))
        return {"session_id": session_id, "status": "STOPPED"}

    async def start_runner(self, session_id, **kwargs):
        self.calls.append(("start_runner", session_id))
        return {"session_id": session_id, "status": "RUNNING", "runner_active": True}

    async def stop_runner(self, session_id):
        self.calls.append(("stop_runner", session_id))
        return {"session_id": session_id, "status": "RUNNING", "runner_active": False}

    async def reset(self, session_id):
        self.calls.append(("reset", session_id))
        return {"session_id": session_id, "status": "STOPPED", "states": {}}

    async def trades(self, session_id):
        self.calls.append(("trades", session_id))
        return [{"trade_id": "trade-1", "session_id": session_id, "simulated": True}]


class _LimitPriceLoader:
    def __init__(self):
        self.calls = []
        self.limit_prices = {
            "603629.SH|2026-07-14": {"up": 22.0, "down": 18.0},
        }

    async def load(self, symbols, start_date, end_date):
        self.calls.append((symbols, start_date, end_date))
        return self.limit_prices


def _client():
    app = FastAPI()
    app.include_router(router, prefix="/api/intraday-t")
    paper = _PaperService()
    limit_price_loader = _LimitPriceLoader()
    app.dependency_overrides[get_intraday_t_market_store] = lambda: _Store()
    app.dependency_overrides[get_intraday_t_paper_service] = lambda: paper
    loader_dependency = getattr(intraday_t_api, "get_intraday_t_limit_price_loader", None)
    if loader_dependency is not None:
        app.dependency_overrides[loader_dependency] = lambda: limit_price_loader
    app.state.intraday_t_limit_price_loader = limit_price_loader
    return TestClient(app), app, paper


def test_capabilities_are_fixed_to_two_symbols_and_simulated_modes():
    client, app, _ = _client()

    response = client.get("/api/intraday-t/capabilities")

    assert response.status_code == 200
    data = response.json()["data"]
    assert [item["symbol"] for item in data["symbols"]] == ["603629.SH", "688008.SH"]
    assert data["modes"] == ["backtest", "paper"]
    assert data["real_order_submit_enabled"] is False
    assert (
        data["defaults"]["strategy"]
        | {
            "max_entry_z": 2.4,
            "realized_vol_window": 10,
            "min_realized_vol_bps": 0.0,
            "max_adverse_day_move_bps": None,
            "max_pairs_per_day": 1,
            "cooldown_minutes": 20,
        }
        == data["defaults"]["strategy"]
    )
    assert data["risk_controls"]["entry_window"] == {
        "start": "10:00",
        "end": "10:30",
        "end_exclusive": True,
        "afternoon_entries": False,
    }
    assert data["risk_controls"]["exact_limit_price_filter"] is True
    assert data["risk_controls"]["missing_limit_price_entry_policy"] == "block_entry"
    assert not any("submit" in path for path in app.openapi()["paths"] if "intraday-t" in path)


def test_coverage_reports_each_requested_symbol():
    client, _, _ = _client()

    response = client.get(
        "/api/intraday-t/coverage",
        params={
            "symbols": "603629.SH,688008.SH",
            "start_date": "2026-07-01",
            "end_date": "2026-07-14",
        },
    )

    assert response.status_code == 200
    coverage = response.json()["data"]["coverage"]
    assert {item["symbol"] for item in coverage} == {"603629.SH", "688008.SH"}
    assert all(item["bars"] == 40 for item in coverage)


def test_backtest_endpoint_runs_local_minute_replay_and_rejects_unknown_symbols():
    client, _, _ = _client()
    payload = {
        "symbols": ["603629.SH"],
        "start_date": "2026-07-01",
        "end_date": "2026-07-14",
        "initial_capital": 100000,
        "base_quantities": {"603629.SH": 2000},
    }

    response = client.post("/api/intraday-t/backtest", json=payload)

    assert response.status_code == 200
    assert response.json()["data"]["period"]["bars"] == 40
    payload["symbols"] = ["600000.SH"]
    invalid = client.post("/api/intraday-t/backtest", json=payload)
    assert invalid.status_code == 422


def test_backtest_passes_dependency_loaded_exact_limits_and_v2_params(monkeypatch):
    captured = {}

    def fake_run(_self, frame, config):
        captured["bars"] = len(frame)
        captured["config"] = config
        return {"period": {"bars": len(frame)}}

    monkeypatch.setattr(intraday_t_api.IntradayTBacktester, "run", fake_run)
    client, app, _ = _client()
    response = client.post(
        "/api/intraday-t/backtest",
        json={
            "symbols": ["603629.SH"],
            "start_date": "2026-07-01",
            "end_date": "2026-07-14",
            "strategy": {
                "max_entry_z": 2.25,
                "realized_vol_window": 12,
                "min_realized_vol_bps": 18.0,
                "max_adverse_day_move_bps": 150.0,
            },
        },
    )

    assert response.status_code == 200
    loader = app.state.intraday_t_limit_price_loader
    assert loader.calls == [
        (["603629.SH"], date(2026, 7, 1), date(2026, 7, 14)),
    ]
    config = captured["config"]
    assert config.limit_prices == loader.limit_prices
    assert config.require_exact_limit_prices is True
    assert config.params.max_entry_z == 2.25
    assert config.params.realized_vol_window == 12
    assert config.params.min_realized_vol_bps == 18.0
    assert config.params.max_adverse_day_move_bps == 150.0


def test_backtest_reports_missing_limit_price_days_and_blocks_entries():
    client, _, _ = _client()

    response = client.post(
        "/api/intraday-t/backtest",
        json={
            "symbols": ["603629.SH", "688008.SH"],
            "start_date": "2026-07-14",
            "end_date": "2026-07-14",
        },
    )

    assert response.status_code == 200
    quality = response.json()["data"]["data_quality"]["limit_prices"]
    assert quality["mode"] == "fail_closed"
    assert quality["missing_symbol_days"] == ["688008.SH|2026-07-14"]


@pytest.mark.parametrize(
    "strategy",
    [
        {"entry_z": 2.4, "max_entry_z": 2.4},
        {"max_entry_z": 3.0, "stop_z": 3.0},
        {"realized_vol_window": 1},
        {"min_realized_vol_bps": -0.1},
        {"max_adverse_day_move_bps": 0.0},
        {"warmup_bars": 40, "volatility_window": 30},
    ],
)
def test_backtest_rejects_invalid_v2_strategy_parameters(strategy):
    client, _, _ = _client()

    response = client.post(
        "/api/intraday-t/backtest",
        json={
            "symbols": ["603629.SH"],
            "start_date": "2026-07-01",
            "end_date": "2026-07-14",
            "strategy": strategy,
        },
    )

    assert response.status_code == 422


class _QueryResult:
    def all(self):
        return [
            ("603629.SH", date(2026, 7, 14), 22.0, 18.0),
            ("688008.SH", "2026-07-14", 88.8, 72.6),
            ("603629.SH", date(2026, 7, 13), None, 18.0),
            ("688008.SH", date(2026, 7, 13), float("nan"), 72.6),
        ]


class _QuerySession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((statement, params))
        return _QueryResult()


@pytest.mark.asyncio
async def test_limit_price_loader_batches_symbols_with_bound_parameters():
    loader_type = getattr(intraday_t_api, "IntradayTLimitPriceLoader", None)
    assert loader_type is not None, (
        "the API must provide a database-backed exact limit-price loader"
    )
    session = _QuerySession()

    result = await loader_type(session).load(
        ["603629.SH", "688008.SH"],
        date(2026, 7, 1),
        date(2026, 7, 14),
    )

    assert result == {
        "603629.SH|2026-07-14": {"up": 22.0, "down": 18.0},
        "688008.SH|2026-07-14": {"up": 88.8, "down": 72.6},
    }
    assert len(session.calls) == 1
    statement, params = session.calls[0]
    assert "603629.SH" not in str(statement)
    assert "688008.SH" not in str(statement)
    assert params == {
        "symbols": ["603629.SH", "688008.SH"],
        "start_date": "2026-07-01",
        "end_date": "2026-07-14",
    }


def test_paper_lifecycle_routes_delegate_without_exposing_real_orders():
    client, _, paper = _client()
    manual_account = {
        "cash": 100000,
        "positions": {
            "603629.SH": {"quantity": 2000, "available": 2000, "avg_cost": 20},
            "688008.SH": {"quantity": 1000, "available": 1000, "avg_cost": 70},
        },
    }

    started = client.post("/api/intraday-t/paper/start", json={"manual_account": manual_account})
    status = client.get("/api/intraday-t/paper/status", params={"session_id": "it-test"})
    evaluated = client.post("/api/intraday-t/paper/it-test/evaluate")
    trades = client.get("/api/intraday-t/paper/it-test/trades")
    stopped = client.post("/api/intraday-t/paper/it-test/stop")
    reset = client.post("/api/intraday-t/paper/it-test/reset")

    assert started.status_code == status.status_code == evaluated.status_code == 200
    assert trades.json()["data"][0]["simulated"] is True
    assert stopped.json()["data"]["status"] == "STOPPED"
    assert reset.status_code == 200
    assert [call[0] for call in paper.calls] == [
        "start",
        "status",
        "evaluate",
        "trades",
        "stop",
        "reset",
    ]


def test_paper_runner_routes_start_and_stop_background_evaluation():
    client, _, paper = _client()

    started = client.post(
        "/api/intraday-t/paper/it-test/runner/start",
        json={"interval_seconds": 30},
    )
    stopped = client.post("/api/intraday-t/paper/it-test/runner/stop")

    assert started.status_code == 200
    assert started.json()["data"]["runner_active"] is True
    assert stopped.status_code == 200
    assert stopped.json()["data"]["runner_active"] is False
    assert [call[0] for call in paper.calls] == ["start_runner", "stop_runner"]
