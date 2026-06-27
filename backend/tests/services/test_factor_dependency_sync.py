from __future__ import annotations

from datetime import date

import pytest

import app.services.factor_dependency_sync as module


def test_cn_paper_factor_names_are_supported_for_prepare() -> None:
    assert "paper_pb_roe_residual" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "paper_trend_fund_vwap_ratio" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "paper_size_rotation_score" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "indicator_buy_signal" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "tsmf_overheat_penalty" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "avoid_high_volume_ratio" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "tsmf_recent_effective_score" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "semibeta_downside_avoid_252" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "limit_ecology_quality_combo" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "trend_support" in module.SUPPORTED_PRECOMPUTE_FACTORS


def test_cn_paper_minute_dependency_builds_full_minute_sync_step(monkeypatch) -> None:
    monkeypatch.setattr(module, "_latest_market_date", lambda dataset, **kwargs: None)
    monkeypatch.setattr(module, "_latest_sqlite_date", lambda table, column: None)

    gaps = module._build_coverage_gaps(
        factor_names=["paper_trend_fund_vwap_ratio"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        index_symbol=None,
        params={},
    )

    assert len(gaps) == 1
    assert gaps[0]["dependency"] == "klines_minute"
    assert gaps[0]["sync_step"] == "kline_minute"
    assert "timer_time" not in gaps[0]

    plan = module._build_sync_plan(
        coverage_gaps=gaps,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        symbols=["000001.SZ"],
        index_symbol=None,
    )

    assert plan is not None
    assert plan["steps"] == [{
        "type": "kline_minute",
        "start_date": "2024-01-01",
        "end_date": "2024-02-01",
    }]


def test_cn_paper_fundamental_dependencies_build_data_sync_steps(monkeypatch) -> None:
    monkeypatch.setattr(module, "_latest_market_date", lambda dataset, **kwargs: "2024-02-01")
    monkeypatch.setattr(module, "_latest_sqlite_date", lambda table, column: None)

    gaps = module._build_coverage_gaps(
        factor_names=["paper_pb_roe_residual"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        index_symbol=None,
        params={},
    )

    dependencies = {item["dependency"] for item in gaps}
    assert dependencies == {"financial_data", "stock_daily_basic"}

    plan = module._build_sync_plan(
        coverage_gaps=gaps,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        symbols=None,
        index_symbol=None,
    )

    assert plan is not None
    assert [step["type"] for step in plan["steps"]] == ["tushare_daily", "financial_data"]
    assert plan["steps"][0]["datasets"] == ["stock_daily_basic"]


def test_independent_limit_ecology_dependency_builds_limit_sync(monkeypatch) -> None:
    monkeypatch.setattr(module, "_latest_market_date", lambda dataset, **kwargs: "2024-02-01")
    monkeypatch.setattr(module, "_latest_sqlite_date", lambda table, column: None)

    gaps = module._build_coverage_gaps(
        factor_names=["limit_ecology_quality_combo"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        index_symbol=None,
        params={},
    )

    dependencies = {item["dependency"] for item in gaps}
    assert dependencies == {"stock_limit_prices"}
    plan = module._build_sync_plan(
        coverage_gaps=gaps,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        symbols=["000001.SZ"],
        index_symbol=None,
    )

    assert plan is not None
    assert plan["steps"] == [{
        "type": "tushare_daily",
        "start_date": "2024-01-01",
        "end_date": "2024-02-01",
        "datasets": ["stock_limit_prices"],
    }]


def test_timer_status_dependencies_check_sparse_timer_dataset(monkeypatch) -> None:
    calls: list[tuple[str, str | None]] = []

    def fake_latest_market_date(dataset, **kwargs):
        calls.append((dataset, kwargs.get("timer_time")))
        if dataset == "klines_minute_timer":
            return "2024-02-01"
        return None

    monkeypatch.setattr(module, "_latest_market_date", fake_latest_market_date)
    monkeypatch.setattr(module, "_latest_sqlite_date", lambda table, column: "2024-02-01")

    gaps = module._build_coverage_gaps(
        factor_names=["is_paused", "is_limit_up", "is_limit_down"],
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 1),
        index_symbol=None,
        params={"time": "10:30"},
    )

    assert gaps == []
    assert ("klines_minute_timer", "10:30") in calls
    assert all(dataset != "klines_minute" for dataset, _ in calls)


def test_timer_status_dependencies_build_sparse_timer_sync_step(monkeypatch) -> None:
    monkeypatch.setattr(module, "_latest_market_date", lambda dataset, **kwargs: None)
    monkeypatch.setattr(module, "_latest_sqlite_date", lambda table, column: "2024-02-01")

    gaps = module._build_coverage_gaps(
        factor_names=["is_paused"],
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 1),
        index_symbol=None,
        params={"time": "10:30"},
    )

    assert len(gaps) == 1
    assert gaps[0]["dependency"] == "klines_minute_timer"
    assert gaps[0]["sync_step"] == "kline_minute"
    assert gaps[0]["timer_time"] == "10:30"

    plan = module._build_sync_plan(
        coverage_gaps=gaps,
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 1),
        symbols=["000001.SZ"],
        index_symbol=None,
    )

    assert plan is not None
    assert plan["steps"] == [{
        "type": "kline_minute",
        "start_date": "2024-02-01",
        "end_date": "2024-02-01",
        "timer_times": ["10:30"],
    }]


@pytest.mark.asyncio
async def test_timer_minute_dependency_step_uses_sparse_timer_sync(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeService:
        async def sync_kline_minute(self, **kwargs):
            raise AssertionError("timer dependency must not run full minute sync")

    async def fake_resolve_symbols(plan):
        captured["plan"] = plan
        return ["000001.SZ"]

    async def fake_sync_timer_minute_points(**kwargs):
        captured.update(kwargs)
        return {
            "symbols": 1,
            "start": kwargs["start"].isoformat(),
            "end": kwargs["end"].isoformat(),
            "times": [item.strftime("%H:%M") for item in kwargs["timer_times"]],
            "fetched": 1,
            "inserted": 1,
            "failures": [],
            "results": [],
        }

    monkeypatch.setattr(module, "_resolve_plan_symbols", fake_resolve_symbols)
    monkeypatch.setattr("app.services.timer_minute_sync.sync_timer_minute_points", fake_sync_timer_minute_points)

    result = await module._sync_kline_minute_step(
        FakeService(),
        {"symbols": ["000001.SZ"], "index_symbol": "399101.SZ"},
        {"type": "kline_minute", "start_date": "2026-06-25", "end_date": "2026-06-25", "timer_times": ["10:30"]},
        run_id="run-1",
        task_id=None,
        failure_strategy="stop",
        outer_progress=object(),
    )

    assert result["sync_type"] == "timer_minute"
    assert result["dataset"] == "klines_minute_timer"
    assert result["times"] == ["10:30"]
    assert captured["symbols"] == ["000001.SZ"]
    assert captured["index_symbol"] == "399101.SZ"
