from __future__ import annotations

from datetime import date

import app.services.factor_dependency_sync as module


def test_cn_paper_factor_names_are_supported_for_prepare() -> None:
    assert "paper_pb_roe_residual" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "paper_trend_fund_vwap_ratio" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "paper_size_rotation_score" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "indicator_buy_signal" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "tsmf_overheat_penalty" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "avoid_high_volume_ratio" in module.SUPPORTED_PRECOMPUTE_FACTORS
    assert "tsmf_recent_effective_score" in module.SUPPORTED_PRECOMPUTE_FACTORS


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
