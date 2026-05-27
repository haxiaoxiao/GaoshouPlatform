from __future__ import annotations

import asyncio
from datetime import date, datetime

import pandas as pd

from app.db.models import FactorResearchRun
from app.models.factor import BoardQuery
from app.services.factor_evaluation import FactorEvaluationService
from app.services.factor_research_runs import FactorResearchRunService, research_match_hash


def test_align_frames_reindexes_requested_symbols() -> None:
    dates = pd.to_datetime(["2025-01-02", "2025-01-03"])
    factor_df = pd.DataFrame({"000001.SZ": [1.0, 2.0]}, index=dates)
    return_df = pd.DataFrame(
        {
            "000001.SZ": [0.01, 0.02],
            "000002.SZ": [0.03, 0.04],
        },
        index=dates,
    )

    aligned_factor, aligned_return = FactorResearchRunService._align_frames(
        factor_df,
        return_df,
        ["000001.SZ", "000002.SZ"],
    )

    assert list(aligned_factor.columns) == ["000001.SZ", "000002.SZ"]
    assert list(aligned_return.columns) == ["000001.SZ", "000002.SZ"]
    assert aligned_factor["000002.SZ"].isna().all()
    assert aligned_return["000002.SZ"].notna().all()


def test_coverage_ratio_uses_membership_denominator() -> None:
    dates = pd.to_datetime(["2025-01-02", "2025-01-03"])
    factor_df = pd.DataFrame(
        {
            "000001.SZ": [1.0, 2.0],
            "000002.SZ": [None, None],
        },
        index=dates,
    )
    membership = pd.DataFrame(
        {
            "000001.SZ": [True, True],
            "000002.SZ": [True, True],
        },
        index=dates,
    )

    assert FactorResearchRunService._coverage_ratio(
        factor_df,
        ["000001.SZ", "000002.SZ"],
        membership,
    ) == 0.5


def test_transaction_cost_rate_sums_fee_tax_and_slippage() -> None:
    assert FactorResearchRunService._transaction_cost_rate({
        "fee_rate": 0.003,
        "stamp_tax_rate": 0.001,
        "transfer_fee_rate": 0.00001,
        "slippage": 0.001,
    }) == 0.00501


def test_normalized_params_keeps_current_factor_value_hash_only() -> None:
    params = FactorResearchRunService()._normalized_params({
        "factor_name": "factor_a",
        "stock_pool_value": "zz500",
        "start_date": "2024-01-02",
        "end_date": "2024-01-31",
        "factor_value_params_hashes": {
            "factor_a": "hash-a",
            "factor_b": "hash-b",
        },
    })

    assert params["factor_value_params_hash"] == "hash-a"
    assert params["factor_value_params_hashes"] == {"factor_a": "hash-a"}


def test_coverage_effective_range_clips_uncached_tail() -> None:
    effective = FactorResearchRunService._coverage_effective_range(
        {"min_date": "2026-02-24", "max_date": "2026-05-20"},
        date(2026, 2, 24),
        date(2026, 5, 24),
    )

    assert effective == (date(2026, 2, 24), date(2026, 5, 20))


def test_trim_to_effective_factor_dates_drops_uncached_dates() -> None:
    dates = pd.to_datetime(["2026-05-20", "2026-05-21", "2026-05-22"])
    factor_df = pd.DataFrame({"000001.SZ": [1.0, None, None]}, index=dates)
    return_df = pd.DataFrame({"000001.SZ": [0.01, 0.02, 0.03]}, index=dates)

    trimmed_factor, trimmed_return, _ = FactorResearchRunService._trim_to_effective_factor_dates(
        factor_df,
        return_df,
    )

    assert list(trimmed_factor.index) == [pd.Timestamp("2026-05-20")]
    assert list(trimmed_return.index) == [pd.Timestamp("2026-05-20")]


def test_research_match_hash_ignores_date_range_and_context_keys() -> None:
    payload_a = {
        "factor_name": "alpha101_001",
        "stock_pool_value": "small_cap",
        "start_date": "2025-05-24",
        "end_date": "2026-05-20",
        "portfolio_type": "long_short_ii",
        "rebalance_period": "monthly",
        "fee_rate": 0.004,
        "slippage": 0.0,
        "filter_limit_up": True,
        "filter_limit_down": True,
        "group_count": 5,
        "direction": "desc",
        "pool_membership_mode": "static_latest",
        "industry_neutralization": False,
        "standardize": False,
    }
    payload_b = {
        **payload_a,
        "factor_name": "alpha101_002",
        "stock_pool_value": "zz500",
        "start_date": "2025-05-25",
        "end_date": "2026-05-25",
    }

    assert research_match_hash(payload_a, ignore_date_range=True) == research_match_hash(payload_b, ignore_date_range=True)


def test_normalized_factor_value_hash_matches_board_payload_without_dates() -> None:
    service = FactorResearchRunService()
    stored_payload = service._normalized_params({
        "factor_name": "paper_pb_roe_residual",
        "stock_pool_value": "small_cap",
        "start_date": "2023-05-20",
        "end_date": "2026-05-20",
        "portfolio_type": "long_only",
        "rebalance_period": "monthly",
        "fee_rate": 0.003,
        "stamp_tax_rate": 0.001,
        "transfer_fee_rate": 0.0,
        "slippage": 0.001,
        "filter_limit_up": True,
        "filter_limit_down": True,
        "group_count": 5,
        "direction": "desc",
        "pool_membership_mode": "static_latest",
        "factor_value_params_hashes": {"paper_pb_roe_residual": "bf21a9e8fbc5a384"},
        "industry_neutralization": False,
        "standardize": False,
    })
    board_payload = service._normalized_params({
        "factor_name": "paper_pb_roe_residual",
        "stock_pool_value": "small_cap",
        "start_date": "2023-05-27",
        "end_date": "2026-05-27",
        "portfolio_type": "long_only",
        "rebalance_period": "monthly",
        "fee_rate": 0.003,
        "stamp_tax_rate": 0.001,
        "transfer_fee_rate": 0.0,
        "slippage": 0.001,
        "filter_limit_up": True,
        "filter_limit_down": True,
        "group_count": 5,
        "direction": "desc",
        "pool_membership_mode": "static_latest",
        "factor_value_params_hash": "bf21a9e8fbc5a384",
        "industry_neutralization": False,
        "standardize": False,
    })

    assert research_match_hash(stored_payload, ignore_date_range=True) == research_match_hash(
        board_payload,
        ignore_date_range=True,
    )


def test_select_best_compatible_run_prefers_covering_requested_range() -> None:
    rows = [
        FactorResearchRun(
            run_id="fr-old",
            factor_name="alpha101_001",
            factor_display_name="Alpha101 #001",
            stock_pool_type="index",
            stock_pool_value="small_cap",
            start_date=date(2025, 5, 24),
            end_date=date(2026, 5, 20),
            params_hash="h1",
            params={},
            status="success",
            created_at=datetime(2026, 5, 24, 23, 0, 0),
            completed_at=datetime(2026, 5, 24, 23, 30, 0),
        ),
        FactorResearchRun(
            run_id="fr-cover",
            factor_name="alpha101_001",
            factor_display_name="Alpha101 #001",
            stock_pool_type="index",
            stock_pool_value="small_cap",
            start_date=date(2025, 5, 1),
            end_date=date(2026, 5, 31),
            params_hash="h2",
            params={},
            status="success",
            created_at=datetime(2026, 5, 20, 23, 0, 0),
            completed_at=datetime(2026, 5, 20, 23, 30, 0),
        ),
    ]

    selected = FactorResearchRunService._select_best_compatible_run(
        rows,
        requested_range=(date(2025, 5, 25), date(2026, 5, 25)),
    )

    assert selected is not None
    assert selected.run_id == "fr-cover"


def test_combination_groups_use_union_and_coverage_counts(monkeypatch) -> None:
    service = FactorResearchRunService()
    common = {
        "stock_pool_value": "small_cap",
        "start_date": "2023-05-20",
        "end_date": "2026-05-20",
        "portfolio_type": "long_only",
        "rebalance_period": "monthly",
        "fee_rate": 0.003,
        "stamp_tax_rate": 0.001,
        "transfer_fee_rate": 0.0,
        "slippage": 0.001,
        "filter_limit_up": True,
        "filter_limit_down": True,
        "group_count": 5,
        "direction": "desc",
        "pool_membership_mode": "static_latest",
        "factor_value_params_hash": "bf21a9e8fbc5a384",
        "industry_neutralization": False,
        "standardize": False,
    }
    rows = [
        FactorResearchRun(
            run_id="fr-a",
            factor_name="factor_a",
            factor_display_name="Factor A",
            stock_pool_type="index",
            stock_pool_value="small_cap",
            start_date=date(2023, 5, 20),
            end_date=date(2026, 5, 20),
            params_hash="h-a",
            params={**common, "factor_name": "factor_a", "factor_value_params_hashes": {"factor_a": "bf21a9e8fbc5a384"}},
            status="success",
            created_at=datetime(2026, 5, 27, 10, 0, 0),
            completed_at=datetime(2026, 5, 27, 10, 1, 0),
            summary={"ic_mean": 0.1, "icir": 0.2},
        ),
        FactorResearchRun(
            run_id="fr-b",
            factor_name="factor_b",
            factor_display_name="Factor B",
            stock_pool_type="index",
            stock_pool_value="small_cap",
            start_date=date(2023, 5, 20),
            end_date=date(2026, 5, 20),
            params_hash="h-b",
            params={**common, "factor_name": "factor_b", "factor_value_params_hashes": {"factor_b": "bf21a9e8fbc5a384"}},
            status="success",
            created_at=datetime(2026, 5, 27, 10, 2, 0),
            completed_at=datetime(2026, 5, 27, 10, 3, 0),
            summary={"ic_mean": 0.2, "icir": 0.3},
        ),
    ]

    async def fake_list_success_rows(factor_names, stock_pool_value=None):
        return rows

    monkeypatch.setattr(service, "_list_success_rows", fake_list_success_rows)
    result = asyncio.run(service.combinations({
        "factor_names": ["factor_a", "factor_b"],
        "selection": {"factor_value_params_hash": "bf21a9e8fbc5a384"},
    }))

    assert result["total_candidates"] == 2
    assert result["combo_groups"][0]["covered_factor_count"] == 2
    assert result["combo_groups"][0]["total_factor_count"] == 2
    assert result["combo_groups"][0]["factor_value_params_hashes"] == {
        "factor_a": "bf21a9e8fbc5a384",
        "factor_b": "bf21a9e8fbc5a384",
    }
    assert result["facets"]["stock_pool_value"][0]["value"] == "small_cap"


def test_board_date_range_prefers_explicit_dates() -> None:
    query = BoardQuery(
        period="3y",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 12, 31),
    )

    assert FactorEvaluationService()._board_date_range(query) == (
        date(2024, 1, 2),
        date(2024, 12, 31),
    )
