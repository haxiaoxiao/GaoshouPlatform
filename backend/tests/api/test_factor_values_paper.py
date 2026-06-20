from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

import app.api.factor_values as factor_values_api
from app.main import app


@pytest.mark.asyncio
async def test_paper_manifest_and_experiments_are_exposed() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        manifest_resp = await client.get("/api/factor-values/paper-manifest")
        experiments_resp = await client.get("/api/factor-values/paper-experiments")

    assert manifest_resp.status_code == 200
    manifest = manifest_resp.json()["data"]
    assert len(manifest) == 44
    assert {"data_dependencies", "factor_rules", "landing_grade", "validation_metrics"}.issubset(manifest[0])

    assert experiments_resp.status_code == 200
    experiments = experiments_resp.json()["data"]
    assert len(experiments) >= 4
    assert all(item["status"] == "offline_only" for item in experiments)


@pytest.mark.asyncio
async def test_paper_experiment_feature_snapshot_endpoint(monkeypatch) -> None:
    def fake_snapshot(*, factor_names, trade_dates, symbols=None):
        assert factor_names == ["paper_composite_value", "paper_reversal_20d"]
        assert trade_dates == [date(2024, 1, 2)]
        assert symbols == ["000001.SZ"]
        return pd.DataFrame(
            [{
                "trade_date": date(2024, 1, 2),
                "symbol": "000001.SZ",
                "paper_composite_value": 0.7,
                "paper_reversal_20d": -0.1,
                "feature_coverage": 1.0,
            }]
        )

    monkeypatch.setattr(factor_values_api, "build_factor_feature_snapshot", fake_snapshot)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/factor-values/paper-experiments/feature-snapshot",
            json={
                "factor_names": ["paper_composite_value", "paper_reversal_20d"],
                "trade_dates": ["2024-01-02"],
                "symbols": ["000001.SZ"],
                "min_feature_coverage": 0.5,
                "limit": 10,
            },
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["summary"]["rows"] == 1
    assert data["items"][0]["trade_date"] == "2024-01-02"
    assert data["items"][0]["feature_coverage"] == 1.0


@pytest.mark.asyncio
async def test_group_precompute_expands_cn_paper_group(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_precompute_cn_paper_factors(*, factor_names, start_date, end_date, symbols, progress_callback=None):
        names = [str(name) for name in factor_names]
        captured["factor_names"] = names
        captured["symbols"] = list(symbols)
        return {
            "symbols": len(symbols),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "rows": {name: 1 for name in names},
            "rows_written": len(names),
            "failed_factor_names": [],
            "errors": {},
        }

    monkeypatch.setattr(factor_values_api, "precompute_cn_paper_factors", fake_precompute_cn_paper_factors)
    monkeypatch.setattr(factor_values_api, "_safe_attach_result_coverage", lambda *args, **kwargs: None)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/factor-values/groups/precompute",
            json={
                "group_name": "cn_paper_fundamental",
                "factor_names": ["paper_pb_roe_residual"],
                "start_date": "2024-01-02",
                "end_date": "2024-01-03",
                "symbols": ["000001.SZ"],
                "params": {},
            },
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert captured["factor_names"] == [
        "paper_pb_roe_residual",
        "paper_composite_value",
        "paper_growth_quality_score",
        "paper_financial_health_score",
    ]
    assert data["requested_factor_count"] == 4
    assert data["written_factor_count"] == 4


@pytest.mark.asyncio
async def test_group_precompute_expands_independent_ashare_30_group(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_execute_factor_bundle(*, task_id, factor_names, start_date, end_date, symbols, index_symbol, params):
        captured["factor_names"] = list(factor_names)
        captured["symbols"] = list(symbols or [])
        return {
            "rows": {name: 1 for name in factor_names},
            "rows_written": len(factor_names),
        }

    monkeypatch.setattr(factor_values_api, "_execute_factor_bundle", fake_execute_factor_bundle)
    monkeypatch.setattr(factor_values_api, "_safe_attach_result_coverage", lambda *args, **kwargs: None)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/factor-values/groups/precompute",
            json={
                "group_name": "independent_ashare_30_20260618",
                "start_date": "2024-01-02",
                "end_date": "2024-01-03",
                "symbols": ["000001.SZ"],
                "params": {},
            },
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert len(captured["factor_names"]) == 30
    assert "semibeta_downside_avoid_252" in captured["factor_names"]
    assert "alpha101_001" in captured["factor_names"]
    assert "high_volume_signal" in captured["factor_names"]
    assert data["rows_written"] == 30


@pytest.mark.asyncio
async def test_param_hashes_endpoint_returns_cached_hashes(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeStore:
        def list_param_hashes(self, factor_names, **kwargs):
            captured["factor_names"] = list(factor_names)
            captured["kwargs"] = kwargs
            return [{
                "factor_name": "paper_pb_roe_residual",
                "params_hash": "abc123",
                "as_of_time": "",
                "total_rows": 10,
                "symbol_count": 2,
                "date_count": 5,
                "min_date": "2024-01-02",
                "max_date": "2024-01-08",
                "latest_created_at": "2024-01-08 10:00:00",
                "source": "test",
                "is_default": True,
            }]

    monkeypatch.setattr(factor_values_api, "get_factor_value_store", lambda: FakeStore())

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/factor-values/param-hashes",
            json={
                "factor_names": ["paper_pb_roe_residual"],
                "start_date": "2024-01-02",
                "end_date": "2024-01-08",
                "symbols": ["000001.SZ"],
            },
        )

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data[0]["params_hash"] == "abc123"
    assert captured["factor_names"] == ["paper_pb_roe_residual"]
    assert captured["kwargs"]["symbols"] == ["000001.SZ"]
