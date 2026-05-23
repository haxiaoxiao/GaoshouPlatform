"""Tests for expression precompute into factor_cache."""

from datetime import date

import pandas as pd
import pytest


@pytest.mark.asyncio
async def test_precompute_factor_expressions_builtin(monkeypatch):
    from app.services.factor_expression_precompute import precompute_factor_expressions

    saved: list[tuple[str, date, dict, str]] = []

    class FakeCache:
        ch_client = None

        @staticmethod
        def make_key(expression):
            return f"hash-{expression}"

        def save_to_parquet(self, expr_hash, trade_date, series, expression="", engine="builtin"):
            saved.append((expr_hash, trade_date, series.to_dict(), engine))

    class FakeStore:
        def load_daily(self, symbols, start_date, end_date):
            return pd.DataFrame(
                [
                    {"symbol": "000001.SZ", "trade_date": date(2025, 1, 2), "close": 10.0, "open": 9.0},
                    {"symbol": "000001.SZ", "trade_date": date(2025, 1, 3), "close": 11.0, "open": 10.0},
                ]
            )

    monkeypatch.setattr("app.services.factor_expression_precompute.get_compute_cache", lambda: FakeCache())
    monkeypatch.setattr("app.services.factor_expression_precompute.get_market_data_store", lambda: FakeStore())

    result = await precompute_factor_expressions(
        expressions=["ts_delta($close, 1)"],
        symbols=["000001.SZ"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        engine="builtin",
    )

    assert result["rows_written"] == 1
    assert result["expressions"][0]["expr_hash"] == "hash-ts_delta($close, 1)"
    assert saved[0][1] == date(2025, 1, 3)
    assert saved[0][2]["000001.SZ"] == 1.0


@pytest.mark.asyncio
async def test_compute_precompute_route(monkeypatch):
    from httpx import ASGITransport, AsyncClient

    from app.main import app

    async def fake_precompute(**kwargs):
        assert kwargs["engine"] == "akquant"
        return {"rows_written": 2, "expressions": [{"expression": "x"}]}

    monkeypatch.setattr("app.services.factor_expression_precompute.precompute_factor_expressions", fake_precompute)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/compute/precompute",
            json={
                "engine": "akquant",
                "expressions": ["x"],
                "symbols": ["000001.SZ"],
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
            },
        )

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["rows_written"] == 2
