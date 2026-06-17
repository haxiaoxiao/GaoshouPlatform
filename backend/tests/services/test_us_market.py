from __future__ import annotations

from datetime import date

import pandas as pd

from app.services import us_market as us_market_module
from app.services.us_market import (
    apply_entry_filter_to_target_weights,
    sync_us_market_history,
    us_overnight_entry_filter_state,
)


def test_us_overnight_entry_filter_blocks_combined_downside(tmp_path):
    path = tmp_path / "us_market_daily.csv"
    pd.DataFrame(
        [
            {"symbol": "QQQ", "date": "2026-06-10", "ret_1d": -0.011},
            {"symbol": "SMH", "date": "2026-06-10", "ret_1d": 0.002},
            {"symbol": "SOXX", "date": "2026-06-10", "ret_1d": 0.001},
            {"symbol": "NVDA", "date": "2026-06-10", "ret_1d": 0.004},
        ]
    ).to_csv(path, index=False)

    state = us_overnight_entry_filter_state(
        date(2026, 6, 11),
        mode="combined_downside",
        data_path=path,
    )

    assert state["entry_filter_enabled"] is True
    assert state["entry_filter_block_buys"] is True
    assert state["us_overnight_reason"] == "combined_caution"
    assert state["us_date"] == "2026-06-10"


def test_entry_filter_keeps_old_weight_and_blocks_new_buys():
    filtered, state = apply_entry_filter_to_target_weights(
        {"000001.SZ": 0.06, "000002.SZ": 0.06},
        current_positions={"000001.SZ": 1000},
        price_map={"000001.SZ": 10.0, "000002.SZ": 20.0},
        portfolio_value=1_000_000,
        entry_filter_state={"entry_filter_block_buys": True},
    )

    assert filtered == {"000001.SZ": 0.01}
    assert state["entry_filter_blocked_new"] == 1
    assert state["entry_filter_blocked_add"] == 1


def test_us_market_sync_merges_existing_history(monkeypatch, tmp_path):
    pd.DataFrame(
        [
            {"symbol": "QQQ", "assetclass": "etf", "date": "2026-06-01", "close": 1.0, "ret_1d": None},
        ]
    ).to_csv(tmp_path / "us_market_daily.csv", index=False)
    pd.DataFrame(
        [
            {"symbol": "QQQ", "assetclass": "etf", "date": "2026-06-01", "close": 1.0, "ret_1d": None},
        ]
    ).to_csv(tmp_path / "QQQ_daily.csv", index=False)

    def fake_fetch(symbol, assetclass, start, end):
        return pd.DataFrame(
            [
                {"symbol": symbol, "assetclass": assetclass, "date": "2026-06-02", "open": 1.0, "high": 1.0, "low": 1.0, "close": 2.0, "volume": 100, "ret_1d": 1.0, "source": "test"},
            ]
        )

    monkeypatch.setattr(us_market_module, "fetch_us_symbol_history", fake_fetch)

    sync_us_market_history(
        start_date=date(2026, 6, 2),
        end_date=date(2026, 6, 2),
        symbol_specs=["QQQ:etf"],
        output_dir=tmp_path,
        sleep_seconds=0,
    )

    combined = pd.read_csv(tmp_path / "us_market_daily.csv")
    symbol_frame = pd.read_csv(tmp_path / "QQQ_daily.csv")
    assert list(combined["date"]) == ["2026-06-01", "2026-06-02"]
    assert list(symbol_frame["date"]) == ["2026-06-01", "2026-06-02"]
