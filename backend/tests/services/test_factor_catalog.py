from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from app.services.alpha101_calculator import Alphas
from app.services.factor_value_store import get_factor_definition, list_factor_groups


def _alpha_input_frame(periods: int = 320) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=periods, freq="B")
    rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(["000001.SZ", "600519.SH", "300750.SZ"], start=1):
        base = 10.0 + symbol_index
        for day_index, trade_date in enumerate(dates, start=1):
            close = base + day_index * 0.03 + np.sin(day_index / 7 + symbol_index)
            open_price = close - 0.12
            high = close + 0.4
            low = close - 0.35
            volume = 1000 + symbol_index * 20 + day_index * 3
            rows.append({
                "symbol": symbol,
                "date": trade_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "market_value": 1_000_000 + symbol_index * 100_000 + day_index * 500,
                "return": 0.0,
                "vwap": close - 0.03,
                "industry": f"IND{symbol_index}",
            })
    frame = pd.DataFrame(rows).sort_values(["symbol", "date"]).reset_index(drop=True)
    frame["return"] = frame.groupby("symbol")["close"].pct_change().fillna(0.0)
    return frame.set_index(["symbol", "date"]).sort_index()


def test_catalog_groups_are_exposed() -> None:
    groups = {item["name"] for item in list_factor_groups()}
    assert "ta_lib_core" in groups
    assert "alpha101" in groups
    assert "research_factor_ideas" in groups
    assert get_factor_definition("ta_rsi_14")["source"] == "catalog.ta_lib"
    assert get_factor_definition("alpha101_001")["source"] == "catalog.alpha101"
    assert get_factor_definition("research_low_beta")["source"] == "catalog.research"


def test_alpha101_definition_exposes_formula() -> None:
    definition = get_factor_definition("alpha101_002")
    assert definition["formula"].startswith("Alpha#2:")
    assert "correlation" in definition["formula"]
    assert "klines_daily.close" in definition["dependencies"]


def test_alpha101_binary_formulas_do_not_raise() -> None:
    calculator = Alphas(_alpha_input_frame())
    factor_methods = [
        calculator.alpha_21,
        calculator.alpha_27,
        calculator.alpha_46,
        calculator.alpha_61,
        calculator.alpha_62,
        calculator.alpha_64,
        calculator.alpha_65,
        calculator.alpha_68,
        calculator.alpha_74,
        calculator.alpha_75,
        calculator.alpha_79,
        calculator.alpha_81,
        calculator.alpha_86,
        calculator.alpha_95,
        calculator.alpha_99,
    ]
    non_empty_results = 0
    for method in factor_methods:
        result = method()
        assert isinstance(result, pd.Series)
        assert len(result) > 0
        if result.notna().sum() > 0:
            non_empty_results += 1
    assert non_empty_results >= 5
