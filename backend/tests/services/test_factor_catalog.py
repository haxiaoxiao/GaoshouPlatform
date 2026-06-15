from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

import app.services.alpha101_calculator as alpha101_module
from app.services.alpha101_calculator import Alphas
from app.services.alpha101_wide_calculator import SUPPORTED_WIDE_ALPHA101, WideAlphas
from app.services.factor_catalog import list_paper_implementation_manifest
from app.services.factor_value_store import get_factor_definition, list_factor_groups


def _alpha_input_frame(periods: int = 320, symbol_count: int = 3) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=periods, freq="B")
    rows: list[dict[str, object]] = []
    base_symbols = ["000001.SZ", "600519.SH", "300750.SZ"]
    extra_symbols = [f"{300750 + index:06d}.SZ" for index in range(1, max(0, symbol_count - len(base_symbols)) + 1)]
    for symbol_index, symbol in enumerate((base_symbols + extra_symbols)[:symbol_count], start=1):
        base = 10.0 + symbol_index
        for day_index, trade_date in enumerate(dates, start=1):
            close = (
                base
                + day_index * 0.03
                + 1.8 * np.sin(day_index / 7 + symbol_index)
                + 0.4 * np.cos(day_index / (symbol_index + 2))
                + 0.35 * np.sin(day_index * 1.13 + symbol_index * 2.1)
            )
            open_price = close - 0.12
            high = close + 0.4
            low = close - 0.35
            volume = (
                3000
                + symbol_index * 25
                + day_index * (0.2 + 0.1 * (symbol_index % 3))
                + 180 * np.sin(day_index / (symbol_index + 3))
                + 700 * np.sin(day_index / 17 + symbol_index * 0.7)
                + 900 * np.cos(day_index / 83 + symbol_index * 0.4)
            )
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
                "industry": f"IND{symbol_index % 4}",
            })
    frame = pd.DataFrame(rows).sort_values(["symbol", "date"]).reset_index(drop=True)
    frame["return"] = frame.groupby("symbol")["close"].pct_change().fillna(0.0)
    return frame.set_index(["symbol", "date"]).sort_index()


def test_catalog_groups_are_exposed() -> None:
    groups = {item["name"] for item in list_factor_groups()}
    assert "ta_lib_core" in groups
    assert "alpha101" in groups
    assert "research_factor_ideas" in groups
    assert "cn_paper_implemented" in groups
    assert "cn_paper_style_rotation" in groups
    assert "tsmf_research_factor_library" in groups
    assert "tsmf_preferred_rotation_pool" in groups
    assert get_factor_definition("ta_rsi_14")["source"] == "catalog.ta_lib"
    assert get_factor_definition("alpha101_001")["source"] == "catalog.alpha101"
    assert get_factor_definition("research_low_beta")["source"] == "catalog.research"
    assert get_factor_definition("paper_pb_roe_residual")["source"] == "catalog.cn_paper"
    assert get_factor_definition("paper_size_rotation_score")["source"] == "catalog.cn_paper"
    assert get_factor_definition("tsmf_recent_effective_score")["source"] == "catalog.cn_paper"
    assert get_factor_definition("avoid_high_volume_ratio")["source"] == "parquet"
    assert get_factor_definition("tsmf_overheat_penalty")["source"] == "parquet"


def test_tsmf_factor_pools_are_exposed() -> None:
    groups = {item["name"]: item for item in list_factor_groups()}
    full = groups["tsmf_research_factor_library"]
    preferred = groups["tsmf_preferred_rotation_pool"]

    assert "indicator_buy_signal" in full["factor_names"]
    assert "tsmf_overheat_penalty" in full["factor_names"]
    assert "tsmf_recent_effective_score" in full["factor_names"]
    assert "avoid_high_volume_ratio" in preferred["factor_names"]
    assert "risk_quality" in preferred["selection_buckets"]
    assert any(item["name"] == "us_entry_filter_combined_downside" for item in preferred["strategy_signals"])


def test_cn_paper_manifest_covers_all_report_rows() -> None:
    manifest = list_paper_implementation_manifest()
    assert len(manifest) == 44
    by_id = {item["paper_id"]: item for item in manifest}
    assert by_id[6]["factor_names"] == ["paper_overnight_turnover_corr"]
    assert by_id[39]["landing_status"] == "backlog_tick"
    assert by_id[23]["landing_status"] == "pending_data"
    assert "paper_trend_fund_vwap_ratio" in by_id[10]["factor_names"]
    assert "data_dependencies" in by_id[6]
    assert "factor_rules" in by_id[6]
    assert "rebalance_frequency" in by_id[6]
    assert "landing_grade" in by_id[6]
    assert "validation_metrics" in by_id[6]
    assert "paper_size_rotation_score" in by_id[28]["factor_names"]
    assert by_id[28]["landing_status"] == "implemented_template"
    assert "paper_asset_allocation_proxy" in by_id[15]["factor_names"]


def test_alpha101_definition_exposes_formula() -> None:
    definition = get_factor_definition("alpha101_002")
    assert definition["formula"].startswith("Alpha#2:")
    assert "correlation" in definition["formula"]
    assert "klines_daily.close" in definition["dependencies"]
    assert "成交量变化" in definition["human_description"]


def test_all_alpha101_definitions_have_human_description() -> None:
    for index in range(1, 102):
        definition = get_factor_definition(f"alpha101_{index:03d}")
        assert definition["human_description"].startswith(f"Alpha101 #{index:03d}")
        assert "阅读时可以从公式最内层开始" in definition["human_description"] or index == 2


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


def test_alpha101_scale_is_cross_sectional_by_date() -> None:
    frame = _alpha_input_frame(periods=2)
    calculator = Alphas(frame)
    idx = pd.MultiIndex.from_tuples(
        [
            ("000001.SZ", pd.Timestamp("2024-01-01")),
            ("600519.SH", pd.Timestamp("2024-01-01")),
            ("300750.SZ", pd.Timestamp("2024-01-01")),
            ("000001.SZ", pd.Timestamp("2024-01-02")),
            ("600519.SH", pd.Timestamp("2024-01-02")),
            ("300750.SZ", pd.Timestamp("2024-01-02")),
        ],
        names=["symbol", "date"],
    )
    values = pd.Series([1.0, -2.0, 3.0, 4.0, 0.0, -4.0], index=idx)

    scaled = calculator._scale(values)

    assert np.isclose(scaled.xs(pd.Timestamp("2024-01-01"), level="date").abs().sum(), 1.0)
    assert np.isclose(scaled.xs(pd.Timestamp("2024-01-02"), level="date").abs().sum(), 1.0)
    assert np.isclose(scaled.loc[("000001.SZ", pd.Timestamp("2024-01-01"))], 1.0 / 6.0)


def test_alpha101_input_frame_normalizes_lot_volume_vwap(monkeypatch) -> None:
    class DummyStore:
        def load_daily(self, symbols, start_date, end_date, columns=None):
            return pd.DataFrame(
                [
                    {
                        "symbol": "000001.SZ",
                        "trade_date": pd.Timestamp("2024-01-02"),
                        "open": 10.0,
                        "high": 10.5,
                        "low": 9.8,
                        "close": 10.0,
                        "volume": 100.0,
                        "amount": 100_000.0,
                    },
                    {
                        "symbol": "000001.SZ",
                        "trade_date": pd.Timestamp("2024-01-03"),
                        "open": 10.2,
                        "high": 10.7,
                        "low": 10.0,
                        "close": 10.5,
                        "volume": 200.0,
                        "amount": 210_000.0,
                    },
                ]
            )

    monkeypatch.setattr(alpha101_module, "get_market_data_store", lambda: DummyStore())
    monkeypatch.setattr(
        alpha101_module,
        "_load_daily_basic_panel",
        lambda symbols, start_date, end_date: pd.DataFrame(),
    )
    monkeypatch.setattr(
        alpha101_module,
        "_load_industry_map",
        lambda symbols: pd.DataFrame({"symbol": ["000001.SZ"], "industry": ["BANK"]}),
    )

    frame = alpha101_module.build_alpha101_input_frame(
        ["000001.SZ"],
        date(2024, 1, 2),
        date(2024, 1, 3),
        lookback_days=0,
    )

    assert np.isclose(frame.loc[("000001.SZ", date(2024, 1, 2)), "vwap"], 10.0)
    assert np.isclose(frame.loc[("000001.SZ", date(2024, 1, 3)), "vwap"], 10.5)


def test_alpha101_wide_matches_legacy_core_formulas() -> None:
    panel = _alpha_input_frame(periods=320, symbol_count=12)
    legacy = Alphas(panel)
    wide = WideAlphas(panel)

    for factor_name in sorted(SUPPORTED_WIDE_ALPHA101):
        alpha_number = int(factor_name.rsplit("_", 1)[1])
        expected = getattr(legacy, f"alpha_{alpha_number}")().rename("legacy")
        actual = wide.compute_series(factor_name).rename("wide")
        joined = pd.concat([expected, actual], axis=1).dropna()

        assert len(joined) > 0
        max_abs = (joined["legacy"] - joined["wide"]).abs().max()
        assert max_abs < 0.1
        if max_abs > 1e-9:
            assert joined["legacy"].corr(joined["wide"]) > 0.999


def test_alpha101_precompute_continues_after_factor_error(monkeypatch) -> None:
    class DummyStore:
        def __init__(self) -> None:
            self.frame = pd.DataFrame()

        def append(self, frame: pd.DataFrame) -> int:
            self.frame = pd.concat([self.frame, frame], ignore_index=True)
            return len(frame)

        def batch_writer(self):
            class Writer:
                def __init__(self, store: DummyStore) -> None:
                    self.store = store
                    self.counts: dict[str, int] = {}
                    self.written = 0

                def write_frame(self, frame: pd.DataFrame) -> int:
                    if "factor_name" in frame.columns:
                        for factor_name, count in frame["factor_name"].astype(str).value_counts().items():
                            self.counts[str(factor_name)] = self.counts.get(str(factor_name), 0) + int(count)
                    written = self.store.append(frame)
                    self.written += written
                    return written

            return Writer(self)

    panel = _alpha_input_frame(periods=20)

    def fake_build_frame(*args, **kwargs) -> pd.DataFrame:
        return panel

    def fake_compute(data: pd.DataFrame, factor_name: str) -> pd.Series:
        if factor_name == "alpha101_008":
            raise RuntimeError("broken formula")
        index = data.index
        return pd.Series(range(1, len(index) + 1), index=index, dtype=float)

    monkeypatch.setattr(alpha101_module, "build_alpha101_input_frame", fake_build_frame)
    monkeypatch.setattr(alpha101_module, "compute_alpha101_factor_series", fake_compute)
    monkeypatch.setattr(
        "app.services.alpha101_wide_calculator.is_alpha101_wide_supported",
        lambda factor_name: False,
    )
    store = DummyStore()

    result = alpha101_module.precompute_alpha101_factors(
        factor_names=["alpha101_007", "alpha101_008", "alpha101_009"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        symbols=["000001.SZ", "600519.SH"],
        store=store,
    )

    assert result["rows"]["alpha101_007"] > 0
    assert result["rows"]["alpha101_008"] == 0
    assert result["rows"]["alpha101_009"] > 0
    assert result["failed_factor_names"] == ["alpha101_008"]
    assert "broken formula" in result["errors"]["alpha101_008"]
