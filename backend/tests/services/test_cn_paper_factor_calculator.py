from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

import app.services.cn_paper_factor_calculator as module
from app.services.factor_value_store import FactorValueBatchWriter


class DummyFactorStore:
    def __init__(self) -> None:
        self.frame = pd.DataFrame()

    def append(self, frame: pd.DataFrame) -> int:
        self.frame = pd.concat([self.frame, frame], ignore_index=True)
        return len(frame)

    def write(self, frame: pd.DataFrame) -> int:
        return self.append(frame)

    def batch_writer(self, *, batch_size: int = 50_000) -> FactorValueBatchWriter:
        return FactorValueBatchWriter(self, batch_size=batch_size)


class TinyBatchFactorStore(DummyFactorStore):
    def batch_writer(self, *, batch_size: int = 50_000) -> FactorValueBatchWriter:
        return FactorValueBatchWriter(self, batch_size=3)


def test_factor_batch_writer_splits_large_frames() -> None:
    store = TinyBatchFactorStore()
    writer = store.batch_writer()
    frame = pd.DataFrame({
        "symbol": [f"00000{index}.SZ" for index in range(6)],
        "trade_date": [date(2024, 1, 2)] * 6,
        "as_of_time": [""] * 6,
        "factor_name": ["paper_pb_roe_residual"] * 6,
        "params_hash": ["empty"] * 6,
        "value": [float(index) for index in range(6)],
        "source": ["test"] * 6,
        "created_at": [pd.Timestamp("2024-01-02")] * 6,
    })

    written = writer.write_frame(frame)

    assert written == 6
    assert writer.written == 6
    assert writer.counts["paper_pb_roe_residual"] == 6
    assert len(store.frame) == 6


def _paper_daily_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = pd.date_range("2024-01-01", periods=300, freq="B").date
    symbols = [f"00000{index}.SZ" for index in range(1, 9)]
    daily_rows: list[dict[str, object]] = []
    basic_rows: list[dict[str, object]] = []
    financial_rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(symbols, start=1):
        for day_index, trade_date in enumerate(dates, start=1):
            close = 10 + symbol_index * 2 + day_index * (0.02 + symbol_index * 0.002)
            daily_rows.append({
                "symbol": symbol,
                "trade_date": trade_date,
                "open": close * (1 + 0.001 * ((day_index % 5) - 2)),
                "high": close * 1.02,
                "low": close * 0.98,
                "close": close,
                "volume": 1000 + symbol_index * 120 + day_index * (3 + symbol_index),
                "amount": close * (1000 + day_index * 3) * 100,
                "turnover_rate": 1.0 + symbol_index * 0.15 + (day_index % 20) * 0.01,
            })
            basic_rows.append({
                "symbol": symbol,
                "trade_date": trade_date,
                "total_mv": 1_000_000 + symbol_index * 120_000 + day_index * 600,
                "circ_mv": 800_000 + symbol_index * 90_000 + day_index * 500,
                "turnover_rate": 1.0 + symbol_index * 0.15 + (day_index % 20) * 0.01,
                "pe_ttm": 8 + symbol_index * 3 + day_index * 0.01,
                "pb": 0.9 + symbol_index * 0.25 + day_index * 0.001,
            })
        for quarter_index, report_date in enumerate(pd.date_range("2023-03-31", periods=8, freq="QE").date, start=1):
            financial_rows.append({
                "symbol": symbol,
                "report_date": pd.Timestamp(report_date),
                "ann_date": pd.Timestamp(report_date) + pd.Timedelta(days=20),
                "roe": 5 + symbol_index * 2 + quarter_index * 0.2,
                "revenue": 100_000 + symbol_index * 20_000 + quarter_index * 5_000,
                "net_profit": 8_000 + symbol_index * 1_500 + quarter_index * 300,
                "revenue_yoy": 8 + symbol_index * 2 + quarter_index,
                "profit_yoy": 6 + symbol_index * 1.5 + quarter_index,
                "gross_margin": 20 + symbol_index * 3 + quarter_index * 0.4,
                "total_assets": 300_000 + symbol_index * 50_000 + quarter_index * 10_000,
                "total_liability": 120_000 + symbol_index * 20_000 + quarter_index * 3_000,
                "total_equity": 180_000 + symbol_index * 30_000 + quarter_index * 7_000,
            })
    return pd.DataFrame(daily_rows), pd.DataFrame(basic_rows), pd.DataFrame(financial_rows)


def test_align_financial_asof_normalizes_datetime_units() -> None:
    daily = pd.DataFrame({
        "symbol": ["000001.SZ", "000001.SZ"],
        "trade_date": np.array(["2024-04-01", "2024-04-02"], dtype="datetime64[s]"),
        "close": [10.0, 10.2],
    })
    financial = pd.DataFrame({
        "symbol": ["000001.SZ"],
        "report_date": np.array(["2024-03-31"], dtype="datetime64[us]"),
        "ann_date": np.array(["2024-04-01"], dtype="datetime64[us]"),
        "roe": [12.5],
        "revenue": [100_000.0],
        "net_profit": [9_000.0],
    })

    result = module._align_financial_asof(daily, financial)

    assert result["roe"].tolist() == [12.5, 12.5]
    assert "trade_date_ts" not in result.columns


def test_align_financial_asof_uses_announcement_date() -> None:
    daily = pd.DataFrame({
        "symbol": ["000001.SZ", "000001.SZ", "000001.SZ"],
        "trade_date": [date(2024, 3, 31), date(2024, 4, 1), date(2024, 4, 30)],
        "close": [10.0, 10.1, 10.2],
    })
    financial = pd.DataFrame({
        "symbol": ["000001.SZ"],
        "report_date": [pd.Timestamp("2024-03-31")],
        "ann_date": [pd.Timestamp("2024-04-30")],
        "roe": [12.5],
        "revenue": [100_000.0],
        "net_profit": [9_000.0],
    })

    result = module._align_financial_asof(daily, financial)

    assert result["roe"].isna().tolist() == [True, True, False]
    assert result.loc[2, "roe"] == 12.5


def test_precompute_cn_paper_daily_factors(monkeypatch) -> None:
    daily, daily_basic, financial = _paper_daily_inputs()
    monkeypatch.setattr(module, "_load_daily", lambda *args, **kwargs: daily)
    monkeypatch.setattr(module, "_load_daily_basic", lambda *args, **kwargs: daily_basic)
    monkeypatch.setattr(module, "_load_financial", lambda *args, **kwargs: financial)
    monkeypatch.setattr(
        module,
        "_load_stock_metadata",
        lambda symbols: pd.DataFrame({
            "symbol": list(symbols),
            "industry": [f"IND{index % 4}" for index, _ in enumerate(symbols)],
            "industry2": "",
            "industry3": "",
            "sector": "",
        }),
    )
    store = DummyFactorStore()

    result = module.precompute_cn_paper_factors(
        factor_names=[
            "paper_pb_roe_residual",
            "paper_composite_value",
            "paper_growth_quality_score",
            "paper_financial_health_score",
            "tsmf_recent_effective_score",
            "paper_overnight_turnover_corr",
            "paper_rsi_reversal_score",
            "paper_new_high_anchor",
            "paper_high_low_volume_event",
            "paper_reversal_20d",
            "paper_size_rotation_score",
            "paper_value_growth_rotation_score",
            "paper_industry_momentum_20d",
            "paper_defensive_quality_lowvol",
            "paper_asset_allocation_proxy",
        ],
        start_date=date(2024, 12, 2),
        end_date=date(2025, 2, 21),
        symbols=[f"00000{index}.SZ" for index in range(1, 9)],
        store=store,
    )

    assert result["rows_written"] == len(store.frame)
    assert result["rows"]["paper_pb_roe_residual"] > 0
    assert result["rows"]["paper_overnight_turnover_corr"] > 0
    assert result["rows"]["paper_new_high_anchor"] > 0
    assert result["rows"]["tsmf_recent_effective_score"] > 0
    assert result["rows"]["paper_size_rotation_score"] > 0
    assert result["rows"]["paper_value_growth_rotation_score"] > 0
    assert result["rows"]["paper_industry_momentum_20d"] > 0
    assert result["rows"]["paper_defensive_quality_lowvol"] > 0
    assert result["rows"]["paper_asset_allocation_proxy"] > 0
    assert set(store.frame["factor_name"]).issuperset({
        "paper_composite_value",
        "paper_growth_quality_score",
        "tsmf_recent_effective_score",
        "paper_high_low_volume_event",
        "paper_size_rotation_score",
        "paper_industry_momentum_20d",
    })
    assert store.frame["value"].replace([np.inf, -np.inf], np.nan).notna().all()


def test_precompute_cn_paper_minute_factors(monkeypatch) -> None:
    minute_rows: list[dict[str, object]] = []
    days = pd.date_range("2024-01-02", periods=8, freq="B")
    for symbol in ["000001.SZ", "000002.SZ"]:
        for day_index, trade_day in enumerate(days, start=1):
            for minute_index, stamp in enumerate(pd.date_range(f"{trade_day.date()} 09:31", periods=40, freq="min"), start=1):
                base_volume = 100 + minute_index
                volume = base_volume * (5 if day_index >= 6 and minute_index in {10, 20, 30} else 1)
                open_price = 10 + day_index * 0.1 + minute_index * 0.001
                close = open_price + (0.03 if minute_index % 3 else -0.02)
                minute_rows.append({
                    "symbol": symbol,
                    "datetime": stamp,
                    "open": open_price,
                    "close": close,
                    "volume": volume,
                })
    minute = pd.DataFrame(minute_rows)
    monkeypatch.setattr(module, "_load_minute", lambda *args, **kwargs: minute.assign(trade_date=minute["datetime"].dt.date))
    store = DummyFactorStore()

    result = module.precompute_cn_paper_factors(
        factor_names=["paper_trend_fund_vwap_ratio", "paper_trend_fund_support"],
        start_date=date(2024, 1, 9),
        end_date=date(2024, 1, 11),
        symbols=["000001.SZ", "000002.SZ"],
        store=store,
    )

    assert result["rows"]["paper_trend_fund_vwap_ratio"] > 0
    assert result["rows"]["paper_trend_fund_support"] > 0
    assert set(store.frame["factor_name"]) == {"paper_trend_fund_vwap_ratio", "paper_trend_fund_support"}
