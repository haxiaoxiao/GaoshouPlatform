import pandas as pd
import pytest

from app.services.intraday_t_backtest import BacktestConfig, IntradayTBacktester
from app.services.intraday_t_strategy import CostModel, StrategyParams

FEATURE_COLUMNS = {
    "vwap": 20.0,
    "zscore": 0.0,
    "previous_zscore": 0.0,
    "fast_ema": 20.0,
    "slow_ema": 20.0,
    "vwap_slope": 0.0,
    "volume_ratio": 1.0,
    "estimated_edge_bps": 100.0,
    "previous_price": 19.95,
    "realized_vol_bps": 30.0,
    "session_return_bps": 0.0,
    "ready": True,
}


def _frame(times: list[str], *, volumes: list[int] | None = None) -> pd.DataFrame:
    rows = []
    for index, value in enumerate(times):
        row = {
            "datetime": pd.Timestamp(f"2026-07-14 {value}"),
            "symbol": "603629.SH",
            "open": 20.0 + index * 0.05,
            "high": 20.1 + index * 0.05,
            "low": 19.9 + index * 0.05,
            "close": 20.0 + index * 0.05,
            "volume": (volumes or [20_000] * len(times))[index],
            "amount": (20.0 + index * 0.05) * (volumes or [20_000] * len(times))[index],
            **FEATURE_COLUMNS,
        }
        rows.append(row)
    return pd.DataFrame(rows).set_index("datetime")


def _config(**overrides) -> BacktestConfig:
    values = {
        "initial_capital": 100_000.0,
        "base_quantities": {"603629.SH": 2_000},
        "params": StrategyParams(
            warmup_bars=2,
            max_trade_fraction=0.25,
            max_pairs_per_day=2,
            cooldown_minutes=0,
        ),
        "cost": CostModel(slippage_bps=2.0),
        "max_bar_volume_fraction": 0.1,
    }
    values.update(overrides)
    return BacktestConfig(**values)


def test_backtest_executes_signals_on_next_bar_and_reports_incremental_pnl():
    frame = _frame(["09:59", "10:00", "10:01", "10:02", "15:00"])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [-0.1, -2.0]

    result = IntradayTBacktester().run(frame, _config())

    assert result["metrics"]["completed_pairs"] == 1
    assert result["metrics"]["restoration_rate"] == 1.0
    assert len(result["trades"]) == 2
    entry, restore = result["trades"]
    assert entry["signal_at"].endswith("10:00:00")
    assert entry["fill_at"].endswith("10:01:00")
    assert entry["side"] == "BUY"
    assert restore["fill_at"].endswith("10:02:00")
    assert restore["side"] == "SELL"
    assert entry["fill_price"] == pytest.approx(frame.loc[entry["fill_at"], "open"] * 1.0002)
    assert restore["fill_price"] == pytest.approx(frame.loc[restore["fill_at"], "open"] * 0.9998)
    assert result["metrics"]["incremental_pnl"] == pytest.approx(
        result["metrics"]["final_equity"] - result["metrics"]["passive_final_equity"]
    )
    assert result["metrics"]["total_fees"] > 10
    assert result["metrics"]["gross_t_pnl"] == pytest.approx(restore["gross_pnl"], abs=1e-4)
    assert result["metrics"]["net_t_pnl"] == pytest.approx(restore["net_pnl"], abs=1e-4)
    assert result["metrics"]["win_rate"] == 1.0
    assert result["metrics"]["profit_loss_ratio"] is None
    assert result["metrics"]["max_drawdown"] <= 0
    assert result["direction_metrics"]["POSITIVE"]["completed_pairs"] == 1
    assert result["direction_metrics"]["REVERSE"]["completed_pairs"] == 0
    assert result["daily_results"][0]["daily_t_pnl"] == pytest.approx(
        result["metrics"]["incremental_pnl"]
    )


def test_backtest_rejects_fill_when_next_bar_volume_cannot_support_full_lot():
    frame = _frame(["10:00", "10:01", "15:00"], volumes=[20_000, 1_000, 20_000])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]

    result = IntradayTBacktester().run(frame, _config(max_bar_volume_fraction=0.1))

    assert result["trades"] == []
    assert result["rejections"][0]["reason"] == "volume_cap"
    assert result["metrics"]["completed_pairs"] == 0


def test_backtest_cancels_a_signal_when_the_next_symbol_bar_is_not_the_next_minute():
    frame = _frame(["10:00", "14:00", "15:00"])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [
        -2.0,
        -2.3,
    ]

    result = IntradayTBacktester().run(frame, _config())

    assert result["trades"] == []
    assert result["rejections"][0]["reason"] == "stale_signal"


def test_backtest_converts_local_lot_volume_to_shares_for_capacity_check():
    frame = _frame(["10:00", "10:01", "10:02", "15:00"], volumes=[200, 100, 100, 200])
    frame["amount"] = frame["close"] * frame["volume"] * 100
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [-0.1, -2.0]

    result = IntradayTBacktester().run(frame, _config(max_bar_volume_fraction=0.1))

    assert result["metrics"]["completed_pairs"] == 1
    assert result["rejections"] == []


def test_backtest_rejects_buy_at_limit_up():
    frame = _frame(["10:00", "10:01", "15:00"])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    limit_up = float(frame.loc[pd.Timestamp("2026-07-14 10:01"), "open"])

    result = IntradayTBacktester().run(
        frame,
        _config(limit_prices={"603629.SH|2026-07-14": {"up": limit_up, "down": 18.0}}),
    )

    assert result["trades"] == []
    assert result["rejections"][0]["reason"] == "limit_up"


def test_backtest_never_applies_slippage_beyond_an_exchange_price_limit():
    frame = _frame(["10:00", "10:01", "10:02", "15:00"])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [
        -2.0,
        -2.3,
    ]
    frame.loc[pd.Timestamp("2026-07-14 10:01"), "open"] = 21.999
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [
        -0.1,
        -2.0,
    ]

    result = IntradayTBacktester().run(
        frame,
        _config(limit_prices={"603629.SH|2026-07-14": {"up": 22.0, "down": 18.0}}),
    )

    assert result["trades"][0]["fill_price"] == 22.0


def test_backtest_rejects_new_reverse_t_when_signal_price_is_at_limit_up():
    frame = _frame(["10:00", "10:01", "15:00"])
    signal_at = pd.Timestamp("2026-07-14 10:00")
    frame.loc[signal_at, ["zscore", "previous_zscore", "previous_price"]] = [2.0, 2.3, 20.1]
    limit_up = float(frame.loc[signal_at, "close"])

    result = IntradayTBacktester().run(
        frame,
        _config(limit_prices={"603629.SH|2026-07-14": {"up": limit_up, "down": 18.0}}),
    )

    assert result["trades"] == []
    assert result["metrics"]["entry_count"] == 0


def test_backtest_blocks_new_entries_when_exact_limit_price_is_required_but_missing():
    frame = _frame(["10:00", "10:01", "15:00"])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [
        -2.0,
        -2.3,
    ]

    result = IntradayTBacktester().run(
        frame,
        _config(require_exact_limit_prices=True),
    )

    assert result["trades"] == []
    assert result["metrics"]["entry_count"] == 0
    assert result["data_quality"]["limit_prices"]["missing_symbol_days"] == ["603629.SH|2026-07-14"]


def test_backtest_forces_an_open_pair_to_restore_before_close():
    frame = _frame(["10:00", "10:01", "14:49", "14:50", "15:00"])
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [-1.9, -2.0]
    frame.loc[pd.Timestamp("2026-07-14 14:49"), ["zscore", "previous_zscore"]] = [-1.8, -1.9]

    result = IntradayTBacktester().run(frame, _config())

    assert result["metrics"]["completed_pairs"] == 1
    assert result["trades"][-1]["reason"] == "force_restore"
    assert result["trades"][-1]["fill_at"].endswith("14:50:00")
    assert result["symbol_summaries"][0]["ending_quantity"] == 2_000


def test_backtest_uses_last_mark_when_one_symbol_has_no_bars_on_a_trade_day():
    first_symbol = _frame(["09:45", "15:00"])
    second_symbol = first_symbol.copy()
    second_symbol["symbol"] = "688008.SH"
    next_day = first_symbol.copy()
    next_day.index = next_day.index + pd.Timedelta(days=1)
    frame = pd.concat([first_symbol, second_symbol, next_day]).sort_index()

    result = IntradayTBacktester().run(
        frame,
        _config(base_quantities={"603629.SH": 2_000, "688008.SH": 1_000}),
    )

    assert result["period"]["trade_days"] == 2
    assert len(result["equity_curve"]) == 2
    assert result["symbol_summaries"][1]["ending_quantity"] == 1_000


def test_unrestored_pair_is_counted_once_when_lock_carries_across_trade_days():
    first_day = _frame(["10:00", "10:01", "15:00"])
    first_day.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    first_day.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [-1.9, -2.0]
    next_day = _frame(["09:31", "09:32", "15:00"])
    next_day.index = next_day.index + pd.Timedelta(days=1)

    result = IntradayTBacktester().run(pd.concat([first_day, next_day]), _config())

    assert result["metrics"]["entry_count"] == 1
    assert result["metrics"]["completed_pairs"] == 1
    assert result["metrics"]["restoration_failures"] == 1
    assert result["metrics"]["restoration_rate"] == 0.0
    assert result["metrics"]["open_pairs_at_end"] == 0
    assert result["trades"][-1]["fill_at"].endswith("09:32:00")
    assert result["trades"][-1]["reason"] == "force_restore"


def test_cross_day_force_restore_tolerates_nullable_unready_features():
    first_day = _frame(["10:00", "10:01", "15:00"])
    first_day.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    first_day.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [-1.9, -2.0]
    next_day = _frame(["09:31", "09:32", "15:00"])
    next_day.index = next_day.index + pd.Timedelta(days=1)
    nullable_features = FEATURE_COLUMNS.keys() - {"ready"}
    for column in nullable_features:
        next_day[column] = pd.array([pd.NA] * len(next_day), dtype="Float64")
    next_day["ready"] = False

    result = IntradayTBacktester().run(pd.concat([first_day, next_day]), _config())

    assert result["metrics"]["completed_pairs"] == 1
    assert result["metrics"]["open_pairs_at_end"] == 0
    assert result["trades"][-1]["fill_at"].endswith("09:32:00")
    assert result["trades"][-1]["reason"] == "force_restore"
