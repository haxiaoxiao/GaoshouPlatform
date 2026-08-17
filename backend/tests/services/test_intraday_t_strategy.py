from datetime import datetime, time

import pandas as pd
import pytest

from app.services.intraday_t_strategy import (
    CostModel,
    Fill,
    IntradayTStrategy,
    MarketSnapshot,
    StrategyParams,
    SymbolDayState,
    TDirection,
    TState,
    compute_intraday_features,
    estimate_round_trip_cost_bps,
    normalize_buy_quantity,
    normalize_sell_quantity,
)


def _snapshot(**overrides) -> MarketSnapshot:
    values = {
        "symbol": "603629.SH",
        "price": 20.0,
        "vwap": 20.2,
        "zscore": -2.0,
        "previous_zscore": -2.3,
        "fast_ema": 20.0,
        "slow_ema": 20.02,
        "vwap_slope": -0.0002,
        "volume_ratio": 1.0,
        "estimated_edge_bps": 100.0,
        "previous_price": 19.98,
        "realized_vol_bps": 30.0,
        "session_return_bps": 0.0,
        "limit_price_available": True,
        "at_price_limit": False,
    }
    values.update(overrides)
    return MarketSnapshot(**values)


def _state(symbol: str = "603629.SH") -> SymbolDayState:
    return SymbolDayState(
        symbol=symbol,
        opening_quantity=2_000,
        opening_sellable=2_000,
        current_quantity=2_000,
        sellable_remaining=2_000,
    )


def test_exchange_quantity_rules_cover_main_board_and_star_market():
    assert normalize_buy_quantity("603629.SH", 299) == 200
    assert normalize_buy_quantity("688008.SH", 199) == 0
    assert normalize_buy_quantity("688008.SH", 237) == 237
    assert normalize_sell_quantity("603629.SH", 299, available=500) == 200
    assert normalize_sell_quantity("688008.SH", 237, available=500) == 237
    assert normalize_sell_quantity("688008.SH", 137, available=137) == 137


def test_cost_estimate_includes_minimum_commission_and_sell_taxes():
    cost = CostModel(
        commission_rate=0.0003,
        min_commission=5.0,
        stamp_duty_rate=0.0005,
        transfer_fee_rate=0.00001,
        slippage_bps=2.0,
    )

    assert estimate_round_trip_cost_bps(price=20.0, quantity=200, cost=cost) == pytest.approx(
        34.2, rel=1e-3
    )


def test_positive_t_round_trip_restores_position_without_replenishing_sellable_inventory():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))
    state = _state()

    entry = strategy.decide(_snapshot(), state, datetime(2026, 7, 14, 10, 5))
    assert entry is not None
    assert entry.direction is TDirection.POSITIVE
    assert entry.side == "BUY"
    assert entry.quantity == 500

    strategy.apply_fill(
        state,
        Fill.from_intent(entry, fill_price=20.01, filled_at=datetime(2026, 7, 14, 10, 6)),
    )
    assert state.state is TState.POSITIVE_T_OPEN
    assert state.current_quantity == 2_500
    assert state.sellable_remaining == 2_000

    exit_intent = strategy.decide(
        _snapshot(zscore=-0.1, previous_zscore=-0.4, price=20.25, estimated_edge_bps=0),
        state,
        datetime(2026, 7, 14, 10, 30),
    )
    assert exit_intent is not None
    assert exit_intent.side == "SELL"

    strategy.apply_fill(
        state,
        Fill.from_intent(exit_intent, fill_price=20.24, filled_at=datetime(2026, 7, 14, 10, 31)),
    )
    assert state.state is TState.READY
    assert state.current_quantity == state.opening_quantity
    assert state.sellable_remaining == 1_500
    assert state.completed_pairs == 1


def test_positive_t_requires_enough_opening_sellable_inventory_for_restoration():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))
    state = _state()
    state.opening_sellable = 0
    state.sellable_remaining = 0

    assert strategy.decide(_snapshot(), state, datetime(2026, 7, 14, 10, 5)) is None


def test_reverse_t_round_trip_sells_only_opening_sellable_position_and_restores_base():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))
    state = _state("688008.SH")

    entry = strategy.decide(
        _snapshot(
            symbol="688008.SH",
            zscore=2.1,
            previous_zscore=2.4,
            price=72.0,
            previous_price=72.1,
            vwap=71.2,
            fast_ema=71.8,
            slow_ema=71.75,
        ),
        state,
        datetime(2026, 7, 14, 10, 10),
    )
    assert entry is not None
    assert entry.direction is TDirection.REVERSE
    assert entry.side == "SELL"
    assert entry.quantity == 500

    strategy.apply_fill(
        state,
        Fill.from_intent(entry, fill_price=71.99, filled_at=datetime(2026, 7, 14, 13, 21)),
    )
    assert state.current_quantity == 1_500
    assert state.sellable_remaining == 1_500

    cover = strategy.decide(
        _snapshot(
            symbol="688008.SH",
            zscore=0.1,
            previous_zscore=0.5,
            price=71.2,
            vwap=71.25,
            estimated_edge_bps=0,
        ),
        state,
        datetime(2026, 7, 14, 10, 20),
    )
    assert cover is not None
    assert cover.side == "BUY"
    strategy.apply_fill(
        state,
        Fill.from_intent(cover, fill_price=71.21, filled_at=datetime(2026, 7, 14, 14, 3)),
    )
    assert state.current_quantity == state.opening_quantity
    assert state.sellable_remaining == 1_500
    assert state.completed_pairs == 1


def test_entry_is_rejected_outside_window_for_insufficient_edge_and_strong_trend():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))

    assert strategy.decide(_snapshot(), _state(), datetime(2026, 7, 14, 9, 40)) is None
    assert (
        strategy.decide(_snapshot(estimated_edge_bps=20.0), _state(), datetime(2026, 7, 14, 10, 5))
        is None
    )
    assert (
        strategy.decide(
            _snapshot(fast_ema=19.6, slow_ema=20.1, vwap_slope=-0.002, volume_ratio=2.0),
            _state(),
            datetime(2026, 7, 14, 10, 5),
        )
        is None
    )


def test_open_pair_is_forced_to_restore_near_close():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))
    state = _state()
    entry = strategy.decide(_snapshot(), state, datetime(2026, 7, 14, 10, 5))
    assert entry is not None
    strategy.apply_fill(
        state,
        Fill.from_intent(entry, fill_price=20.0, filled_at=datetime(2026, 7, 14, 10, 6)),
    )

    restore = strategy.decide(
        _snapshot(zscore=-1.8, previous_zscore=-1.9),
        state,
        datetime(2026, 7, 14, 14, 49),
    )
    assert restore is not None
    assert restore.side == "SELL"
    assert restore.reason == "force_restore"
    assert restore.quantity == 500


def test_open_pair_restores_on_stop_loss_and_before_lunch():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))
    stop_state = _state()
    stop_entry = strategy.decide(_snapshot(), stop_state, datetime(2026, 7, 14, 10, 5))
    assert stop_entry is not None
    strategy.apply_fill(
        stop_state,
        Fill.from_intent(stop_entry, fill_price=20.0, filled_at=datetime(2026, 7, 14, 10, 6)),
    )

    stop = strategy.decide(
        _snapshot(zscore=-3.2, previous_zscore=-3.0, price=19.4),
        stop_state,
        datetime(2026, 7, 14, 10, 20),
    )
    assert stop is not None
    assert stop.side == "SELL"
    assert stop.reason == "risk_restore"

    lunch_state = _state()
    lunch_entry = strategy.decide(_snapshot(), lunch_state, datetime(2026, 7, 14, 10, 5))
    assert lunch_entry is not None
    strategy.apply_fill(
        lunch_state,
        Fill.from_intent(lunch_entry, fill_price=20.0, filled_at=datetime(2026, 7, 14, 10, 6)),
    )
    lunch_restore = strategy.decide(
        _snapshot(zscore=-1.8, previous_zscore=-1.9),
        lunch_state,
        datetime(2026, 7, 14, 11, 29),
    )
    assert lunch_restore is not None
    assert lunch_restore.reason == "force_restore"


def test_daily_loss_limit_locks_new_entries_but_not_active_restoration():
    strategy = IntradayTStrategy(
        StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0, max_daily_loss_bps=45)
    )
    state = _state()
    state.realized_net_pnl = -200.0

    assert strategy.decide(_snapshot(), state, datetime(2026, 7, 14, 10, 5)) is None
    assert state.state is TState.LOCKED


def test_feature_calculation_is_causal_and_marks_warmup_rows():
    index = pd.date_range("2026-07-14 09:31", periods=40, freq="min")
    frame = pd.DataFrame(
        {
            "open": [20 + i * 0.01 for i in range(40)],
            "high": [20.02 + i * 0.01 for i in range(40)],
            "low": [19.98 + i * 0.01 for i in range(40)],
            "close": [20 + i * 0.01 for i in range(40)],
            "volume": [10_000 + i * 100 for i in range(40)],
            "amount": [(20 + i * 0.01) * (10_000 + i * 100) for i in range(40)],
        },
        index=index,
    )

    features = compute_intraday_features(frame, StrategyParams(warmup_bars=30))

    assert features.loc[index[28], "ready"] == False  # noqa: E712
    assert features.loc[index[29], "ready"] == True  # noqa: E712
    assert features.loc[index[-1], "session_vwap"] > 20
    assert features.loc[index[-1], "realized_vol_bps"] > 0
    assert features.loc[index[-1], "previous_price"] == pytest.approx(frame.loc[index[-2], "close"])
    assert features.loc[index[-1], "session_return_bps"] > 0
    changed = frame.copy()
    changed.loc[index[-1], "close"] = 99
    changed_features = compute_intraday_features(changed, StrategyParams(warmup_bars=30))
    pd.testing.assert_series_equal(
        features.loc[: index[-2], "zscore"], changed_features.loc[: index[-2], "zscore"]
    )


def test_feature_calculation_prefers_reported_amount_for_session_vwap():
    index = pd.date_range("2026-07-14 09:31", periods=2, freq="min")
    frame = pd.DataFrame(
        {
            "close": [10.0, 20.0],
            "volume": [100.0, 100.0],
            "amount": [95_000.0, 190_000.0],
        },
        index=index,
    )

    features = compute_intraday_features(
        frame,
        StrategyParams(warmup_bars=2, volatility_window=2),
    )

    assert features.loc[index[-1], "session_vwap"] == pytest.approx(14.25)


def test_v2_entry_gates_reject_extreme_z_low_volatility_and_price_limits():
    strategy = IntradayTStrategy(
        StrategyParams(
            max_trade_fraction=0.25,
            cooldown_minutes=0,
            max_entry_z=2.4,
            min_realized_vol_bps=20.0,
        )
    )
    now = datetime(2026, 7, 14, 10, 5)

    assert strategy.decide(_snapshot(zscore=-2.4), _state(), now) is None
    assert strategy.decide(_snapshot(realized_vol_bps=19.99), _state(), now) is None
    assert strategy.decide(_snapshot(limit_price_available=False), _state(), now) is None
    assert strategy.decide(_snapshot(at_price_limit=True), _state(), now) is None


def test_v2_default_entry_window_is_right_open_and_disables_afternoon_entries():
    strategy = IntradayTStrategy(StrategyParams(max_trade_fraction=0.25, cooldown_minutes=0))

    assert strategy.decide(_snapshot(), _state(), datetime(2026, 7, 14, 10, 0)) is not None
    assert strategy.decide(_snapshot(), _state(), datetime(2026, 7, 14, 10, 29)) is not None
    assert strategy.decide(_snapshot(), _state(), datetime(2026, 7, 14, 10, 30)) is None
    assert strategy.decide(_snapshot(), _state(), datetime(2026, 7, 14, 13, 10)) is None

    extended = IntradayTStrategy(
        StrategyParams(
            max_trade_fraction=0.25,
            cooldown_minutes=0,
            morning_entry_start=time(9, 45),
            morning_entry_end=time(11, 21),
            allow_afternoon_entries=True,
        )
    )
    assert extended.decide(_snapshot(), _state(), datetime(2026, 7, 14, 13, 10)) is not None


def test_v2_params_require_entry_band_inside_stop_band():
    with pytest.raises(ValueError, match="entry_z < max_entry_z < stop_z"):
        StrategyParams(entry_z=2.5, max_entry_z=2.4, stop_z=3.0)
    with pytest.raises(ValueError, match="entry_z < max_entry_z < stop_z"):
        StrategyParams(entry_z=1.75, max_entry_z=3.0, stop_z=3.0)


def test_strategy_params_require_warmup_not_to_exceed_volatility_window():
    with pytest.raises(ValueError, match="warmup_bars must not exceed volatility_window"):
        StrategyParams(warmup_bars=40, volatility_window=30)
