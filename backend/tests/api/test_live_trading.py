from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import settings
from app.db.models.live_trading import LiveStrategyProfile, LiveTradeRecord
from app.db.models.strategy import Strategy
from app.db.sqlite import async_session_factory, init_db
from app.main import app
from app.services.live_trading import StrategyProfileBundle, live_trading_service


STABLE_CODE = """
FACTOR_CONFIGS = [{"factor_name": "pe_ttm", "weight": 1.0}]
FILTER_FACTORS = ["is_st", "is_suspend"]
CASH_AWARE_TWO_STAGE_REBALANCE = True
CASH_EXECUTION_RESERVE_PCT = 0.12
CASH_AWARE_BUY_FEE_BUFFER_PCT = 0.003
REQUIRE_CURRENT_MARKET_DATA_FOR_ORDERS = True
ENABLE_LIMIT_UP_HEAT_FILTER = True
MISSING_GUARD_ENABLED = True
"""

AGGRESSIVE_CODE = """
FACTOR_CONFIGS = [{"factor_name": "momentum_20d", "weight": 1.0}]
FILTER_FACTORS = ["is_st"]
CASH_AWARE_TWO_STAGE_REBALANCE = False
CASH_EXECUTION_RESERVE_PCT = 0.08
CASH_AWARE_BUY_FEE_BUFFER_PCT = 0.002
REQUIRE_CURRENT_MARKET_DATA_FOR_ORDERS = True
ENABLE_LIMIT_UP_HEAT_FILTER = False
MISSING_GUARD_ENABLED = True
"""

EXTRA_CODE = """
FACTOR_CONFIGS = [{"factor_name": "turnover_rate", "weight": 1.0}]
FILTER_FACTORS = []
CASH_AWARE_TWO_STAGE_REBALANCE = True
CASH_EXECUTION_RESERVE_PCT = 0.1
CASH_AWARE_BUY_FEE_BUFFER_PCT = 0.001
REQUIRE_CURRENT_MARKET_DATA_FOR_ORDERS = False
ENABLE_LIMIT_UP_HEAT_FILTER = False
"""


async def _prepare_live_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "gaoshou.db"
    monkeypatch.setattr(settings, "database_url", f"sqlite+aiosqlite:///{db_path.as_posix()}")
    monkeypatch.setattr(settings, "debug", False)
    monkeypatch.setattr("app.db.sqlite.apply_dev_data_mode_to_settings", lambda: None)
    monkeypatch.setattr("app.main.apply_dev_data_mode_to_settings", lambda: None)
    await init_db()
    async with async_session_factory() as session:
        session.add_all(
            [
                Strategy(
                    id=62,
                    name="[TSMF] 科技主线小市值多因子-当前最优CashAware",
                    code=STABLE_CODE,
                    parameters={"top_n": 8, "max_position_pct": 0.06},
                    description="stable",
                ),
                Strategy(
                    id=63,
                    name="[TSMF] 科技主线小市值多因子-进攻档MissingGuard",
                    code=AGGRESSIVE_CODE,
                    parameters={"top_n": 10, "max_position_pct": 0.08},
                    description="aggressive",
                ),
                Strategy(
                    id=70,
                    name="[TSMF] Demo Live Strategy",
                    code=EXTRA_CODE,
                    parameters={"top_n": 6, "max_position_pct": 0.05},
                    description="demo",
                ),
            ]
        )
        await session.commit()
    await live_trading_service.stop_runner()
    await live_trading_service.ensure_default_profiles()


@pytest.mark.asyncio
async def test_grid_trading_routes_return_404():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        status_resp = await client.get("/api/grid-trading/status")
        signals_resp = await client.post("/api/grid-trading/signals", json={"params": {}})

    assert status_resp.status_code == 404
    assert signals_resp.status_code == 404


@pytest.mark.asyncio
async def test_live_trading_profiles_seed_and_crud(monkeypatch, tmp_path):
    await _prepare_live_db(monkeypatch, tmp_path)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/live-trading/strategy-profiles")
        body = resp.json()
        profiles = body["data"]
        keys = {item["profile_key"] for item in profiles}
        assert {"tsmf_cashaware_stable", "tsmf_cashaware_aggressive"}.issubset(keys)
        assert next(item for item in profiles if item["profile_key"] == "tsmf_cashaware_stable")["is_default"] is True

        create_resp = await client.post(
            "/api/live-trading/strategy-profiles",
            json={
                "strategy_id": 70,
                "profile_key": "demo_live_profile",
                "display_name": "Demo Live Profile",
                "enabled": True,
                "is_default": False,
                "adapter_type": "multi_factor_cash_aware",
                "params_override": {"cash_execution_reserve_pct": 0.11},
                "universe_config": {"type": "strategy"},
                "execution_policy": {
                    "allow_auto_trade": True,
                    "allow_manual_submit": True,
                    "allow_live_submit": False,
                },
            },
        )
        created = create_resp.json()["data"]
        assert created["profile_key"] == "demo_live_profile"
        assert created["enabled"] is True

        default_resp = await client.put(
            "/api/live-trading/strategy-profiles/demo_live_profile",
            json={"is_default": True},
        )
        defaulted = default_resp.json()["data"]
        assert defaulted["is_default"] is True

        disable_resp = await client.put(
            "/api/live-trading/strategy-profiles/demo_live_profile",
            json={"enabled": False},
        )
        disabled = disable_resp.json()["data"]
        assert disabled["enabled"] is False

        resp = await client.get("/api/live-trading/strategy-profiles")
        profiles = resp.json()["data"]
        assert next(item for item in profiles if item["profile_key"] == "demo_live_profile")["enabled"] is False
        assert next(item for item in profiles if item["profile_key"] == "demo_live_profile")["is_default"] is True
        assert next(item for item in profiles if item["profile_key"] == "tsmf_cashaware_stable")["is_default"] is False


@pytest.mark.asyncio
async def test_disabled_profile_blocks_signals_and_runner_and_writes_audit(monkeypatch, tmp_path):
    await _prepare_live_db(monkeypatch, tmp_path)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.put(
            "/api/live-trading/strategy-profiles/tsmf_cashaware_stable",
            json={"enabled": False},
        )
        signals_resp = await client.post(
            "/api/live-trading/signals",
            json={
                "profile_key": "tsmf_cashaware_stable",
                "mode": "paper",
                "params": {"trade_date": "2026-05-20"},
            },
        )
        runner_resp = await client.post(
            "/api/live-trading/runner/start",
            json={
                "profile_key": "tsmf_cashaware_stable",
                "mode": "paper",
                "params": {},
                "interval_seconds": 60,
            },
        )
        audits_resp = await client.get(
            "/api/live-trading/orders/audit",
            params={"profile_key": "tsmf_cashaware_stable", "limit": 5},
        )

    assert signals_resp.status_code == 400
    assert "disabled" in signals_resp.json()["detail"]
    assert runner_resp.status_code == 400
    assert "disabled" in runner_resp.json()["detail"]
    audits = audits_resp.json()["data"]
    assert audits[0]["status"] == "blocked"
    assert "disabled" in audits[0]["skip_reason"]


def test_lunch_break_blocks_runner_but_not_manual_signal_generation():
    phase = {"phase": "lunch_break", "note": "午间休市，runner 会等待下午交易窗口。"}

    assert live_trading_service._phase_signal_block(phase) is None
    assert live_trading_service._phase_runner_block(phase) is None


@pytest.mark.asyncio
async def test_cashaware_parser_and_duplicate_paper_audit(monkeypatch, tmp_path):
    await _prepare_live_db(monkeypatch, tmp_path)
    bundle = await live_trading_service._load_profile_bundle("tsmf_cashaware_stable")
    assert bundle.constants["CASH_AWARE_TWO_STAGE_REBALANCE"] is True
    assert bundle.params["cash_execution_reserve_pct"] == 0.12
    assert bundle.params["cash_aware_buy_fee_buffer_pct"] == 0.003
    assert bundle.params["factor_configs"][0]["factor_name"] == "pe_ttm"
    assert bundle.params["filter_factors"] == ["is_st", "is_suspend"]

    order = {
        "profile_key": bundle.profile.profile_key,
        "strategy_id": bundle.profile.strategy_id,
        "strategy_name": bundle.strategy.name,
        "signal_hash": "hash-duplicate-001",
        "trade_date": "2026-05-20",
        "symbol": "600519.SH",
        "side": "BUY",
        "quantity": 100,
        "reference_price": 100.0,
        "remark": "paper test",
    }

    first = await live_trading_service.submit_orders([order], mode="paper", confirm=True, trigger_source="auto")
    second = await live_trading_service.submit_orders([order], mode="paper", confirm=True, trigger_source="auto")
    audits = await live_trading_service.list_audits(limit=10, profile_key="tsmf_cashaware_stable")

    assert first["submitted"] is True
    assert second["submitted"] is False
    assert second["duplicate"] is True
    assert any(item["status"] == "paper_filled" for item in audits)
    assert any(item["status"] == "duplicate" for item in audits)


def test_live_execution_filter_time_uses_filter_timer_and_early_completed_minute(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return cls(2026, 6, 25, 9, 35, 30)

    monkeypatch.setattr("app.services.live_trading.datetime", FixedDatetime)

    filters = [
        {"name": "is_paused", "as_of_time": "10:30", "params": {"time": "10:30"}},
        {"name": "is_limit_up", "as_of_time": "10:30", "params": {"time": "10:30"}},
    ]

    timer_text = live_trading_service._live_execution_filter_time(
        params={},
        filters=filters,
        trade_date=date(2026, 6, 25),
    )
    adjusted_params, adjusted_filters = live_trading_service._apply_live_execution_filter_time(
        params={},
        filters=filters,
        mode="live",
        trade_date=date(2026, 6, 25),
    )

    assert timer_text == "09:35"
    assert adjusted_params["live_execution_filter_time"] == "09:35"
    assert adjusted_filters[0]["as_of_time"] == "09:35"
    assert adjusted_filters[0]["params"]["time"] == "09:35"


def test_live_execution_filter_time_snaps_to_latest_configured_timer(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return cls(2026, 6, 25, 10, 15, 0)

    monkeypatch.setattr("app.services.live_trading.datetime", FixedDatetime)

    timer_text = live_trading_service._live_execution_filter_time(
        params={"timer_times": ["10:00", "10:30"]},
        filters=[],
        trade_date=date(2026, 6, 25),
    )

    assert timer_text == "10:00"


def test_live_intraday_sync_plan_skips_full_minute_sync() -> None:
    preflight = {
        "dependency_prepare": {
            "sync_plan": {
                "steps": [
                    {"type": "kline_minute", "start_date": "2026-06-26", "end_date": "2026-06-26"},
                    {
                        "type": "kline_minute",
                        "start_date": "2026-06-26",
                        "end_date": "2026-06-26",
                        "timer_times": ["14:30"],
                    },
                    {
                        "type": "tushare_daily",
                        "start_date": "2026-06-26",
                        "end_date": "2026-06-26",
                        "datasets": ["stock_daily_basic", "stock_limit_prices"],
                    },
                ],
                "coverage_gaps": [
                    {"sync_step": "kline_minute", "dependency": "klines_minute"},
                    {"sync_step": "kline_minute", "dependency": "klines_minute_timer", "timer_time": "14:30"},
                    {"sync_step": "tushare_daily", "dependency": "stock_daily_basic"},
                    {"sync_step": "tushare_daily", "dependency": "stock_limit_prices"},
                ],
            }
        }
    }

    plan = live_trading_service._intraday_sync_plan(preflight)

    assert plan is not None
    assert plan["steps"] == [
        {
            "type": "kline_minute",
            "start_date": "2026-06-26",
            "end_date": "2026-06-26",
            "timer_times": ["14:30"],
        },
        {
            "type": "tushare_daily",
            "start_date": "2026-06-26",
            "end_date": "2026-06-26",
            "datasets": ["stock_limit_prices"],
        },
    ]
    assert plan["coverage_gaps"] == [
        {"sync_step": "kline_minute", "dependency": "klines_minute_timer", "timer_time": "14:30"},
        {"sync_step": "tushare_daily", "dependency": "stock_limit_prices"},
    ]


@pytest.mark.asyncio
async def test_sync_closes_cancel_requested_order_missing_from_qmt(monkeypatch, tmp_path):
    await _prepare_live_db(monkeypatch, tmp_path)
    async with async_session_factory() as session:
        session.add(
            LiveTradeRecord(
                record_id="trade-missing-cancel",
                run_id=None,
                profile_key="tsmf_cashaware_aggressive",
                strategy_id=63,
                trade_date=date(2026, 6, 24),
                signal_hash="hash-missing-cancel",
                trigger_source="manual",
                mode="live",
                status="cancel_requested",
                symbol="002587.SZ",
                stock_name="奥拓电子",
                side="BUY",
                quantity=400,
                reference_price=7.18,
                order_value=2872,
                order_id="135266319",
                message="已向 QMT 发送撤单请求，等待确认。",
                order_payload={"symbol": "002587.SZ", "side": "BUY", "quantity": 400},
                result_payload={"submitted": True, "pending": True, "status": "cancel_requested", "order_id": "135266319"},
            )
        )
        await session.commit()

    async def fake_query_order_updates(order_ids):
        assert list(order_ids) == ["135266319"]
        return {"orders": [], "trades": [], "by_order_id": {}}

    monkeypatch.setattr("app.services.live_trading.qmt_trading_service.query_order_updates", fake_query_order_updates)

    result = await live_trading_service.sync_order_status(profile_key="tsmf_cashaware_aggressive", mode="live", limit=10)
    pending = await live_trading_service.list_pending_orders(profile_key="tsmf_cashaware_aggressive", mode="live", sync=False)

    assert result["updated_count"] == 1
    assert result["pending_count"] == 0
    assert result["locally_closed"][0]["record_id"] == "trade-missing-cancel"
    assert pending == []


@pytest.mark.asyncio
async def test_close_local_pending_orders_marks_cancelled(monkeypatch, tmp_path):
    await _prepare_live_db(monkeypatch, tmp_path)
    async with async_session_factory() as session:
        session.add(
            LiveTradeRecord(
                record_id="trade-local-close",
                run_id=None,
                profile_key="tsmf_cashaware_aggressive",
                strategy_id=63,
                trade_date=date(2026, 6, 24),
                signal_hash="hash-local-close",
                trigger_source="manual",
                mode="live",
                status="cancel_requested",
                symbol="002835.SZ",
                stock_name="同为股份",
                side="BUY",
                quantity=300,
                reference_price=12.12,
                order_value=3636,
                order_id="135266309",
                message="已向 QMT 发送撤单请求，等待确认。",
                order_payload={"symbol": "002835.SZ", "side": "BUY", "quantity": 300},
                result_payload={"submitted": True, "pending": True, "status": "cancel_requested", "order_id": "135266309"},
            )
        )
        await session.commit()

    result = await live_trading_service.close_local_pending_orders(
        profile_key="tsmf_cashaware_aggressive",
        mode="live",
        record_ids=["trade-local-close"],
        reason="unit_test_client_cancelled",
        confirm=True,
    )
    pending = await live_trading_service.list_pending_orders(profile_key="tsmf_cashaware_aggressive", mode="live", sync=False)

    assert result["closed"] is True
    assert result["closed_count"] == 1
    assert pending == []


@pytest.mark.asyncio
async def test_live_mode_guardrails_block_runner_and_order_submit(monkeypatch, tmp_path):
    await _prepare_live_db(monkeypatch, tmp_path)

    fake_profile = LiveStrategyProfile(
        id=999,
        strategy_id=999,
        profile_key="manual_live_profile",
        display_name="Manual Live Profile",
        description=None,
        enabled=True,
        is_default=True,
        adapter_type="multi_factor_cash_aware",
        params_override={},
        universe_config={"type": "strategy"},
        execution_policy={},
    )
    fake_strategy = Strategy(
        id=999,
        name="Manual Live Strategy",
        code="FACTOR_CONFIGS = []\nFILTER_FACTORS = []\n",
        parameters={},
        description=None,
    )
    async with async_session_factory() as session:
        session.add(fake_strategy)
        await session.commit()
    fake_bundle = StrategyProfileBundle(profile=fake_profile, strategy=fake_strategy, constants={}, params={})

    async def fake_load_profile_bundle(_profile_key):
        return fake_bundle

    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fake_load_profile_bundle)

    submit_resp = await live_trading_service.submit_orders(
        [
            {
                "profile_key": fake_profile.profile_key,
                "strategy_id": fake_profile.strategy_id,
                "strategy_name": fake_strategy.name,
                "signal_hash": "hash-live-guardrail",
                "trade_date": "2026-05-20",
                "symbol": "600519.SH",
                "side": "BUY",
                "quantity": 100,
                "reference_price": 100.0,
            }
        ],
        mode="live",
        confirm=True,
        trigger_source="manual",
    )

    with pytest.raises(ValueError, match="LIVE_TRADING_ENABLE_ORDER_SUBMIT=false"):
        await live_trading_service.start_runner(
            profile_key=fake_profile.profile_key,
            mode="live",
            params={},
            interval_seconds=60,
        )

    audits = await live_trading_service.list_audits(limit=5, profile_key=fake_profile.profile_key)

    assert submit_resp["submitted"] is False
    assert submit_resp["enabled"] is False
    assert any(item["status"] == "blocked" for item in audits)
