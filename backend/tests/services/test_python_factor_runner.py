"""Python factor runner isolation and normalization tests."""

import asyncio
import time
from datetime import date

import pandas as pd
import pytest

from app.services import python_factor_runner as runner


def _stub_factor_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "_load_daily_bars", lambda *_args: pd.DataFrame())
    monkeypatch.setattr(runner, "_resolve_trading_days", lambda *_args: [])
    monkeypatch.setattr(runner, "_load_stock_info", lambda *_args: pd.DataFrame())
    monkeypatch.setattr(runner, "_load_financial", lambda *_args: pd.DataFrame())


def test_timeout_terminates_run_instead_of_waiting_for_executor_shutdown(monkeypatch):
    _stub_factor_data(monkeypatch)
    started = time.monotonic()

    result = runner.run_python_factor(
        code="def compute(data, context):\n    while True:\n        pass",
        symbols=["000001.SZ"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        timeout_seconds=0.2,
    )

    assert time.monotonic() - started < 3
    assert result["rows"] == []
    assert result["errors"] == ["执行超时（0.2 秒）"]


def test_runner_executes_supported_code_in_isolated_process(monkeypatch):
    _stub_factor_data(monkeypatch)

    result = runner.run_python_factor(
        code=(
            "def compute(data, context):\n"
            "    return [{'symbol': context['symbols'][0], "
            "'trade_date': context['start_date'], 'value': 1.25}]"
        ),
        symbols=["000001.SZ"],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 31),
        timeout_seconds=5,
    )

    assert result["errors"] == []
    assert result["rows"] == [
        {"symbol": "000001.SZ", "trade_date": "2025-01-02", "value": 1.25}
    ]


def test_normalization_drops_symbols_outside_requested_universe():
    rows, errors = runner._validate_and_normalize(
        [
            {"symbol": "000001.SZ", "trade_date": "2025-01-02", "value": 1},
            {"symbol": "600519.SH", "trade_date": "2025-01-02", "value": 2},
        ],
        {
            "symbols": ["000001.SZ"],
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
        },
    )

    assert errors == []
    assert [row["symbol"] for row in rows] == ["000001.SZ"]


def test_runner_rejects_imports_from_user_code(monkeypatch):
    _stub_factor_data(monkeypatch)

    result = runner.run_python_factor(
        code="import os\ndef compute(data, context):\n    return []",
        symbols=["000001.SZ"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        timeout_seconds=5,
    )

    assert result["rows"] == []
    assert any("import" in error.lower() for error in result["errors"])


@pytest.mark.parametrize(
    "expression",
    [
        "pd.read_pickle('secret.pkl')",
        "pd.io.common.os.system('whoami')",
        "data['daily'].to_pickle('secret.pkl')",
        "pd.DataFrame.__subclasses__()",
        "np.load('secret.npy')",
    ],
)
def test_runner_rejects_module_and_dataframe_file_process_escape_paths(expression):
    code = f"""
def compute(data, context):
    {expression}
    return []
"""

    error = runner.validate_python_factor_source(code)

    assert error is not None
    assert "不允许访问属性" in error


@pytest.mark.asyncio
async def test_async_runner_does_not_block_event_loop(monkeypatch):
    def slow_runner(**_kwargs):
        time.sleep(0.3)
        return {"rows": [], "errors": [], "elapsed_seconds": 0.3}

    monkeypatch.setattr(runner, "run_python_factor", slow_runner)
    started = time.monotonic()

    async def ticker() -> float:
        await asyncio.sleep(0.02)
        return time.monotonic() - started

    _, tick_elapsed = await asyncio.gather(
        runner.run_python_factor_async(
            code="",
            symbols=[],
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
        ),
        ticker(),
    )

    assert tick_elapsed < 0.15
