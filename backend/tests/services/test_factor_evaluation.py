"""因子评估服务测试"""
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from app.services.factor_evaluation import FactorEvaluationService


def _make_fake_ohlcv(symbols, n_days=100):
    """生成模拟 OHLCV 数据，与 ClickHouse execute() 返回格式一致"""
    np.random.seed(42)
    start = date(2024, 1, 2)
    rows = []
    for sym in symbols:
        for i in range(n_days):
            rows.append((
                sym,
                start,
                10.0 + 0.01 * i,    # open
                11.0 + 0.01 * i,    # high
                9.0 + 0.01 * i,     # low
                10.5 + 0.01 * i,    # close
                1_000_000 + i * 100,  # volume
                10_500_000.0 + i * 1000,  # amount
                0.01 + 0.001 * i,   # turnover_rate
            ))
    return rows


def _make_fake_close_data(symbols, n_days=100):
    """生成模拟收盘价数据"""
    np.random.seed(42)
    start = date(2024, 1, 2)
    rows = []
    for sym in symbols:
        for i in range(n_days):
            rows.append((sym, start, 10.5 + 0.01 * i))
    return rows


@pytest.fixture
def mock_ch_client():
    """Mock ClickHouse 客户端 — 返回固定数据"""
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    ohlcv_rows = _make_fake_ohlcv(symbols, 100)
    close_rows = _make_fake_close_data(symbols, 100)

    mock_client = MagicMock()
    # Return ohlcv for the factor matrix query, close for return matrix query
    mock_client.execute.side_effect = [ohlcv_rows, close_rows]
    return mock_client


@pytest.fixture
def mock_evaluate_expression():
    """Mock evaluate_expression 返回已知因子值"""
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    dates = pd.date_range("2024-01-02", periods=100, freq="B")[:97]

    result = {}
    for sym in symbols:
        np.random.seed(42)
        values = np.random.randn(len(dates))
        result[sym] = pd.Series(values, index=dates)

    with patch("app.services.factor_evaluation.evaluate_expression", return_value=result):
        yield


@pytest.mark.asyncio
async def test_run_ic_analysis_structure(mock_ch_client, mock_evaluate_expression):
    """IC 分析返回结构完整性"""
    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_ch_client):
        service = FactorEvaluationService()
        result = await service.run_ic_analysis(
            expression="Mean($close, 5)",
            symbols=["000001.SZ", "000002.SZ", "000003.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
        )

    # IC 序列
    assert "ic_series" in result
    assert isinstance(result["ic_series"], list)

    # IC 统计量
    assert "ic_stats" in result
    stats = result["ic_stats"]
    assert "mean" in stats
    assert "std" in stats
    assert "icir" in stats
    assert "positive_rate" in stats
    assert isinstance(stats["mean"], float)
    assert isinstance(stats["icir"], float)

    # IC 衰减
    assert "ic_decay" in result
    decay = result["ic_decay"]
    assert len(decay) == 5  # lags [1, 3, 5, 10, 20]
    for entry in decay:
        assert "lag" in entry
        assert "ic_mean" in entry
        assert isinstance(entry["lag"], int)
        assert isinstance(entry["ic_mean"], float)


@pytest.mark.asyncio
async def test_run_ic_analysis_empty_data(mock_ch_client):
    """空数据时 IC 分析返回默认值"""
    mock_empty = MagicMock()
    mock_empty.execute.return_value = []

    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_empty):
        service = FactorEvaluationService()
        result = await service.run_ic_analysis(
            expression="Mean($close, 5)",
            symbols=["000001.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
        )

    assert result["ic_series"] == []
    assert result["ic_stats"]["mean"] == 0.0
    assert result["ic_stats"]["std"] == 0.0
    assert result["ic_stats"]["icir"] == 0.0
    assert result["ic_stats"]["positive_rate"] == 0.0
    assert len(result["ic_decay"]) == 5


@pytest.mark.asyncio
async def test_run_quantile_backtest_delegates(mock_ch_client):
    """分层回测委托给 BacktestRunner"""
    from app.backtest.config import BacktestResult

    fake_result = BacktestResult(
        total_return=0.15,
        annual_return=0.08,
        sharpe_ratio=1.2,
        max_drawdown=-0.05,
        n_trading_days=100,
    )

    mock_runner = AsyncMock()
    mock_runner.run.return_value = fake_result

    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_ch_client), \
         patch("app.services.factor_evaluation.get_backtest_runner", return_value=mock_runner):
        service = FactorEvaluationService()
        result = await service.run_quantile_backtest(
            expression="Mean($close, 5)",
            symbols=["000001.SZ", "000002.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
            n_groups=5,
            rebalance_freq="monthly",
        )

    # Verify delegation — runner.run should be called with a BacktestConfig
    assert mock_runner.run.called
    call_args = mock_runner.run.call_args
    config = call_args[0][0]
    assert config.mode == "vectorized"
    assert config.n_groups == 5
    assert config.rebalance_freq == "monthly"
    assert config.factor_expression == "Mean($close, 5)"

    # Result shape
    assert "total_return" in result
    assert result["total_return"] == 0.15
    assert "sharpe_ratio" in result
    assert result["sharpe_ratio"] == 1.2


@pytest.mark.asyncio
async def test_run_full_report_merges(mock_ch_client):
    """完整报告合并 IC 分析和分层回测结果"""
    from app.backtest.config import BacktestResult

    fake_result = BacktestResult(
        total_return=0.15,
        annual_return=0.08,
        sharpe_ratio=1.2,
        max_drawdown=-0.05,
        n_trading_days=100,
    )
    mock_runner = AsyncMock()
    mock_runner.run.return_value = fake_result

    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_ch_client), \
         patch("app.services.factor_evaluation.get_backtest_runner", return_value=mock_runner), \
         patch("app.services.factor_evaluation.evaluate_expression") as mock_eval_expr:
        # Mock evaluate_expression to return known data
        symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
        dates = pd.date_range("2024-01-02", periods=97, freq="B")
        result_dict = {}
        for sym in symbols:
            result_dict[sym] = pd.Series(np.random.randn(len(dates)), index=dates)
        mock_eval_expr.return_value = result_dict

        service = FactorEvaluationService()
        result = await service.run_full_report(
            expression="Mean($close, 5)",
            symbols=["000001.SZ", "000002.SZ", "000003.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
            n_groups=5,
            rebalance_freq="monthly",
        )

    # Top-level fields
    assert "expression" in result
    assert result["expression"] == "Mean($close, 5)"
    assert "parameters" in result
    assert "ic_analysis" in result
    assert "quantile_backtest" in result
    assert "summary" in result

    # Summary merges IC and backtest metrics
    summary = result["summary"]
    assert "ic_mean" in summary
    assert "icir" in summary
    assert "long_short_annual_return" in summary
    assert "long_short_sharpe" in summary
    assert "max_drawdown" in summary
