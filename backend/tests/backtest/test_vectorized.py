"""向量化回测引擎测试"""
import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig
from app.backtest.vectorized import VectorizedBacktestEngine


def make_test_data(n_symbols=100, n_days=252):
    """生成测试用因子矩阵和收益矩阵"""
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"{i:06d}.SZ" for i in range(n_symbols)]

    factor_data = np.random.randn(n_days, n_symbols)
    factor_matrix = pd.DataFrame(factor_data, index=dates, columns=symbols)

    return_data = factor_data * 0.01 + np.random.randn(n_days, n_symbols) * 0.02
    return_matrix = pd.DataFrame(return_data, index=dates, columns=symbols)

    return factor_matrix, return_matrix


class TestVectorizedBacktest:
    def test_basic_run(self):
        factor, returns = make_test_data(50, 200)
        config = BacktestConfig(
            mode="vectorized",
            factor_expression="test_factor",
            rebalance_freq="monthly",
            n_groups=5,
            initial_capital=1_000_000,
        )

        engine = VectorizedBacktestEngine()
        result = engine.run(factor, returns, config)

        assert result is not None
        assert result.n_trading_days > 0
        assert result.group_navs is not None
        assert len(result.group_navs) == 5

    def test_empty_data(self):
        engine = VectorizedBacktestEngine()
        result = engine.run(
            pd.DataFrame(), pd.DataFrame(),
            BacktestConfig(initial_capital=100_000),
        )
        assert result.total_return == 0

    def test_nav_series_format(self):
        factor, returns = make_test_data(30, 100)
        config = BacktestConfig(rebalance_freq="monthly", n_groups=3)
        engine = VectorizedBacktestEngine()
        result = engine.run(factor, returns, config)

        assert len(result.nav_series) > 0
        assert "date" in result.nav_series[0]
        assert "nav" in result.nav_series[0]
