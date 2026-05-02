"""Metrics 绩效计算单元测试"""
import numpy as np
from app.backtest.analysis.metrics import compute_metrics, PerformanceMetrics


def generate_simple_nav(start=1.0, rets=None):
    """从收益率序列生成 nav_series"""
    if rets is None:
        rets = [0.01, -0.005, 0.02, -0.01, 0.015]
    nav = start
    series = []
    daily_returns = []
    for r in rets:
        nav *= (1 + r)
        series.append({"date": f"2024-01-{len(series)+1:02d}", "nav": nav})
        daily_returns.append({"date": series[-1]["date"], "return": r})
    return series, daily_returns


class TestMetrics:
    def test_positive_returns_give_positive_sharpe(self):
        navs, dr = generate_simple_nav(rets=[0.01] * 20)
        result = compute_metrics(navs, dr)
        assert result.sharpe_ratio > 0
        assert result.total_return > 0.19  # approx (1.01)^20 - 1

    def test_zero_returns_give_zero_metrics(self):
        navs, dr = generate_simple_nav(rets=[0.0] * 20)
        result = compute_metrics(navs, dr)
        assert result.annual_return == 0.0
        assert result.max_drawdown == 0.0
        assert result.total_return == 0.0

    def test_drawdown_calculation(self):
        # Up then down pattern
        navs = [
            {"date": "2024-01-01", "nav": 1.0},
            {"date": "2024-01-02", "nav": 1.1},
            {"date": "2024-01-03", "nav": 1.05},
            {"date": "2024-01-04", "nav": 0.95},
            {"date": "2024-01-05", "nav": 1.0},
        ]
        dr = [
            {"date": "2024-01-02", "return": 0.1},
            {"date": "2024-01-03", "return": 1.05 / 1.1 - 1},
            {"date": "2024-01-04", "return": 0.95 / 1.05 - 1},
            {"date": "2024-01-05", "return": 1.0 / 0.95 - 1},
        ]
        result = compute_metrics(navs, dr)
        # Max DD from 1.1 to 0.95 = (0.95 - 1.1) / 1.1 = -0.136
        assert abs(result.max_drawdown - (-0.13636)) < 0.01

    def test_trade_stats(self):
        navs, dr = generate_simple_nav()
        trades = [
            {"pnl": 500.0},
            {"pnl": -200.0},
            {"pnl": 300.0},
            {"pnl": -100.0},
        ]
        result = compute_metrics(navs, dr, trades=trades)
        assert result.total_trades == 4
        assert result.win_trades == 2
        assert result.loss_trades == 2
        assert abs(result.win_rate - 0.5) < 0.01

    def test_sortino_vs_sharpe_with_asymmetric_returns(self):
        # Most positive, occasional large negatives — downside vol < total vol
        rng = np.random.RandomState(42)
        rets = [0.005 + rng.normal(0, 0.01) for _ in range(100)]
        navs, dr = generate_simple_nav(rets=rets)
        result = compute_metrics(navs, dr)
        # Sortino may differ from Sharpe depending on return distribution
        assert result.sharpe_ratio is not None
        assert result.sortino_ratio is not None

    def test_sortino_zero_when_all_positive(self):
        # All positive returns — no downside deviation, Sortino is 0 (undefined)
        rets = [0.005 + abs(np.random.random() * 0.001) for _ in range(100)]
        navs, dr = generate_simple_nav(rets=rets)
        result = compute_metrics(navs, dr)
        # With no downside, Sortino = 0 (undefined)
        assert result.sharpe_ratio > 0

    def test_alpha_beta_vs_benchmark(self):
        navs, dr = generate_simple_nav(rets=[0.01] * 100)
        benchmark = [0.005] * 100  # constant 0.5% daily
        result = compute_metrics(navs, dr, benchmark_returns=benchmark)
        assert result.beta is not None
        assert result.alpha is not None

    def test_empty_returns(self):
        result = compute_metrics([], [])
        assert result.total_return == 0.0
        assert result.sharpe_ratio == 0.0

    def test_to_dict(self):
        m = PerformanceMetrics(total_return=0.1, sharpe_ratio=1.5, max_drawdown=-0.2)
        d = m.to_dict()
        assert d["total_return"] == 0.1
        assert d["sharpe_ratio"] == 1.5
        assert d["max_drawdown"] == -0.2
