"""事件驱动回测引擎测试"""
import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig
from app.backtest.event_driven import EventDrivenBacktestEngine


def test_basic_event_driven():
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=100, freq="B")

    close = np.cumsum(np.random.randn(100) * 0.01) + 10.0
    df = pd.DataFrame({
        "trade_date": dates,
        "open": close * 0.99,
        "high": close * 1.02,
        "low": close * 0.98,
        "close": close,
        "volume": np.random.randint(1e6, 1e7, 100),
        "amount": close * np.random.randint(1e6, 1e7, 100) * 0.8,
    })
    df.set_index("trade_date", inplace=True)

    def always_buy(df):
        s = pd.Series(0, index=df.index)
        s.iloc[10] = 1
        return s

    engine = EventDrivenBacktestEngine()
    result = engine.run({"test": df}, always_buy, BacktestConfig(initial_capital=100_000))

    assert result.n_trading_days > 0
