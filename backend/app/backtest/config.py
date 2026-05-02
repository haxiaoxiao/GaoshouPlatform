"""回测配置与结果数据结构"""
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal


@dataclass
class BacktestConfig:
    """回测配置"""

    mode: Literal["vectorized", "event_driven"] = "vectorized"
    symbols: list[str] = field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    initial_capital: float = 1_000_000.0

    # 向量化模式
    factor_expression: str | None = None
    rebalance_freq: str = "monthly"
    n_groups: int = 5
    weight_method: str = "equal"

    # 事件驱动模式
    buy_condition: str | None = None
    sell_condition: str | None = None
    bar_type: str = "daily"

    # 风控
    stop_loss: float | None = None
    stop_profit: float | None = None
    max_positions: int | None = None

    # 交易成本
    commission_rate: float = 0.0003
    slippage: float = 0.001


@dataclass
class BacktestResult:
    """回测结果"""

    total_return: float = 0.0
    annual_return: float = 0.0
    annual_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    calmar_ratio: float = 0.0
    alpha: float = 0.0
    beta: float = 0.0
    information_ratio: float = 0.0

    total_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    turnover_rate: float = 0.0

    nav_series: list[dict[str, Any]] = field(default_factory=list)
    daily_returns: list[dict[str, Any]] = field(default_factory=list)

    group_navs: list[dict[str, Any]] | None = None

    trades: list[dict[str, Any]] = field(default_factory=list)

    start_date: date | None = None
    end_date: date | None = None
    initial_capital: float = 0.0
    final_capital: float = 0.0
    n_trading_days: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "annual_volatility": self.annual_volatility,
            "sharpe": self.sharpe_ratio,  # frontend compat
            "sharpe_ratio": self.sharpe_ratio,
            "sortino": self.sortino_ratio,
            "max_drawdown": self.max_drawdown,
            "calmar": self.calmar_ratio,
            "alpha": self.alpha,
            "beta": self.beta,
            "information_ratio": self.information_ratio,
            "total_trades": self.total_trades,
            "win_trades": self.win_trades,
            "loss_trades": self.loss_trades,
            "win_rate": self.win_rate,
            "avg_return": self.avg_return,
            "turnover_rate": self.turnover_rate,
            "nav_series": self.nav_series,
            "daily_returns": self.daily_returns,
            "group_navs": self.group_navs,
            "trades": self.trades,
            "start_date": str(self.start_date) if self.start_date else None,
            "end_date": str(self.end_date) if self.end_date else None,
            "initial_capital": self.initial_capital,
            "final_capital": self.final_capital,
            "n_trading_days": self.n_trading_days,
        }
