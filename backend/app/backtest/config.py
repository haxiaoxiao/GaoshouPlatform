"""Backtest configuration and result schemas."""
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal


@dataclass
class BacktestConfig:
    """Backtest configuration."""

    mode: Literal["vectorized", "event_driven"] = "vectorized"
    symbols: list[str] = field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    initial_capital: float = 1_000_000.0

    # Vectorized mode
    factor_expression: str | None = None
    rebalance_freq: str = "monthly"
    n_groups: int = 5
    weight_method: str = "equal"

    # 事件驱动模式
    buy_condition: str | None = None
    sell_condition: str | None = None
    bar_type: str = "daily"
    timer_times: list[str] | tuple[str, ...] | None = None
    strategy_params: dict | None = None  # 用户策略参数

    # 风控
    stop_loss: float | None = None
    stop_profit: float | None = None
    max_positions: int | None = None
    risk_config: dict[str, Any] | None = None

    # 交易成本
    commission_rate: float = 0.0003
    slippage: float = 0.001
    stamp_tax_rate: float = 0.001
    transfer_fee_rate: float = 0.00001
    min_commission: float = 5.0
    volume_limit_pct: float | None = 0.25
    lot_size: int | dict[str, int] | None = 100
    t_plus_one: bool = True
    exit_on_last_bar: bool = True

    # 引擎选择
    engine: str = "builtin"  # "builtin" | "akquant"
    benchmark_symbol: str | None = "000300.SH"  # 基准指数，例如 "000300.SH"
    warm_start: dict[str, Any] | None = None
    strategy_id: int | None = None
    strategy_code: str | None = None  # akquant 策略代码

    instruments_config: list[dict[str, Any]] | dict[str, dict[str, Any]] | None = None
    indicator_mode: str = "precompute"
    bootstrap_samples: int = 1000
    analysis_config: dict[str, Any] | None = None

    # Runtime-only cache, not persisted as user config.
    _task_id: str | None = field(default=None, repr=False)
    _warm_start_runtime: dict[str, Any] | None = field(default=None, repr=False)
    index_symbol: str | None = None
    universe_mode: str = "symbols"


@dataclass
class BacktestResult:
    """Backtest result."""

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
    total_positions: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    turnover_rate: float = 0.0

    nav_series: list[dict[str, Any]] = field(default_factory=list)
    daily_returns: list[dict[str, Any]] = field(default_factory=list)
    benchmark_symbol: str | None = None
    benchmark_name: str | None = None
    benchmark_nav_series: list[dict[str, Any]] = field(default_factory=list)
    benchmark_daily_returns: list[dict[str, Any]] = field(default_factory=list)
    excess_nav_series: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    warm_start: dict[str, Any] | None = None

    group_navs: list[dict[str, Any]] | None = None

    trades: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)
    report_path: str | None = None

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
            "total_positions": self.total_positions,
            "win_rate": self.win_rate,
            "avg_return": self.avg_return,
            "turnover_rate": self.turnover_rate,
            "nav_series": self.nav_series,
            "daily_returns": self.daily_returns,
            "benchmark_symbol": self.benchmark_symbol,
            "benchmark_name": self.benchmark_name,
            "benchmark_nav_series": self.benchmark_nav_series,
            "benchmark_daily_returns": self.benchmark_daily_returns,
            "excess_nav_series": self.excess_nav_series,
            "warnings": self.warnings,
            "warm_start": self.warm_start,
            "group_navs": self.group_navs,
            "trades": self.trades,
            "orders": self.orders,
            "report_path": self.report_path,
            "start_date": str(self.start_date) if self.start_date else None,
            "end_date": str(self.end_date) if self.end_date else None,
            "initial_capital": self.initial_capital,
            "final_capital": self.final_capital,
            "n_trading_days": self.n_trading_days,
        }

