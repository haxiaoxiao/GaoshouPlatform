"""投资组合 — 净值追踪 + 日度快照"""
from dataclasses import dataclass, field
from datetime import date

from app.backtest.event.events import Event, EventType
from app.backtest.portfolio.account import Account
from app.backtest.portfolio.position import PositionManager


@dataclass
class DailySnapshot:
    """日度资产快照"""

    date: date
    nav: float
    daily_return: float = 0.0
    cash: float = 0.0
    position_value: float = 0.0


@dataclass
class Portfolio:
    """投资组合 — 整合账户 + 仓位管理"""

    account: Account = field(default_factory=lambda: Account())
    position_manager: PositionManager = field(default_factory=PositionManager)
    snapshots: list[DailySnapshot] = field(default_factory=list)
    _prev_nav: float | None = None
    _current_date: date | None = None

    def register_listeners(self, event_bus) -> None:
        """注册事件监听器到 EventBus"""
        event_bus.add_listener(EventType.BEFORE_TRADING, self._on_before_trading, system=True)
        event_bus.add_listener(EventType.AFTER_TRADING, self._on_after_trading, system=True)

    def _on_before_trading(self, event: Event) -> None:
        """更新持仓市价"""
        bars = event.data.get("bars", {})
        self.position_manager.update_prices(bars)

    def _on_after_trading(self, event: Event) -> None:
        """记录日终快照"""
        dt = event.data.get("date")
        if dt is None:
            return
        self._current_date = dt
        mv = self.position_manager.total_market_value
        total = self.account.total_value(mv)
        daily_ret = 0.0
        if self._prev_nav is not None and self._prev_nav > 0:
            daily_ret = total / self._prev_nav - 1
        self.snapshots.append(DailySnapshot(
            date=dt,
            nav=total,
            daily_return=daily_ret,
            cash=self.account.cash,
            position_value=mv,
        ))
        self._prev_nav = total

    @property
    def total_value(self) -> float:
        return self.account.total_value(self.position_manager.total_market_value)

    @property
    def nav_series(self) -> list[dict]:
        init = self.account.initial_cash
        return [
            {"date": s.date.isoformat() if hasattr(s.date, 'isoformat') else str(s.date),
             "nav": s.nav / init if init > 0 else 0}
            for s in self.snapshots
        ]

    @property
    def daily_returns(self) -> list[dict]:
        return [
            {"date": s.date.isoformat() if hasattr(s.date, 'isoformat') else str(s.date),
             "return": s.daily_return}
            for s in self.snapshots
        ]
