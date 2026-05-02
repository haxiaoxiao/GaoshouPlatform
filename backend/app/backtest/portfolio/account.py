"""账户 — 资金管理"""
from dataclasses import dataclass, field


@dataclass
class Account:
    """账户资金"""

    cash: float = 0.0
    frozen_cash: float = 0.0
    _initial_cash: float = 0.0

    def __post_init__(self):
        self._initial_cash = self.cash

    @property
    def available_cash(self) -> float:
        return self.cash - self.frozen_cash

    @property
    def initial_cash(self) -> float:
        return self._initial_cash

    def freeze(self, amount: float) -> bool:
        """冻结资金（委托下单时）"""
        if amount > self.available_cash:
            return False
        self.frozen_cash += amount
        return True

    def unfreeze(self, amount: float) -> None:
        """解冻资金（撤单时）"""
        self.frozen_cash = max(0.0, self.frozen_cash - amount)

    def commit_buy(self, cost: float) -> None:
        """确认买入 — 扣除资金"""
        self.frozen_cash = max(0.0, self.frozen_cash - cost)
        self.cash -= cost

    def commit_sell(self, proceeds: float) -> None:
        """确认卖出 — 增加资金"""
        self.cash += proceeds

    def total_value(self, position_market_value: float) -> float:
        """总资产 = 现金 + 持仓市值"""
        return self.cash + position_market_value

    @property
    def total_return(self) -> float:
        return (self.cash / self._initial_cash - 1) if self._initial_cash > 0 else 0.0
