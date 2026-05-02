"""仓位管理 — FIFO 成本核算，T+1 锁仓"""
from dataclasses import dataclass, field
from datetime import date


@dataclass
class PositionLot:
    """单个买入批次 — FIFO 队列元素"""

    trade_date: date
    shares: int
    cost_price: float  # 加权成交均价

    @property
    def cost(self) -> float:
        return self.shares * self.cost_price


@dataclass
class Position:
    """单品种仓位 — 管理多个买入批次（FIFO）"""

    symbol: str
    lots: list[PositionLot] = field(default_factory=list)
    _locked_until: date | None = None  # T+1 锁仓

    @property
    def total_shares(self) -> int:
        return sum(lot.shares for lot in self.lots)

    @property
    def avg_cost(self) -> float:
        t = self.total_shares
        return sum(lot.cost for lot in self.lots) / t if t > 0 else 0.0

    @property
    def market_value(self) -> float:
        return self._last_price * self.total_shares

    @property
    def unrealized_pnl(self) -> float:
        return (self._last_price - self.avg_cost) * self.total_shares if self.total_shares > 0 else 0.0

    def __post_init__(self):
        self._last_price: float = 0.0

    def update_price(self, price: float) -> None:
        self._last_price = price

    def buy(self, shares: int, price: float, trade_date: date) -> float:
        """买入开仓 — 返回成本金额"""
        if shares <= 0:
            raise ValueError(f"Buy shares must be positive, got {shares}")
        self.lots.append(PositionLot(trade_date=trade_date, shares=shares, cost_price=price))
        self._last_price = price
        self._locked_until = trade_date  # T+1 不可卖出
        return shares * price

    def sell(self, shares: int, price: float, trade_date: date) -> tuple[int, float]:
        """卖出平仓 (FIFO) — 返回 (实际卖出股数, 实现盈亏)

        Raises:
            ValueError: 如果 T+1 锁仓中
        """
        if self._locked_until and trade_date <= self._locked_until:
            raise ValueError(f"{self.symbol} locked until {self._locked_until} (T+1)")
        if shares <= 0:
            raise ValueError(f"Sell shares must be positive, got {shares}")
        if shares > self.total_shares:
            raise ValueError(
                f"Cannot sell {shares} shares of {self.symbol}, only {self.total_shares} held"
            )

        remaining = shares
        realized_pnl = 0.0
        new_lots = []

        for lot in self.lots:
            if remaining <= 0:
                new_lots.append(lot)
                continue

            if lot.shares <= remaining:
                remaining -= lot.shares
                realized_pnl += (price - lot.cost_price) * lot.shares
            else:
                lot.shares -= remaining
                realized_pnl += (price - lot.cost_price) * remaining
                remaining = 0
                if lot.shares > 0:
                    new_lots.append(lot)

        self.lots = new_lots
        self._last_price = price
        return (shares - remaining, realized_pnl)

    def close(self, price: float, trade_date: date) -> float:
        """平掉全部仓位 — 返回实现盈亏"""
        if self._locked_until and trade_date <= self._locked_until:
            raise ValueError(f"{self.symbol} locked until {self._locked_until}")
        realized = 0.0
        for lot in self.lots:
            realized += (price - lot.cost_price) * lot.shares
        self.lots.clear()
        self._last_price = price
        return realized


@dataclass
class PositionManager:
    """多品种仓位管理"""

    positions: dict[str, Position] = field(default_factory=dict)

    def get_or_create(self, symbol: str) -> Position:
        if symbol not in self.positions:
            self.positions[symbol] = Position(symbol=symbol)
        return self.positions[symbol]

    def get(self, symbol: str) -> Position | None:
        return self.positions.get(symbol)

    def update_prices(self, bars: dict) -> None:
        """逐日更新所有持仓市价"""
        for sym, bar in bars.items():
            if sym in self.positions:
                self.positions[sym].update_price(bar.close)

    @property
    def total_market_value(self) -> float:
        return sum(p.market_value for p in self.positions.values())

    @property
    def total_unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    def remove_empty(self) -> None:
        self.positions = {s: p for s, p in self.positions.items() if p.total_shares > 0}
