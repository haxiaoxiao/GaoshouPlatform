"""双标的底仓网格 AKQuant 分钟回测预设。

这是一类执行型策略，不是横截面选股策略。它只盯住两个固定标的，
先建立底仓，再围绕锚价用网格方式做加减仓，适合观察执行节奏、仓位
分层和区间震荡中的盈亏结构。
"""

DUAL_STOCK_GRID_STRATEGY_CODE = r'''
# 这份模板刻意做得很窄：它是两只固定标的的执行模式策略，不是横截面
# 选股器。这样更容易分析网格执行本身的效果。
from collections import deque
import akquant as aq


class DualStockGridStrategy(aq.Strategy):
    """面向 002624.SZ 和 300418.SZ 的底仓网格策略。"""

    symbols = ["002624.SZ", "300418.SZ"]

    def __init__(
        self,
        grid_pct=0.025,
        anchor_window_minutes=240,
        max_grid_levels=6,
        base_position_pct=0.60,
        grid_sleeve_pct=0.40,
        anchor_reset_pct=0.08,
        cash_buffer_pct=0.05,
        symbol_weights=None,
        lot_size=100,
        initial_cash=1_000_000.0,
        **kwargs,
    ):
        super().__init__()
        self.grid_pct = grid_pct
        self.anchor_window_minutes = anchor_window_minutes
        self.max_grid_levels = max_grid_levels
        self.base_position_pct = base_position_pct
        self.grid_sleeve_pct = grid_sleeve_pct
        self.anchor_reset_pct = anchor_reset_pct
        self.cash_buffer_pct = cash_buffer_pct
        self.symbol_weights = symbol_weights or {"002624.SZ": 0.5, "300418.SZ": 0.5}
        self.lot_size = lot_size
        self.initial_cash = initial_cash
        self._windows = {}
        self._last_grid_price = {}
        self._base_position_qty = {}
        self._grid_position_qty = {}
        self._grid_levels = {}
        self._initialized = set()

    def on_start(self):
        try:
            self.initial_cash = float(self.get_cash())
        except Exception:
            pass
        # 这里不需要预加载历史深度，因为锚价窗口由实时 bar 自己滚动构建。
        self.set_history_depth(0)
        self._windows = {symbol: deque(maxlen=int(self.anchor_window_minutes)) for symbol in self.symbols}

    def on_bar(self, bar):
        symbol = str(bar.symbol)
        if symbol not in self.symbols:
            return
        price = float(getattr(bar, "close", 0.0) or 0.0)
        volume = float(getattr(bar, "volume", 0.0) or 0.0)
        if price <= 0:
            return

        window = self._windows.setdefault(symbol, deque(maxlen=int(self.anchor_window_minutes)))
        window.append((price, max(volume, 1.0)))
        anchor = self._anchor_price(symbol)
        if anchor <= 0:
            anchor = price

        if symbol not in self._initialized:
            self._prepare_symbol(symbol, price)
            self._last_grid_price[symbol] = anchor
            self._initialized.add(symbol)

        # 先建立底仓，再允许网格仓位围绕锚价上下波动；只有底仓建立后，
        # 网格逻辑才会真正接管加减仓。
        current_pos = float(self.get_position(symbol) or 0.0)
        base_qty = float(self._base_position_qty.get(symbol, 0.0) or 0.0)
        if current_pos < base_qty:
            self._submit_base_position(symbol, price, current_pos)
            return

        last_price = float(self._last_grid_price.get(symbol) or anchor or price)
        if anchor > 0 and abs(price / anchor - 1.0) > float(self.anchor_reset_pct):
            last_price = anchor
            self._last_grid_price[symbol] = anchor

        grid_qty = max(0.0, current_pos - base_qty)
        grid_lot = self._grid_lot(symbol, price)
        if grid_lot <= 0:
            return

        if price <= last_price * (1.0 - float(self.grid_pct)):
            max_grid_qty = grid_lot * int(self.max_grid_levels)
            if grid_qty + grid_lot <= max_grid_qty and self._can_buy(grid_lot, price):
                self.buy(symbol=symbol, quantity=grid_lot, price=price)
                self._grid_position_qty[symbol] = grid_qty + grid_lot
                self._grid_levels[symbol] = int(self._grid_levels.get(symbol, 0) or 0) + 1
                self._last_grid_price[symbol] = price
                self.log(f"GRID BUY {symbol} qty={grid_lot} price={price:.3f} anchor={anchor:.3f}")
        elif price >= last_price * (1.0 + float(self.grid_pct)):
            sell_qty = min(grid_lot, max(0.0, current_pos - base_qty))
            sell_qty = self._round_lot(sell_qty)
            if sell_qty > 0:
                self.sell(symbol=symbol, quantity=sell_qty, price=price)
                self._grid_position_qty[symbol] = max(0.0, grid_qty - sell_qty)
                self._grid_levels[symbol] = max(0, int(self._grid_levels.get(symbol, 0) or 0) - 1)
                self._last_grid_price[symbol] = price
                self.log(f"GRID SELL {symbol} qty={sell_qty} price={price:.3f} anchor={anchor:.3f}")

    def _prepare_symbol(self, symbol, price):
        budget = float(self.initial_cash) * float(self.symbol_weights.get(symbol, 0.5))
        base_value = budget * float(self.base_position_pct)
        qty = self._round_lot(base_value / price)
        self._base_position_qty[symbol] = qty
        self._grid_position_qty[symbol] = 0.0
        self._grid_levels[symbol] = 0

    def _submit_base_position(self, symbol, price, current_pos=0.0):
        qty = self._base_position_qty.get(symbol, 0.0) or 0.0
        buy_qty = self._round_lot(max(0.0, qty - float(current_pos or 0.0)))
        if buy_qty > 0:
            self.buy(symbol=symbol, quantity=buy_qty, price=price)

    def _anchor_price(self, symbol):
        window = self._windows.get(symbol)
        if not window:
            return 0.0
        amount = sum(price * volume for price, volume in window)
        volume = sum(volume for _price, volume in window)
        return amount / volume if volume > 0 else 0.0

    def _grid_lot(self, symbol, price):
        budget = float(self.initial_cash) * float(self.symbol_weights.get(symbol, 0.5))
        sleeve = budget * float(self.grid_sleeve_pct)
        levels = max(1, int(self.max_grid_levels))
        return self._round_lot(sleeve / levels / price)

    def _round_lot(self, qty):
        lot = max(1, int(self.lot_size))
        return int(float(qty) // lot * lot)

    def _can_buy(self, qty, price):
        try:
            cash = float(self.get_cash())
        except Exception:
            cash = float(self.initial_cash)
        return cash * (1.0 - float(self.cash_buffer_pct)) >= float(qty) * float(price)
'''


DEFAULT_DUAL_STOCK_GRID_PARAMS = {
    "grid_pct": 0.025,
    "anchor_window_minutes": 240,
    "max_grid_levels": 6,
    "base_position_pct": 0.60,
    "grid_sleeve_pct": 0.40,
    "anchor_reset_pct": 0.08,
    "cash_buffer_pct": 0.05,
    "symbol_weights": {"002624.SZ": 0.5, "300418.SZ": 0.5},
    "lot_size": 100,
    "ak_history_depth": 0,
}


DEFAULT_DUAL_STOCK_GRID_PARAM_GRID = {
    "grid_pct": [0.015, 0.02, 0.025, 0.03, 0.04],
    "anchor_window_minutes": [60, 120, 240, 480],
    "max_grid_levels": [4, 6, 8],
    "grid_sleeve_pct": [0.25, 0.35, 0.40],
    "anchor_reset_pct": [0.06, 0.08, 0.10],
}


DUAL_STOCK_GRID_SYMBOLS = ["002624.SZ", "300418.SZ"]
