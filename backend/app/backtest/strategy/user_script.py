"""用户策略引擎 — exec() 沙箱执行 init(context) + handle_bar(context, bar)"""
import numpy as np
import pandas as pd
from datetime import date
from typing import Any

from loguru import logger

from app.backtest.event.events import Event, EventType
from app.backtest.event.event_source import Bar, BarEventSource
from app.backtest.portfolio.account import Account
from app.backtest.portfolio.position import PositionManager, Position


class UserContext:
    """用户策略上下文 — 封装 StrategyContext，提供 RQAlpha 风格 API"""

    def __init__(
        self,
        symbols: list[str],
        account: Account,
        position_manager: PositionManager,
        event_source: BarEventSource,
        params: dict | None = None,
    ):
        self.symbols = symbols
        self._account = account
        self._position_manager = position_manager
        self._event_source = event_source
        self._params = params or {}

        self.current_bar: Bar | None = None
        self.current_date: date | None = None
        self._pending_orders: list[dict] = []
        self._order_counter: int = 0
        self._history_cache: dict[str, pd.DataFrame] = {}
        self._log_messages: list[str] = []

    # ── 账户信息 ──
    @property
    def account(self):
        return self._account

    @property
    def cash(self) -> float:
        return self._account.available_cash

    @property
    def total_value(self) -> float:
        return self._account.total_value(self._position_manager.total_market_value)

    @property
    def params(self) -> dict:
        return self._params

    # ── 持仓 ──
    def get_position(self, symbol: str) -> Position | None:
        return self._position_manager.get(symbol)

    def get_or_create_position(self, symbol: str) -> Position:
        return self._position_manager.get_or_create(symbol)

    @property
    def positions(self) -> dict[str, Position]:
        return self._position_manager.positions

    def has_position(self, symbol: str) -> bool:
        pos = self._position_manager.get(symbol)
        return pos is not None and pos.total_shares > 0

    # ── 下单 API ──
    def order_shares(self, symbol: str, shares: int, price: float | None = None) -> str | None:
        """按股数下单。正=买入，负=卖出。返回 order_id"""
        if shares == 0:
            return None
        if price is None and self.current_bar:
            price = self.current_bar.close
        if price is None or price <= 0:
            return None

        direction = "buy" if shares > 0 else "sell"
        quantity = abs(shares)
        amount = quantity * price

        self._order_counter += 1
        oid = f"ord_{self._order_counter:06d}"

        self._pending_orders.append({
            "order_id": oid, "symbol": symbol, "direction": direction,
            "price": price, "quantity": quantity, "amount": amount,
            "date": self.current_date,
        })
        return oid

    def order_value(self, symbol: str, value: float = 0, price: float | None = None) -> str | None:
        """按金额下单。value > 0 买入，value < 0 卖出"""
        if value == 0:
            return None
        if price is None and self.current_bar:
            price = self.current_bar.close
        if price is None or price <= 0:
            return None

        direction = "buy" if value > 0 else "sell"
        abs_value = abs(value)
        shares = int(abs_value / price / 100) * 100
        if shares == 0:
            return None

        return self.order_shares(symbol, shares if direction == "buy" else -shares, price)

    def order_target_pct(self, symbol: str, pct: float, price: float | None = None) -> str | None:
        """目标仓位百分比下单"""
        if price is None and self.current_bar:
            price = self.current_bar.close
        if price is None or price <= 0:
            return None

        target_value = self.total_value * pct
        current = self._position_manager.get(symbol)
        current_value = current.market_value if current else 0
        diff = target_value - current_value

        if abs(diff) < price * 100:  # 不足1手
            return None

        if diff > 0:
            return self.order_value(symbol, diff, price)
        else:
            shares = int(-diff / price / 100) * 100
            if shares > 0:
                return self.order_shares(symbol, -shares, price)
        return None

    # ── 数据获取 ──
    def get_history(self, symbol: str, n_days: int = 252) -> pd.DataFrame:
        """获取截止当前的 N 日历史数据"""
        if self.current_date is None:
            return pd.DataFrame()
        cache_key = f"{symbol}:{self.current_date}:{n_days}"
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]

        df = self._event_source.get_history(symbol, self.current_date, n_days)
        self._history_cache[cache_key] = df
        return df

    # ── 日志 ──
    def log(self, msg: str) -> None:
        self._log_messages.append(msg)
        logger.info("[Strategy] {}", msg)

    # ── 兼容 runner.py 的订单调度管线 ──
    @property
    def pending_orders(self) -> list[dict]:
        return self._pending_orders

    def clear_orders(self) -> None:
        self._pending_orders.clear()


# ── 脚本沙箱提供的辅助函数 ──
def _make_sandbox_globals() -> dict:
    """构建用户脚本可用的全局函数"""
    return {
        # Basic Python
        "np": np,
        "pd": pd,
        # Math helpers
        "MA": _ma,
        "EMA": _ema,
        "RSI": _rsi,
        "MACD": _macd,
        "HHV": _hhv,
        "LLV": _llv,
        "CROSS": _cross,
        "REF": _ref,
        "ROUND": lambda x, n=2: round(float(x), n) if x is not None else 0,
    }


def _ma(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.rolling(window=period, min_periods=max(1, period // 2)).mean()


def _ema(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series | np.ndarray, period: int = 14) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _macd(series: pd.Series | np.ndarray, fast=12, slow=26, signal=9):
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    bar = 2 * (dif - dea)
    return dif, dea, bar


def _hhv(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.rolling(window=period, min_periods=1).max()


def _llv(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.rolling(window=period, min_periods=1).min()


def _cross(a: pd.Series, b: pd.Series) -> pd.Series:
    """上穿/下穿检测: +1=上穿, -1=下穿, 0=无"""
    a, b = pd.Series(a), pd.Series(b)
    above = a > b
    cross_up = above & (~above.shift(1).fillna(False))
    cross_down = (~above) & (above.shift(1).fillna(False))
    result = pd.Series(0, index=a.index)
    result[cross_up] = 1
    result[cross_down] = -1
    return result


def _ref(series: pd.Series | np.ndarray, n: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.shift(n)


class UserScriptStrategy:
    """用户脚本策略 — exec() 执行 init + handle_bar"""

    def __init__(self, code: str, symbols: list[str], params: dict | None = None):
        self.code = code
        self.symbols = symbols
        self._params = params or {}
        self._init_fn = None
        self._handle_bar_fn = None
        self._ctx: UserContext | None = None

    def register(self, event_bus, ctx: UserContext) -> None:
        self._ctx = ctx
        namespace = _make_sandbox_globals()
        namespace["context"] = ctx

        try:
            exec(self.code, namespace)
        except Exception as exc:
            logger.error("Strategy script compile failed: {}", exc)
            raise ValueError(f"策略脚本编译失败: {exc}") from exc

        self._init_fn = namespace.get("init")
        self._handle_bar_fn = namespace.get("handle_bar")

        if self._handle_bar_fn is None:
            raise ValueError("策略脚本必须定义 handle_bar(context, bar) 函数")

        # 调用 init
        if self._init_fn:
            try:
                self._init_fn(ctx)
            except Exception as exc:
                logger.error("init() failed: {}", exc)
                raise ValueError(f"init() 执行失败: {exc}") from exc

        # 注册 BAR 监听
        event_bus.add_listener(EventType.BAR, self._on_bar)

    def _on_bar(self, event: Event) -> None:
        ctx = self._ctx
        if ctx is None:
            return

        bar: Bar = event.data["bar"]
        ctx.current_date = event.data.get("date")
        ctx.current_bar = bar

        try:
            self._handle_bar_fn(ctx, bar)
        except Exception as exc:
            logger.error("handle_bar failed for {} on {}: {}", bar.symbol, ctx.current_date, exc)
