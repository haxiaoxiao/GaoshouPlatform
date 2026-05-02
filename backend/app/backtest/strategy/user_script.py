"""用户策略引擎 — exec() 沙箱执行 init(context) + handle_bar(context, bar)

兼容 RQAlpha API:
  - context.now, context.run_info, context.portfolio, context.stock_account
  - context.universe, context.get_open_orders(), context.cancel_order()
  - bar.symbol, bar.order_book_id, bar.datetime, bar.open/high/low/close/volume
  - bar.limit_up, bar.limit_down, bar.prev_close, bar.suspended, bar.is_trading
  - bar.mavg(intervals), bar.vwap(intervals)
  - order_shares / order_value / order_target_pct / order
  - get_open_orders / cancel_order
  - history_bars / current_snapshot / get_history
"""
import uuid
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from loguru import logger

from app.backtest.event.events import Event, EventType
from app.backtest.event.event_source import Bar, BarEventSource
from app.backtest.portfolio.account import Account
from app.backtest.portfolio.position import PositionManager, Position


# ── Order ──

class OrderStatus:
    PENDING_NEW = "pending_new"
    ACTIVE = "active"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


@dataclass
class Order:
    """订单对象 — 兼容 RQAlpha Order"""
    order_id: str
    symbol: str
    direction: str  # "buy" | "sell"
    quantity: int
    price: float
    amount: float
    order_date: date | None = None
    status: str = OrderStatus.PENDING_NEW
    filled_quantity: int = 0
    avg_price: float = 0.0
    transaction_cost: float = 0.0
    message: str = ""

    @property
    def order_book_id(self) -> str:
        return self.symbol

    @property
    def side(self) -> str:
        return self.direction

    @property
    def unfilled_quantity(self) -> int:
        return self.quantity - self.filled_quantity

    @property
    def is_final(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED)

    @property
    def is_active(self) -> bool:
        return self.status == OrderStatus.ACTIVE

    def fill(self, quantity: int, price: float, cost: float = 0) -> None:
        new_filled = self.filled_quantity + quantity
        self.avg_price = (
            (self.avg_price * self.filled_quantity + price * quantity) / new_filled
            if new_filled > 0 else price
        )
        self.filled_quantity = new_filled
        self.transaction_cost += cost
        if self.unfilled_quantity == 0:
            self.status = OrderStatus.FILLED

    def mark_rejected(self, reason: str) -> None:
        self.message = reason
        self.status = OrderStatus.REJECTED

    def mark_cancelled(self, reason: str) -> None:
        self.message = reason
        self.status = OrderStatus.CANCELLED


# ── RunInfo ──

@dataclass
class RunInfo:
    """策略运行信息 — 兼容 RQAlpha context.run_info"""
    start_date: date
    end_date: date
    frequency: str = "1d"
    capital: float = 1_000_000
    symbols: list[str] = field(default_factory=list)


# ── Portfolio / Account Proxy ──

class AccountProxy:
    """账户信息代理 — 兼容 RQAlpha context.stock_account"""
    def __init__(self, account: Account, position_manager: PositionManager):
        self._account = account
        self._pm = position_manager

    @property
    def cash(self) -> float:
        return self._account.cash

    @property
    def available_cash(self) -> float:
        return self._account.available_cash

    @property
    def frozen_cash(self) -> float:
        return self._account.frozen_cash

    @property
    def total_value(self) -> float:
        return self._account.total_value(self._pm.total_market_value)

    @property
    def market_value(self) -> float:
        """持仓总市值"""
        return self._pm.total_market_value

    @property
    def positions(self) -> dict[str, Position]:
        return self._pm.positions

    def __repr__(self) -> str:
        return f"StockAccount(cash={self.available_cash:.2f}, total_value={self.total_value:.2f})"


class PortfolioProxy:
    """投资组合代理 — 兼容 RQAlpha context.portfolio"""
    def __init__(self, account: Account, position_manager: PositionManager):
        self._account = account
        self._pm = position_manager
        self.accounts = {"STOCK": AccountProxy(account, position_manager)}

    @property
    def stock_account(self) -> AccountProxy:
        return self.accounts["STOCK"]

    @property
    def total_value(self) -> float:
        return self._account.total_value(self._pm.total_market_value)

    @property
    def unit_net_value(self) -> float:
        cap = self._account.initial_cash
        return self.total_value / cap if cap > 0 else 1.0

    @property
    def positions(self) -> dict[str, Position]:
        return self._pm.positions

    def get_position(self, symbol: str) -> Position | None:
        return self._pm.get(symbol)

    def get_positions(self) -> list[Position]:
        return [p for p in self._pm.positions.values() if p.total_shares > 0]

    def __repr__(self) -> str:
        return f"Portfolio(total_value={self.total_value:.2f}, positions={len(self.positions)})"


# ── UserContext ──

class UserContext:
    """用户策略上下文 — 兼容 RQAlpha ExecutionContext + Environment API

    策略可通过 context 访问:
      - context.now / context.now_dt            当前时间
      - context.run_info                        运行参数
      - context.portfolio                       投资组合
      - context.stock_account                   股票账户
      - context.universe                        当前股票池
      - context.cash / context.total_value      快捷资金查询
      - context.order_shares / order_value / order_target_pct  下单
      - context.get_open_orders() / cancel_order()  订单管理
      - context.get_history(symbol, n_days)     历史数据 (DataFrame)
    """

    def __init__(
        self,
        symbols: list[str],
        account: Account,
        position_manager: PositionManager,
        event_source: BarEventSource,
        params: dict | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        capital: float = 1_000_000,
        bar_type: str = "daily",
    ):
        self.symbols = symbols
        self._account = account
        self._position_manager = position_manager
        self._event_source = event_source
        self._params = params or {}
        self._orders: dict[str, Order] = {}
        self.bar_type = bar_type

        self.current_bar: Bar | None = None
        self.current_date: date | None = None
        self._bar_dict = None  # BarDict — 当日所有标的 Bar
        self._pending_orders: list[dict] = []
        self._order_counter: int = 0
        self._history_cache: dict[str, pd.DataFrame] = {}
        self._log_messages: list[str] = []

        # 预构建代理对象
        self._portfolio = PortfolioProxy(account, position_manager)
        self._stock_account = self._portfolio.stock_account
        self._run_info = RunInfo(
            start_date=start_date or date(2000, 1, 1),
            end_date=end_date or date(2099, 12, 31),
            frequency="1d",
            capital=capital,
            symbols=symbols,
        )
        self._universe: set[str] = set(symbols)

    # ── 时间 ──
    @property
    def now(self) -> datetime:
        """当前时间 — 兼容 RQAlpha context.now"""
        if self.current_date:
            return datetime.combine(self.current_date, datetime.min.time())
        return datetime.now()

    @property
    def now_dt(self) -> datetime:
        return self.now

    # ── 运行信息 ──
    @property
    def run_info(self) -> RunInfo:
        return self._run_info

    # ── 组合 & 账户 ──
    @property
    def portfolio(self) -> PortfolioProxy:
        return self._portfolio

    @property
    def stock_account(self) -> AccountProxy:
        return self._stock_account

    # ── 快捷持仓查询（向后兼容） ──
    def get_position(self, symbol: str) -> Position | None:
        return self._position_manager.get(symbol)

    def get_or_create_position(self, symbol: str) -> Position:
        return self._position_manager.get_or_create(symbol)

    def has_position(self, symbol: str) -> bool:
        pos = self._position_manager.get(symbol)
        return pos is not None and pos.total_shares > 0

    # ── 快捷资金 ──
    @property
    def cash(self) -> float:
        return self._account.available_cash

    @property
    def total_value(self) -> float:
        return self._account.total_value(self._position_manager.total_market_value)

    @property
    def params(self) -> dict:
        return self._params

    # ── 股票池 ──
    @property
    def universe(self) -> set[str]:
        return self._universe

    def update_universe(self, symbols: list[str]) -> None:
        self._universe = set(symbols)

    def _get_bar_price(self, symbol: str) -> float:
        """从 bar_dict 获取当前标的价格"""
        if self._bar_dict and symbol in self._bar_dict:
            return self._bar_dict[symbol].close
        if self.current_bar and self.current_bar.symbol == symbol:
            return self.current_bar.close
        return 0.0

    # ── 下单 API ──
    def _create_order(self, symbol: str, direction: str, quantity: int, price: float) -> Order | None:
        if quantity <= 0 or price <= 0:
            return None
        self._order_counter += 1
        oid = f"ord_{self._order_counter:06d}"
        order = Order(
            order_id=oid, symbol=symbol, direction=direction,
            quantity=quantity, price=price,
            amount=quantity * price, order_date=self.current_date,
        )
        self._orders[oid] = order

        self._pending_orders.append({
            "order_id": oid, "symbol": symbol, "direction": direction,
            "price": price, "quantity": quantity, "amount": quantity * price,
            "date": self.current_date,
        })
        return order

    def order_shares(self, symbol: str, shares: int, price: float | None = None) -> Order | None:
        """按股数下单。正=买入，负=卖出。兼容 RQAlpha"""
        if shares == 0:
            return None
        if price is None:
            price = self._get_bar_price(symbol)
        if price is None or price <= 0:
            return None
        direction = "buy" if shares > 0 else "sell"
        return self._create_order(symbol, direction, abs(shares), price)

    def order_value(self, symbol: str, value: float, price: float | None = None) -> Order | None:
        """按金额下单。value > 0 买入，value < 0 卖出。兼容 RQAlpha"""
        if value == 0:
            return None
        if price is None:
            price = self._get_bar_price(symbol)
        if price is None or price <= 0:
            return None
        direction = "buy" if value > 0 else "sell"
        abs_value = abs(value)
        shares = int(abs_value / price / 100) * 100
        if shares == 0:
            return None
        return self._create_order(symbol, direction, shares, price)

    def order_target_pct(self, symbol: str, pct: float, price: float | None = None) -> Order | None:
        """目标仓位百分比下单。兼容 RQAlpha order_target_percent"""
        if price is None:
            price = self._get_bar_price(symbol)
        if price is None or price <= 0:
            return None
        target_value = self.total_value * pct
        current = self._position_manager.get(symbol)
        current_value = current.market_value if current else 0
        diff = target_value - current_value
        if abs(diff) < price * 100:
            return None
        return self.order_value(symbol, diff, price)

    # ── 订单管理 ──
    def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """获取未成交订单。兼容 RQAlpha get_open_orders()"""
        orders = [o for o in self._orders.values() if o.is_active]
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        return orders

    def cancel_order(self, order: Order) -> Order | None:
        """撤单。兼容 RQAlpha cancel_order()"""
        if order.is_final:
            return None
        order.mark_cancelled("用户撤单")
        return order

    def get_order(self, order_id: str) -> Order | None:
        return self._orders.get(order_id)

    def _activate_order(self, order_id: str) -> None:
        if order_id in self._orders:
            self._orders[order_id].status = OrderStatus.ACTIVE

    def _fill_order(self, order_id: str, quantity: int = 0, price: float = 0, cost: float = 0) -> None:
        if order_id in self._orders:
            order = self._orders[order_id]
            fill_qty = quantity or order.quantity
            fill_price = price or order.price
            order.fill(fill_qty, fill_price, cost)

    # ── 数据获取 ──
    def get_history(self, symbol: str, n_days: int = 252) -> pd.DataFrame:
        """获取截止当前的 N 日历史数据（DataFrame）。兼容原有 API"""
        if self.current_date is None:
            return pd.DataFrame()
        cache_key = f"{symbol}:{self.current_date}:{n_days}"
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]
        df = self._event_source.get_history(symbol, self.current_date, n_days)
        self._history_cache[cache_key] = df
        return df

    def get_intraday(self, symbol: str, trade_date: date | None = None) -> list[Bar]:
        """获取某标的某日的所有分钟 Bar。未指定日期则用 current_date"""
        td = trade_date or self.current_date
        if td is None:
            return []
        return self._event_source.get_intraday(symbol, td)

    def get_intraday_history(self, symbol: str, n_days: int = 5,
                              end_date: date | None = None) -> pd.DataFrame:
        """获取某标的截止某日的 N 日分钟 OHLCV 数据"""
        ed = end_date or self.current_date
        if ed is None:
            return pd.DataFrame()
        return self._event_source.get_intraday_history(symbol, ed, n_days)

    def history_bars(self, symbol: str, bar_count: int, frequency: str = "1d",
                     fields: str | list[str] | None = None) -> np.ndarray:
        """获取历史 Bar 数据返回 numpy 数组。兼容 RQAlpha history_bars()"""
        df = self.get_history(symbol, bar_count)
        if df.empty:
            return np.array([])
        if fields is None:
            fields = ["close"]
        elif isinstance(fields, str):
            fields = [fields]
        result = df.tail(bar_count)
        if len(fields) == 1:
            return result[fields[0]].to_numpy()
        return result[fields].to_numpy()

    def current_snapshot(self, symbol: str) -> dict | None:
        """当前市场快照。兼容 RQAlpha current_snapshot()"""
        bar = self.current_bar
        if bar is None:
            return None
        return {
            "symbol": symbol,
            "datetime": bar.datetime,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "last": bar.last,
            "close": bar.close,
            "volume": bar.volume,
            "total_turnover": bar.total_turnover,
            "prev_close": bar.prev_close,
            "limit_up": bar.limit_up,
            "limit_down": bar.limit_down,
        }

    def get_trading_dates(self, start_date: str, end_date: str) -> list[date]:
        """获取交易日列表"""
        from datetime import datetime as dt
        s = dt.strptime(start_date, "%Y-%m-%d").date() if isinstance(start_date, str) else start_date
        e = dt.strptime(end_date, "%Y-%m-%d").date() if isinstance(end_date, str) else end_date
        return list(self._event_source.iter_trading_dates())

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

    # ── 扩展 ──
    def __repr__(self) -> str:
        return (
            f"UserContext(date={self.current_date}, cash={self.cash:.0f}, "
            f"total_value={self.total_value:.0f}, positions={len(self._position_manager.positions)})"
        )


# ── 沙箱辅助函数 ──

def _make_sandbox_globals() -> dict:
    """构建用户脚本可用的全局函数（兼容 RQAlpha 内置指标）"""
    return {
        "np": np,
        "pd": pd,
        # 技术指标
        "MA": _ma,
        "EMA": _ema,
        "RSI": _rsi,
        "MACD": _macd,
        "HHV": _hhv,
        "LLV": _llv,
        "CROSS": _cross,
        "REF": _ref,
        "ROUND": lambda x, n=2: round(float(x), n) if x is not None else 0,
        "SMA": _sma,
        "ATR": _atr,
        "STD": _std,
        "COUNT": _count,
        "EVERY": _every,
        # 订单 & 状态常亮
        "Order": Order,
        "OrderStatus": OrderStatus,
        "MARKETORDER_SIGNAL": "market",
        # 供策略内直接调用的顶层函数
        "history_bars": None,  # 在 register 时注入
        "current_snapshot": None,
        "get_open_orders": None,
        "cancel_order": None,
        "order": None,
        "get_positions": None,
    }


# ── 内置指标 ──

def _ma(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.rolling(window=period, min_periods=max(1, period // 2)).mean()


def _ema(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.ewm(span=period, adjust=False).mean()


def _sma(series: pd.Series | np.ndarray, period: int, weight: float = 1.0) -> pd.Series:
    """扩展移动平均 SMA(X, N, M) = (M*X + (N-M)*SMA')/N"""
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    result = pd.Series(np.nan, index=s.index, dtype=float)
    if len(s) == 0:
        return result
    result.iloc[0] = s.iloc[0]
    alpha = weight / period
    for i in range(1, len(s)):
        if pd.notna(s.iloc[i]):
            result.iloc[i] = alpha * s.iloc[i] + (1 - alpha) * result.iloc[i - 1]
        else:
            result.iloc[i] = result.iloc[i - 1]
    return result


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


def _std(series: pd.Series | np.ndarray, period: int) -> pd.Series:
    s = pd.Series(series) if not isinstance(series, pd.Series) else series
    return s.rolling(window=period, min_periods=max(1, period // 2)).std()


def _atr(close: pd.Series, high: pd.Series, low: pd.Series, period: int = 14) -> pd.Series:
    """平均真实波幅 ATR"""
    c, h, l = pd.Series(close), pd.Series(high), pd.Series(low)
    tr = pd.concat([
        (h - l).abs(),
        (h - c.shift(1)).abs(),
        (l - c.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


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


def _count(condition: pd.Series | np.ndarray, n: int) -> pd.Series:
    """统计 N 周期内满足条件的次数"""
    c = pd.Series(condition) if not isinstance(condition, pd.Series) else condition
    return c.rolling(window=n, min_periods=1).sum()


def _every(condition: pd.Series | np.ndarray, n: int) -> pd.Series:
    """N 周期内是否全部满足条件"""
    return _count(condition, n) >= n


# ── UserScriptStrategy ──

class UserScriptStrategy:
    """用户脚本策略 — exec() 执行 init + handle_bar(context, bar_dict)

    RQAlpha 兼容: handle_bar 每天触发一次，bar_dict 提供 dict-like 访问所有标的 Bar。
    同时支持 before_trading(context) / after_trading(context) 生命周期回调。

    沙箱提供:
      - 全局函数: MA, EMA, RSI, MACD, HHV, LLV, CROSS, REF, SMA, ATR, STD, COUNT, EVERY
      - 顶层 API: order_shares, order_value, order_target_pct, order_target_percent, order,
                  history_bars, current_snapshot, get_open_orders, cancel_order, get_positions
      - 类型: Order, OrderStatus
      - 数学库: np, pd
    """

    def __init__(self, code: str, symbols: list[str], params: dict | None = None):
        self.code = code
        self.symbols = symbols
        self._params = params or {}
        self._init_fn = None
        self._handle_bar_fn = None
        self._before_trading_fn = None
        self._after_trading_fn = None
        self._ctx: UserContext | None = None

    def register(self, event_bus, ctx: UserContext) -> None:
        self._ctx = ctx
        namespace = _make_sandbox_globals()

        # 注入上下文引用
        namespace["context"] = ctx

        # 注入顶层 API 函数（绑定到 ctx）
        namespace["order_shares"] = ctx.order_shares
        namespace["order_value"] = ctx.order_value
        namespace["order_target_pct"] = ctx.order_target_pct
        namespace["order_target_percent"] = ctx.order_target_pct  # RQAlpha 全名别名
        namespace["order"] = ctx.order_shares
        namespace["history_bars"] = ctx.history_bars
        namespace["current_snapshot"] = ctx.current_snapshot
        namespace["get_open_orders"] = ctx.get_open_orders
        namespace["cancel_order"] = ctx.cancel_order
        namespace["get_positions"] = ctx.portfolio.get_positions
        namespace["get_history"] = ctx.get_history
        namespace["get_intraday"] = ctx.get_intraday
        namespace["get_intraday_history"] = ctx.get_intraday_history
        namespace["log"] = ctx.log

        try:
            exec(self.code, namespace)
        except Exception as exc:
            logger.error("Strategy script compile failed: {}", exc)
            raise ValueError(f"策略脚本编译失败: {exc}") from exc

        self._init_fn = namespace.get("init")
        self._handle_bar_fn = namespace.get("handle_bar")
        self._before_trading_fn = namespace.get("before_trading")
        self._after_trading_fn = namespace.get("after_trading")

        if self._handle_bar_fn is None and self._before_trading_fn is None:
            raise ValueError("策略脚本必须定义 handle_bar(context, bar_dict) 或 before_trading(context) 函数")

        # 调用 init
        if self._init_fn:
            try:
                self._init_fn(ctx)
            except Exception as exc:
                logger.error("init() failed: {}", exc)
                raise ValueError(f"init() 执行失败: {exc}") from exc

        # 注册事件监听
        event_bus.add_listener(EventType.BAR, self._on_bar)
        if self._before_trading_fn:
            event_bus.add_listener(EventType.BEFORE_TRADING, self._on_before_trading)
        if self._after_trading_fn:
            event_bus.add_listener(EventType.AFTER_TRADING, self._on_after_trading)

    def _on_before_trading(self, event: Event) -> None:
        ctx = self._ctx
        if ctx is None or self._before_trading_fn is None:
            return
        ctx.current_date = event.data.get("date")
        ctx._bar_dict = event.data.get("bar_dict")
        try:
            self._before_trading_fn(ctx)
        except Exception as exc:
            logger.error("before_trading failed on {}: {}", ctx.current_date, exc)

    def _on_bar(self, event: Event) -> None:
        ctx = self._ctx
        if ctx is None or self._handle_bar_fn is None:
            return

        bar_dict = event.data.get("bar_dict")
        ctx.current_date = event.data.get("date")
        ctx._bar_dict = bar_dict

        # bar_dict 是 BarDict，handle_bar(context, bar_dict)
        try:
            self._handle_bar_fn(ctx, bar_dict)
        except Exception as exc:
            logger.error("handle_bar failed on {}: {}", ctx.current_date, exc)

    def _on_after_trading(self, event: Event) -> None:
        ctx = self._ctx
        if ctx is None or self._after_trading_fn is None:
            return
        ctx.current_date = event.data.get("date")
        ctx._bar_dict = event.data.get("bar_dict")
        try:
            self._after_trading_fn(ctx)
        except Exception as exc:
            logger.error("after_trading failed on {}: {}", ctx.current_date, exc)
