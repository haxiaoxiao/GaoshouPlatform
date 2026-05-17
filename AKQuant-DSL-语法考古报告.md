# AKQuant Python DSL 深度语法考古报告

> 生成日期：2026-05-18
> 基于源码分析：`python/akquant/`, `examples/`, `tests/`, `src/`

---

## Phase 1：项目结构勘探

### 分类汇总

| 分类 | 文件数 | 代表文件 |
|------|--------|----------|
| **核心绑定层** | 28 个 | `strategy.py` (2445行), `strategy_trading_api.py` (1942行), `strategy_events.py` (207行), `strategy_framework_hooks.py` (686行), `__init__.py` (356行) |
| **回测引擎** | 2 个 | `backtest/engine.py` (5303行), `backtest/result.py` (1769行) |
| **官方示例** | 64 个 | `01_quickstart.py` ~ `64_indicator_live_web.py` |
| **测试用例** | 41 个 | `tests/test_*.py` |
| **工具/数据** | 多个子包 | `plot/` (7文件), `gateway/` (10文件), `factor/` (4文件), `ml/` (2文件), `talib/` (4文件), `utils/` (2文件) |

### 厚度评估

| 判定 | 阈值 | 核心文件数 |
|------|------|-----------|
| **THICK** | >200行 | 35 (56%) |
| **Thin** | <200行 | 28 (44%) |

**结论**：Python 层**不是薄层**。`strategy.py` 有 2445 行，`strategy_trading_api.py` 有 1942 行，`backtest/engine.py` 有 5303 行。DSL 的大部分逻辑在 Python 层实现，Rust 层负责核心数据结构（Bar、Order、Tick、Trade）和高性能指标计算。

---

## Phase 2：Strategy 基类与生命周期钩子

### 2.1 钩子定义表

| 钩子名称 | 触发时机 | 参数类型与关键属性 | 返回值 | 源码位置 | Example 引用 |
|---------|---------|-------------------|--------|---------|-------------|
| `on_bar` | 每根 K 线到达 | `bar: Bar` (字段: `symbol`, `open`, `high`, `low`, `close`, `volume`, `timestamp: int`(纳秒), `timestamp_str: str`, `extra: dict`) | `None` | `strategy.py:1465` | `01_quickstart.py:36`, `06_complex_orders.py:25` |
| `on_tick` | 每笔 Tick 到达 | `tick: Tick` (字段: `symbol`, `price`, `volume`, `timestamp: int`, `timestamp_str: str`) | `None` | `strategy.py:1488` | — |
| `on_timer` | 定时器触发 | `payload: str` (注册时携带的字符串) | `None` | `strategy.py:1496` | — |
| `on_order` | 订单状态变更 | `order: Order` (字段: `id`, `symbol`, `side`, `order_type`, `status`, `quantity`, `filled_quantity`, `price`, `trigger_price`, `average_filled_price`, `tag`, `commission`, 等) | `None` | `strategy.py:1383` | `06_complex_orders.py:59` |
| `on_trade` | 订单成交 | `trade: Trade` (字段: `id`, `order_id`, `symbol`, `side`, `quantity`, `price`, `commission`, `timestamp: int`, `timestamp_str: str`, `bar_index`, `position_effect`) | `None` | `strategy.py:1392` | `06_complex_orders.py:49` |
| `on_start` | 回测开始前 | 无参数 | `None` | `strategy.py:751` | — |
| `on_stop` | 回测结束后 | 无参数 | `None` | `strategy.py:783` | — |
| `__init__` | 策略实例化 | 用户自定义参数 | — | `strategy.py:499` | `01_quickstart.py:30`, `06_complex_orders.py:11` |

### 2.2 Strategy `__init__` 分析

`Strategy.__init__()` (`strategy.py:499-501`) 是空的（`pass`），策略参数通过**子类 `__init__` 中设置实例属性**的方式声明：

```python
class MyStrategy(Strategy):
    def __init__(self, period=20, stop_loss_pct=0.02, **kwargs):
        self.period = period
        self.stop_loss_pct = stop_loss_pct
```

另外，AKQuant 还提供了 **Pydantic 参数系统** (`params.py`)：通过 `ParamModel` 基类 + `IntParam`/`FloatParam`/`BoolParam`/`ChoiceParam` 声明参数并自动校验。

### 2.3 关键问题回答

#### Q: `on_bar` 在多标的中是逐标的触发还是聚合触发？

**逐标的触发**。每次回调只携带一个 `Bar` 对象，通过 `bar.symbol` 区分当前处理的是哪个标的。证据：

- `strategy_events.py:55`：`symbol = bar.symbol`
- `strategy_events.py:88`：`strategy._last_prices[bar.symbol] = bar.close`
- `strategy.py:1465-1471`：`def on_bar(self, bar: Bar) -> None:` — 单个 bar 参数
- `01_quickstart.py:42`：用户代码 `symbol = bar.symbol` 来区分标的

#### Q: `bar.timestamp_str` 的时区格式？

**`"YYYY-MM-DD HH:MM:SS"` 格式，Asia/Shanghai 时区**。证据：

- Rust 源码 `src/model/order.rs:57-60`：`format_timestamp_str` 将纳秒时间戳除以 10^9 后格式化
- Rust 测试 `src/model/order.rs:721-723`：`timestamp=1_735_801_200_000_000_000` (纳秒) → `"2025-01-02 15:00:00"`（+08:00 时区）
- `strategy_logging.py:13`：Python 日志也使用 `strftime("%Y-%m-%d %H:%M:%S")`

#### Q: 策略参数如何声明？

两种方式：
1. **`__init__` 中硬编码实例属性**（最常见）：`self.period = period`
2. **Pydantic ParamModel 系统** (`params.py`)：声明式参数 + 自动校验

---

## Phase 3：交易/订单 API 全谱系

### 3.1 完整 API 表

| API 名称 | 参数签名 | 返回值 | 使用场景 | 特殊约束 | 源码位置 | Example 引用 |
|---------|---------|--------|---------|---------|---------|-------------|
| `buy` | `symbol=None, quantity=None, price=None, time_in_force=None, trigger_price=None, tag=None, order_type=None, trail_offset=None, trail_reference_price=None, fill_policy=None, slippage=None, commission=None, position_effect=None, reduce_only=False` | `str` (order_id) | 市价/限价/止损买入 | 无特殊回调限制 | `strategy_trading_api.py:210` | `04_mixed_assets.py:51` |
| `sell` | 同 buy | `str` (order_id) | 市价/限价/止损卖出 | 无特殊回调限制 | `strategy_trading_api.py:270` | `01_quickstart.py:74` |
| `short` | `symbol=None, quantity=None, price=None, time_in_force=None, trigger_price=None, tag=None, fill_policy=None, slippage=None, commission=None, reduce_only=False` | `None` | 卖出开空 | `position_effect="open"` | `strategy_trading_api.py:1811` | — |
| `cover` | 同 short | — | 买入平空 | `position_effect="close"` | `strategy_trading_api.py:1878` | — |
| `close_position` | `symbol=None` | `None` | 清仓（多/空均平） | 不支持部分平仓，全仓平 | `strategy_trading_api.py:1800` | `01_quickstart.py:81` |
| `cancel_order` | `order_id: str` | `None` | 撤销指定订单 | — | `strategy_trading_api.py:177` | — |
| `cancel_all_orders` | `symbol=None` | `None` | 撤销所有未完成订单 | — | `strategy_trading_api.py:204` | — |
| `place_bracket_order` | `symbol: str, quantity: float, entry_price=None, stop_trigger_price=None, take_profit_price=None, time_in_force=None, entry_tag=None, stop_tag=None, take_profit_tag=None` | `str` (entry_order_id) | 一次性挂入场+止损+止盈 | Entry 成交后自动挂止盈/止损单；止盈止损之间自动绑定 OCO | `strategy.py:1770` | `06_complex_orders.py:38` |
| `create_oco_order_group` | `first_order_id: str, second_order_id: str, group_id=None` | `str` (group_id) | 手动绑定两条订单为 OCO | 任一成交自动撤销另一条 | `strategy.py:1727` | — |
| `order_target` | `symbol=None, target: int` | `None` | 调整持仓至目标数量 | 自动计算买卖方向 | `strategy_trading_api.py:1410` | — |
| `order_target_value` | `symbol=None, target_value: float` | `None` | 调整持仓市值至目标 | — | `strategy_trading_api.py:1432` | — |
| `order_target_percent` | `symbol=None, target_percent: float` | `None` | 调仓至目标仓位比例(0~1) | — | `strategy_trading_api.py:1487` | `01_quickstart.py:60` |
| `order_target_weights` | `weights: Dict[str, float]` | `None` | 多标的调仓至目标权重 | 支持多标的一次调仓 | `strategy_trading_api.py:1500` | — |
| `order_target_positions` | `targets: Dict[str, float]` | — | 多标的调整至目标持仓数 | — | `strategy_trading_api.py:1582` | — |
| `buy_all` | `symbol=None` | `None` | 全仓买入 | — | `strategy_trading_api.py:1777` | — |
| `submit_order` | `symbol=None, side="Buy", quantity=None, price=None, time_in_force=None, trigger_price=None, tag=None, client_order_id=None, order_type=None, extra=None, broker_options=None, trail_offset=None, trail_reference_price=None, fill_policy=None, slippage=None, commission=None, position_effect=None, reduce_only=False` | `str` (order_id) | 通用下单入口 | 所有 buy/sell 最终调用此方法 | `strategy_trading_api.py:740` | — |

### 3.2 关键问题回答

#### Q: `buy/sell` 是否支持 `limit_price`, `stop_price`？

**支持**。`price` 参数即为限价（`None` 时为市价），`trigger_price` 为触发价格（止损/止盈条件单触发价）。不需要仅通过 `place_bracket_order` 做条件单。此外还有独立的 `stop_buy()` / `stop_sell()` 快捷方法。

#### Q: 订单 API 返回值类型？

**`str` (order_id)** 或 `None`。`buy/sell/submit_order/place_bracket_order` 返回 `str`；`close_position/order_target/buy_all/short/cover` 返回 `None`。

#### Q: 多标的场景下能否在 `on_bar` 中遍历全市场标的并批量下单？

**可以**。`on_bar` 每次只回调当前 symbol，但 `strategy.ctx.positions` 可以获取所有标的的持仓，`buy/sell` 可以指定任意 `symbol` 参数来在任何 symbol 上下单。`order_target_weights` / `order_target_positions` 原生支持多标的一次调仓。

---

## Phase 4：持仓/账户/数据查询 API

| API 名称 | 参数 | 返回值结构与类型 | 含义 | 源码位置 | Example 引用 |
|---------|------|----------------|------|---------|-------------|
| `get_position` | `symbol=None` | `float` (正=多仓, 负=空仓) | 当前持仓数量 | `strategy.py:1567` | `01_quickstart.py:43` |
| `get_available_position` | `symbol=None` | `float` | 可用持仓（考虑T+1） | `strategy.py:1579` | — |
| `get_positions` | 无 | `Dict[str, float]` | 所有持仓 | `strategy.py:1603` | — |
| `get_cash` | 无 | `float` | 可用资金 | `strategy.py:2394` | — |
| `get_account` | 无 | `dict` (含 cash, equity, market_value, frozen_cash, margin, borrowed_cash, short_market_value, maintenance_ratio, account_mode, accrued_interest, daily_interest) | 账户详情快照 | `strategy.py:1678` | — |
| `get_portfolio_value` | 无 | `float` | 总权益(组合市值) | `strategy_trading_api.py:1036` | — |
| `get_history` | `count: int, symbol=None, field="close"` | `np.ndarray` | 历史数据序列 | `strategy.py:915` | — |
| `get_history_df` | `count: int, symbol=None` | `pd.DataFrame` (OHLCV) | 历史数据 DataFrame | `strategy.py:1050` | — |
| `get_history_map` | `count: int, symbols: List[str], field="close"` | `Dict[str, np.ndarray]` | 多标的历史数据 | `strategy.py:928` | — |
| `get_rolling_data` | `length=None, symbol=None` | `tuple[DataFrame, Optional[Series]]` | 滚动训练数据 | `strategy_history.py:73` | — |
| `get_open_orders` | `symbol=None` | `List[Order]` | 未完成订单 | `strategy.py:1654` | — |
| `get_order` | `order_id: str` | `Order \| None` | 订单详情 | `strategy.py:1666` | — |
| `get_trades` | 无 | `List[ClosedTrade]` | 已平仓交易 | `strategy.py:1698` | — |
| `get_instrument` | `symbol: str` | `InstrumentSnapshot` | 标的属性快照 | `strategy.py:1618` | — |
| `get_instruments` | `symbols=None` | `Dict[str, InstrumentSnapshot]` | 多标的属性 | `strategy.py:1624` | — |
| `hold_bar` | `symbol=None` | `int` | 持仓已持有的 Bar 数 | `strategy.py:1591` | — |
| `calculate_max_buy_qty` | `symbol=None, price=None` | `float` | 最大可买数量 | `strategy_trading_api.py:1372` | — |
| `position` (property) | — | `Position` 对象 (`.size`, `.available`) | 当前标的持仓辅助 | `strategy.py:1473` | — |

### 关键问题回答

#### Q: 是否提供内置历史数据查询 API？

**提供**。`self.get_history(count, symbol, field)` → `np.ndarray`（`strategy.py:915-929`）。底层通过 `self.ctx.history(symbol, field, count)` 调用 Rust 层的 HistoryBuffer。使用前需调用 `self.set_history_depth(N)` 启用。还有 `self.get_history_df(count, symbol)` 返回完整 OHLCV DataFrame。

#### Q: `bar` 对象是否包含 `prev_close`, `change_pct` 等衍生字段？

**不包含**。`Bar` 只有原始 OHLCV 字段 + `extra: dict[str, float]`（可选扩展字典）。没有 `prev_close`、`change_pct` 等衍生字段，需要用户自行计算。

---

## Phase 5：run_backtest 参数与数据格式

### 完整参数表

| 参数 | 类型 | 必填 | 说明 | 源码位置 |
|-----|------|------|------|---------|
| `data` | `DataFrame \| Dict[str, DataFrame] \| List[Bar] \| DataFeed \| DataFeedAdapter \| None` | 否 | K线数据；None 时配合 DataFeed 使用 | `backtest/__init__.pyi:103` |
| `strategy` | `Type[Strategy] \| Strategy \| Callable \| None` | 否 | 策略（类/实例/回调函数） | `backtest/__init__.pyi:104` |
| `symbols` | `str \| List[str] \| Tuple[str] \| set[str]` | 否 | 交易标的列表 | `backtest/__init__.pyi:108` |
| `initial_cash` | `float` | 否 | 初始资金（默认值在 BacktestConfig 中） | `backtest/__init__.pyi:109` |
| `commission_rate` | `float` | 否 | 手续费率 | `backtest/__init__.pyi:110` |
| `stamp_tax_rate` | `float` | 否 | 印花税率（仅卖出） | `backtest/__init__.pyi:111` |
| `transfer_fee_rate` | `float` | 否 | 过户费率 | `backtest/__init__.pyi:112` |
| `min_commission` | `float` | 否 | 最低手续费 | `backtest/__init__.pyi:113` |
| `slippage` | `float \| SlippagePolicyInput` | 否 | 滑点设置 | `backtest/__init__.pyi:114` |
| `volume_limit_pct` | `float` | 否 | 成交量限制比例 | `backtest/__init__.pyi:115` |
| `timezone` | `str` | 否 | 时区（默认 "Asia/Shanghai"） | `backtest/__init__.pyi:116` |
| `t_plus_one` | `bool` | 否 | 是否启用 T+1 | `backtest/__init__.pyi:117` |
| `lot_size` | `int \| Dict[str, int]` | 否 | 最小交易单位 | `backtest/__init__.pyi:140` |
| `start_time` / `end_time` | `str \| Any` | 否 | 回测起止时间 | `backtest/__init__.pyi:142-143` |
| `config` | `BacktestConfig` | 否 | 综合配置对象（含 StrategyConfig + InstrumentConfig） | `backtest/__init__.pyi:144` |
| `on_event` | `Callable[[BacktestStreamEvent], None]` | 否 | 流式事件回调 | `backtest/__init__.pyi:170` |
| `fill_policy` | `FillPolicyInput` | 否 | 成交价基准（open/close/ohlc4/hl2/mid_quote/vwap_window/twap_window） | `backtest/__init__.pyi:172` |
| `stream_mode` | `"observability" \| "audit"` | 否 | 流模式 | `backtest/__init__.pyi:173` |
| `strict_strategy_params` | `bool` | 否 (默认 True) | 严格参数校验 | `backtest/__init__.pyi:174` |
| `warmup_period` | `int` | 否 (默认 0) | 预热期（跳过前 N 根 bar） | `backtest/__init__.pyi:139` |
| `risk_config` | `Dict \| RiskConfig` | 否 | 风控配置 | `backtest/__init__.pyi:146` |

### 关键问题回答

#### Q: 多标的 data 格式？

**支持两种方式**：
1. **`dict[str, DataFrame]`**：`{"600000": df_1, "600004": df_2}` — 每个 symbol 一个 DataFrame（推荐，见 `01_quickstart.py:20`）
2. **单个 `DataFrame`（含 `symbol` 列）**：所有标的数据在一个大 DataFrame 中，通过 `symbol` 列区分（见 `06_complex_orders.py:80-86`）
3. 也支持 `List[Bar]`、`DataFeed`、`DataFeedAdapter` 等

#### Q: 手续费、滑点、印花税在哪配置？

**在 `run_backtest` 参数中直接配置**：
- `commission_rate` / `stamp_tax_rate` / `transfer_fee_rate` / `min_commission` 为直接参数
- `slippage` 可以是 `float`（简易滑点）或 `SlippagePolicy` 字典（百分比/固定/跳点/零滑点）
- `fill_policy` 可指定成交价基准和时序
- 也支持按 strategy 级别的差异化配置（`strategy_fill_policy`、`strategy_slippage`、`strategy_commission` 参数）

#### Q: `symbols` 与 `data` 的关系？

`symbols` 和 `data` 可以同时传入，也可以只传其一：
- 如果 `data` 是 `dict[str, DataFrame]`：`symbols` 可用来过滤/排序处理顺序
- 如果 `data` 是含 `symbol` 列的单个 DataFrame：`symbols` 指定要交易的标的子集
- 如果 `data` 为 `None`：由 DataFeed 或 DataFeedAdapter 提供数据，symbols 指定标的

---

## Phase 6：AKQuant 策略语法速查表

| API/概念 | 使用场景 | 示例代码 | 注意事项 | 依据 |
|---------|---------|---------|---------|------|
| `on_bar` | 接收每根K线 | `def on_bar(self, bar): print(bar.symbol)` | 逐标的回调；用 `bar.symbol` 区分 | `strategy.py:1465` |
| `on_tick` | 接收每笔Tick | `def on_tick(self, tick): print(tick.price)` | 需要 Tick 数据源 | `strategy.py:1488` |
| `on_order` | 订单状态变更通知 | `def on_order(self, order): if order.status == OrderStatus.Filled: ...` | 检查 `order.status` 枚举 | `strategy.py:1383` |
| `on_trade` | 订单成交通知 | `def on_trade(self, trade): print(trade.price)` | 用于精确记录成交价 | `strategy.py:1392` |
| `buy` | 买入下单 | `self.buy(symbol="600000", quantity=100)` | 返回 `str` order_id；`price` 限价，`trigger_price` 条件触发 | `strategy.py:1926` |
| `sell` | 卖出下单 | `self.sell(symbol, 50, price=10.5)` | limit_price 通过 `price` 参数 | `strategy.py:1965` |
| `short` | 卖出开空 | `self.short(symbol="IF", quantity=1)` | 自动 `position_effect="open"` | `strategy_trading_api.py:1811` |
| `cover` | 买入平空 | `self.cover(symbol="IF", quantity=1)` | 自动 `position_effect="close"` | `strategy_trading_api.py:1878` |
| `close_position` | 全平仓 | `self.close_position()` | 不支持部分平仓 | `strategy_trading_api.py:1800` |
| `place_bracket_order` | 入场+止损+止盈 | `self.place_bracket_order(symbol, 100, stop_trigger_price=95, take_profit_price=110)` | Entry成交后自动挂止损止盈并绑OCO | `strategy.py:1770` |
| `create_oco_order_group` | 手动OCO绑定 | `self.create_oco_order_group(oid1, oid2)` | 任一成交自动撤销另一 | `strategy.py:1727` |
| `cancel_order` | 撤单 | `self.cancel_order(order_id)` | — | `strategy.py:1709` |
| `order_target` | 调仓至目标数量 | `self.order_target(symbol, 200)` | 自动计算买卖方向 | `strategy_trading_api.py:1410` |
| `order_target_percent` | 调仓至目标仓位比 | `self.order_target_percent(target_percent=0.33, symbol=symbol)` | 0~1 之间 | `strategy_trading_api.py:1487` |
| `order_target_weights` | 多标权重调仓 | `self.order_target_weights({"A": 0.6, "B": 0.4})` | 自动计算各标的买卖量 | `strategy_trading_api.py:1500` |
| `order_target_positions` | 多标的目标持仓数 | `self.order_target_positions({"A": 100, "B": -50})` | 负数为空头 | `strategy_trading_api.py:1582` |
| `get_position` | 查询持仓数量 | `pos = self.get_position("600000")` | 正=多仓，负=空仓，0=无 | `strategy.py:1567` |
| `get_cash` | 查询可用资金 | `cash = self.get_cash()` | — | `strategy.py:2394` |
| `get_account` | 账户详情快照 | `acc = self.get_account(); print(acc["equity"])` | 返回 dict | `strategy.py:1678` |
| `get_history` | 获取历史数据 | `closes = self.get_history(20, symbol, "close")` | 返回 `np.ndarray`；需先 `set_history_depth()` | `strategy.py:915` |
| `get_history_df` | 获取历史OHLCV | `df = self.get_history_df(20, symbol)` | 返回完整 DataFrame | `strategy.py:1050` |
| `get_history_map` | 多标历史数据 | `data = self.get_history_map(20, ["A","B"], "close")` | 返回 `dict[str, np.ndarray]` | `strategy.py:928` |
| `schedule` | 注册定时回调 | `self.schedule(timestamp_ns, "my_payload")` | 触发 `on_timer(payload)` | `strategy.py:850` |
| `add_daily_timer` | 每日定时器 | `self.add_daily_timer("14:55", "close_positions")` | 简洁的日定时器 | `strategy.py:861` |
| `record_indicator` | 记录指标值 | `self.record_indicator("ma", ma_val, symbol)` | 用于后续可视化和导出 | `strategy.py:1521` |
| `get_instrument` | 获取标的配置 | `inst = self.get_instrument(symbol); print(inst.multiplier)` | 返回 `InstrumentSnapshot` | `strategy.py:1618` |
| `position` (property) | 当前标的持仓 | `if self.position.size == 0: ...` | 语法糖，自动取当前 bar symbol | `strategy.py:1473` |
| `run_backtest` | 启动回测 | `aq.run_backtest(data=df, strategy=MyStrategy, initial_cash=1e6)` | data 支持 dict/DataFrame/List[Bar]/DataFeed | `backtest/__init__.pyi:102` |
| `Bar` (Rust) | K线数据结构 | `bar.symbol`, `bar.close`, `bar.timestamp` (纳秒), `bar.timestamp_str` ("2025-01-02 15:00:00"), `bar.extra` | Rust 绑定导出，Python 层无源码 | `akquant.pyi:307` |
| `Order` (Rust) | 订单对象 | `order.id`, `order.status`, `order.filled_quantity`, `order.side`, `order.order_type` | Rust 绑定导出 | `akquant.pyi:938` |
| `Trade` (Rust) | 成交记录 | `trade.order_id`, `trade.price`, `trade.quantity`, `trade.commission` | Rust 绑定导出 | `akquant.pyi:2682` |

---

## 附录：关键源码索引

| 文件 | 行数 | 职责 |
|------|------|------|
| `python/akquant/strategy.py` | 2445 | Strategy 基类、所有用户 API 方法定义 |
| `python/akquant/strategy_trading_api.py` | 1942 | 订单/交易/账户查询的实现逻辑 |
| `python/akquant/strategy_events.py` | 207 | `on_bar`/`on_tick`/`on_timer` 事件分发 |
| `python/akquant/strategy_framework_hooks.py` | 686 | 框架生命周期钩子（边界定时器、组合更新等） |
| `python/akquant/strategy_history.py` | 84 | `get_history`/`get_history_df`/`get_rolling_data` |
| `python/akquant/strategy_position.py` | 33 | `Position` 持仓辅助类 |
| `python/akquant/strategy_order_events.py` | 230 | 订单事件处理、成交去重 |
| `python/akquant/strategy_scheduler.py` | 103 | 定时器调度 |
| `python/akquant/params.py` | 224 | Pydantic 参数声明系统 |
| `python/akquant/backtest/engine.py` | 5303 | 回测引擎主逻辑 |
| `python/akquant/backtest/__init__.pyi` | 216 | `run_backtest` 完整签名 |
| `python/akquant/akquant.pyi` | 2822 | Rust 绑定导出的所有类型定义（自动生成） |
| `src/model/market_data.rs` | — | Bar/Tick Rust 实现 |
| `src/model/order.rs` | — | Order/Trade Rust 实现含 `timestamp_str` 格式化 |

---

## Phase 7：全量 Examples 索引表

> 共 76 个 .py 文件，分类：64 主示例 + 8 策略 + 16 教科书章节 + 2 工具

### 7.1 主示例 (01–64)

| 编号 | 主题 | 关键API/模式 | 要点摘要 |
|------|------|-------------|---------|
| 01 | 快速入门回测 | `run_backtest()`, `order_target_percent()`, `sell()`, `close_position()`, `result.metrics` | 三标的回测：33%仓位买入，止盈10%或持有100bar平仓；手动NumPy交叉验证总收益/年化/波动率/最大回撤/R2 |
| 02 | 参数网格搜索 | `ParamModel`, `IntParam`, `run_grid_search()`, `get_strategy_param_schema()` | Pydantic声明式参数模型，`sort_by="total_return"`排序，`max_workers=2`并行 |
| 03 | 高级优化(WFO) | `run_grid_search()`多字段排序, `run_walk_forward()`, `warmup_calc`/`constraint`回调 | `sort_by=["sharpe_ratio","total_return"]`多目标；`train_period=100, test_period=50` |
| 04 | 混合资产(股票+期货) | `InstrumentConfig(symbol, asset_type, multiplier, margin_ratio)`, `BacktestConfig` | 期货`asset_type="FUTURES"`/`multiplier=300`/`margin_ratio=0.1`；`BacktestConfig(instruments_config=[...])`注入 |
| 05 | CTP实时仿真交易 | `LiveRunner(strategy_cls, instruments, md_front)`, `Instrument()`完整字段 | `use_aggregator=False`将每个Tick当Bar处理；`duration="1m"`设定运行时长 |
| 06 | Bracket Order条件单 | `place_bracket_order(symbol, quantity, entry_price, stop_trigger_price, take_profit_price)`, `on_trade()`, `on_order()` | entry/stop/take三个tag标记子订单；`on_trade`判断进场成交后自动挂止损止盈；OCO自动绑定 |
| 07 | 期权到期结算 | `InstrumentConfig(asset_type="OPTION", option_type, strike_price, expiry_date, settlement_type="cash")` | 现金结算到期价值归零验证；`RiskConfig(safety_margin=0.0001)` |
| 08 | 全部事件回调演示 | `on_start/bar/timer/order/trade/reject/portfolio_update/stop` | `add_daily_timer("14:55:00","close_check")`注册定时器；`fill_policy`精准撮合控制 |
| 09 | ML框架适配器 | `SklearnAdapter(model)`, `PyTorchAdapter(network, criterion, optimizer_cls)`, `.fit()/.predict()/.save()` | Sklearn和PyTorch统一接口；跨进程序列化验证 |
| 10 | ML滚动训练 | `self.model.set_validation(method="walk_forward", train_window, test_window, rolling_step)`, `prepare_features()`, `is_model_ready()` | 滚动窗口WFO；`current_validation_window()`获取active/pending状态 |
| 11 | 回测可视化 | `result.report(title, filename, show, market_data, plot_symbol, include_trade_kline)`, `format_metric_value()` | 一键生成含交易标记的K线回放HTML报告 |
| 12 | 独立Walk-Forward | `run_walk_forward(strategy, param_grid, data, train_period, test_period, metric, compounding, constraint)` | `compounding=False`简单累加；`metric="sharpe_ratio"` |
| 13 | QuantStats报告集成 | `result.to_quantstats()`, `result.report_quantstats(benchmark, filename)` | 返回pd.Series含时区收益率序列；`benchmark=None`跳过基准下载 |
| 14 | 多频率数据混合 | `BasePandasFeedAdapter.replay(freq, align, session_windows)`, `register_incremental_indicator()` | 分钟线聚合为日线；`session_windows`定义A股交易时段；通过不同symbol区分频率(`000001.SZ` vs `000001.SZ_1D`) |
| 15 | 日内分钟级回测 | `result.report()`分钟数据自适应X轴 | 日线数据线性插值+噪声扩展为240根分钟线(上下午各120) |
| 16 | 前复权信号+真实撮合 | `bar.extra.get("adj_close")`, `get_history(count, symbol, "adj_close")` | 从前复权收盘价计算信号，撮合使用真实close；`lot_size=100` |
| 17 | 最简入门 | `Strategy.on_bar`, `bar.close > bar.open`阴阳线交易, `on_event=events.append` | 双通道输出：result + 事件列表；最简策略结构模板 |
| 18 | 多标的性能基准 | `dict[str, DataFrame]`输入, `self.set_history_depth(0)`, `fill_policy={"price_basis":"open"}` | 关闭Python历史缓存提升性能；按symbol独立管理指标字典 |
| 19 | 因子表达式引擎 | `ParquetDataCatalog`, `FactorEngine`, `engine.run("Ts_Mean(Close,5)")`, `engine.run_batch()` | Alpha101风格表达式：Ts_Mean/Std/Delta/Rank/Ts_Corr/If；独立于回测的因子计算 |
| 20 | 盘前风控管理 | `risk_config={"max_position_pct":0.10, "sector_concentration":(...)}`, `result.orders_df["status"]`, `reject_reason` | 单标仓位限制+行业集中度；订单被拒后附带reject_reason |
| 21 | 热启动断点续传 | `save_snapshot(engine, strategy, path)`, `run_warm_start(checkpoint_path=, data=)`, `is_restored`, `on_resume` | 阶段一运行后pkl快照；阶段二从快照恢复无需重新传入strategy类 |
| 22 | 运行时配置覆盖 | `StrategyRuntimeConfig`, `runtime_config_override`, `strategy_runtime_config={"error_mode":"continue"}` | `error_mode`支持"raise"/"continue"两种模式覆盖策略默认值 |
| 23 | 函数式回调策略 | `run_backtest(strategy=on_bar, initialize=initialize, on_tick=, on_order=, ...)` | 无需class继承Strategy；ctx对象替代self管理状态和交易API |
| 24 | 函数式Tick仿真 | `FunctionalStrategy(initialize=, on_bar=, on_tick=, ...)`, `Tick(timestamp, price, volume, symbol)`, `_on_tick_event` | 手动构造Tick调用内部事件分发；`DemoContext`模拟StrategyContext接口 |
| 25 | 流式回测事件驱动 | `stream_error_mode="continue"/"fail_fast"`, `on_event`, `BacktestStreamEvent`, `stream_batch_size` | "continue"跳过异常累计callback_error_count；"fail_fast"立即抛RuntimeError |
| 26 | 流式快速启动 | `BacktestConfig`+`StrategyConfig`+`RiskConfig`三层配置, `fill_policy={"price_basis":"ohlc4"}`, `strategy_id` | ohlc4均价成交基准；`stream_progress_interval=1`每个bar都发progress事件 |
| 27 | 流式监控控制台 | `get_history(count=N, symbol=, field="close")`, strategy_factory闭包, `StreamMonitor` dataclass | 手动获取历史计算均线替代SMA；双参数集并行对比运行 |
| 28 | 流式告警与持久化 | `stream_equity_interval=4`, equity事件`payload["equity"]`, drawdown告警, csv持久化 | drawdown<=-3%触发[alert]；所有事件seq/ts/run_id写入CSV |
| 29 | 流式事件报表 | `load_events(csv_path) -> pd.DataFrame`, plotly交互图表 | 累计事件计数折线图+柱状分布+告警散点标记；`include_plotlyjs="cdn"` |
| 30 | 一键报表流水线 | `subprocess.run`串联脚本, `ThreadingHTTPServer`+`partial(handler, directory=...)`, argparse | 执行28→29两步；`--serve`启动HTTP预览；`webbrowser.open`自动打开 |
| 31 | 流式实盘控制台 | `sparkline(values, width=42)` unicode迷你走势图, `\r`+`flush=True`原地刷新, `LiveState` dataclass | unicode方块字符▁▂▃▄▅▆▇█绘制走势图；单行动态刷新终端仪表盘 |
| 32 | 流式实盘Web仪表盘 | `threading.Thread`双线程, `BaseHTTPRequestHandler`+JSON `/state`, Canvas前端, `setInterval`轮询 | 回测线程+HTTP Server；前端Canvas增量绘制权益曲线；`--sleep-ms`控制实时感 |
| 33 | 报告与分析输出 | `result.report()`, `result.exposure_df()`, `result.attribution_df(by=)`, `result.capacity_df()`, `result.orders_by_strategy()` | 多维度分析DataFrame导出；`compact_currency=True`格式化金额 |
| 34 | 多策略槽位风控 | `StrategyConfig(strategies_by_slot={...}, strategy_max_order_size={...}, strategy_risk_cooldown_bars={...})` | 主副策略槽位独立风控限制；`owner_strategy_id`列区分订单归属 |
| 35 | 自定义Broker注册 | `register_broker("demo", builder_fn)`, `create_gateway_bundle()`, `unregister_broker()`, `GatewayBundle` | 动态注册/注销券商网关；builder需返回`GatewayBundle`(含MarketGateway+TraderGateway) |
| 36 | 追踪止损单 | `place_trailing_stop(symbol=, quantity=, trail_offset=, side=, trail_reference_price=, tag=)` | `trail_offset`控制跟随距离；`trail_reference_price`设定初始参考价；入场后自动提交 |
| 37 | Feed数据重放对齐 | `BasePandasFeedAdapter.replay(freq="15min", align="session"/"global"/"day", emit_partial=, session_windows=[...])` | 对比session/global/day三种对齐模式对分时数据重采样的影响 |
| 38 | 实盘函数式策略 | `LiveRunner(strategy_cls=on_bar, initialize=, on_order=, context={...}, broker="ctp", trading_mode="paper")` | 函数式回调+LiveRunner；ctx.buy/sell直接交易；期货配置 |
| 39 | 实盘broker_live下单 | `LiveRunner(trading_mode="broker_live", td_front=, broker_id=, user_id=, ...)`, `ctx.submit_order(client_order_id=)` | broker_live真单模式与paper模拟的区别；client_order_id与broker_order_id映射 |
| 40 | 函数式多槽位风控 | `run_backtest(strategy=alpha_on_bar, strategies_by_slot={"beta": beta_on_bar}, strategy_max_order_value={...})` | 主副策略均函数式；风控事件event_type="risk"含owner_strategy_id归因 |
| 41 | 实盘多slot编排 | `LiveRunner(strategy_cls=primary_on_bar, strategy_id="alpha", strategies_by_slot={"beta": SecondarySlotStrategy})` | 主策略函数式+副策略类式混用；通过strategy_id区分配置 |
| 42 | 实盘Broker事件审计 | `LiveRunner(on_broker_event=on_broker_event)`, event["event_type/owner_strategy_id/payload"] | 统一监听所有order/trade/report事件；适用于第三方日志或风控对接 |
| 43 | 目标权重组合调仓 | `order_target_weights(target_weights=dict, liquidate_unmentioned=True, rebalance_tolerance=0.01)` | 横截面动量选股→等权目标权重；`rebalance_tolerance=0.01`避免微小调仓 |
| 44 | 动态策略源码加载 | `strategy_source=str(path)`, `strategy_loader="python_plain"/"encrypted_external"`, `strategy_attr`/`decrypt_and_load` | python_plain从磁盘加载；encrypted_external通过回调解密加载；strategy参数设None |
| 45 | TA-Lib指标策略 | `from akquant import talib as ta`, `ta.EMA(close, timeperiod=20, backend="rust")`, `ta.ADX/BBANDS/RSI/NATR/MOM` | 组合趋势跟踪和均值回归信号；所有指标`backend="rust"`加速；akshare数据+合成数据双源 |
| 46 | Broker配置模板 | `run_backtest(broker_profile="cn_stock_t1_low_fee", ...)` | 一个字符串一键设定A股T+1低费率(佣金/印花税/过户费/最小佣金/lot_size) |
| 47 | 保证金强平审计 | `RiskConfig(account_mode="margin", enable_short_sell=True, initial_margin_ratio=0.5, allow_force_liquidation=True, liquidation_priority="short_first")` | 做多后暴跌触发维持保证金不足；`get_account()`返回完整快照；`liquidation_audit_df`记录强平 |
| 48 | 强平优先级对比 | `RiskConfig(liquidation_priority="short_first"/"long_first", maintenance_margin_ratio=4.0)` | 双标的多空对冲持仓下对比两种强平优先级；`liquidated_symbols`字段显示被强平标的 |
| 49 | 期货到期回调 | `on_expiry(event)`, `InstrumentConfig(asset_type="FUTURES", expiry_date=, settlement_type=, settlement_price=)`, `BacktestStreamEvent` | event含symbol/expiry_date/quantity_closed/cash_flow/settlement_type |
| 50 | 框架生命周期钩子 | `on_session_start/end`, `on_before/after_trading`, `on_daily_rebalance`, `on_portfolio_update`, `on_reject`, `enable_precise_day_boundary_hooks` | 完整框架级生命周期回调序列；`strategy_max_position_size`触发`on_reject` |
| 51 | Tick级别回调(类式) | `on_tick(tick)`, `Tick(timestamp,price,volume,symbol)`, `_on_tick_event(tick, ctx)`, `StrategyContext` cast | 手动构造Tick调用内部事件；`SimpleNamespace`模拟上下文cast为StrategyContext |
| 52 | 开盘前决策回调 | `on_pre_open(event)`, event含`trading_date`/`expected_open_at`, `format_time()` | "盘前决策，开盘成交"模型；on_pre_open下单，开盘Bar执行 |
| 53 | 跨日定时+盘前执行 | `add_daily_timer("15:00:00", "prepare_next_day")`, `on_timer(payload)`, `self.close`/`self.now` | "隔日准备-执行"两阶段：收盘定时保存计划，次日on_pre_open执行 |
| 54 | 函数式盘前回调 | `run_backtest(strategy=on_bar, initialize=, on_pre_open=, on_order=, on_trade=)` | 函数式风格等价52；所有回调函数接收ctx作为第一参数 |
| 55 | 函数式ML滚动训练 | `SklearnAdapter(LogisticRegression())`, `set_validation(method="walk_forward", train_window=50, test_window=20, rolling_step=10)` | ctx风格ML；`ctx.get_history_df()`获取训练数据；`ctx.is_model_ready()` |
| 56 | 函数式温启动 | `save_snapshot()`, `run_warm_start()`, `on_resume(ctx)`, `ctx.is_restored` | 两阶段运行；状态(processed_closes等)自动持久化 |
| 57 | 多槽位函数式温启动 | `run_backtest(strategy_id="alpha", strategies_by_slot={"beta": beta_on_bar})`, `save_snapshot`, `run_warm_start` | 每个slot独立on_start/on_resume/on_bar；`_slot_strategies`字典访问子策略 |
| 58 | 增量指标历史预热 | `register_incremental_indicator("sma3", indicator_factory=lambda: aq.SMA(3), source="close", symbols=[], warmup_bars=3)` | `start_time`前Bar自动预热；`indicator_factory`每个symbol独立实例；活跃区间首个Bar即得有效值 |
| 59 | AKShare ETF轮动 | `ak.fund_etf_hist_em()`, `get_history_map(count=, symbols=, field=)`, `order_target_weights()`, `on_daily_rebalance` | 真实ETF横截面动量轮动；中文列名映射英文；`rebalance_tolerance`容差 |
| 60 | 自定义指标 | `Indicator("name", lambda)`, `register_precomputed_indicator()`, 继承`Indicator`实现`update(value)`/`value`属性 | 预计算模式：`get_value(symbol, timestamp)`；增量模式：`indicator_factory`+`warmup_bars` |
| 61 | 指标可视化导出 | `record_indicator(name=, value=, display_name=, pane=, render_type=, precision=)`, `result.indicator_df()`, `result.plot_indicators()`, `result.export_indicators(path, format="json")` | 完整指标工作流：记录→DataFrame→HTML图→JSON导出 |
| 62 | 指标流式事件 | `on_event=events.append`, `indicator_stream_point_interval`, `indicator_stream_snapshot_interval` | 事件类型分indicator_point(单点)和indicator_snapshot(快照)；seq单调递增 |
| 63 | 指标WebSocket桥接 | `to_indicator_messages(events)`, `to_indicator_message(event)`, 消息含type/"point"/"snapshot"/indicator_key/value | BacktestStreamEvent转前端友好WebSocket格式；可直接推浏览器 |
| 64 | 实时指标Web演示 | `ThreadingHTTPServer`, `to_indicator_message(event)`, 浏览器轮询`/state?since_seq=`, Canvas绘图 | 完整实时演示：回测线程+HTTP Server+Canvas前端增量绘制；`--port/sleep-ms/keep-seconds` |

### 7.2 策略模板 (examples/strategies/)

| 文件 | 主题 | 关键API | 要点 |
|------|------|---------|------|
| 01_stock_dual_moving_average.py | A股双均线 | `ak.stock_zh_a_daily(adjust="qfq")`, `get_history()`, `np.mean()`, `order_target_percent()` | A股实盘双均线金叉死叉；`warmup_period`必须设置才能用`get_history`；`commission_rate=0.0003`/`stamp_tax_rate=0.001` |
| 02_stock_grid_trading.py | 股票网格交易 | `buy(symbol, quantity)`, `sell(symbol, quantity)`, `last_trade_price`字典 | 经典网格：下跌grid_pct买入一份，上涨grid_pct卖出一份；`lot_size=100` |
| 03_stock_atr_breakout.py | ATR通道突破 | 手动计算TR和ATR, `get_history(count=, field="high"/"low"/"close")` | 取N+1数据`[:-1]`剔除当前bar避免未来函数；`warmup_period=period+1` |
| 04_stock_momentum_rotation.py | 多股票动量轮动 | `data={symbol: DataFrame}`多标输入, `order_target_percent(, symbol=)`按symbol调仓 | 在最后一个symbol的Bar触发轮动(`bar.symbol != self.symbols[-1]`) |
| 05_stock_momentum_rotation_timer.py | on_daily_rebalance轮动 | `on_daily_rebalance(trading_date, timestamp)`, `get_history_map()`, `rebalance_to_topn(scores, top_n, weight_mode, long_only, liquidate_unmentioned)` | `rebalance_to_topn`核心API自动处理调仓；`get_history_map`一次获取所有标的 |
| 06_stock_momentum_rotation_bucket.py | 收齐同时间戳轮动 | `defaultdict(set)`, `_pending_by_ts[bar.timestamp]`收集同一timestamp | 所有symbol同timestamp到达后统一轮动，避免on_bar先后顺序问题 |
| 07_stock_momentum_rotation_on_timer.py | on_timer定时轮动 | `add_daily_timer("10:00:00", "rebalance")`, `on_timer(payload)`, `rebalance_to_topn()` | 固定时点调仓；payload过滤触发 |
| 08_target_positions_long_short.py | 目标仓位多空 | `order_target_positions({"AAA":100.0}, liquidate_unmentioned=True)`, `allow_short=True`, `get_last_target_positions_plan()`, `RiskConfig(account_mode="margin")` | 正数多头负数空头；`get_last_target_positions_plan()`返回reduce/increase/submitted/skipped legs |

### 7.3 教科书章节 (examples/textbook/)

| 文件 | 主题 | 关键API | 要点 |
|------|------|---------|------|
| ch01_quickstart.py | 快速开始 | `ak.stock_zh_a_daily()`, `run_backtest()`, `order_target_percent()` | 最小完整回测流程；策略类直接传入而非实例化 |
| ch02_programming.py | Python生存指南 | `pd.DataFrame.rolling().mean()`, `pd.resample("5D")`, 向量化vs循环 | Pandas/NumPy速成；`Optional[float]`模拟Rust Option |
| ch03_data.py | 金融数据获取 | `ak.stock_zh_a_hist(period="daily", adjust="qfq")`, 中文列名映射, `df.to_parquet()` | 完整ETL流程：获取→映射→类型转换→添加symbol→排序去重→Parquet存储 |
| ch04_comparison.py | Pandas vs Backtrader vs AKQuant | Pandas向量化(`shift(1)`防未来函数), Backtrader(`bt.Strategy`, `bt.ind.CrossOver`), AKQuant(Rust引擎) | 同一双均线三种实现对比；都需`shift(1)`避免未来函数 |
| ch05_strategy.py | 构建第一个策略 | 生命周期`__init__/on_start/on_bar/on_stop`, `self.get_history()`, `self.log()` | 完整策略结构模板；`warmup_period=long_window+1`；止损逻辑模板 |
| ch06_stock_a.py | A股T+1与涨跌停 | `get_available_position(symbol)`, `t_plus_one=True`, `buy()`/`sell()` | T+1限制：当天买入后卖出被拒；`get_available_position`返回可用持仓 |
| ch07_futures.py | 期货与衍生品 | `InstrumentConfig(asset_type="FUTURES", multiplier=10.0, margin_ratio=0.1)`, `ChinaFuturesConfig`, `short()` | 合约乘数和保证金率配置；`fill_policy`+`slippage`显式声明 |
| ch08_options.py | 期权与衍生品 | `InstrumentConfig(asset_type="OPTION", margin_ratio=0.0)`, `ChinaOptionsConfig(fee_per_contract=5.0)` | Covered Call备兑看涨；买方不收保证金，引擎自动计算卖方保证金 |
| ch09_funds.py | ETF网格交易 | `commission_rate=0.0001`, `stamp_tax_rate=0.0`基金免印花税 | ETF佣金万一免印花税；中枢定价+分档挂单 |
| ch09_portfolio.py | 股债平衡配置 | `order_target_value(target_value, symbol)`, `get_portfolio_value()`, 日计数器定期再平衡 | 60/40策略；每20交易日触发；`bar.symbol != self.stock_symbol`防重复 |
| ch10_analysis.py | 策略评价体系 | `result.metrics_df`, `result.trades_df`, `result.equity_curve`/日频版本, `result.report(curve_freq="D")` | 安全封装`get_metric`函数防缺失字段；bar级和日频双分辨率曲线 |
| ch11_optimization.py | 参数优化与过拟合 | `run_grid_search(strategy=, data=, param_grid=, max_workers=4)`, `OptimizationResult.params.metrics` | param_grid键名必须与`__init__`参数名一致；多进程parallel |
| ch12_ml.py | ML量化应用(手动) | `StandardScaler`+`LogisticRegression`, `get_history_df(count=)`, `shift(-1)`构造label | 自实现ML框架(非内置)；特征工程`calculate_features()`统一；概率阈值0.55/0.45 |
| ch13_visualization.py | 可视化与基准对比 | `result.report(benchmark=benchmark_returns)`, `include_trade_kline=True` | Plotly交互报告含基准对比；K线图标注交易点位 |
| ch14_factor.py | 因子表达式引擎 | `ParquetDataCatalog(root_path=)`, `FactorEngine(catalog)`, `engine.run("Ts_Mean(Close,5)")` | 多标的Parquet存储；Ts_Mean/Std/Rank/Ts_Corr等因子函数 |
| ch15_live_trading.py | 实盘交易CTP | `LiveRunner(strategy_cls=, instruments=[], md_front=, td_front=, broker_id=, ...)` | SimNow模拟环境；需`akquant[ctp]`额外依赖 |
| ch15_strategy_loader.py | 动态策略加载 | `strategy_source=str`, `strategy_loader="python_plain"/"encrypted_external"`, `strategy_attr`/`decrypt_and_load` | 运行时从源码或加密文件加载策略；支持加密策略分发场景 |

### 7.4 工具文件

| 文件 | 用途 | 关键API |
|------|------|---------|
| benchmark_utils.py | 大规模模拟数据生成 | `get_benchmark_data(n=200000, symbol=, freq=, start_time=, seed=)`, 几何布朗运动, MD5确定性种子 |
| pb_mock.py | ML Walk-Forward参考实现 | `SklearnAdapter(Pipeline([StandardScaler(), LogisticRegression()]))`, `set_validation()`, `prepare_features(df, mode="training"/"inference")` |
