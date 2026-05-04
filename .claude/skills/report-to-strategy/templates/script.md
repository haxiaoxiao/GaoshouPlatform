# 脚本策略模板

适用于多条件选股 + 定时调仓。通过事件引擎 `handle_bar` 每个交易日执行。

## 模板骨架

```python
def init(context):
    # 策略参数
    context.param1 = value
    context.positions = {}

def handle_bar(context, bar_dict):
    today = context.now.date()

    # 调仓频率控制
    if not is_rebalance_day(today, context):
        return

    # 平仓
    for sym in list(context.positions.keys()):
        p = context.portfolio.get_position(sym)
        if p and p.total_shares > 0:
            order_shares(sym, -p.total_shares)
        del context.positions[sym]

    # 筛选候选
    candidates = []
    for sym in context.universe:
        close = context.get_daily_close(sym, today)
        pe = context.get_indicator(sym, "pe_ttm", today)
        # 条件判断
        if condition:
            candidates.append((sym, close))

    # 排序买入
    candidates.sort(key=lambda x: -x[1])
    for sym, price in candidates[:N]:
        order_value(sym, cash / len(candidates[:N]))
        context.positions[sym] = {"entry_price": price}
```

## 可用函数

- `context.get_daily_close(symbol, as_of_date)` → float
- `context.get_weekly_ma(symbol, period, as_of_date)` → float
- `context.get_indicator(symbol, name, date)` → float (查 stock_indicators 表)
- `context.get_all_symbols()` → list[str] (全量A股，非ST非退市)
- `order_shares(symbol, shares)` → 下单
- `order_value(symbol, value)` → 按金额下单
- `context.portfolio.get_position(symbol)` → 持仓对象
