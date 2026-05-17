# ID=43 小市值策略 JQ 对齐阶段记录

本文记录截至 2026-05-17，为了让本地 AKQuant 版本的小市值 V4 策略对齐聚宽源码与日志，已经确认并落地的参数、数据和行为口径调整。

## 当前对齐目标

- 阶段目标：先把 `2020-01-01` 到 `2020-12-31` 的收益、选股、持仓路径与聚宽日志大体对上。
- 聚宽源码：`docs/小市值.py`
- 聚宽日志：`C:\Users\Albert\Downloads\log.zip`
- 本地策略：`backend/app/backtest/strategies/small_cap_v4_akquant.py`
- 策略 ID：`43`

## 当前推荐回测参数

用于 2020 对齐的当前稳定参数：

```text
engine=akquant
index_symbol=399101.SZ
bar_type=minute_timer
smart_timer_candidate_top_n=0
smart_timer_full_universe_times=10:30
ak_history_depth=0
limit_price_source=table
high_volume_mode=daily_include_now
high_volume_check_time=14:30
industry_mode=jq_unknown
exit_on_last_bar=false
debug_logging=false
```

说明：

- `smart_timer_candidate_top_n=0` 当前语义是：只在 `10:30` 全池加载，其它 timer 只保留持仓/必要标的，避免全池分钟事件拖慢回测。
- `high_volume_mode=daily_include_now` 是为了复现 JQ 的 `get_bars(... unit='1d', include_now=True)` 行为；如果要做更真实的盘中成交量判断，可以切成 `minute_to_now`，但这会偏离当前 JQ 日志。
- `exit_on_last_bar=false` 是为了避免回测结束日强平污染和 JQ 的持仓路径对比。
- `industry_mode=jq_unknown` 是当前对齐日志时使用的降级口径，避免本地行业映射与 JQ 行业接口不一致导致候选池提前变化。

## 已落地的数据口径

### 指数池

- 使用 `399101.SZ` / 聚宽 `399101.XSHE`。
- 每次调仓按当时的历史指数成分取池子，不再用当前自选股 960 只静态股票池。
- 这是选股路径对齐的前提。

### 日线与市值

- 补齐了 2019 年日线和 `daily_basic`，用于 2020 年初指标、放量窗口和市值排序。
- 小市值排序优先使用 point-in-time 的 `stock_daily_basic.total_mv/circ_mv`。
- 避免使用当前 SQLite `stocks.total_mv` 或当天 close 估算导致未来数据。

### 涨跌停价

- 默认 `limit_price_source=table`。
- 优先读取 SQLite `stock_limit_prices`，数据来自 Tushare `stk_limit`。
- 对分钟线撮合时，会把 `stk_limit` 的日线价格空间按前一交易日分钟 15:00 close / 日线 close 做价格空间调整。
- 这样避免自己用 `prev_close * 1.1/0.9` 算涨跌停时被复权口径污染。

### 分钟线

- 当前已导入本地聚宽版全 A 1 分钟线到 Parquet。
- ID=43 对齐阶段只依赖固定 timer 点，不走全量分钟逐 bar 策略。
- 放量卖出当前默认不用分钟累计量，而是用日线 `include_now` 近似口径，以匹配 JQ 日志。

## 已落地的策略行为口径

### V4GV/MACD 指标

已调整为更接近 JQ 源码：

- 使用 `akquant.talib` 的 `SMA/EMA/MIN/MAX/MACD`，不再用本地手写 EMA/SMA 作为主路径。
- `calculate_indicator()` 按 JQ 源码使用日历天窗口：
  - `end_date = context.previous_date`
  - `start_date = end_date - timedelta(days=max(n, m) * 2)`
- `get_market_volatility()` 也改为 JQ 源码的日历天窗口：
  - `start_date = previous_date - timedelta(days=days + 10)`
- 这个修复非常关键：2020 年春节后，JQ 因日历窗口内交易日不足，V4GV 会出现 `inputs are all NaN`，本地之前用固定交易日根数补足，导致 2020-02-05 提前卖出。

### 放量卖出

当前默认：

```text
high_volume_mode=daily_include_now
```

原因：

- JQ 源码使用 `get_bars(stock, count=120, unit='1d', fields=['volume'], include_now=True)`。
- 本地若使用 1 分钟线累计到 14:30，部分股票会比 JQ 更早触发放量。
- 典型差异：`002921.SZ` 在 `2020-03-11` 本地分钟累计会触发放量，但 JQ 日志没有卖出；切回日线 include_now 口径后该差异消失。

### T+1 与日志口径

- `close_position_()` 已检查可卖数量，避免同日买入后 14:30 实际不可卖。
- V4GV 死叉日志改为只有卖单实际通过 T+1/可卖数量检查并提交后才记录。
- 早期问题：2020-02-04 买入后本地 14:30 曾记录死叉卖出信号，但实际没有成交，污染了日志对比。

### 市场止损日期

- 市场止损使用 `context.previous_date` 对应的前一交易日指数 open/close。
- 避免使用当前交易日数据形成未来函数。

## 2020 阶段结果

当前完整 2020 本地结果：

```text
报告目录: backend/app/reports/small_cap_2020_step8_full_year
区间: 2020-01-01..2020-12-31
最终资产: 129631.78
总收益: 29.63%
交易数: 28
订单数: 58
```

与 JQ 日志对比摘要：

```text
JQ 买入数: 30
AK 买入数: 30
JQ 卖出数: 29
AK 卖出数: 27
最后共同资产点: 2020-12-25
JQ: 124651.87
AK: 129241.78
差异: +4589.91 / +3.68%
```

已基本对齐的关键节点：

- `2020-02-04` 首批选股一致：
  - `002473.SZ, 002633.SZ, 002112.SZ, 002209.SZ, 002921.SZ`
- `2020-03-05` `002112.SZ` 放量卖出对齐。
- `2020-03-16` `002633.SZ / 002209.SZ / 002921.SZ` V4GV 死叉卖出对齐。
- `2020-05-07` 调仓选股已对齐：
  - `002071.SZ, 002633.SZ, 002209.SZ, 002473.SZ, 002112.SZ`

## 当前剩余差异

下一阶段建议从以下日期继续单点 debug：

- `2020-05-18/2020-05-19`：`002071.SZ` 止损卖出相差 1 天。
- `2020-06-19`：JQ 有 `002633.SZ:high_volume`，本地没有。
- `2020-06-22`：JQ 只卖 `002473.SZ`，本地还卖了 `002209.SZ`。
- `2020-06-23`：本地发生一次额外调仓，JQ 没有。
- `2020-07-07`：JQ 有调仓买入，本地路径已经偏离。

这些差异大概率来自：

- 止损使用的当前价口径仍有差异。
- JQ `get_current_data().last_price/high_limit` 与本地分钟/日线价格空间不完全一致。
- 日线估值和分钟成交价来自不同来源，本地资产净值仍存在 1%-3% 的价格口径偏差。
- 放量 `include_now` 在 JQ 内部到底使用“当前日线快照”还是“当日截至当前时点聚合值”，仍需按单只股票反查。

## 常用对比命令

短区间回测：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
$env:MARKET_DATA_BACKEND='parquet'
$env:CLICKHOUSE_ENABLED='false'
$env:AKQUANT_BULK_MINUTE_TIMER_BATCH='250'
.\backend\.venv\Scripts\python.exe -m app.scripts.run_small_cap_full_debug `
  --strategy-id 43 `
  --index-symbol 399101.SZ `
  --start 2020-01-01 `
  --end 2020-03-20 `
  --initial-capital 100000 `
  --out backend/app/reports/small_cap_2020_debug `
  --industry-mode jq_unknown `
  --no-exit-on-last-bar `
  --smart-timer-candidate-top-n 0 `
  --smart-timer-full-universe-times 10:30 `
  --enable-timer-snapshots `
  --no-debug-logging `
  --ak-history-depth 0 `
  --limit-price-source table
```

日志对比：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\backend\.venv\Scripts\python.exe backend/app/scripts/compare_small_cap_logs.py `
  --jq-log C:\Users\Albert\Downloads\log.zip `
  --ak-log backend/app/reports/small_cap_2020_debug/strategy.log `
  --out backend/app/reports/small_cap_2020_debug/jq_compare/daily_event_compare.csv `
  --asset-out backend/app/reports/small_cap_2020_debug/jq_compare/asset_compare.csv `
  --summary-out backend/app/reports/small_cap_2020_debug/jq_compare/summary.json `
  --start 2020-01-01 `
  --end 2020-12-31 `
  --limit 40
```

