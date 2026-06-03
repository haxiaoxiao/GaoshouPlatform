# Dev 数据接口覆盖审计

Last updated: 2026-06-01.

本审计回答：逐页开发/重构时，当前 `data/dev_sample` 是否能覆盖各数据接口需要的 SQLite/Parquet 存储。

## 结论

核心前端开发链路已有 dev 样本存储覆盖：

- 数据中心：股票、行业、自选股、同步日志、日线、分钟线。
- DataSkill：股票快照、财务、日线、分钟线、截面指标、时序指标。
- 数据浏览器核心表：`klines_daily`、`klines_minute`、`klines_minute_timer`、`klines_minute_cum_timer`、`factor_values`、`stock_indicators`、`indicator_timeseries`。
- 因子定义/因子评估：因子元数据、因子值缓存、预计算依赖、研究 run 记录。
- 策略回测：策略/回测元数据、指数成分、涨跌停、日线/分钟/timer 数据、因子值。
- 舆情核心接口：SQLite `sentiment_posts`。

仍需明确边界：

- 旧 `/api/strategy/signals/*` 与 `/api/strategy/backtest` 里的 `TrendCapitalStrategy` 仍直接依赖 ClickHouse，不是 dev Parquet 隔离链路。
- `/api/grid-trading/*` 是 QMT/账户桥接型接口，不应该用真实账户数据做 dev 数据替身；当前通过 `GRID_TRADING_ENABLE_ORDER_SUBMIT=false` 保持安全。
- `factor_cache`、`block_moneyflow`、`announcements`、`research_reports`、`market_news` 当前源端没有可抽样数据，因此 dev 样本标记为 optional missing；页面如果要展示新闻/公告，应先走 SQLite `sentiment_posts` 或后续接入真实源数据后再抽样。
- `backtests` 有 6 条记录引用已经不存在的 `strategies.id`，生产源库也存在同样问题；当前审计作为 warning，不回写 prod。

## 覆盖矩阵

| 接口组 | 代表路由 | Dev 覆盖状态 | 存储口径 |
|---|---|---|---|
| System status | `/api/system/status` | covered | `klines_daily`、`klines_minute`、`klines_minute_timer`、`klines_minute_cum_timer` |
| Data center | `/api/data/*` | covered | `stocks`、`watchlist_*`、`sync_*`、`index_components`、日/分钟 Parquet |
| DataSkill | `/api/skill/*` | covered | `stocks`、`financial_data`、`stock_daily_basic`、`stock_limit_prices`、指标 Parquet |
| Explorer core | `/api/explorer/*` | covered | 核心行情、因子值、指标时序 Parquet |
| Explorer auxiliary | `/api/explorer/*` | core covered | `adj_factors`、`moneyflow`、`auction_replay`、`ths_*` 已有；部分源端缺失 |
| Factor definition | `/api/factor/*`、`/api/factors/*` | covered | `factors`、`factor_analysis`、日线 |
| Factor values | `/api/factor-values/*` | core covered | `factor_values`、日/分钟、涨跌停、市值、财务 |
| Factor research | `/api/factor-research/*` | covered | `factor_research_*`、`factor_values`、日线、股票池/风控基础表 |
| Indicators | `/api/indicators/*` | covered | `stock_indicators`、`indicator_timeseries`、`financial_data` |
| Backtest | `/api/backtest/*`、`/api/v2/backtest/*` | covered | `strategies`、`backtests`、指数成分、行情、因子值 |
| Sentiment | `/api/sentiment/*` | core covered | `sentiment_posts`；Parquet 新闻/公告源端暂无 |
| Legacy strategy signals | `/api/strategy/signals/*` | external dependency | 仍直接读 ClickHouse |
| Grid trading | `/api/grid-trading/*` | external dependency | QMT/账户桥接，dev 不模拟真实账户 |

## 当前样本数据规模

```text
SQLite:
  stocks=65
  financial_data=607
  stock_daily_basic=520
  stock_limit_prices=520
  index_components=61
  sentiment_posts=22
  strategies=7
  backtests=9
  factor_research_runs=100
  factor_research_run_items=100
  sync_runs=50
  sync_logs=100

Parquet:
  klines_daily=520
  klines_minute=158337
  klines_minute_timer=1314
  klines_minute_cum_timer=520
  factor_values=73731
  stock_indicators=640
  indicator_timeseries=585
  ths_index=409
  ths_member=38
  adj_factors=1
  moneyflow=1
  auction_replay=1
```

## 审计命令

```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.\.venv\Scripts\python.exe -m app.scripts.validate_dev_sample_data `
  --json-report E:\Projects\GaoshouPlatform-dev\data\dev_sample\validation-report.json
```

`validation-report.json` 在 ignored 的样本目录里，适合每次重切样本后检查，不提交到仓库。
