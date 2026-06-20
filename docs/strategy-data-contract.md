# 策略数据契约

Last updated: 2026-06-10.

本文定义策略脚本、因子缓存和底层行情/派生缓存之间的边界，目标是避免策略绕过公共接口直接依赖内部数据集，后续维护时不会出现隐形依赖。

## 三层边界

```text
原始/派生行情层
klines_daily / klines_minute / klines_minute_timer / klines_minute_cum_timer

↓ 只能由数据同步、预聚合、因子预计算任务读取

因子契约层
factor_values / stock_indicators / indicator_timeseries

↓ 策略脚本只读取这里和标准 AKQuant 行情 feed

策略层
AKQuant strategy scripts / UserScript strategies
```

## 策略允许读取

- AKQuant 标准行情 feed 提供的 `bar`、timer 事件、成交/组合对象。
- `FactorPipeline`、`FactorValueStore`、`stock_indicators` 等公开因子/指标契约。
- SQLite 中稳定的股票元数据，例如 `stocks` 的行业、市值、概念字段；后续如有 DataSkill 同步接口，应优先迁移到服务接口。

## 策略禁止直读

- 不直接 `read_parquet` 或硬编码 `E:/Projects/Data/parquet`。
- 不直接查询 DuckDB 或底层存储。
- 不直接读取 `klines_minute_cum_timer` 这类派生行情缓存。
- 不把 `klines_minute_timer` 当作因子来源；timer bar 只能通过 AKQuant data feed 进入撮合/事件循环。

## 内部缓存规则

内部派生缓存可以存在，但必须满足：

- 可从上游原始数据重建。
- 有明确生成脚本或同步任务。
- 有覆盖率验证方式。
- 只作为预计算依赖，不作为策略脚本的稳定输入。
- 最终策略信号要落在 `factor_values` 或公开指标表里。

`klines_minute_cum_timer` 的定位是内部派生行情缓存：它保存指定时点的累计成交量，用于快速生成 `high_volume_ratio`、`high_volume_signal` 等策略因子。策略应读取这些最终因子，而不是读取累计量缓存。

## 新策略上线检查

新增或修改策略脚本后，运行：

```powershell
cd E:\Projects\GaoshouPlatform-prod
.\backend\.venv\Scripts\python.exe .\factor_eval_runs\strategy_scripts\validate_strategy_contract.py .\factor_eval_runs\strategy_scripts\tech_small_cap_multi_factor_akquant.py
```

如果校验失败，应优先把底层数据读取迁移到因子预计算任务，再让策略读取 `factor_values`。只有明确、临时、可审计的例外才允许在对应行添加 `strategy-contract: allow` 注释。

## 本策略推荐口径

科技小市值多因子策略默认使用日频/周频调仓。股票池不再绑定 `399101.SZ` 中小综指，而是使用全市场 A 股作为输入，在调仓日做以下过滤：

- 上市满 365 天，剔除新股/次新股。
- 剔除调仓日前已退市的股票。
- 通过 point-in-time 因子剔除 ST、停牌、涨跌停不可交易状态。
- 按当日市值升序排序，剔除最小 50 只，从倒数第 51 只开始参与小市值排序。
- 再做科技主线行业/概念白名单和传统行业黑名单过滤。

盘中信息先预计算为 timer 因子：

- 10:30：`is_paused`、`is_limit_up`、`is_limit_down`
- 14:30：`high_volume_ratio`、`high_volume_signal`

长区间 `minute_timer` 回测只有在需要真实盘中撮合、盘中止损或 timer 事件驱动时才启用。否则优先用 `daily` bar type，加上预计算的 timer 因子完成过滤和风控。
