# 数据存储清单与保留决策

本文件记录当前平台里容易混淆的历史数据集，避免把“派生缓存”误判成无用冗余。审计时间：2026-06-10。

注意：文件数和大小是本机快照，不是代码层面的硬约束。清理前以当前公共数据目录 `E:\Projects\Data\parquet` 和 SQLite 实际状态为准。

## 当前本机快照

### Parquet

| 数据集 | 文件数 | 大小 | 决策 | 原因 |
|---|---:|---:|---|---|
| `klines_daily` | 90 | 203.0 MB | 保留 | 股票与指数日线统一入口，回测、因子、DataSkill 都依赖。 |
| `klines_minute` | 37,020 | 53.5 GB | 保留 | 完整分钟线，是分钟回测和 timer 派生数据的源数据。 |
| `klines_minute_timer` | 当前未落地 | - | 条件保留 | 固定时点分钟线，用于 `minute_timer` 回测；有数据时不能用累计成交量表替代。 |
| `klines_minute_cum_timer` | 77 | 57.6 MB | 内部保留 | 派生行情缓存，只供预计算任务读取；最终放量信号应写入 `factor_values` 后再给策略使用。 |
| `factor_values` | 77+ | 289.5 MB+ | 保留 | 当前 Factor Value Store 主缓存，TA-Lib、Alpha101、研究因子和小市值因子写这里。 |
| `factor_cache` | 当前未落地 | - | 兼容保留 | 旧表达式计算缓存，`/api/compute/precompute` 和 Compute Engine 仍引用。 |
| `stock_indicators` | 66 | 4.0 MB | 保留 | Indicator 体系、DataSkill 指标查询、分红/深度价值策略仍引用。 |
| `indicator_timeseries` | 78 | 3.2 MB | 候选收敛 | 时序指标缓存，当前体量很小；只有确认不再运行分钟指标调度后才可归档。 |

### SQLite

| 表 | 行数 | 决策 | 原因 |
|---|---:|---|---|
| `stocks` | 5,304 | 保留 | 股票基础信息主表。 |
| `financial_data` | 48,606 | 保留 | 研究因子和财务筛选依赖。 |
| `index_components` | 133,742 | 保留 | 严格历史指数成分，不能用当前成分替代。 |
| `stock_daily_basic` | 8,424,547 | 保留 | 市值、换手率等截面因子依赖。 |
| `stock_limit_prices` | 9,594,979 | 保留 | 涨跌停、停牌相关因子和策略过滤依赖。 |
| `stock_name_changes` | 2,143 | 保留 | ST/名称变化历史。 |
| `sync_runs` / `sync_logs` / `sync_tasks` | 25 / 96 / 0 | 保留 | 数据同步进度和排障记录。 |
| `factor_analysis` / `factors` | 1 / 1 | 保留 | 因子研究元数据。 |
| `strategies` / `backtests` / `orders` / `trades` | 6 / 48 / 0 / 0 | 保留 | 回测与交易记录。 |

`financial_data.ann_date` 口径：

- `ann_date` 用于财务因子的 point-in-time 对齐，因子或回测只能在 `trade_date >= ann_date` 后使用对应报告期。
- 优先来源为 Tushare `disclosure_date.actual_date`，按 `end_date` 批量获取实际披露日，并映射到 `financial_data(symbol, report_date)`。
- 少量 `disclosure_date` 缺失时，使用 `balancesheet.f_ann_date` 做保守兜底；不要优先使用更早的 `balancesheet.ann_date`。
- 2026-06-03 prod 快照：`financial_data` 48,606 行，其中季度报告行 33,469 行，已补 `ann_date` 33,463 行，剩余 6 条为 2026 年新上市股票的 IPO 前历史年报；非季末/`Unknown` 行缺口 15,137 条，不作为财务 PIT 因子输入。

## 容易混淆的数据集

### `klines_minute_timer` 与 `klines_minute_cum_timer`

二者不是重复。

`klines_minute_timer` 保存固定时点的原始分钟 K 线，用于只在指定时点驱动的回测。

`klines_minute_cum_timer` 保存指定时点的累计成交量，是高成交量、放量类因子的计算加速缓存。它可以从完整分钟线重建，但不能直接替代 timer K 线。

策略脚本不应直接读取 `klines_minute_cum_timer`。正确链路是：预计算任务读取累计量缓存，生成 `high_volume_ratio`、`high_volume_signal` 等 `factor_values`，策略只读取最终因子。

### `factor_cache` 与 `factor_values`

二者目前并存是兼容历史造成的，但不是同一张表。

`factor_cache` 面向表达式计算引擎，key 以 `expr_hash` 为主。

`factor_values` 面向当前因子库和策略复用，key 以 `factor_name + params_hash + as_of_time` 为主。

收敛方向：新增内置因子只写 `factor_values`；旧表达式缓存先保留，等 `/api/compute/precompute` 和相关回测读取迁移完成后再归档。

Alpha101、TA-Lib 和小市值相关内置因子的新预计算结果都应进入 `factor_values`。如果公式口径变更，需要重算缓存，而不是直接改写历史文件。

### `stock_indicators` 与 `indicator_timeseries`

二者属于 Indicator 体系。

`stock_indicators` 是截面指标，仍被 DataSkill、分红同步、深度价值策略、表达式指标算子引用。

`indicator_timeseries` 是时序指标，当前体量很小，可作为候选收敛对象；历史测试脚本 `compute_minute_indicators.py` 已删除，后续只需确认 Indicator Scheduler 不再写入。

## 清理原则

1. 不直接删除被 API、策略、因子或同步任务引用的数据集。
2. 对派生缓存先提供“重建脚本 + 覆盖验证”，再允许归档。
3. 归档优先于删除：移动到 `data/archive/` 并记录时间、来源和恢复方式。
4. 新增因子缓存统一写 `factor_values`，不要再扩展 `factor_cache` 的内置因子用途。
5. 新增策略不得直接读取内部派生缓存；策略层只依赖 `factor_values`、公开指标表和标准行情 feed。详细约定见 `docs/strategy-data-contract.md`。
6. 如果要真正清理，第一批只建议评估 `indicator_timeseries` 的归档，暂不动分钟线、日线、指数成分和当前因子缓存。
