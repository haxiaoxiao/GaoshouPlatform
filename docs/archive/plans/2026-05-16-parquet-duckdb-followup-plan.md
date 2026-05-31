# Parquet/DuckDB 改造后续执行计划

> 创建日期：2026-05-16  
> 目标读者：继续接手的执行 Agent  
> 状态：待实施  
> 前置状态：`MarketDataStore` 抽象、Parquet/DuckDB store、AKQuant `minute_timer` 关键路径、K 线同步 Parquet 写入、timer 稀疏分钟线 Parquet 分支已完成并通过核心测试。

## 1. 当前已完成的能力

已完成：

- 新增 `MarketDataStore` 抽象。
- 新增 `ParquetMarketDataStore` 和 `ClickHouseMarketDataStore`。
- 新增 `StoreDataProvider`，回测数据可通过 store 读取。
- AKQuant adapter 的 bulk/lazy 数据加载已改走 `IDataProvider/MarketDataStore`，不再内部直连 ClickHouse。
- `sync_kline_daily` / `sync_kline_minute` 在默认 `parquet` 模式下不再创建 ClickHouse client。
- `timer_minute_sync` 已有 Parquet 分支：
  - 已有 timer 点检查
  - 预期点数估算
  - 缺失月份判断
  - 写入 `klines_minute_timer`
  - timer 覆盖率查询
- Parquet 写入已按 `year/month` 分区合并并按唯一键去重，重复同步保留最新值。
- `ParquetMarketDataStore.has_data(bar_type="minute_timer")` 已优先检查 `klines_minute_timer`。

已验证：

```powershell
cd E:\Projects\GaoshouPlatform\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\.venv\Scripts\python.exe -m pytest tests\data_stores\test_parquet_store.py tests\backtest\test_akquant_parquet_provider.py tests\backtest\test_akquant_integration.py -q
```

当前结果：`36 passed, 1 warning`。

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

当前结果：通过，仅有 Vite chunk size warning。

## 2. 当前目标

继续推进目标不是“删除 ClickHouse”，而是：

1. 默认 `MARKET_DATA_BACKEND=parquet` 时，平台核心研究/回测功能不依赖 Docker/ClickHouse。
2. ClickHouse 继续保留为可选高性能后端。
3. 所有业务模块逐步从“直接 `get_ch_client()`”改为：
   - `MarketDataStore`，用于行情 K 线。
   - `IndicatorStore` 或现有 SQLite/Parquet 扩展，用于指标。
   - 明确标记为 ClickHouse-only 的工具脚本/API。

## 3. 必须遵守的原则

- 不要替换 SQLite。
- 不要删除 ClickHouse 代码。
- 不要改变前端 API 返回结构。
- 不要让策略硬编码数据目录、日期、资金、股票池。
- 新增配置必须走 `backend/app/core/config.py`。
- 修改 AKQuant/回测路径后必须跑 `tests/backtest/test_akquant_integration.py`。
- 修改前端后必须跑 `npm run build`。

## 4. 剩余 ClickHouse 直连分类

### 4.1 必须清理：默认 Parquet 模式会影响核心功能

这些模块如果继续直连 CH，会破坏“无 Docker 也能研究/回测”的目标。

| 模块 | 问题 | 建议处理 |
|---|---|---|
| `backend/app/backtest/api.py` 的 `/pools/{pool_name}` | top100/top300/top500 仍从 `klines_daily` CH 查询 | 改用 `MarketDataStore.load_daily` 或新增 store 聚合方法 |
| `backend/app/backtest/api.py` 的 `/stock-names` | 从 CH `stock_info` 查名称 | 改用 SQLite `stocks` |
| `backend/app/backtest/runner.py` | 内置回测 runner 仍查 CH 日线 | 改用 `StoreDataProvider` 或 `MarketDataStore` |
| `backend/app/backtest/event/event_source.py` | event source 仍从 CH 读 bar | 改用 `MarketDataStore` |
| `backend/app/backtest/strategy/user_script.py` | user script helper 仍查 CH | 先封装为 data helpers，再接 store |
| `backend/app/services/akquant_factor.py` | AKQuant factor 原始行情仍查 CH | 改用 `MarketDataStore.load_daily` |
| `backend/app/services/data_skill.py` 指标读取 | K 线已接 store，但 `stock_indicators` 仍查 CH | 新增指标 store 或明确指标仍 CH-only |
| `backend/app/api/strategy.py` | 策略相关 pool 查询仍查 CH | 改用 store |

### 4.2 P1 清理：指标/因子体系

| 模块 | 当前状态 | 建议 |
|---|---|---|
| `backend/app/compute/cache.py` | 已有 Parquet factor cache 方法，但 docstring 和 CH 懒加载仍在 | 抽象为 `FactorCacheStore`，避免默认 Parquet 模式误连 CH |
| `backend/app/compute/operators/indicator_ops.py` | `indicator()` 从 CH `stock_indicators` 查询 | 新增 `IndicatorStore` 后替换 |
| `backend/app/indicators/scheduler.py` | 指标计算写 CH | 支持写 Parquet `stock_indicators` / `indicator_timeseries`，CH 可选 |
| `backend/app/api/indicator.py` | 指标 API 从 CH 查询 | 接 `IndicatorStore` |
| `backend/app/services/factor_evaluation.py` | 行情已部分改 store，但仍检查 CH column/table | 清理 CH-specific schema 探测 |
| `backend/app/services/compute_service.py` | 已部分接 store | 补测试，确认无 CH 环境可运行 |

### 4.3 P2 清理：老策略和研究脚本

| 模块 | 建议 |
|---|---|
| `backend/app/strategies/trend_capital.py` | 该策略需要完整分钟线，短期可标记为 ClickHouse-only；后续再迁移 |
| `backend/app/strategies/deep_value.py` | 改用 `MarketDataStore` + 指标 store |
| `backend/app/backtest/strategies/small_cap_v4_akquant.py` | 仍有多处直接 CH 查询，建议逐步替换为 strategy data helpers |
| `backend/app/scripts/fill_small_cap_missing_data.py` | 补数脚本应支持 Parquet 写入和 CH 可选 |
| `backend/app/scripts/compute_minute_indicators.py` | 暂标 ClickHouse-only，后续看是否需要 |
| `backend/app/scripts/batch_indicator_redis.py` | 暂标 ClickHouse-only |
| `backend/app/scripts/sync_data.py` / `sync_stock_info.py` | 老脚本，建议弃用或迁移到统一 SyncService |

### 4.4 可以保留为 ClickHouse-only

| 模块 | 原因 |
|---|---|
| `backend/app/api/data_explorer.py` | 当前就是 ClickHouse 数据浏览器；后续可新增 Parquet Explorer，不必强行改 |
| `backend/app/scripts/export_clickhouse_to_parquet.py` | 本来就是 CH → Parquet 迁移工具 |
| `backend/app/scripts/validate_parquet_store.py` | 本来就是对比 CH 与 Parquet 的验证工具 |
| `backend/app/db/clickhouse.py` | ClickHouse 可选后端必须保留 |
| `backend/app/data_stores/clickhouse_store.py` | ClickHouse backend 必须保留 |

## 5. P0：打通默认 Parquet 模式的核心体验

### P0.1 修 `/api/v2/backtest/pools/{pool_name}`

现状：

- `top100/top300/top500` 从 CH `klines_daily` 聚合过去一年成交额。

目标：

- 使用 `MarketDataStore`。
- 默认 Parquet 模式不连接 CH。

实现建议：

1. 在 `MarketDataStore` 增加可选方法：

```python
def top_by_avg_amount(self, start_date: date, end_date: date, limit: int) -> list[str]:
    ...
```

2. `ParquetMarketDataStore` 用 DuckDB 聚合。
3. `ClickHouseMarketDataStore` 保持 SQL 聚合。
4. `/pools/{pool_name}` 调用 store，不直接查 CH。

验收：

- 无 ClickHouse 时 `/api/v2/backtest/pools/top100` 可返回结果或空结果，不报连接错误。
- 有 Parquet 日线数据时能正常返回 symbol 列表。

### P0.2 修 `/api/v2/backtest/stock-names`

现状：

- 从 CH `stock_info` 查名称。

目标：

- 从 SQLite `stocks` 查名称。

验收：

- 无 CH 时接口可用。

### P0.3 修内置回测 runner 和 EventSource

现状：

- `backend/app/backtest/runner.py` 和 `backend/app/backtest/event/event_source.py` 仍有 CH 查询。

目标：

- 内置事件驱动回测在 `MARKET_DATA_BACKEND=parquet` 下可读取 Parquet 日线。
- `TradingCalendar` 已改为 MarketDataStore，不要回退到 CH。

实现建议：

- 新增 `MarketDataEventSource` 或改造 `BarEventSource.from_clickhouse()` 内部委托 store。
- 暂时保留 `from_clickhouse()` 名称以避免大范围改调用，但内部按配置走 store。

验收：

```powershell
.\.venv\Scripts\python.exe -m pytest tests\backtest -q
```

如果历史测试依赖 CH，可先加 Parquet fixture 的新 smoke test，不强行跑全部。

### P0.4 修 `akquant_factor.py`

目标：

- AKQuant factor 原始行情从 `MarketDataStore.load_daily` 读取。

验收：

- `POST /api/v2/compute/evaluate` 在 Parquet backend 下可运行最小表达式。

## 6. P1：指标与因子缓存 store 化

### P1.1 新增 IndicatorStore

新增：

```text
backend/app/data_stores/indicator_base.py
backend/app/data_stores/parquet_indicator_store.py
backend/app/data_stores/clickhouse_indicator_store.py
```

接口建议：

```python
class IndicatorStore:
    def load_cross_section(self, names: list[str], trade_date: date, symbols: list[str] | None = None) -> pd.DataFrame: ...
    def load_timeseries(self, names: list[str], start: datetime, end: datetime, symbols: list[str] | None = None) -> pd.DataFrame: ...
    def write_cross_section(self, df: pd.DataFrame) -> int: ...
    def write_timeseries(self, df: pd.DataFrame) -> int: ...
```

Parquet dataset：

```text
data/parquet/stock_indicators/indicator_name=<name>/year=YYYY/month=MM/part-*.parquet
data/parquet/indicator_timeseries/indicator_name=<name>/year=YYYY/month=MM/part-*.parquet
```

### P1.2 改 `indicators/scheduler.py`

目标：

- 默认 Parquet 模式写指标 Parquet。
- `CLICKHOUSE_ENABLED=true` 时可双写 CH。

### P1.3 改 `api/indicator.py`、`DataSkill.get_indicator`

目标：

- 指标 API 和 DataSkill 指标读取不依赖 CH。

验收：

- 无 CH 时前端指标概览不因连接失败崩溃。
- 有指标 Parquet 时能查到值。

### P1.4 抽象 FactorCacheStore

现状：

- `ComputeCache` 里同时有 CH 和 Parquet 方法，且 `ch_client` 属性会懒连 CH。

目标：

- 新增 `FactorCacheStore`。
- 默认 Parquet 模式不访问 `ch_client`。

验收：

- `POST /api/v2/compute/evaluate` 重复计算能命中 Parquet factor cache。

## 7. P2：策略和脚本清理

### P2.1 `small_cap_v4_akquant.py`

该策略仍有多处直接 CH 查询。

建议：

1. 先不要在策略内部到处替换。
2. 新增 `SmallCapDataHelper`：
   - `load_daily_close`
   - `load_previous_close`
   - `load_timer_prices`
   - `load_index_components`
   - `load_indicator`
3. helper 内部走 `MarketDataStore` / SQLite / IndicatorStore。
4. 策略只依赖 helper。

验收：

- ID=43 短区间 Parquet 回测可跑。
- 不要求一次完全对齐聚宽收益。

### P2.2 老策略分类

| 策略 | 建议 |
|---|---|
| `trend_capital.py` | 标记 ClickHouse-only，等完整分钟 Parquet 稳定后再迁移 |
| `deep_value.py` | 优先迁移到 store，数据需求简单 |

### P2.3 补数脚本迁移

重点脚本：

- `fill_small_cap_missing_data.py`
- `sync_timer_minute_points.py`
- `run_small_cap_full_debug.py`
- `run_small_cap_yearly_debug.py`

要求：

- 默认 Parquet 模式不要求 CH。
- `--backend clickhouse|parquet|auto` 可选。

## 8. P3：Parquet 体验和运维

### P3.1 Parquet Explorer

不要改掉现有 `DataExplorer` 的 ClickHouse 浏览器。新增：

```text
GET /api/explorer/parquet/datasets
GET /api/explorer/parquet/{dataset}/schema
GET /api/explorer/parquet/{dataset}/preview
```

前端可新增 tab：

- ClickHouse
- Parquet

### P3.2 Compaction 工具

新增脚本：

```text
backend/app/scripts/compact_parquet_dataset.py
```

参数：

```text
--dataset klines_daily|klines_minute_timer|factor_cache|stock_indicators
--start YYYYMMDD
--end YYYYMMDD
--max-files-per-partition 1
```

目标：

- 合并小文件。
- 按唯一键去重。
- 输出压缩前后文件数/行数。

### P3.3 覆盖率仪表盘

系统状态页展示：

- `MARKET_DATA_BACKEND`
- `PARQUET_DATA_DIR`
- 各 dataset 行数、symbol 数、日期范围
- 最近一次同步时间
- ClickHouse 是否启用

## 9. 建议执行顺序

第一批：

1. `/pools/{pool_name}` 改 store。
2. `/stock-names` 改 SQLite。
3. `akquant_factor.py` 改 store。
4. 补对应测试。

第二批：

1. `runner.py` / `event_source.py` 改 store。
2. 内置回测 Parquet smoke test。
3. 清理 backtest API 中剩余非必要 CH 查询。

第三批：

1. 新增 IndicatorStore。
2. 改 `api/indicator.py`、`DataSkill.get_indicator`、`indicators/scheduler.py`。
3. 新增 Parquet indicator tests。

第四批：

1. `small_cap_v4_akquant.py` 数据 helper 化。
2. 小市值脚本默认 Parquet 化。
3. 年度 debug 跑通一个短区间。

第五批：

1. Parquet Explorer。
2. Parquet compaction。
3. 系统状态页增强。

## 10. 每批验收命令

后端基础：

```powershell
cd E:\Projects\GaoshouPlatform\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\.venv\Scripts\python.exe -m pytest tests\data_stores\test_parquet_store.py tests\backtest\test_akquant_parquet_provider.py tests\backtest\test_akquant_integration.py -q
```

如果改 backtest runner：

```powershell
.\.venv\Scripts\python.exe -m pytest tests\backtest -q
```

如果改 compute/factor：

```powershell
.\.venv\Scripts\python.exe -m pytest tests\compute tests\api\test_evaluation_api.py -q
```

前端：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

## 11. 风险提示

- 不要让默认 Parquet 模式在 import 阶段就连接 ClickHouse。
- 不要把 DuckDB 当事务数据库使用；策略、回测、任务仍在 SQLite。
- Parquet 写入要避免小文件爆炸；后续必须做 compaction。
- 指标体系迁移要先保证字段 schema，避免前端看板断裂。
- 旧策略可以先标记 ClickHouse-only，不要为了彻底清零 `get_ch_client()` 牵出过大改动。

## 12. 完成定义

阶段性完成标准：

- 不启动 ClickHouse/Docker，后端可以启动。
- 不启动 ClickHouse/Docker，前端可以打开回测页、查询 Parquet K 线、运行 AKQuant `minute_timer` smoke 回测。
- 数据同步日线/分钟线可写 Parquet。
- timer 覆盖率 API 可在 Parquet 模式下返回真实覆盖结果。
- ClickHouse backend 打开后旧路径仍可用。

最终完成标准：

- 核心投研流程默认不依赖 ClickHouse：
  - 数据同步
  - K 线查询
  - DataSkill K 线
  - AKQuant 回测
  - 因子表达式计算
  - 指标查询
  - ID=43 小市值调试
- ClickHouse 只作为可选高性能后端和迁移/对照工具存在。
