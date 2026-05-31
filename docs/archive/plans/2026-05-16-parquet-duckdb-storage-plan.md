# Parquet + DuckDB 数据层改造执行计划

> 创建日期：2026-05-16  
> 目标读者：执行改造的 AI Agent / 开发者  
> 状态：待实施  
> 目标：将 ClickHouse 从平台硬依赖降级为可选高性能后端，新增 Parquet + DuckDB 作为个人研究默认行情/因子数据层；SQLite 继续保留为元数据和任务账本。

## 1. 背景判断

当前架构：

```text
miniQMT / Tushare / AKShare
        ↓
SQLite + ClickHouse
        ↓
FastAPI / DataSkill / 因子 / 回测 / 前端
```

这个架构适合长期平台化、多用户、高并发查询和较大规模分钟数据。但对刚起步的个人量化研究者来说，ClickHouse 运维成本偏高：

- 需要 Docker 或本地服务进程。
- 需要维护端口、数据卷、容器状态。
- 本地迁移和备份不如文件型数据直接。
- 当前很多策略已经改为日线或稀疏分钟线，未必需要 ClickHouse 常驻服务。

建议目标架构：

```text
miniQMT / Tushare / AKShare
        ↓
数据同步服务
        ↓
SQLite：元数据、策略、任务、回测记录、指数成分
Parquet：日线、稀疏分钟线、因子缓存、研究中间结果
        ↓
DuckDB / Polars：查询、筛选、因子计算、回测数据加载
        ↓
AKQuant / 内置回测 / 前端

ClickHouse：保留为可选高性能后端
```

核心原则：

1. 不删除 SQLite。
2. 不立刻删除 ClickHouse。
3. 先做存储抽象，让业务层不直接依赖 ClickHouse。
4. 先迁移读多写少、个人研究最常用的数据：日线、`minute_timer` 稀疏分钟线、因子缓存。
5. 用配置切换默认后端：`parquet` / `clickhouse`。

## 2. 非目标

本次改造不做：

- 不把 SQLite 替换为 DuckDB。
- 不迁移策略、任务、回测记录、自选股等事务型数据。
- 不删除已有 ClickHouse 表和 Docker 配置。
- 不一次性重写所有策略。
- 不改变前端 API 返回结构。
- 不改变 AKQuant 策略参数控制方式。

## 3. 存储职责划分

### 3.1 保留 SQLite

SQLite 继续作为控制平面和系统账本。

保留数据：

| 数据 | 表/模块 |
|---|---|
| 股票基础信息 | `stocks` |
| 财务数据 | `financial_data` |
| 指数历史成分 | `index_components` |
| 自选股 | `watchlist_groups`, `watchlist_stocks` |
| 策略 | `strategies` |
| 回测任务和结果 | `backtests`, `orders`, `trades` |
| 同步日志 | `sync_logs` / 现有同步任务表 |
| 用户配置 | 现有配置表或后续新增 |

### 3.2 新增 Parquet + DuckDB

Parquet 用于文件型分析数据，DuckDB 用于 SQL 查询。

优先承载：

| 数据 | 当前 ClickHouse 表 | 新 Parquet dataset |
|---|---|---|
| 日 K | `klines_daily` | `data/parquet/klines_daily/` |
| 稀疏分钟 K | `klines_minute` | `data/parquet/klines_minute_timer/` |
| 完整分钟 K，可选 | `klines_minute` | `data/parquet/klines_minute/` |
| 因子缓存 | `factor_cache` | `data/parquet/factor_cache/` |
| 指标时序，可选 | `indicator_timeseries` | `data/parquet/indicator_timeseries/` |
| 截面指标，可选 | `stock_indicators` | `data/parquet/stock_indicators/` |

### 3.3 保留 ClickHouse

ClickHouse 保留为可选加速层：

- 大规模完整分钟线查询。
- 多用户/高并发 API 服务。
- 大批量历史数据浏览。
- 后续如果做长期平台化，可以继续使用。

## 4. 目标目录结构

新增：

```text
backend/app/data_stores/
  __init__.py
  base.py
  parquet_store.py
  clickhouse_store.py
  factory.py

backend/app/db/
  duckdb.py

backend/app/scripts/
  export_clickhouse_to_parquet.py
  validate_parquet_store.py

data/parquet/
  klines_daily/
    year=2025/month=05/part-*.parquet
  klines_minute_timer/
    year=2025/month=05/part-*.parquet
  factor_cache/
    expr_hash=.../year=2025/part-*.parquet
```

说明：

- `backend/data/` 可继续存 SQLite。
- `data/parquet/` 建议放项目根目录或通过配置指定。
- 如果担心 repo 目录膨胀，确保 `data/parquet/` 在 `.gitignore`。

## 5. 配置设计

在 `backend/app/core/config.py` 增加：

```python
market_data_backend: str = "parquet"  # parquet | clickhouse
parquet_data_dir: str = "E:/Projects/GaoshouPlatform/data/parquet"
duckdb_path: str = ":memory:"          # 或 backend/data/research.duckdb
clickhouse_enabled: bool = False
```

环境变量建议：

```text
MARKET_DATA_BACKEND=parquet
PARQUET_DATA_DIR=E:/Projects/GaoshouPlatform/data/parquet
DUCKDB_PATH=:memory:
CLICKHOUSE_ENABLED=false
```

默认值：

- 新默认：`MARKET_DATA_BACKEND=parquet`
- 保留兼容：如果配置为 `clickhouse`，现有路径继续工作。

## 6. 抽象接口设计

新增 `backend/app/data_stores/base.py`：

```python
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Iterable, Sequence

import pandas as pd


class MarketDataStore(ABC):
    @abstractmethod
    def load_daily(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def load_minute(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        columns: Sequence[str] | None = None,
        timer_times: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def write_daily(self, df: pd.DataFrame) -> int:
        ...

    @abstractmethod
    def write_minute(self, df: pd.DataFrame, *, dataset: str = "klines_minute") -> int:
        ...

    @abstractmethod
    def coverage(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        *,
        dataset: str,
        timer_times: Sequence[str] | None = None,
    ) -> dict:
        ...
```

新增 `backend/app/data_stores/factory.py`：

```python
def get_market_data_store() -> MarketDataStore:
    if settings.market_data_backend == "clickhouse":
        return ClickHouseMarketDataStore()
    return ParquetMarketDataStore(settings.parquet_data_dir)
```

实现要求：

- `ClickHouseMarketDataStore` 包装现有 `get_ch_client()` 查询，不改变现有 SQL 语义。
- `ParquetMarketDataStore` 使用 DuckDB 查询 Parquet dataset。
- 返回 DataFrame 字段和类型尽量保持一致。

## 7. Parquet 文件规范

### 7.1 日线 dataset

路径：

```text
data/parquet/klines_daily/year=YYYY/month=MM/part-*.parquet
```

字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `symbol` | string | `600000.SH` |
| `trade_date` | date | 交易日 |
| `open` | float64 | 开盘 |
| `high` | float64 | 最高 |
| `low` | float64 | 最低 |
| `close` | float64 | 收盘 |
| `volume` | float64/int64 | 成交量 |
| `amount` | float64 | 成交额，统一为元 |
| `turnover_rate` | float64/null | 换手率 |
| `source` | string/null | miniqmt/tushare/akshare |
| `updated_at` | timestamp | 更新时间 |

唯一键：

```text
symbol + trade_date
```

### 7.2 稀疏分钟 dataset

路径：

```text
data/parquet/klines_minute_timer/year=YYYY/month=MM/part-*.parquet
```

字段：

| 字段 | 类型 |
|---|---|
| `symbol` | string |
| `datetime` | timestamp |
| `trade_date` | date |
| `minute` | int32，例：630 表示 10:30 |
| `open` | float64 |
| `high` | float64 |
| `low` | float64 |
| `close` | float64 |
| `volume` | float64/int64 |
| `amount` | float64 |
| `source` | string/null |
| `updated_at` | timestamp |

唯一键：

```text
symbol + datetime
```

说明：

- `minute` 是为了快速过滤 timer 点。
- `trade_date` 是为了按日期分区和回测过滤。

### 7.3 因子缓存 dataset

路径：

```text
data/parquet/factor_cache/expr_hash=<hash>/year=YYYY/part-*.parquet
```

字段：

| 字段 | 类型 |
|---|---|
| `symbol` | string |
| `trade_date` | date |
| `expr_hash` | string |
| `value` | float64 |
| `engine` | string |
| `expression` | string/null |
| `updated_at` | timestamp |

## 8. DuckDB 查询示例

日线：

```sql
SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
FROM read_parquet('data/parquet/klines_daily/**/*.parquet', hive_partitioning=true)
WHERE symbol IN ('000001.SZ', '600000.SH')
  AND trade_date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
ORDER BY symbol, trade_date
```

稀疏分钟：

```sql
SELECT symbol, datetime, open, high, low, close, volume, amount
FROM read_parquet('data/parquet/klines_minute_timer/**/*.parquet', hive_partitioning=true)
WHERE symbol IN ('000001.SZ', '600000.SH')
  AND trade_date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
  AND minute IN (600, 630, 870, 890)
ORDER BY symbol, datetime
```

## 9. 分阶段实施

### Phase 0：准备和保护现有行为

目标：不改行为，先加测试和配置。

任务：

1. 增加配置项：
   - `MARKET_DATA_BACKEND`
   - `PARQUET_DATA_DIR`
   - `DUCKDB_PATH`
   - `CLICKHOUSE_ENABLED`
2. 增加 `.gitignore`：
   - `data/parquet/`
   - `*.duckdb`
   - `*.duckdb.wal`
3. 为现有 ClickHouse 关键路径补烟测：
   - `ClickHouseDataProvider.load_daily`
   - `ClickHouseDataProvider.load_minute(timer_times=...)`
   - `/api/v2/backtest/timer-coverage`
   - AKQuant `minute_timer` 测试继续通过

验收：

```powershell
cd E:\Projects\GaoshouPlatform\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
```

前端不应受影响：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

### Phase 1：新增 Parquet/DuckDB 基础设施

目标：新增数据层，不接业务路径。

任务：

1. 新增 `backend/app/db/duckdb.py`：
   - 创建 DuckDB 连接函数。
   - 支持 `:memory:` 和文件路径。
   - 提供只读查询工具函数。
2. 新增 `backend/app/data_stores/base.py`。
3. 新增 `backend/app/data_stores/parquet_store.py`。
4. 新增 `backend/app/data_stores/clickhouse_store.py`。
5. 新增 `backend/app/data_stores/factory.py`。
6. 新增单元测试：
   - 用临时目录写入小型 Parquet。
   - 用 DuckDB 查询日线。
   - 用 DuckDB 查询 timer 分钟。

实现注意：

- 写 Parquet 推荐使用 `pyarrow` 或 `pandas.to_parquet(engine="pyarrow")`。
- DuckDB 查询参数不要直接拼接用户输入；symbols 和日期要做规范化。
- 小文件过多会影响性能，写入时按 year/month 合并。

验收：

- 无 ClickHouse 环境也能跑 Parquet store 单测。
- `get_market_data_store()` 在 `MARKET_DATA_BACKEND=parquet` 时返回 Parquet store。

### Phase 2：导出 ClickHouse 到 Parquet

目标：把已有 ClickHouse 数据导出成 Parquet，便于对照和切换。

任务：

1. 新增脚本 `backend/app/scripts/export_clickhouse_to_parquet.py`。
2. 支持导出：
   - `klines_daily`
   - `klines_minute` 中的 timer 点到 `klines_minute_timer`
   - `factor_cache`
3. 支持参数：

```text
--dataset klines_daily|klines_minute_timer|factor_cache|all
--start YYYYMMDD
--end YYYYMMDD
--symbols 000001.SZ,600000.SH
--index-symbol 399101.SZ
--timer-times 10:00,10:30,14:30,14:50
--output E:/Projects/GaoshouPlatform/data/parquet
--overwrite
```

4. 新增校验脚本 `backend/app/scripts/validate_parquet_store.py`：
   - 对比 ClickHouse 和 Parquet 行数。
   - 对比指定 symbol/date 的 OHLC。
   - 对比 timer 覆盖率。

验收：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\backend\.venv\Scripts\python.exe backend/app/scripts/export_clickhouse_to_parquet.py `
  --dataset klines_daily `
  --start 20250101 `
  --end 20251231 `
  --output E:\Projects\GaoshouPlatform\data\parquet

.\backend\.venv\Scripts\python.exe backend/app/scripts/validate_parquet_store.py `
  --dataset klines_daily `
  --start 20250101 `
  --end 20251231
```

### Phase 3：DataProvider 接入存储抽象

目标：AKQuant 和回测引擎可以从 Parquet 或 ClickHouse 读取，不改前端。

任务：

1. 将 `backend/app/backtest/engine/data_provider.py` 改造为：
   - 保留 `ClickHouseDataProvider` 兼容类。
   - 新增 `StoreDataProvider` 或将内部实现委托给 `get_market_data_store()`。
2. `load_daily` 通过 `MarketDataStore.load_daily`。
3. `load_minute(..., timer_times=...)` 通过 `MarketDataStore.load_minute`。
4. AKQuant `minute_timer` 路径保持语义一致。
5. 保持测试 `tests/backtest/test_akquant_integration.py` 通过。
6. 新增 Parquet backend 下的 AKQuant smoke test：
   - 用临时 Parquet 写入 2 只股票、2 天、2 个 timer 点。
   - 验证 `minute_timer` 只加载指定时间点。

验收：

- `MARKET_DATA_BACKEND=clickhouse` 行为不变。
- `MARKET_DATA_BACKEND=parquet` 下，AKQuant `minute_timer` 数据加载通过单测。

### Phase 4：DataSkill 和数据查询服务接入

目标：前端 K 线查询、DataSkill 读取可以使用 Parquet backend。

任务：

1. 修改 `backend/app/services/data_skill.py`：
   - `_query_ch_klines` 改为 `_query_market_klines`。
   - `_query_ch_klines_minute` 改为 `_query_market_klines_minute`。
   - 通过 `get_market_data_store()` 查询。
2. 修改 `backend/app/services/data_service.py`：
   - K 线查询使用 store。
3. 保持 `/api/skill/kline/daily/{symbol}` 和 `/api/skill/kline/minute/{symbol}` 返回结构不变。
4. 数据浏览器 `data_explorer.py` 暂时仍只浏览 ClickHouse；后续单独做 Parquet Explorer。

验收：

- 前端个股 K 线页面能读取 Parquet 日线。
- Skill API 返回字段不变。

### Phase 5：同步服务双写/可选写入

目标：新数据同步可以写入 Parquet，ClickHouse 可选。

任务：

1. 修改 `backend/app/services/sync_service.py` 的 K 线写入路径：
   - 当 `MARKET_DATA_BACKEND=parquet`，写入 Parquet。
   - 当 `CLICKHOUSE_ENABLED=true`，同时写 ClickHouse。
2. 新增写入策略：
   - `write_mode=append`：追加后由 compaction 去重。
   - `write_mode=replace_partition`：替换 year/month 分区。
3. 对 `symbol + trade_date`、`symbol + datetime` 去重。
4. 保持数据单位：
   - amount 统一为元。
   - Tushare `amount` 如果是千元，写入前乘 `1000`。

验收：

- 同步少量股票日线后，Parquet 可查询。
- 重复同步不会产生重复有效记录。

### Phase 6：因子缓存接入 Parquet

目标：`factor_cache` 支持 Parquet 后端。

任务：

1. 修改 `backend/app/compute/cache.py`：
   - 增加 `ParquetFactorCacheStore`。
   - 保留 ClickHouse factor_cache 路径。
2. 修改 `backend/app/services/akquant_factor.py` 和相关 compute 服务：
   - 原始行情从 MarketDataStore 读取。
   - 因子结果按配置写入 Parquet 或 ClickHouse。
3. 增加测试：
   - 同一个表达式重复计算可命中 Parquet factor cache。

验收：

- `POST /api/v2/compute/evaluate` 在 Parquet backend 下可运行。
- AKQuant Polars 因子计算路径不依赖 ClickHouse。

### Phase 7：脚本和策略去直接 ClickHouse 化

目标：减少硬编码 `get_ch_client()`。

优先改造文件：

| 文件 | 处理方式 |
|---|---|
| `backend/app/backtest/strategies/small_cap_v4_akquant.py` | 将直接 CH 查询封装到 strategy data helpers 或 MarketDataStore |
| `backend/app/scripts/run_small_cap_full_debug.py` | 使用 store provider |
| `backend/app/scripts/run_small_cap_yearly_debug.py` | 使用 store provider |
| `backend/app/services/timer_minute_sync.py` | 写入 Parquet 支持；CH 写入可选 |
| `backend/app/services/factor_evaluation.py` | 行情读取使用 store |
| `backend/app/compute/api.py` | 行情读取使用 store |
| `backend/app/backtest/event/calendar.py` | 交易日历使用 store 或 SQLite calendar cache |

不要求一次全部改完，但每改一个模块必须补最小测试或 smoke command。

### Phase 8：前端与文档

目标：用户可理解当前使用的是哪个数据后端。

任务：

1. 系统状态页显示：
   - `MARKET_DATA_BACKEND`
   - `PARQUET_DATA_DIR`
   - `CLICKHOUSE_ENABLED`
   - Parquet dataset 覆盖摘要
2. 数据管理页显示：
   - 日线 Parquet 覆盖
   - timer 分钟 Parquet 覆盖
3. 更新文档：
   - `README.md`
   - `AGENTS.md`
   - `docs/user-manual.md`
   - `docs/data-source-cheatsheet.md`

## 10. 验收标准

### 10.1 功能验收

- 无 ClickHouse/Docker 时，平台可以启动后端和前端。
- SQLite 继续保存策略、任务、回测记录、自选股、指数成分。
- Parquet backend 下可查询日 K。
- Parquet backend 下可查询 `minute_timer`。
- AKQuant `minute_timer` 回测可读取 Parquet 数据。
- ID=43 小市值 debug 脚本可在 Parquet backend 下运行至少一个短区间。
- `MARKET_DATA_BACKEND=clickhouse` 时旧路径仍可用。

### 10.2 测试验收

必须通过：

```powershell
cd E:\Projects\GaoshouPlatform\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
```

新增测试建议：

```text
tests/data_stores/test_parquet_store.py
tests/data_stores/test_market_data_store_factory.py
tests/backtest/test_akquant_parquet_provider.py
tests/compute/test_parquet_factor_cache.py
```

前端构建：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

### 10.3 性能验收

以个人研究为目标，不要求超过 ClickHouse，但要满足：

- 查询 960 只股票 5 年日线：可接受，建议 < 5 秒。
- 查询 960 只股票 5 年 `minute_timer` 四个时间点：可接受，建议 < 10 秒。
- ID=43 回测短区间不因数据读取明显阻塞。

如果不满足：

- 检查 Parquet 小文件数量。
- 增加按 year/month 分区。
- 对 symbol 过滤改用 DuckDB 临时表 join。
- 对常用指数池/timer 覆盖增加 Redis 或本地缓存。

## 11. 风险和规避

| 风险 | 影响 | 规避 |
|---|---|---|
| 小文件过多 | DuckDB 查询慢 | 按 year/month 合并 part 文件，定期 compaction |
| 重复写入 | 回测数据重复 | 写入前按唯一键去重，或 replace partition |
| 类型不一致 | 前端/回测异常 | 明确 schema，统一 date/timestamp/float |
| amount 单位混乱 | 成交额和因子错误 | 写入层统一成元 |
| 业务层仍直连 ClickHouse | 无法关闭 Docker | 按模块逐步替换为 MarketDataStore |
| DuckDB 并发写 | 写入冲突 | 写入通过同步任务串行化；读取多、写入少 |
| Parquet 删除/更新麻烦 | 增量修正复杂 | 先 replace partition，不做行级更新 |

## 12. 推荐实施顺序

执行 Agent 应按以下顺序推进：

1. Phase 0：配置和测试保护。
2. Phase 1：新增 Parquet/DuckDB store。
3. Phase 2：导出 ClickHouse 数据到 Parquet。
4. Phase 3：回测 DataProvider 接入 store。
5. Phase 4：DataSkill 和 DataService 接入 store。
6. Phase 5：同步服务支持写 Parquet。
7. Phase 6：因子缓存支持 Parquet。
8. Phase 7：逐步清理直接 ClickHouse 查询。
9. Phase 8：前端状态和文档。

每个阶段都要保持：

- 前端 API 返回结构不变。
- ClickHouse backend 可回退。
- 不删除旧数据。
- 不改变策略参数从控制面板传入的原则。

## 13. 第一阶段建议提交范围

如果只做第一批 PR/提交，建议包含：

```text
backend/app/core/config.py
backend/app/db/duckdb.py
backend/app/data_stores/base.py
backend/app/data_stores/parquet_store.py
backend/app/data_stores/clickhouse_store.py
backend/app/data_stores/factory.py
backend/tests/data_stores/test_parquet_store.py
.gitignore
README.md
AGENTS.md
docs/user-manual.md
```

第一批不要改 `sync_service.py` 和策略脚本，避免范围过大。

## 14. 给执行 Agent 的注意事项

- 先读 `AGENTS.md`、`README.md`、`docs/user-manual.md`、`docs/data-source-cheatsheet.md`。
- 不能删除 ClickHouse 代码。
- 不能把 SQLite 替换成 DuckDB。
- 不要让策略代码硬编码数据目录、日期、资金或股票池。
- 新增路径要通过配置读取。
- 修改 AKQuant 相关路径后必须跑 `tests/backtest/test_akquant_integration.py`。
- 修改前端类型或页面后必须跑 `npm run build`。
- 如果遇到现有工作区脏文件，不要回滚用户或其他 agent 的改动。
