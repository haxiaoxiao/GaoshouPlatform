# AGENTS.md — AI Coding Agent 指南

> 为 AI agent 提供项目全貌，快速理解数据能力、因子系统和策略开发流程。

---

## 工作区边界（强制）

- 临时边界（2026-06-25 起，用户明确指定）：这段时间直接在 `E:\Projects\GaoshouPlatform-prod` 进行 GaoshouPlatform 代码、配置、脚本、测试和文档开发；`E:\Projects\GaoshouPlatform-dev` 暂时放置，不作为默认开发仓库。
- `E:\Projects\GaoshouPlatform-prod` 当前兼作生产运行仓库和临时开发仓库：改动前先看 git 状态和相关 diff，严格保持改动范围，避免覆盖已有未提交变更。
- `E:\Projects\GaoshouPlatform-dev` 暂时只在用户明确要求时使用。
- 如果当前工作目录在 prod，任务涉及代码/配置/依赖/测试变更，直接在 prod 实施；除非用户重新恢复 dev-first 规则，否则不要切到 dev。
- prod 中如发现此前误改或临时文件，只能清理自己造成的变更；不得回滚、覆盖或删除用户/系统已有变更。

---

## 项目概述

**GaoshouPlatform** 是一个 A 股量化投研平台，核心能力链：

```
华泰 miniQMT (xtquant) → SQLite + ClickHouse → FastAPI → Vue 3 前端
                                    ↓
                           DataSkill 统一数据接口
                                    ↓
                     因子计算 / 策略回测 / 实盘交易
```

## 技术栈

| 层 | 技术 | 说明 |
|---|---|---|
| 后端框架 | FastAPI + SQLAlchemy (async) | Python 3.12+ |
| 元数据库 | SQLite via aiosqlite | 默认 `E:\Projects\Data\gaoshou.db` |
| 时序数据库(默认) | DuckDB + Parquet | 文件型分析数据，零运维 |
| 时序数据库(可选) | ClickHouse | 端口 19000，大规模高并发查询 |
| 数据源 | xtquant (华泰 miniQMT) | 同步阻塞，必须用 `run_in_executor` 包装 |
| 前端 | Vue 3 + TypeScript + Element Plus | Vite 构建，深色主题 |
| 缓存 | Redis (可选) | 无 Redis 也可运行 |

### 数据后端切换

配置项 `MARKET_DATA_BACKEND` 控制行情数据来源：

| 值 | 说明 | 启动要求 |
|---|---|---|
| `parquet` (默认) | DuckDB 查询 Parquet 文件 | 无需 Docker/ClickHouse |
| `clickhouse` | ClickHouse 列式存储 | 需 ClickHouse 服务运行 |

环境变量: `GAOSHOU_DATA_DIR=E:/Projects/Data`, `MARKET_DATA_BACKEND=parquet`, `PARQUET_DATA_DIR=E:/Projects/Data/parquet`, `DUCKDB_PATH=:memory:`, `CLICKHOUSE_ENABLED=false`

---

## 当前重点能力与文档入口

| 文档 | 说明 |
|---|---|
| `README.md` | 项目入口、启动方式、AKQuant 回测入口、ID=43 推荐流程 |
| `docs/user-manual.md` | 使用手册，面向平台使用和策略回测操作 |
| `docs/data-source-cheatsheet.md` | 数据源小抄，记录 miniQMT/Tushare/AKShare 的优势、限制和使用场景 |
| `docs/indevs-tushare-pro-guide.md` | Indevs Tushare Pro Replay 新接口小抄，记录历史分钟、集合竞价、财务、公告、指数等验证结果 |
| `docs/akquant-integration-todo.md` | AKQuant 当前集成状态、验证命令和仍需跟进事项 |
| `docs/factor-value-store.md` | 因子研究 Factor Value Store：通用特征定义、覆盖率、预计算和 ID=43 接入方式 |
| `docs/small-cap-jq-alignment-notes.md` | ID=43 小市值策略对齐聚宽的阶段记录：当前参数、数据口径、已对齐节点和剩余差异 |

当前 AKQuant 重点能力：

- `engine="akquant"` 是策略事件驱动回测的主路径之一。
- 支持 `bar_type="daily"`、`bar_type="minute"`、`bar_type="minute_timer"`。
- `minute_timer` 用于只需要固定盘中时点的策略，从 Parquet `klines_minute_timer/klines_minute` 或 ClickHouse `klines_minute` 读取分钟数据。
- 已接入 AKQuant capabilities、Grid Search、Walk-forward Validation、策略参数 schema/validate。
- 已接入 AKQuant Polars 因子计算入口：`POST /api/v2/compute/evaluate`，`engine="akquant"`。

参数原则：

- 回测日期、资金、费用、滑点、股票池、bar type、timer times 必须由前端控制面板/API payload 控制。
- 策略代码读取 `strategy_params`，不要硬编码日期、资金、股票池或固定 timer。
- 只要用户在前端选择指数池，就通过 `index_symbol` 传递，不要把当前自选股静态展开成历史股票池。

---

## 数据能力全景

### 数据存储分布

| 数据 | 存储位置 | 表名 |
|------|----------|------|
| 股票基础信息 | SQLite | `stocks` — 代码、名称、行业、市值、股本、财务指标 |
| 财务数据 | SQLite | `financial_data` — 按季度，EPS/ROE/毛利率/营收增速等 |
| 日 K 线 | Parquet / ClickHouse | `klines_daily/` — OHLCV + 换手率 |
| 分钟 K 线 | Parquet / ClickHouse | `klines_minute_timer/` + `klines_minute/` |
| 截面指标 | ClickHouse | `stock_indicators` — (symbol, indicator_name, trade_date, value) |
| 时序指标 | ClickHouse | `indicator_timeseries` — (symbol, indicator_name, datetime, value) |
| 因子缓存 | Parquet / ClickHouse | `factor_cache/` — (symbol, trade_date, expr_hash, value) |
| 自选股 | SQLite | `watchlist_groups` + `watchlist_stocks` |
| 策略/回测 | SQLite | `strategies` / `backtests` / `orders` / `trades` |
| 主题标注 | SQLite | `theme_annotations` — 人工标注的业务纯度/产业链定位 |
| 指数历史成分 | SQLite | `index_components` — point-in-time 指数成分快照 |

### Parquet 目录结构

```text
E:\Projects\Data\parquet\
  klines_daily/       year=YYYY/month=MM/part-*.parquet
  klines_minute_timer/ year=YYYY/month=MM/part-*.parquet
  klines_minute/      year=YYYY/month=MM/part-*.parquet
  factor_cache/       expr_hash=<hash>/year=YYYY/part-*.parquet
```

当前 `klines_minute` 已导入本地聚宽版全 A 1 分钟线：

- 覆盖：`2005-01-04 09:31:00` 至 `2026-05-15 15:00:00`
- 规模：约 `3.75B` 行、`5580` 个代码
- 导入脚本：`backend/app/scripts/import_jq_minute_parquet.py`
- zip/tar.gz 归档导入：`backend/app/scripts/import_jq_minute_archives.py`
- 异常日期清理：`backend/app/scripts/clean_minute_parquet_dates.py`
- 状态库：`E:\Projects\Data\parquet\import_state\jq_minute_import.sqlite`

### SQLite stocks 表核心字段

```
symbol, name, exchange, industry(申万一级), industry2, industry3, sector, concept
list_date, delist_date, is_st, is_delist, is_suspend
total_shares, float_shares, a_float_shares (万股)
total_mv, circ_mv (万元)
eps, bvps, roe, pe_ttm, pb
total_assets, total_liability, total_equity
net_profit, revenue (万元)
revenue_yoy, profit_yoy, gross_margin
```

### SQLite financial_data 表核心字段

```
symbol, report_date, report_type(Q1/H1/Q3/Annual)
eps, bvps, roe, revenue, net_profit
revenue_yoy, profit_yoy, gross_margin
total_assets, total_liability, total_equity
total_shares, float_shares
total_mv, circ_mv, pe_ttm, pb
raw_data (原始 JSON)
```

### ClickHouse K 线表结构

```sql
-- klines_daily: (symbol, trade_date, open, high, low, close, volume, amount, turnover_rate)
-- klines_minute: (symbol, datetime, open, high, low, close, volume, amount)
-- 分区: toYYYYMM(trade_date/datetime)
-- 排序键: (symbol, trade_date/datetime)
```

---

## DataSkill — 统一数据访问接口

策略和因子开发通过 `DataSkill` 获取所有数据，**无需关心数据来源**（SQLite/ClickHouse/QMT）。

### 服务层 (`backend/app/services/data_skill.py`)

```python
from app.services.data_skill import DataSkill, StockSnapshot, KlineBar, FinancialReport, ScreenResult
from app.db.sqlite import get_async_session

skill = DataSkill(session)

# ── 股票快照 ──
snapshot = await skill.get_stock("600051.SH")
snapshots = await skill.get_stocks(["600051.SH", "000001.SZ"])

# ── 条件选股 ──
result = await skill.screen_stocks(
    min_mv=10_000_000, max_mv=100_000_000,
    min_pe=5, max_pe=30, min_roe=10, limit=100
)

# ── K 线数据 ──
bars = await skill.get_kline_daily("600051.SH", start_date=date(2024,1,1))
bars = await skill.get_kline_minute("600051.SH")

# ── 财务数据 ──
reports = await skill.get_financial("600051.SH", report_count=8)  # 最近8期
batch = await skill.get_financial_batch(["600051.SH", "000001.SZ"])

# ── 实时行情 ──
quote = await skill.get_realtime_quote("600051.SH")
quotes = await skill.get_realtime_quotes(["600051.SH", "000001.SZ"])

# ── 行业与股票列表 ──
industries = await skill.get_industries()
all_symbols = await skill.get_all_symbols()

# ── 指标查询 ──
value = skill.get_indicator("600051.SH", "pe_ttm", date(2025,4,1))
indicators = skill.get_indicators_batch(["600051.SH"], date(2025,4,1))
```

### API 端点 (`/api/skill/*`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/skill/stock/{symbol}` | 股票快照 |
| POST | `/skill/stocks/batch` | 批量股票快照 |
| GET | `/skill/screen` | 条件选股（industry/exchange/is_st/min_mv/max_mv/min_pe/max_pe/min_roe） |
| GET | `/skill/kline/daily/{symbol}` | 日K线（start_date/end_date/limit） |
| GET | `/skill/kline/minute/{symbol}` | 分钟K线 |
| GET | `/skill/financial/{symbol}` | 财务数据（report_count） |
| POST | `/skill/financial/batch` | 批量财务数据 |
| GET | `/skill/quote/{symbol}` | 实时行情 |
| POST | `/skill/quote/batch` | 批量实时行情 |
| GET | `/skill/industries` | 行业列表及统计 |
| GET | `/skill/symbols` | 所有股票代码（?industry=行业名） |
| GET | `/skill/indicator/{symbol}` | 查询指标值（?name=指标名&trade_date=日期） |

### 数据优先级策略

| 数据类型 | 优先来源 | 兜底来源 |
|----------|----------|----------|
| 股票快照 | SQLite `stocks` 表 | QMT `get_stock_full_info` |
| K 线 | Parquet/DuckDB | ClickHouse → QMT |
| 财务数据 | SQLite `financial_data` 表 | QMT `download_financial_data2` |
| 实时行情 | QMT `get_realtime_quotes` | 无兜底 |
| 指标 | ClickHouse `stock_indicators` | 无兜底 |
| 指数历史成分 | SQLite `index_components` | Tushare `index_weight` |
| 固定时间点分钟线 | Parquet/DuckDB | ClickHouse `klines_minute` |
| 因子缓存 | Parquet/DuckDB | ClickHouse `factor_cache` |

---

## 因子开发

项目有 **两套因子系统**，用途不同：

### 系统一：Indicator 体系（`backend/app/indicators/`）

适用于**简单截面/时序指标**，有明确的注册表和自动发现机制。

**已有指标（20+ 个）：**

| 类别 | 指标 | 数据来源 |
|------|------|----------|
| 估值 | pe_ttm, pb, ps_ttm, dividend_yield | stock_info / ClickHouse |
| 动量 | return_5d, return_20d, return_60d, ma5_slope | kline_data |
| 波动 | volatility_20d, avg_amplitude | kline_data |
| 流动性 | turnover_rate, avg_amount_20d, free_float_mv | stock_info / kline_data |
| 质量 | roe, debt_ratio, gross_margin | stock_info |
| 成长 | revenue_growth, profit_growth | stock_info |
| 技术 | ma5, ma10, ma20, rsi_14 | kline_data |
| 主题 | business_purity, chain_position, revenue_ratio | SQLite theme_annotations |

**添加新指标的步骤：**

1. 在对应类别文件（如 `valuation.py`）创建子类：

```python
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry

@IndicatorRegistry.register
class MyNewFactor(IndicatorBase):
    name = "my_new_factor"
    display_name = "我的新因子"
    category = "valuation"       # valuation/growth/quality/momentum/volatility/liquidity/technical/theme
    tags = ["估值", "自定义"]
    data_type = "截面"            # "截面" 用 stock_info，"时序" 用 kline_data
    is_precomputed = True        # True=同步后自动计算
    dependencies = []            # 依赖的其他指标名
    description = "因子描述"
    unit = "%"                   # % / x / CNY / 10k CNY / ""

    def compute(self, context: IndicatorContext) -> float | None:
        # context.symbol    → 股票代码
        # context.trade_date → 交易日期
        # context.stock_info → dict (截面: 从 stocks + financial_data 合并)
        # context.kline_data → list[dict] (时序: 按日期倒序)
        ...
        return value
```

2. 确保 `is_precomputed = True` 则同步后自动计算；`False` 则只在前端查询时计算。
3. 自动发现机制：`IndicatorRegistry.auto_discover()` 会扫描 `indicators/` 包下所有模块（除 `base.py`、`scheduler.py`、`__init__.py`）。

### 系统二：Compute Engine（`backend/app/compute/`）

适用于**基于表达式的自定义因子**，支持复杂公式组合。

**算子体系（4 级）：**

| 级别 | 说明 | 示例 |
|------|------|------|
| L0 | 原始字段 | `$close`, `$open`, `$high`, `$low`, `$volume`, `$amount`, `$turnover` |
| L1 | 指标算子 | `indicator(name, series)`, `pe_ttm(series)`, `dividend_yield(series)` |
| L2 | 滚动/数学 | `Mean`, `Std`, `Sum`, `Min`, `Max`, `Corr`, `Cov`, `Lag`, `Delta`, `Rank` |
| L3 | 技术分析 | `SMA`, `EMA`, `RSI`, `MACD`, `ATR`, `BB`, `CCI`, `WILLR`, `OBV`, `KDJ_K` |

**表达式语法：**

```
Mean($close, 5) / Std($close, 20)           -- 均值/标准差
($close - Mean($close, 20)) / Std($close, 20) -- Z-Score
RSI($close, 14)                              -- RSI 指标
$close > Mean($close, 20)                    -- 布尔条件
```

**添加新算子的步骤：**

1. 在对应算子文件中创建子类：

```python
from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry

@OperatorRegistry.register
class MyRollingOp(Operator):
    name = "my_op"
    level = 2
    category = "rolling"
    signature = "my_op(series, period)"

    def evaluate(self, df, **kwargs):
        series = kwargs["series"]
        period = kwargs.get("period", 10)
        # 计算逻辑...
        return result
```

2. 算子文件在 `backend/app/compute/operators/`：
   - `raw_fields.py` — L0 原始字段
   - `indicator_ops.py` — L1 指标
   - `math_ops.py` — L2 数学/比较
   - `rolling_ops.py` — L2 滚动/窗口
   - `ta_ops.py` — L3 技术分析

---

## 策略开发

### 已有策略

**趋势资金策略** (`backend/app/strategies/trend_capital.py`)：
- 基于研报十一的日内交易行为事件驱动策略
- 三路信号融合：Signal A（小单净流出）+ Signal B（VWAP ratio）+ Signal C（净支撑量）
- 3 交易日窗口，每个信号至少 2 日触发
- 仓位管理：5 通道，持有 20 天
- 从 ClickHouse `klines_minute` 直接查询计算

**UserScript 策略** (`backend/app/backtest/strategies/trend_capital_script.py`)：
- RQAlpha 兼容的策略脚本格式
- 通过 `strategy_loader.py` 动态加载

### 编写新策略的方式

**方式一：独立策略类**（推荐用于复杂策略）

```python
class MyStrategy:
    def __init__(self):
        self.ch_client = get_ch_client()

    def generate_signals(self, start_date, end_date, symbols):
        # 从 ClickHouse 查询数据，生成交易信号
        ...

    def run_backtest(self, ...):
        # 模拟交易
        ...
```

**方式二：UserScript**（RQAlpha 兼容）

```python
def init(context):
    context.stocks = []

def handle_bar(context, bar):
    # 每个 bar 调用一次
    ...
```

### 回测引擎 (`backend/app/backtest/`)

| 模块 | 文件 | 说明 |
|------|------|------|
| 向量化回测 | `vectorized.py` | 基于 quantile 分组收益 |
| 事件驱动回测 | `event_driven.py` | 完整的事件驱动引擎 |
| AKQuant 引擎 | `engine/akquant/` | AKQuant adapter、runner、normalizer、capabilities、reporter |
| 数据提供器 | `engine/data_provider.py` | 通过 MarketDataStore 从 Parquet/ClickHouse 加载 daily/minute/minute_timer |
| 事件定义 | `event/events.py` | MarketEvent, SignalEvent, OrderEvent, FillEvent |
| 事件总线 | `event/event_bus.py` | 事件注册与分发 |
| 日历 | `event/calendar.py` | 交易日历 |
| 组合管理 | `portfolio/` | Position, Account, Portfolio, RiskValidators |
| 分析 | `analysis/` | Collectors, Metrics (夏普/最大回撤/年化收益等) |
| 配置 | `config.py` | BacktestConfig |
| 运行器 | `runner.py` | BacktestRunner |
| 策略加载 | `strategy_loader.py` | 动态加载 UserScript |

### AKQuant 回测 API

| 路径 | 说明 |
|---|---|
| `GET /api/backtest/capabilities` | 查询 AKQuant 可用性、版本、功能和 TA-Lib 函数数 |
| `POST /api/backtest/run` | 运行统一回测，`engine="akquant"` 走 AKQuant |
| `POST /api/backtest/optimize/grid` | AKQuant Grid Search |
| `POST /api/backtest/optimize/walk-forward` | AKQuant Walk-forward Validation |
| `POST /api/backtest/strategy-params/schema` | 获取策略参数 schema |
| `POST /api/backtest/strategy-params/validate` | 校验策略参数 payload |
| `GET /api/backtest/index-pools/{index_symbol}` | 查询指数池成分覆盖 |
| `GET /api/backtest/timer-coverage` | 查询稀疏分钟 timer 覆盖 |

### ID=43 小市值策略约束

- 聚宽源码在每次调仓时取 `get_index_stocks('399101.XSHE')`；平台应使用 `399101.SZ` 的历史成分快照，不能用当前自选股 960 只静态替代。
- 前端股票池选择应走指数下拉菜单，传 `index_symbol="399101.SZ"`。
- 策略若只需要固定时点，不要使用完整分钟线；使用 `bar_type="minute_timer"`。
- 当前推荐先将所需 timer 点分钟数据同步到 Parquet 或 ClickHouse，再运行回测。
- 日线交易价避免未来数据，不能用当天 close 模拟当日成交价。
- 和聚宽对齐时重点检查：指数成分、市值排序输入、行业集中度、ST、停牌、退市、涨跌停、成交时点。
- 年度 debug 推荐使用：
  - `backend/app/scripts/run_small_cap_full_debug.py --start auto`
  - `backend/app/scripts/run_small_cap_yearly_debug.py`
  - `backend/app/scripts/compare_small_cap_logs.py`

---

## API 路由全景

| 前缀 | 来源 | 功能 |
|------|------|------|
| `/api/system/*` | `api/system.py` | 系统状态、健康检查 |
| `/api/data/*` | `api/data.py` | 股票列表、同步、自选股 |
| `/api/explorer/*` | `api/data_explorer.py` | 数据浏览器（ClickHouse 表查询） |
| `/api/skill/*` | `api/data_skill.py` | DataSkill 统一数据接口 |
| `/api/backtest/*` | `api/backtest.py` | 回测管理 |
| `/api/factor/*` | `api/factor.py` | 因子管理 |
| `/api/v2/factors/*` | `api/factors.py` | 因子模板、表达式验证 |
| `/api/indicators/*` | `api/indicator.py` | 指标元数据、计算触发 |
| `/api/strategy/*` | `api/strategy.py` | 策略 CRUD |
| `/compute/*` | `compute/api.py` | 计算引擎（因子表达式求值） |
| `/backtest/*` | `backtest/api.py` | 回测引擎 V2 |

---

## 前端页面

| 路由 | 组件 | 功能 |
|------|------|------|
| `/data` | `DataManage/index.vue` | 数据管理主页 |
| `/data/sync` | `DataManage/SyncPanel.vue` | 数据同步面板 |
| `/data/stock-list` | `DataManage/StockList.vue` | 股票列表 |
| `/data/kline` | `DataManage/KlineQuery.vue` | K线查询 |
| `/explorer` | `DataExplorer.vue` | 数据浏览器（动态 SQL 查询） |
| `/factors` | `FactorResearch/index.vue` | 因子研究主页 |
| `/factors/overview` | `FactorResearch/IndicatorOverview.vue` | 指标概览 |
| `/factors/list` | `FactorResearch/FactorList.vue` | 因子列表 |
| `/factors/board` | `FactorResearch/FactorBoard.vue` | 因子看板 |
| `/factors/screen` | `FactorResearch/StockScreen.vue` | 选股筛选 |
| `/factors/analysis` | `FactorResearch/FactorAnalysis.vue` | 因子分析 |
| `/backtest` | `StrategyBacktest/index.vue` | 策略回测主页 |
| `/backtest/list` | `StrategyBacktest/BacktestList.vue` | 回测记录列表 |
| `/backtest/report/:id` | `StrategyBacktest/BacktestReport.vue` | 回测报告 |
| `/factor-backtest` | `FactorBacktest/index.vue` | 因子回测 |
| `/watchlist` | `Watchlist.vue` | 自选股管理 |
| `/stock/:symbol` | `StockDetail.vue` | 个股详情 |
| `/live` | `LiveTrading/index.vue` | 实盘交易（规划中） |
| `/system` | `SystemMonitor/index.vue` | 系统监控 |
| `/docs` | `Docs/index.vue` | 使用文档 |

---

## ⚠️ 关键注意事项

### xtquant 使用

1. **永远不要用 `download_financial_data`** — 会在 miniQMT 上无限阻塞。用 `download_financial_data2(callback=None)`。
2. **所有 xtquant 调用都是同步阻塞的**，必须用 `asyncio.get_running_loop().run_in_executor()` 包装。不要用 `asyncio.get_event_loop()`（Python 3.10+ 已废弃）。
3. **`get_financial_data` 返回 DataFrame**，不是 dict。`m_timetag` 列是 **STRING 类型**，必须用字符串比较（`'20240331'` 而非 `20240331`）。
4. **`download_sector_data` 可能挂死** — 代码有兜底：`_scan_all_stocks()` 通过 SW1 板块扫描获取 A 股列表。
5. **clean_local_cache 不要清理 `Sector/` 和 `TradeDateAndETFStockListCache`** — 清理后板块扫描会失败。
6. **分钟线主动下载优先使用 `download_history_data2`**，再用 `get_local_data` 或平台封装读取本地缓存。
7. **策略运行时不要反复读 QMT 分钟线**，应先同步到 Parquet/ClickHouse，回测只读本地列式库。

### 数据库操作

- **行情数据查询** — 使用 `get_market_data_store()` 获取抽象数据层实例，自动根据配置选择 Parquet 或 ClickHouse 后端。不要直接使用 `get_ch_client()`。
- **ClickHouse 查询** — 仅在 ClickHouse backend 内部使用 `get_ch_client()`。
- **SQLite 操作** — 使用 `get_async_session()` 依赖注入，`insert().on_conflict_do_update()` 做 upsert。
- **SQLite 同步引擎** — 指标调度器中需创建同步引擎：`create_engine(settings.database_url.replace("+aiosqlite", ""))`。
- **指数成分** — 使用 `backend/app/services/index_components.py`，按 `trade_date <= as_of` 最近快照取成分。
- **timer 覆盖率** — 使用 `backend/app/services/timer_minute_sync.py` 和 `/api/backtest/timer-coverage`。

### 代码修改注意事项

- 修改 `sync_service.py`（1400+ 行）后验证语法：`python -c "import ast; ast.parse(...)"`
- 修改 `qmt_gateway.py` 后验证语法（class 方法缩进为 4 空格）
- 新增 API 路由需在 `api/router.py` 注册
- 新增前端页面需在 `frontend/src/router/index.ts` 添加路由，在 `layouts/MainLayout.vue` 添加导航
- **不要用 AKShare 替代 xtquant** — 用户明确拒绝
- 修改 AKQuant 集成后至少跑：
  - `.\backend\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q`
  - `cd frontend; npm run build`

---

## 启动命令

### 环境端口隔离

dev 和 prod 端口必须严格分开；启动、关闭、健康检查、前端代理、接口调用都先确认当前仓库路径和端口，不要混用。

| 环境 | 根目录 | 后端 API | 同步服务 | 前端 |
|---|---|---:|---:|---:|
| dev | `E:\Projects\GaoshouPlatform-dev` | `18800` | `18810` | `13500` |
| prod | `E:\Projects\GaoshouPlatform-prod` | `8800` | `8810` | `3500` |

```powershell
# dev 后端
cd E:\Projects\GaoshouPlatform-dev\backend
.venv\Scripts\activate
uvicorn app.main:app --host 127.0.0.1 --port 18800

# dev 前端
cd E:\Projects\GaoshouPlatform-dev\frontend
npm run dev -- --host 127.0.0.1 --port 13500 --strictPort
```

### 桌面启动/关闭脚本维护规则

- 桌面脚本位置：
  - `C:\Users\Albert\Desktop\启动GaoshouPlatform.bat`
  - `C:\Users\Albert\Desktop\关闭GaoshouPlatform.bat`
- 每次完成任务后按影响范围重启对应模块：改前端则重启前端 dev server，改后端 API/服务则重启后端，改数据同步服务/脚本/数据管线则重启数据同步模块；跨模块改动只重启受影响模块，并做必要健康检查。
- 每次新增后端服务、前端 dev server、Docker 容器、外部依赖健康检查、常驻任务或实盘接口，都必须同步更新这两个脚本。
- 启动脚本应包含：必要配置读取、依赖启动或检查、健康检查、失败提示、最终访问地址。
- 关闭脚本应包含：平台自管进程停止、可选 Docker 容器停止、端口校验；外部客户端如 miniQMT 只提示状态，不由脚本强杀。
- miniQMT 实盘桥接是可选外部依赖，启动脚本只提示配置和检查入口，不得因为 miniQMT 未打开而阻塞平台启动。账户配置来自 `.env.local` 的 `QMT_ACCOUNT_ID`、`QMT_ACCOUNT_TYPE`、`QMT_TRADER_PATH`，状态可在 `/api/grid-trading/status` 检查，真实下单默认保持 `GRID_TRADING_ENABLE_ORDER_SUBMIT=false`。

---

## 数据同步流程

| API | sync_type | 数据源 | 写入目标 | 说明 |
|-----|-----------|--------|----------|------|
| `POST /api/data/sync` | `stock_info` | `get_stock_list_in_sector` | SQLite stocks | 快速，无需下载 |
| `POST /api/data/sync` | `stock_full` | `get_stock_list` + `get_full_tick` + `get_financial_data` | SQLite stocks | 含市值+财务 |
| `POST /api/data/sync` | `financial_data` | `download_financial_data2` + `get_financial_data` | SQLite financial_data | 需 QMT 在线 |
| `POST /api/data/sync` | `kline_daily` | `download_history_data` + `get_market_data_ex` | ClickHouse klines_daily | 日 K 线 |
| `POST /api/data/sync` | `kline_minute` | `download_history_data` + `get_market_data_ex` | ClickHouse klines_minute | 分钟 K 线 |
| `POST /api/data/sync` | `realtime_mv` | `get_full_tick` | SQLite stocks | 实时市值 |

同步流程：下载 → 读取 → 写入数据库 → 清理本地缓存 → 触发指标自动计算

固定 timer 分钟线流程：主动下载 1m → 读取本地缓存 → 抽取 timer 点 → 写入 Parquet `klines_minute_timer` 或 ClickHouse `klines_minute` → AKQuant `minute_timer` 回测。
