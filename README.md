# GaoshouPlatform - 量化投研平台

Last updated: 2026-06-10.

基于 Vue 3 + FastAPI 的 A 股量化投研平台，支持数据管理、因子研究、策略回测和实盘交易。

---

## 当前能力概览

- 数据链路：华泰 miniQMT/xtquant 为主数据源，SQLite 存元数据，Parquet/DuckDB 为默认行情/因子数据层，ClickHouse 为可选高性能后端。
- 回测引擎：支持内置事件驱动引擎和 AKQuant 引擎，AKQuant 已接入能力探测、事件驱动回测、Grid Search、Walk-forward Validation、参数 schema/校验。
- 策略数据模式：支持 `daily`、`minute`、`minute_timer`。只需要固定盘中时点的策略应优先用 `minute_timer`，从 Parquet 或 ClickHouse 读取稀疏分钟数据。
- **无需 ClickHouse**：默认 `MARKET_DATA_BACKEND=parquet`，零运维启动。切换 `MARKET_DATA_BACKEND=clickhouse` 可启用 ClickHouse。
- 指数池：回测可通过 `index_symbol` 使用动态指数成分池，例如中小综指 `399101.SZ`，避免用当前自选股静态替代历史股票池。
- 因子能力：既有指标注册体系，也有表达式 Compute Engine；Factor Value Store 已承载 TA-Lib、研究因子、小市值因子和 Alpha101 缓存；Alpha101 101 个公式已接入宽表向量化批量计算。
- 前端信息架构：`/home` 是投研决策工作台，`/monitor` 是系统运维控制台；数据查看 `/data` 与数据同步 `/data/sync` 已拆分，因子定义 `/factor` 与因子评估 `/factor/evaluation` 已拆分。
- 同步状态语义：`/api/data/sync/status` 中 `can_trigger=true` 表示“同步服务可接收新提交/排队”，不表示当前没有任务运行；前端展示为“提交入口/运行说明”，避免把运行中任务误判为空闲。

关键文档：

| 文档 | 用途 |
|---|---|
| `docs/user-manual.md` | 使用手册：启动、数据同步、AKQuant 回测、ID=43 小市值流程 |
| `docs/data-source-cheatsheet.md` | 数据源小抄：miniQMT、Tushare、AKShare 的优先级和适用场景 |
| `docs/local-data-onboarding.md` | 本地数据接入配置：SQLite + Parquet/DuckDB + 前端验证流程 |
| `docs/indevs-tushare-pro-guide.md` | Indevs Tushare Pro Replay 新接口：历史分钟、集合竞价、财务、公告、指数等已验证能力 |
| `docs/tushare-relay-ingestion.md` | Tushare Relay 接入：配置、同步目录、首批 Parquet 数据集、新闻公告护栏和轻量因子 |
| `docs/akquant-integration-todo.md` | AKQuant 当前集成状态、验证命令和仍需跟进事项 |
| `docs/factor-value-store.md` | 因子研究 Factor Value Store：通用特征定义、覆盖率、预计算、Relay 结构化因子和 ID=43 接入方式 |
| `docs/strategy-data-contract.md` | 策略数据契约：策略只读公开因子/行情契约，内部派生缓存只供预计算任务使用 |
| `docs/alpha101-factor-guide.md` | Alpha101 因子说明：真实公式、宽表计算、覆盖率、IC 解读和使用建议 |
| `docs/frontend-information-architecture.md` | 新版前端页面职责、菜单分层、工作台/运维差异化和颜色语义 |
| `docs/archive/README.md` | 已完成或过期的历史计划、旧 specs 和调研报告归档 |
| `AGENTS.md` | AI coding agent 项目指南和关键约束 |

---

## 技术栈

| 层 | 技术 | 说明 |
|---|---|---|
| 后端 | Python 3.12 + FastAPI | REST API，异步架构 |
| ORM | SQLAlchemy (async) + aiosqlite | SQLite 存储元数据（股票信息、同步日志、自选股等） |
| 时序库(默认) | DuckDB + Parquet | 文件型分析数据，零运维 |
| 时序库(可选) | ClickHouse | 大规模多用户高并发查询 |
| 数据源 | 华泰 QMT (miniQMT) via xtquant | A 股行情 + 财务数据 |
| 前端 | Vue 3 + TypeScript + Element Plus | 暗色主题 UI |
| 图表 | ECharts | K 线图、因子分析 |
| 构建 | Vite | 前端构建 |

---

## 项目结构

```
GaoshouPlatform/
├── backend/                     # FastAPI 后端
│   ├── app/
│   │   ├── main.py              # 应用入口 + lifespan
│   │   ├── core/
│   │   │   └── config.py        # 配置（DB路径、ClickHouse端口等）
│   │   ├── api/
│   │   │   ├── router.py        # 路由汇总 /api/system /api/data /api/explorer ...
│   │   │   ├── data.py          # 股票/K线/同步代理/自选股 API
│   │   │   ├── data_explorer.py # ClickHouse 数据浏览器 API
│   │   │   ├── indicator.py     # 指标库 API
│   │   │   ├── factor.py        # 因子研究 API
│   │   │   ├── backtest.py      # 回测 API
│   │   │   └── system.py        # 系统状态 / data-summary / runtime tasks API
│   │   ├── engines/
│   │   │   ├── qmt_gateway.py   # ⭐ QMT 数据网关（核心，封装 xtquant）
│   │   │   └── vn_engine.py     # VeighNa 回测引擎
│   │   ├── db/
│   │   │   ├── sqlite.py        # SQLite async 连接
│   │   │   ├── clickhouse.py    # ClickHouse 连接 + 建表
│   │   │   └── models/          # SQLAlchemy 模型
│   │   │       ├── base.py      # Base + TimestampMixin
│   │   │       ├── stock.py     # Stock 模型
│   │   │       ├── financial.py # FinancialData (按季度)
│   │   │       ├── watchlist.py # 自选股模型
│   │   │       ├── sync.py      # SyncLog 模型
│   │   │       └── factor.py    # 因子模型
│   │   ├── indicators/           # 指标库（估值/成长/质量/动量/波动/流动性/技术/主题）
│   │   │   ├── base.py          # 指标基类 + 注册表
│   │   │   └── scheduler.py     # 指标计算调度器
│   │   └── services/
│   │       ├── data_service.py  # 数据查询服务
│   │       ├── sync_service.py  # ⭐ 数据同步服务（stock_info/stock_full/financial_data/kline_daily/kline_minute/realtime_mv）
│   │       ├── factor_service.py
│   │       └── backtest_service.py
│   ├── data/                    # SQLite 数据文件 + 运行时数据
│   ├── .opencode/skills/        # 技能文件
│   │   └── xtquant-data-api.md  # ⭐ xtquant API 完整参考
│   └── requirements.txt
├── frontend/                    # Vue 3 前端
│   ├── src/
│   │   ├── api/                 # API 调用封装
│   │   │   ├── request.ts       # Axios 实例
│   │   │   ├── data.ts          # 数据管理 API
│   │   │   ├── explorer.ts      # 数据浏览器 API
│   │   │   ├── sync.ts          # 同步状态 API
│   │   │   ├── indicator.ts     # 指标 API
│   │   │   └── factor.ts       # 因子 API
│   │   ├── views/
│   │   │   ├── HomeWorkbench.vue# 投研决策工作台
│   │   │   ├── DataManage/      # 数据查看 + 独立数据同步页
│   │   │   ├── DataExplorer.vue # ⭐ Parquet/DuckDB / ClickHouse 数据浏览器
│   │   │   ├── FactorResearch/  # 因子定义 / 因子评估页
│   │   │   ├── StrategyBacktest/# 策略回测页
│   │   │   └── SystemMonitor/   # 系统运维控制台
│   │   ├── layouts/
│   │   │   └── MainLayout.vue   # 主布局（含侧边导航）
│   │   ├── router/index.ts      # 路由配置
│   │   └── styles/
│   │       └── design-system.css# 暗色主题变量
│   └── package.json
└── docs/
    ├── user-manual.md
    ├── factor-value-store.md
    └── archive/                 # 过期计划/specs/调研报告
```

---

## 快速开始

### 环境要求

| 软件 | 版本 | 说明 |
|------|------|------|
| Python | 3.12+ | 后端 |
| Node.js | 18+ | 前端 |
| Docker Desktop | 最新 | ClickHouse 容器（可选，默认使用 Parquet） |
| 华泰 QMT / miniQMT | - | 数据源（必须在线） |

### 1. 初始化后端

```powershell
cd E:\projects\GaoshouPlatform\backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 初始化前端

```powershell
cd E:\projects\gaoshouplatform\frontend
npm install
```

### 3. 启动 ClickHouse（可选）

仅当 `MARKET_DATA_BACKEND=clickhouse` 时需要。默认 Parquet 模式无需此步骤。

```powershell
# 首次：创建目录 + 启动容器
mkdir E:\clickhouse-data, E:\clickhouse-logs
docker run -d --name clickhouse-server --restart always `
  -p 19000:9000 -p 18123:8123 `
  -v E:\clickhouse-data:/var/lib/clickhouse `
  -v E:\clickhouse-logs:/var/log/clickhouse-server `
  clickhouse/clickhouse-server:24.1

# 创建数据库
docker exec clickhouse-server clickhouse-client -q "CREATE DATABASE IF NOT EXISTS gaoshou"
```

> **端口 19000**（非默认 9000），避免与 Windows 保留端口冲突。

### 4. 启动服务

端口必须按环境隔离，避免 dev/prod 互相代理或重启错服务：

| 环境 | 根目录 | 后端 API | 同步服务 | 前端 |
|---|---|---:|---:|---:|
| dev | `E:\Projects\GaoshouPlatform-dev` | `18800` | `18810` | `13500` |
| prod | `E:\Projects\GaoshouPlatform-prod` | `8800` | `8810` | `3500` |

**后端：**
```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.venv\Scripts\activate
uvicorn app.main:app --host 127.0.0.1 --port 18800
```

**同步服务：**
```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.venv\Scripts\activate
uvicorn app.sync_main:app --host 127.0.0.1 --port 18810
```

**前端：**
```powershell
cd E:\Projects\GaoshouPlatform-dev\frontend
npm run dev -- --host 127.0.0.1 --port 13500 --strictPort
```

- 后端 API 文档：http://localhost:18800/docs
- 同步服务健康：http://localhost:18810/health
- 前端页面：http://localhost:13500
- 健康检查：http://localhost:18800/health

> 后端启动时会自动创建 SQLite 表和 ClickHouse 表。
> 生产环境对应端口为 `8800/8810/3500`，不要把 dev 前端代理到 prod API，也不要从 dev 页面触发 prod 写入任务。

---

## 核心数据流

```
QMT Server → download_financial_data2()/download_history_data()
                ↓ (写入本地 .DAT 缓存)
          get_financial_data()/get_market_data_ex()
                ↓ (读取到 Python)
          QMTGateway.parse → StockInfo / FinancialQuarter / KlineData
                ↓
          SyncService → 写入 SQLite (股票信息/财务)
                     → 写入 Parquet (K线/因子, 默认)
                     → 写入 ClickHouse (K线/指标, 可选)
                ↓
          QMTGateway.clean_local_cache() → 删除本地 .DAT 缓存释放磁盘
```

### 关键数据同步类型

| sync_type | 说明 | 数据源 | 写入目标 |
|-----------|------|--------|----------|
| `stock_info` | 股票基础信息 | `get_stock_list_in_sector` → `get_instrument_detail` | SQLite |
| `stock_full` | 股票完整信息（含市值+财务） | `get_stock_list` + `get_full_tick` + `get_financial_data` | SQLite |
| `financial_data` | ⭐ 财务数据（按季度） | `download_financial_data2` + `get_financial_data` | SQLite (FinancialData表) |
| `kline_daily` | 日 K 线 | `download_history_data` + `get_market_data_ex` | Parquet / ClickHouse |
| `kline_minute` | 分钟 K 线 | `download_history_data` + `get_market_data_ex` | Parquet / ClickHouse |
| `realtime_mv` | 实时市值 | `get_full_tick` | SQLite |
| `tushare_relay` | Relay 增强数据 | Indevs Tushare Relay | Parquet (`adj_factors`/`moneyflow`/`auction_replay`/`ths_index`/`ths_member`/`block_moneyflow`) |

同步相关 API：

| 接口 | 说明 |
|---|---|
| `POST /api/data/sync` | 提交同步任务；长任务进入队列串行执行 |
| `GET /api/data/sync/status` | 查询当前同步状态、服务可用性、提交入口和队列详情 |
| `GET /api/data/sync/catalog` | 同步任务目录、预设方案、Relay 配置和覆盖概览 |
| `GET /api/data/sync/logs` | 最近同步审计日志 |
| `POST /api/data/sync/cancel` | 请求取消当前同步任务 |

`can_trigger=true` 只表示服务可以接收新提交；当 `status=running/queued` 且 `details.queue_mode=true` 时，新提交会排队，不代表当前任务已经空闲。

固定时间点分钟线策略推荐链路：

```
miniQMT download_history_data2(period='1m')
        ↓
get_local_data / get_market_data_ex 读取本地缓存
        ↓
抽取 10:00 / 10:30 / 14:30 / 14:50 等 timer 点
        ↓
Parquet klines_minute_timer / ClickHouse klines_minute
        ↓
AKQuant bar_type="minute_timer" 回测
```

本地聚宽版全 A 1 分钟线已可直接作为 Parquet 数据源使用：

- 数据集：`E:\Projects\Data\parquet\klines_minute\year=YYYY\month=MM\part-*.parquet`
- 当前覆盖：`2005-01-04 09:31:00` 至 `2026-05-15 15:00:00`
- 规模：约 `3.75B` 行、`5580` 个代码
- 导入脚本：`backend/app/scripts/import_jq_minute_parquet.py`
- zip/tar.gz 归档导入：`backend/app/scripts/import_jq_minute_archives.py`
- 异常日期清理：`backend/app/scripts/clean_minute_parquet_dates.py`

---

## AKQuant 回测入口

后端统一回测 API 位于 `/api/backtest/*`：

| 接口 | 说明 |
|---|---|
| `GET /api/backtest/capabilities` | 查看 AKQuant 是否可用、版本、TA-Lib 函数数、可选模块 |
| `POST /api/backtest/run` | 运行回测，`engine="akquant"` 使用 AKQuant 引擎 |
| `POST /api/backtest/optimize/grid` | AKQuant Grid Search |
| `POST /api/backtest/optimize/walk-forward` | AKQuant Walk-forward Validation |
| `POST /api/backtest/strategy-params/schema` | 读取策略参数 schema |
| `POST /api/backtest/strategy-params/validate` | 校验策略参数 |
| `GET /api/backtest/index-pools/{index_symbol}` | 查询动态指数池覆盖情况 |
| `GET /api/backtest/timer-coverage` | 查询稀疏 timer 分钟数据覆盖率 |

参数原则：

- 前端右侧控制面板/API 请求控制日期、初始资金、股票池、费用、滑点、bar type、timer times。
- 策略代码通过 `strategy_params` 读取参数，不要硬编码回测日期、股票池或固定资金。
- 小市值类策略优先使用 `index_symbol="399101.SZ"`，不要用当前自选股 960 只静态替代历史中小综指成分。

---

## ID=43 小市值策略推荐流程

1. 在回测页选择 AKQuant 引擎。
2. 股票池选择指数池“中小综指 / `399101.SZ`”。
3. `bar_type` 选择 `minute_timer`。
4. timer 时间从控制面板传入，例如 `10:00,10:30,14:30,14:50`。
5. 先用 timer 覆盖接口确认最早可跑日期。
6. 若缺分钟数据，先用 miniQMT 主动下载，再同步所需 timer 点到 Parquet/ClickHouse；若已有完整分钟 Parquet，则直接读取本地数据集。
7. 回测阶段只读 Parquet/ClickHouse，避免策略运行时反复读取 QMT。
8. 和聚宽结果对齐时，按年度导出持仓、订单和日志，重点对比指数成分、市值排序、行业集中度、ST/停牌/退市、涨跌停成交语义。

常用脚本：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'

# 同步中小综指动态成分所需的稀疏分钟点
.\backend\.venv\Scripts\python.exe backend/app/scripts/sync_timer_minute_points.py `
  --index-symbol 399101.SZ `
  --start 20210515 `
  --end 20260508 `
  --times 10:00,10:30,14:30,14:50

# 自动从最早可用 timer 数据起跑 ID=43

# 年度切片调试，便于和聚宽日志逐年对比
```

---

## QMT 数据网关关键注意事项

> ⚠️ 这些是踩过的坑，非常重要！

### 1. `download_financial_data` 会卡死 — 永远使用 `download_financial_data2`

```python
# ❌ 永远不要用这个 — 会在 miniQMT 上无限阻塞
xt.download_financial_data(stock_list, table_list)

# ✅ 用这个 — 有回调，0.1-0.5s 完成
xt.download_financial_data2(stock_list, table_list, callback=None)
```

### 2. `get_financial_data` 返回 pandas DataFrame（不是嵌套 dict）

```python
fnd = xt.get_financial_data(['600051.SH'], ['PershareIndex','Balance','Income','Capital'], start_time='20240101')
# 返回: {'600051.SH': {'PershareIndex': DataFrame, 'Balance': DataFrame, ...}}
# 每个 DataFrame 有 m_timetag 列（str 类型如 '20240331'）— 必须用字符串比较！
```

### 3. `download_sector_data` 可能挂死

如果 QMT 本地缓存被清理过，`download_sector_data` 可能无限阻塞。`get_stock_list_in_sector("沪深A股")` 会返回空列表。此时 `_scan_all_stocks()` 会通过 SW1 行业板块扫描兜底获取 A 股列表。

### 4. 本地缓存清理策略

`clean_local_cache()` 只清理以下目录（数据已写入 ClickHouse 后）：
- `{SH,SZ}/{60,86400}/*.DAT` — K 线缓存
- `DividData/`, `Weight/` — 分红/权重缓存
- `{SH,SZ}/{Balance,Income,...}/*.DAT` — 财务缓存

**不要**清理 `Sector/`（板块数据）、`TradeDateAndETFStockListCache`（交易日历）—— 清理后会导致板块扫描失败。

### 5. 异步调用 xtquant

所有 `xt.download_*` 和 `xt.get_*` 函数都是同步阻塞的，在 FastAPI 异步环境中必须通过 `run_in_executor` 调用：
```python
loop = asyncio.get_running_loop()
data = await loop.run_in_executor(None, lambda: xt.get_market_data_ex(...))
```

**永远不要用 `asyncio.get_event_loop()`**（Python 3.10+ 已废弃），用 `asyncio.get_running_loop()`。

完整 API 参考见 `backend/.opencode/skills/xtquant-data-api.md`。数据源选择经验见 `docs/data-source-cheatsheet.md`。

---

## 前端页面与信息架构

| 路由 | 页面职责 |
|---|---|
| `/home` | 今日投研工作台：研究就绪度、今日行动建议、投研输入口径和流水线推进 |
| `/data` | 数据查看：最新日线、分钟线、基础股票、财务、指标、概念和舆情口径 |
| `/data/sync` | 数据同步：任务目录、预设、执行参数、队列、进度、同步日志 |
| `/explorer` | 数据浏览器：按需预览 Parquet/DuckDB 或 ClickHouse 表，不默认全量 `count(*)` |
| `/factor` | 因子定义：因子目录、覆盖率、参数版本、预计算入口；表达式只在创建/编辑时打开 |
| `/factor/evaluation` | 因子评估：IC、ICIR、多空收益、回撤、换手和已计算组合 |
| `/research` | 研究实验室：假设、证据链、外部链接、实验记录和复盘 |
| `/backtest` | 策略回测：代码编辑、参数配置、运行日志和报告入口 |
| `/trade` | 模拟/实盘：信号、账户、订单预览和真实下单护栏 |
| `/monitor` | 系统运维控制台：服务拓扑、值班排障、任务表、存储巡检、同步审计 |

颜色语义：平台状态中红色表示需要关注/异常，绿色表示正常/就绪；A 股行情中上涨为红、下跌为绿。

---

## 数据浏览器

dev 环境访问 `http://localhost:13500/explorer` 可以浏览 Parquet/DuckDB 或 ClickHouse 数据：
- 自动列出可查询表和分区/元数据
- 动态展示表结构（不硬编码字段）
- 支持 WHERE 过滤、排序、分页
- 支持自定义 SQL 查询（仅 SELECT/SHOW/DESCRIBE）
- Parquet 表未知行数显示为“未统计”，精确行数需要显式触发，避免默认全量扫描

后端 API：`/api/explorer/tables`、`/api/explorer/tables/{name}/schema`、`/api/explorer/tables/{name}/preview`

---

## 数据库配置

### SQLite（默认 `E:\Projects\Data\gaoshou.db`）

路径通过 `backend/app/core/config.py` 自动解析为绝对路径，默认使用仓库同级公共数据目录 `E:\Projects\Data`；可用 `GAOSHOU_DATA_DIR`、`DATABASE_URL`、`PARQUET_DATA_DIR` 覆盖。

### ClickHouse

| 参数 | 值 |
|------|---|
| Host | localhost |
| Port | 19000 |
| Database | gaoshou |
| User | default |
| Password | (空) |

### ClickHouse 表结构

| 表 | 用途 | 分区 | 排序键 |
|----|------|------|--------|
| `klines_daily` | 日 K 线 | toYYYYMM(trade_date) | (symbol, trade_date) |
| `klines_minute` | 分钟 K 线 | toYYYYMM(datetime) | (symbol, datetime) |
| `stock_indicators` | 截面指标 | toYYYYMM(trade_date) | (symbol, indicator_name, trade_date) |
| `indicator_timeseries` | 时序指标 | toYYYYMM(datetime) | (symbol, indicator_name, datetime) |

---

## 常见问题

### Q1: SQLite "unable to open database file"

后端配置已改为绝对路径。如果仍然遇到此问题，确保工作目录正确或检查 `config.py` 中的 `_DB_PATH`。

### Q2: `get_stock_list_in_sector("沪深A股")` 返回空列表

QMT 本地 Sector 缓存被清理后会出现此问题。代码已有兜底：自动通过 `_scan_all_stocks()` 扫描 SW1 行业板块获取 A 股列表（约 4000+ 只）。

如需恢复完整板块数据，需在 QMT 客户端中手动下载数据。

### Q3: 财务数据同步很慢

`sync_financial_data` 使用 `download_financial_data2` 分批下载（每批 200 只），约 4000 只股票分 21 批。每批含下载 + 读取 + 解析 + 写入 SQLite，整体需约 10-20 分钟。

### Q4: ClickHouse 连接失败

```powershell
docker ps | findstr clickhouse
docker exec -it clickhouse-server clickhouse-client -q "SHOW DATABASES"
```

### Q5: 前端热更新失效

```powershell
cd frontend
Remove-Item -Recurse node_modules\.vite -ErrorAction SilentlyContinue
npm run dev
```

---

## 开发规范

### 提交信息

```
feat: 新功能
fix: 修复 bug
docs: 文档更新
refactor: 重构
chore: 构建/工具
```

### 关键配置

| 文件 | 说明 |
|------|------|
| `backend/app/core/config.py` | 后端配置（DB绝对路径、ClickHouse端口） |
| `frontend/vite.config.ts` | 前端代理（dev 默认指向 localhost:18800） |
| `frontend/src/styles/design-system.css` | 暗色主题变量 |

### 代码风格

- Python：类型注解（`str | None`），docstring 用中文
- Vue/TS：Composition API + `<script setup lang="ts">`
- 不添加不必要的注释——代码应自解释
- 异步代码使用 `asyncio.get_running_loop()`，不用 `asyncio.get_event_loop()`

---

*最后更新: 2026-06-03*
