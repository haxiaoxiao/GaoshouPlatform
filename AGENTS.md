# AGENTS.md — Coding Agent 指南

> 本文件为 AI coding agent 提供项目关键信息，确保快速上手。

---

## 项目概述

**GaoshouPlatform** 是一个 A 股量化投研平台，核心功能：
1. 从华泰 miniQMT (xtquant) 获取行情和财务数据
2. 存储到 SQLite (元数据) + ClickHouse (时序数据)
3. 提供 Vue 3 前端进行数据浏览、因子研究、选股筛选
4. 支持策略回测（VeighNa）和实盘交易

## 用户偏好

- **界面和交互使用中文**
- **不要用 AKShare 替代 xtquant** — 用户明确说"先不要用akshare替代"
- 代码风格简洁，不加多余注释

---

## 技术栈速览

| 层 | 技术 | 版本/说明 |
|---|---|---|
| 后端 | FastAPI + SQLAlchemy (async) | Python 3.12+ |
| 元数据库 | SQLite via aiosqlite | `backend/data/gaoshou.db` |
| 时序数据库 | ClickHouse | 端口 19000 |
| 数据源 | xtquant (华泰 miniQMT) | 必须在线 |
| 前端 | Vue 3 + TypeScript + Element Plus | Vite 构建 |

## 项目路径

```
E:\projects\gaoshouplatform\
├── backend\              # FastAPI 后端 (工作目录)
│   ├── app\
│   │   ├── engines\qmt_gateway.py     # ⭐ QMT 数据网关
│   │   ├── services\sync_service.py    # ⭐ 数据同步服务
│   │   ├── api\data_explorer.py       # ClickHouse 浏览器 API
│   │   ├── api\data.py                # 股票/同步/自选股 API
│   │   ├── core\config.py             # 配置（DB路径等）
│   │   └── db\                         # 数据库连接和模型
│   ├── data\                           # SQLite 数据文件
│   ├── .opencode\skills\xtquant-data-api.md  # ⭐ xtquant 完整 API 参考
│   └── requirements.txt
└── frontend\             # Vue 3 前端
    └── src\
        ├── api\explorer.ts             # 数据浏览器 API 客户端
        ├── views\DataExplorer.vue      # 数据浏览器页面
        └── views\DataManage\          # 数据管理页面
```

---

## 启动命令

```powershell
# 后端（从 backend 目录）
cd E:\projects\GaoshouPlatform\backend
.venv\Scripts\activate
uvicorn app.main:app --host 0.0.0.0 --port 8000

# 前端（从 frontend 目录）
cd E:\projects\Gaoshouplatform\frontend
npm run dev
```

---

## ⚠️ 关键踩坑记录（必读）

### 1. `download_financial_data` 永远不要用

```python
# ❌ 会在 miniQMT 上无限阻塞
xt.download_financial_data(stock_list, table_list)

# ✅ 用带回调的版本，0.1-0.5s 完成
xt.download_financial_data2(stock_list, table_list, callback=None)
```

### 2. `get_financial_data` 返回 DataFrame（不是 dict）

```python
fnd = xt.get_financial_data(['600051.SH'], ['PershareIndex','Balance'], start_time='20240101')
# fnd['600051.SH']['PershareIndex'] 是 DataFrame，不是 dict
# m_timetag 列是 STRING 类型（如 '20240331'），必须用字符串比较！
# 正确: df[df['m_timetag'] == '20240331']
# 错误: df[df['m_timetag'] == 20240331]  ← 比较失败！
```

### 3. `download_sector_data` 可能挂死

如果本地 Sector 缓存已被清理。代码有兜底：`_scan_all_stocks()` 通过 SW1 行业板块扫描获取 A 股列表。

### 4. 异步调用 xtquant

所有 xtquant 函数都是同步阻塞的，必须用 `asyncio.get_running_loop().run_in_executor()` 调用。
**绝对不要用 `asyncio.get_event_loop()`**（Python 3.10+ 已废弃）。

### 5. SQLite 路径

`config.py` 中 SQLite 路径已改为绝对路径（基于 `_BASE_DIR`），可以从任何工作目录启动 uvicorn。

### 6. clean_local_cache 清理范围

只清理 K 线和财务 .DAT 文件。**不要清理 `Sector/`、`TradeDateAndETFStockListCache`**——清理后板块扫描会失败。

---

## 数据同步流程

| API 调用 | sync_type | 数据源 | 写入目标 | 说明 |
|----------|-----------|--------|----------|------|
| `POST /api/data/sync` | `stock_info` | `get_stock_list_in_sector` | SQLite | 快速，无需下载 |
| `POST /api/data/sync` | `stock_full` | `get_stock_list` + `get_full_tick` + `get_financial_data` | SQLite | 含市值+财务 |
| `POST /api/data/sync` | `financial_data` | `download_financial_data2` + `get_financial_data` | SQLite | ⭐ 核心，需 QMT 在线 |
| `POST /api/data/sync` | `kline_daily` | `download_history_data` + `get_market_data_ex` | ClickHouse | 日 K 线 |
| `POST /api/data/sync` | `kline_minute` | `download_history_data` + `get_market_data_ex` | ClickHouse | 分钟 K 线 |
| `POST /api/data/sync` | `realtime_mv` | `get_full_tick` | SQLite | 实时市值 |

同步流程：下载 → 读取 → 写入数据库 → 清理本地缓存

---

## 数据浏览器

- 前端路由：`/explorer`
- 后端 API：`/api/explorer/tables`、`/api/explorer/tables/{name}/schema`、`/api/explorer/tables/{name}/preview`
- 支持 WHERE 过滤、排序、分页、自定义 SQL

---

## 现有功能完成度

| 功能 | 状态 | 说明 |
|------|------|------|
| 股票列表同步 | ✅ 完成 | 含 SW1 板块兜底 |
| 股票完整信息同步 | ✅ 完成 | 含财务+市值 |
| 财务数据同步 | ✅ 完成 | 使用 download_financial_data2 |
| 日 K 线同步 | ✅ 完成 | 写入 ClickHouse |
| 分钟 K 线同步 | ✅ 完成 | 写入 ClickHouse |
| 实时市值同步 | ✅ 完成 | get_full_tick |
| 本地缓存清理 | ✅ 完成 | 清理 .DAT 释放磁盘 |
| 数据浏览器 | ✅ 完成 | 动态列、WHERE过滤、排序、分页 |
| 自选股 | ✅ 完成 | 分组管理、CSV批量导入 |
| 数据技能 (DataSkill) | ✅ 完成 | 策略模块统一数据访问接口，`/api/skill/*` |
| 因子研究 | 🔧 框架完成 | 指标库已搭建，IC/IR分析待完善 |
| 策略回测 | 🔧 框架完成 | VeighNa 引擎待集成 |
| 实盘交易 | 📋 规划中 | 尚未开始 |

---

## 数据技能 (DataSkill) — 策略模块数据接口

策略模块通过 `DataSkill` 获取所有数据，无需关心数据来自 QMT/SQLite/ClickHouse。

### 服务层 (`backend/app/services/data_skill.py`)

```python
from app.services.data_skill import DataSkill, StockSnapshot, KlineBar, FinancialReport, ScreenResult
from app.db.sqlite import get_async_session

# 在 API 中使用
skill = DataSkill(session)  # session = AsyncSession

# 股票快照（本地优先，QMT 兜底）
snapshot = await skill.get_stock("600051.SH")
snapshots = await skill.get_stocks(["600051.SH", "000001.SZ"])

# 条件选股
result = await skill.screen_stocks(
    min_mv=1_000_000, max_mv=100_000_000,
    min_pe=5, max_pe=30, min_roe=10, limit=100
)

# K线数据（ClickHouse 优先，QMT 兜底）
bars = await skill.get_kline_daily("600051.SH", start_date=date(2024,1,1))
bars = await skill.get_kline_minute("600051.SH")

# 财务数据（SQLite 优先，QMT 兜底）
reports = await skill.get_financial("600051.SH", report_count=8)
batch = await skill.get_financial_batch(["600051.SH", "000001.SZ"])

# 实时行情
quote = await skill.get_realtime_quote("600051.SH")
quotes = await skill.get_realtime_quotes(["600051.SH", "000001.SZ"])

# 行业统计 & 股票列表
industries = await skill.get_industries()
all_symbols = await skill.get_all_symbols()

# 指标查询（ClickHouse）
value = skill.get_indicator("600051.SH", "pe_ttm", date(2025,4,1))
indicators = skill.get_indicators_batch(["600051.SH"], date(2025,4,1))
```

### API 端点 (`/api/skill/*`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/skill/stock/{symbol}` | 获取股票快照 |
| POST | `/skill/stocks/batch` | 批量获取股票快照 |
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
| K线 | ClickHouse | QMT `get_kline_daily/minute` |
| 财务数据 | SQLite `financial_data` 表 | QMT `_fetch_financial_data` |
| 实时行情 | QMT `get_realtime_quotes` | 无兜底 |
| 指标 | ClickHouse `stock_indicators` | 无兜底 |

---

## 修改代码时的注意事项

1. **修改 sync_service.py 后要验证语法** — 该文件较长（1400+ 行），修改后用 `python -c "import ast; ast.parse(...)"` 检查
2. **修改 qmt_gateway.py 后要验证语法** — `_scan_all_stocks` 等方法确保缩进正确（class 方法，4 空格缩进）
3. **新增 API 路由** — 需在 `api/router.py` 中注册
4. **新增前端页面** — 需在 `router/index.ts` 添加路由，在 `layouts/MainLayout.vue` 添加导航
5. **ClickHouse 查询** — 使用 `get_ch_client()` 获取单例客户端，不要手动创建连接
6. **SQLite 操作** — 使用 `get_async_session()` 依赖注入，用 `insert().on_conflict_do_update()` 做 upsert