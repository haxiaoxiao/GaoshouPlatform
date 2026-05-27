# 本地数据接入配置指南

Last updated: 2026-05-25.

本文说明如何在不启动 ClickHouse 的情况下，让平台读取本地 SQLite + Parquet 数据目录，并跑通数据管理、数据浏览器、因子研究和回测页面。

## 目标

默认推荐模式：

```text
MARKET_DATA_BACKEND=parquet
CLICKHOUSE_ENABLED=false
```

接入后应满足：

- `/data` 可读取 SQLite 中的股票、自选股、策略和同步状态。
- `/explorer` 可浏览本地 Parquet 数据集。
- `/factors` 可读取 Factor Value Store 的因子定义、覆盖率和预览。
- `/backtest` 可读取本地行情并运行 `daily`、`minute_timer` 或必要的 `minute` 回测。
- `/api/system/status` 显示 `market_data_backend=parquet`。

## 数据目录约定

当前项目默认数据目录：

```text
E:\Projects\GaoshouPlatform\data
```

Parquet 数据集默认位于：

```text
data/parquet/
  klines_daily/
  klines_minute/
  klines_minute_timer/
  klines_minute_cum_timer/
  factor_values/
  factor_cache/
  stock_indicators/
  indicator_timeseries/
```

SQLite 元数据库默认位于：

```text
data/gaoshou.db
```

说明：

- `factor_values` 是当前因子值缓存主目录。
- 旧 `feature_values` 只作为历史迁移来源，不再作为新 API 的目标目录。
- `klines_minute` 是完整分钟线源数据；`klines_minute_timer` 是固定时点分钟 K 线；`klines_minute_cum_timer` 是累计成交量派生缓存，三者不能互相直接替代。

## 环境配置

仓库提交 `.env.example` 作为模板。每台机器复制一份到 `.env.local`：

```powershell
Copy-Item .env.example .env.local
```

Windows 示例：

```text
MARKET_DATA_BACKEND=parquet
PARQUET_DATA_DIR=E:/Projects/GaoshouPlatform/data/parquet
DATABASE_URL=sqlite+aiosqlite:///E:/Projects/GaoshouPlatform/data/gaoshou.db
DUCKDB_PATH=:memory:
CLICKHOUSE_ENABLED=false
DEBUG=false
```

macOS / Linux 示例：

```text
MARKET_DATA_BACKEND=parquet
PARQUET_DATA_DIR=/Users/albert/MyProjects/GaoshouPlatform/data/parquet
DATABASE_URL=sqlite+aiosqlite:////Users/albert/MyProjects/GaoshouPlatform/data/gaoshou.db
DUCKDB_PATH=:memory:
CLICKHOUSE_ENABLED=false
DEBUG=false
```

`.env.local`、`.env.<hostname>.local`、`backend/.env.local` 已被 `.gitignore` 忽略，可以放本机路径和密钥。

## 启动

后端：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\activate
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

前端：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run dev
```

常见开发会话可能使用 `3500`/`8800` 端口，以启动脚本输出为准。

## 验证

系统状态：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/system/status
```

健康检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/system/health
```

数据浏览器表：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/explorer/tables
```

因子定义：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/factor-values/definitions
```

Factor Value Store 覆盖率：

```text
GET /api/factor-values/coverage?factor_name=alpha101_001
```

timer 覆盖率：

```text
GET /api/backtest/timer-coverage?index_symbol=399101.SZ&start_date=2021-05-15&end_date=2026-05-08&times=10:00,10:30,14:30,14:50
```

## 常见问题

### DataExplorer 看不到表

检查：

1. `PARQUET_DATA_DIR` 是否指向 `data/parquet`。
2. 目录下是否有 `klines_daily/year=YYYY/month=MM/part-*.parquet`。
3. 后端启动日志是否显示 `ClickHouse disabled, using Parquet/DuckDB backend`。

### 因子看板有因子但没有结果

检查：

1. 是否已经在因子值缓存页完成对应因子或集合预计算。
2. 研究页股票池、日期和参数 hash 是否匹配已有研究结果。
3. 覆盖率是否足够，特别是 Alpha101 长窗口公式。

### 旧 feature_values 数据如何处理

需要迁移时运行：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\python.exe -m app.scripts.migrate_feature_values_to_factor_values --overwrite
```

迁移后新功能只读写 `factor_values`。
