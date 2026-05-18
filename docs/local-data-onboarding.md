# 本地数据接入配置指南

本文记录从同步项目代码后，把本地 SQLite、Parquet 行情数据、指标数据接入平台并跑通前端页面的配置动作。

Last updated: 2026-05-18.

## 目标

让平台在不启动 ClickHouse 的情况下，直接读取本地数据目录：

```text
/Users/albert/MyProjects/Data/GSdata
```

接入后应满足：

- `/data` 可读取 SQLite 中的股票、自选股、策略和回测记录。
- `/explorer` 可浏览本地 Parquet 数据集。
- `/backtest` 可读取本地行情并运行回测。
- `/api/system/status` 显示 `market_data_backend=parquet` 且 `clickhouse_enabled=false`。

## 本地数据目录约定

目标目录需要同时包含 SQLite 元数据库和 Parquet 数据集：

```text
GSdata/
  gaoshou.db
  klines_daily/
    year=YYYY/month=MM/part-*.parquet
  klines_minute/
    year=YYYY/month=MM/part-*.parquet
  klines_minute_cum_timer/
    year=YYYY/month=MM/part-*.parquet
  feature_values/
    year=YYYY/month=MM/part-*.parquet
```

当前已验证的数据规模：

| 数据集 | 用途 | 已验证规模 |
|---|---|---|
| `gaoshou.db` | 股票、财务、指数成分、策略、回测、自选股 | `stocks=5257`，`index_components=82442`，`strategies=36+` |
| `klines_daily` | 日 K 线、向量化回测、数据浏览器 | `8,458,457` 行，`5745` 个代码 |
| `klines_minute` | 完整 1 分钟线 | `3,753,384,618` 行，`5580` 个代码 |
| `klines_minute_cum_timer` | 小市值策略固定时点分钟数据 | `7,209,415` 行，`5489` 个代码 |
| `feature_values` | 指标/特征值 | `7,938,034` 行 |

## 本机配置文件

仓库提交 `.env.example` 作为配置模板。每台电脑复制一份到本地配置文件，再填自己的路径。

推荐使用：

```text
.env.local
```

也可以为每台电脑保留独立文件：

```text
.env.<hostname>.local
```

运行时读取顺序为：

1. 项目根目录 `.env`
2. 项目根目录 `.env.local`
3. 项目根目录 `.env.<hostname>.local`
4. `backend/.env`
5. `backend/.env.local`
6. `backend/.env.<hostname>.local`

后面的文件会覆盖前面的同名配置。`.env.local`、`.env.<hostname>.local` 和 backend 下对应文件都已加入 `.gitignore`，适合放机器本地路径和密钥。

### macOS / Linux 示例

```bash
cd /Users/albert/MyProjects/GaoshouPlatform
cp .env.example .env.local

cat > .env.local <<'EOF'
MARKET_DATA_BACKEND=parquet
PARQUET_DATA_DIR=/Users/albert/MyProjects/Data/GSdata
DATABASE_URL=sqlite+aiosqlite:////Users/albert/MyProjects/Data/GSdata/gaoshou.db
DUCKDB_PATH=:memory:
CLICKHOUSE_ENABLED=false
DEBUG=false
EOF
```

字段说明：

| 变量 | 说明 |
|---|---|
| `MARKET_DATA_BACKEND=parquet` | 使用 DuckDB 查询本地 Parquet，而不是 ClickHouse |
| `PARQUET_DATA_DIR` | Parquet 数据集根目录 |
| `DATABASE_URL` | SQLite 元数据库路径，注意 macOS/Linux 绝对路径需要四个 `/` |
| `DUCKDB_PATH=:memory:` | DuckDB 使用内存数据库执行查询 |
| `CLICKHOUSE_ENABLED=false` | 启动时跳过 ClickHouse 初始化 |
| `DEBUG=false` | 本地数据模式下减少调试噪音 |

### Windows 示例

```powershell
Copy-Item .env.example .env.local

# 编辑 .env.local:
MARKET_DATA_BACKEND=parquet
PARQUET_DATA_DIR=E:/Projects/Data/GSdata
DATABASE_URL=sqlite+aiosqlite:///E:/Projects/Data/GSdata/gaoshou.db
DUCKDB_PATH=:memory:
CLICKHOUSE_ENABLED=false
DEBUG=false
```

## 后端依赖

从干净仓库同步后，先创建后端虚拟环境并安装依赖。

```bash
cd /Users/albert/MyProjects/GaoshouPlatform
python3 -m venv backend/.venv
backend/.venv/bin/pip install -r backend/requirements.txt
```

本地 Parquet 模式必须具备：

- `duckdb`
- `pandas`
- `pyarrow`
- `loguru`
- `aiosqlite`
- `fastapi`
- `sqlalchemy`

注意：

- `xtquant`、`veighna`、`ta-lib`、`pandas-ta` 属于可选能力，不是本地数据浏览和基础回测的必需依赖。
- 如果只验证本地数据接入，不需要启动 ClickHouse、Redis 或 QMT。

## 前端依赖

```bash
cd /Users/albert/MyProjects/GaoshouPlatform/frontend
npm install
```

## 启动服务

后端：

```bash
cd /Users/albert/MyProjects/GaoshouPlatform
PYTHONPATH=backend backend/.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

看到以下日志说明已经进入本地 Parquet 模式：

```text
ClickHouse disabled, using Parquet/DuckDB backend
Uvicorn running on http://127.0.0.1:8000
```

前端：

```bash
cd /Users/albert/MyProjects/GaoshouPlatform/frontend
npm run dev -- --host 127.0.0.1 --port 3000
```

访问：

```text
http://127.0.0.1:3000
```

## 验证命令

### 系统状态

```bash
curl -sS http://127.0.0.1:3000/api/system/status | backend/.venv/bin/python -m json.tool
```

期望关键字段：

```json
{
  "status": "running",
  "database": "connected",
  "market_data_backend": "parquet",
  "parquet_data_dir": "/Users/albert/MyProjects/Data/GSdata",
  "clickhouse_enabled": false
}
```

### 数据浏览器表列表

```bash
curl -sS http://127.0.0.1:3000/api/explorer/tables
```

期望返回：

```json
{
  "code": 0,
  "data": [
    {"name": "klines_daily", "row_count": 8458457},
    {"name": "klines_minute", "row_count": 3753384618},
    {"name": "klines_minute_cum_timer", "row_count": 7209415},
    {"name": "feature_values", "row_count": 7938034}
  ]
}
```

### 日 K 预览

```bash
curl -sS 'http://127.0.0.1:3000/api/explorer/tables/klines_daily/preview?page=1&page_size=2&order_by=trade_date&order_dir=DESC'
```

期望 `rows` 中有 `symbol/trade_date/open/high/low/close/volume/amount`。

### DataSkill 日线

```bash
curl -sS 'http://127.0.0.1:3000/api/skill/kline/daily/000001.SZ?start_date=2026-05-06&end_date=2026-05-08&limit=2'
```

期望返回非空日 K。

### 回测股票池

```bash
curl -sS http://127.0.0.1:3000/api/v2/backtest/pools/top100
```

期望返回 `symbols` 列表。

### 指数池

```bash
curl -sS 'http://127.0.0.1:3000/api/v2/backtest/index-pools/399101.SZ?start_date=2026-05-01&end_date=2026-05-15'
```

期望：

```json
{
  "code": 0,
  "data": {
    "index_symbol": "399101.SZ",
    "jq_symbol": "399101.XSHE",
    "symbol_count": 963,
    "snapshot_count": 13
  }
}
```

### Timer 分钟覆盖率

```bash
curl -sS 'http://127.0.0.1:3000/api/v2/backtest/timer-coverage?index_symbol=399101.SZ&start_date=2026-05-01&end_date=2026-05-15&times=10:00,10:30,14:30,14:50'
```

期望 `earliest_date` 有值，并且 coverage 中各交易日 `point_coverage=1.0`。

### 最小回测

```bash
SYMS=$(curl -sS http://127.0.0.1:3000/api/v2/backtest/pools/top100 \
  | backend/.venv/bin/python -c 'import sys,json; data=json.load(sys.stdin); print(",".join(data["data"]["symbols"][:20]))')

TASK=$(backend/.venv/bin/python - <<'PY' "$SYMS"
import json, sys, urllib.request

symbols = sys.argv[1].split(",")
payload = {
    "engine": "builtin",
    "mode": "vectorized",
    "factor_expression": "$close",
    "symbols": symbols,
    "start_date": "2026-05-06",
    "end_date": "2026-05-15",
    "initial_capital": 1000000,
    "rebalance_freq": "daily",
    "n_groups": 5,
    "bar_type": "daily",
}
req = urllib.request.Request(
    "http://127.0.0.1:3000/api/v2/backtest/run",
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"},
)
res = json.loads(urllib.request.urlopen(req).read())
print(res["data"]["task_id"])
PY
)

sleep 1
curl -sS "http://127.0.0.1:3000/api/v2/backtest/result/$TASK"
```

期望：

- `n_trading_days > 0`
- `nav_series` 非空
- `final_capital` 为实际回测值，不是空结果

## 前端页面检查

### 数据浏览器

打开：

```text
http://127.0.0.1:3000/explorer
```

应看到以下表：

- `klines_daily`
- `klines_minute`
- `klines_minute_cum_timer`
- `feature_values`

选择 `klines_daily` 后，表格应显示日 K 数据行。

### 回测

打开：

```text
http://127.0.0.1:3000/backtest
```

应看到：

- 策略列表非空。
- 回测记录非空。
- 页面底部状态为 `后端服务已连接`。
- 回测运行页可以加载 `top100`、指数池和已有策略。

## 本次为接入数据做过的代码侧适配

这些变更已经固化在项目代码中，使用者只需要配置 `.env.local` 和安装依赖。

| 文件 | 作用 |
|---|---|
| `.env.example` | 提交到 Git 的本地配置模板 |
| `.gitignore` | 忽略 `.env.local`、`.env.<hostname>.local` 和 backend 下对应本地配置 |
| `backend/app/core/config.py` | 支持根目录/backend 的本地配置文件；`settings.data_dir` 跟随 `DATABASE_URL` 所在目录 |
| `backend/app/main.py` | `CLICKHOUSE_ENABLED=false` 时跳过 ClickHouse 初始化 |
| `backend/app/api/system.py` | 系统状态展示 Parquet 行情覆盖率和 ClickHouse 开关 |
| `backend/app/api/data_explorer.py` | 兼容旧 `/api/explorer/tables` 前端接口，在 Parquet 模式下用 DuckDB 读本地数据 |
| `backend/app/api/parquet_explorer.py` | 增加 `klines_minute_cum_timer`、`feature_values` 数据集 |
| `backend/app/data_stores/*indicator*` | 指标读取支持 Parquet fallback，`feature_values` 可作为 `stock_indicators` 的本地数据源 |
| `backend/app/services/data_skill.py` | 指标不指定日期时按本地最新交易日查询 |
| `backend/app/services/index_components.py` | 指数成分优先读本地 SQLite，已有快照时不强制依赖 Tushare |
| `backend/app/services/timer_minute_sync.py` | 指数成分 SQLite 路径跟随 `DATABASE_URL` |
| `backend/app/backtest/vectorized.py` | 向量化回测处理最后一期下一日收益缺失，避免 NAV 被 NaN 污染 |
| `backend/requirements.txt`、`backend/pyproject.toml` | 补齐 DuckDB/Parquet 本地数据模式依赖 |
| `frontend/src/layouts/MainLayout.vue` | 数据浏览器文案改为本地数据查询 |
| `frontend/src/views/StrategyBacktest/index.vue` | timer 覆盖率提示改为本地稀疏分钟数据 |

## 常见问题

### `/api/explorer/tables` 返回 500 或连接 ClickHouse

检查 `.env.local` 是否在项目根目录，且后端是否重启。

```bash
grep -E 'MARKET_DATA_BACKEND|CLICKHOUSE_ENABLED|PARQUET_DATA_DIR|DATABASE_URL' .env.local
```

应为：

```text
MARKET_DATA_BACKEND=parquet
CLICKHOUSE_ENABLED=false
```

### 前端显示 502 / 后端服务不可用

通常是后端 uvicorn 没有在 8000 端口运行。

```bash
lsof -nP -iTCP:8000 -sTCP:LISTEN
```

如果没有监听，重新启动后端：

```bash
PYTHONPATH=backend backend/.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

### 指数池接口报 `No module named 'tushare'`

说明代码走到了 Tushare fallback。正常本地接入应优先命中 SQLite `index_components`。

检查：

```bash
sqlite3 /Users/albert/MyProjects/Data/GSdata/gaoshou.db \
  "select index_symbol, count(*), min(trade_date), max(trade_date) from index_components group by index_symbol;"
```

如果本地库有 `399101.SZ` 快照，重启后端后再验证：

```bash
curl -sS 'http://127.0.0.1:3000/api/v2/backtest/index-pools/399101.SZ?start_date=2026-05-01&end_date=2026-05-15'
```

### 数据浏览器加载慢

`klines_minute` 是 54GB 级别、37.5 亿行数据。首次 DuckDB 扫描和统计会慢一些。日常验证建议先选择：

- `klines_daily`
- `klines_minute_cum_timer`
- `feature_values`

### 回测完成但 `n_trading_days=0`

常见原因：

- 股票数太少，分组回测默认 `n_groups=5`，需要至少足够多股票。
- 选择的日期区间没有日线数据。
- `symbols` 为空，或指数池没有匹配到历史成分。

推荐先用 `top100` 和已验证日期区间跑 smoke test。

## 使用者接入清单

1. 准备 `GSdata/gaoshou.db` 和 Parquet 数据集。
2. 在项目根目录复制 `.env.example` 为 `.env.local`，配置 `DATABASE_URL` 和 `PARQUET_DATA_DIR`。
3. 安装后端依赖：`backend/.venv/bin/pip install -r backend/requirements.txt`。
4. 安装前端依赖：`cd frontend && npm install`。
5. 启动后端，确认日志显示 `ClickHouse disabled, using Parquet/DuckDB backend`。
6. 启动前端。
7. 访问 `/explorer`，选择 `klines_daily` 验证表格数据。
8. 访问 `/backtest`，确认策略列表和回测记录非空。
9. 跑最小回测，确认 `nav_series` 和 `n_trading_days` 非空。
