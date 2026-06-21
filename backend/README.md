# GaoshouPlatform Backend

Last updated: 2026-06-03.

FastAPI backend for GaoshouPlatform. 后端负责 REST API、SQLite 元数据、Parquet/DuckDB 或 ClickHouse 行情读取、数据同步代理、因子/回测/交易服务。

## 启动

开发环境端口与生产环境必须隔离：

| 环境 | 后端 API | 同步服务 |
|---|---:|---:|
| dev | `18800` | `18810` |
| prod | `8800` | `8810` |

```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.\.venv\Scripts\activate

# 主 API
uvicorn app.main:app --host 127.0.0.1 --port 18800

# 同步服务，长任务队列独立运行
uvicorn app.sync_main:app --host 127.0.0.1 --port 18810
```

常用健康检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18800/health
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18800/api/system/status
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18800/api/system/data-summary
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18800/api/data/sync/status
```

## 关键 API

| 前缀 | 模块 | 说明 |
|---|---|---|
| `/api/system/*` | `app/api/system.py` | 系统状态、数据总览、健康检查、运行任务。 |
| `/api/data/*` | `app/api/data.py` | 股票、K 线、自选股、同步服务代理。 |
| `/api/explorer/*` | `app/api/data_explorer.py` | Parquet/DuckDB 或 ClickHouse 数据浏览。 |
| `/api/skill/*` | `app/api/data_skill.py` | DataSkill 统一数据接口。 |
| `/api/backtest/*` | `app/api/backtest.py` | 回测、AKQuant、优化、timer 覆盖。 |
| `/api/factor/*` | `app/api/factor.py` | 因子定义相关接口。 |
| `/api/factor-values/*` | `app/api/factor_values.py` | Factor Value Store、预计算、覆盖率、研报因子。 |
| `/api/grid-trading/*` | `app/api/grid_trading.py` | QMT/账户桥接、信号、订单预览、真实下单护栏。 |

## 同步状态契约

`GET /api/data/sync/status` 由主 API 代理同步服务 live status，并补充服务可用性与队列语义。

- `can_trigger=true` 表示同步服务可以接受新提交或排队，不表示当前没有任务运行。
- `status=running/queued` 且 `details.queue_mode=true` 时，前端应显示“运行中，可排队”或明确运行说明。
- 如果同步服务健康检查通过但 status proxy 失败，主 API 会降级为 `sync_service_available=false`、`can_trigger=false`，避免误导前端继续提交。
- 长任务应进入 `app/services/task_queue.py` 的 `data_sync` 队列串行执行。

相关测试：

```powershell
cd E:\Projects\GaoshouPlatform-dev
.\backend\.venv\Scripts\python.exe -m pytest backend\tests\api\test_sync_logs_fallback.py backend\tests\api\test_sync_queue.py -q
```

## 数据后端

默认开发口径：

```text
GAOSHOU_DATA_DIR=E:/Projects/data/BaiduSyncdisk
MARKET_DATA_BACKEND=parquet
CLICKHOUSE_ENABLED=false
PARQUET_DATA_DIR=E:/Projects/data/BaiduSyncdisk/parquet
DATABASE_URL=sqlite+aiosqlite:///E:/Projects/data/BaiduSyncdisk/gaoshou.db
DUCKDB_PATH=:memory:
```

查询行情、因子缓存或 timer 分钟线时优先通过 `get_market_data_store()` 访问抽象数据层，不要在业务代码里直接拼接 Parquet 路径或直接访问 ClickHouse。

## 安全约束

- 不要硬编码 token、账户、QMT 路径，使用 `.env.local` 或 `.env.<hostname>.local`。
- dev 默认使用 `E:\Projects\GaoshouPlatform-dev`、`18800/18810`；prod 默认使用 `E:\Projects\GaoshouPlatform-prod`、`8800/8810`。
- `GRID_TRADING_ENABLE_ORDER_SUBMIT=false` 是默认安全状态；真实下单需要显式开启并保留前端二次确认。
- xtquant 同步阻塞，所有 QMT 调用必须放入 executor 或 `asyncio.to_thread()`。
