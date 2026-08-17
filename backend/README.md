# GaoshouPlatform Backend

Last updated: 2026-07-17.

FastAPI backend for GaoshouPlatform. 后端负责 REST API、SQLite 元数据、Parquet/DuckDB 行情读取、数据同步代理、因子/回测/交易服务。

## 启动

开发环境端口与生产环境必须隔离：

| 环境 | 后端 API | 同步服务 |
|---|---:|---:|
| dev | `18800` | `18810` |
| prod | `8800` | `8810` |

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\activate

# 主 API
uvicorn app.main:app --host 127.0.0.1 --port 8800

# 同步服务，长任务队列独立运行
uvicorn app.sync_main:app --host 127.0.0.1 --port 8810
```

常用健康检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8800/health
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8800/api/system/status
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8800/api/system/data-summary
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8800/api/data/sync/status
```

## 关键 API

| 前缀 | 模块 | 说明 |
|---|---|---|
| `/api/system/*` | `app/api/system.py` | 系统状态、数据总览、健康检查、运行任务。 |
| `/api/data/*` | `app/api/data.py` | 股票、K 线、自选股、同步服务代理。 |
| `/api/explorer/*` | `app/api/data_explorer.py` | Parquet/DuckDB 结构化浏览；不接受任意 SQL 或自由文本 WHERE。 |
| `/api/skill/*` | `app/api/data_skill.py` | DataSkill 统一数据接口。 |
| `/api/backtest/*` | `app/api/backtest.py` | 回测、AKQuant、优化、timer 覆盖。 |
| `/api/factor/*` | `app/api/factor.py` | 因子定义相关接口。 |
| `/api/factor-values/*` | `app/api/factor_values.py` | Factor Value Store、预计算、覆盖率、研报因子。 |
| `/api/live-trading/*` | `app/api/live_trading.py` | 状态、模拟、信号和审计；旧真实提交端点固定返回 410。 |
| `/api/v1/*` | `app/api/v1.py` | 数据快照、发布、受控回测和唯一真实下单入口。 |

## 同步状态契约

`GET /api/data/sync/status` 由主 API 代理同步服务 live status，并补充服务可用性与队列语义。

- `can_trigger=true` 表示同步服务可以接受新提交或排队，不表示当前没有任务运行。
- `status=running/queued` 且 `details.queue_mode=true` 时，前端应显示“运行中，可排队”或明确运行说明。
- 如果同步服务健康检查通过但 status proxy 失败，主 API 会降级为 `sync_service_available=false`、`can_trigger=false`，避免误导前端继续提交。
- 主 API、同步服务和调度器的写入任务都进入 `app/services/task_queue.py` 中名为 `sync` 的单 worker FIFO。

相关测试：

```powershell
cd E:\Projects\GaoshouPlatform-prod
.\backend\.venv\Scripts\python.exe -m pytest backend\tests\api\test_sync_logs_fallback.py backend\tests\api\test_sync_queue.py -q
```

## 数据后端

当前唯一支持口径：

```text
GAOSHOU_DATA_DIR=E:/Projects/data/BaiduSyncdisk
MARKET_DATA_BACKEND=parquet
PARQUET_DATA_DIR=E:/Projects/data/BaiduSyncdisk/parquet
DATABASE_URL=sqlite+aiosqlite:///E:/Projects/data/BaiduSyncdisk/gaoshou.db
DUCKDB_PATH=:memory:
```

非 `parquet` 值会被启动器拒绝或忽略。查询行情、因子缓存或 timer 分钟线时通过 `get_market_data_store()` 访问抽象数据层，不要在业务代码里直接拼接 Parquet 路径。

## 依赖与验证

Python 要求 3.12+。`pyproject.toml` 是依赖和工具配置的唯一事实来源，`requirements.txt` 仅保留 `-e .[dev]` 兼容入口。

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\ruff.exe check app tests
.\.venv\Scripts\python.exe -m pytest tests -q
```

## 安全约束

- 不要硬编码 token、账户、QMT 路径，使用 `.env.local` 或 `.env.<hostname>.local`。
- dev 默认使用 `E:\Projects\GaoshouPlatform-dev`、`18800/18810`；prod 默认使用 `E:\Projects\GaoshouPlatform-prod`、`8800/8810`。
- `LIVE_TRADING_ENABLE_ORDER_SUBMIT=false` 和 `LIVE_TRADING_AUTO_EXECUTE_ENABLED=false` 是默认安全状态。
- 真实订单仅允许 `POST /api/v1/live/orders/submit`，并要求 `live_approved` release、control session、预期账户掩码和 idempotency key；不要恢复旧端点或添加旁路。
- xtquant 同步阻塞，所有 QMT 调用必须放入 executor 或 `asyncio.to_thread()`。
