# Agent.md — 外部 Agent 快速读本

> 这个文件给不会自动读取 `AGENTS.md` 的外部 agent / 工具使用。完整且权威的规则仍以 `AGENTS.md` 为准；开始任何代码修改前请先阅读它。

## 绝对边界

- `E:\Projects\GaoshouPlatform-prod` 是生产仓库：默认只读，只做验证、日志、健康检查，以及用户明确要求的数据清理。
- `E:\Projects\GaoshouPlatform-dev` 是开发仓库：代码、脚本、测试、依赖、文档变更默认都在这里实施。
- 如果当前目录在 prod，但任务涉及代码/配置/依赖/测试变更，先切到 dev；不要擅自把 dev 改动同步到 prod。
- 不要回滚、覆盖、删除用户或系统已有变更；动手前先看 `git status --short`。

## 工作原则

- 先研究、再复用：优先沿用项目已有服务、工具、脚本和风格，不为小事引入新抽象。
- 修根因、查同类：修完一个问题后，搜索项目里是否有类似问题。
- 安全默认：不要硬编码密钥；敏感配置走环境变量或 `.env.local`，并确保被 `.gitignore` 保护。
- 输入防御：显式校验参数、处理边界和错误，不做静默失败。
- 依赖克制：优先标准库和项目现有依赖，除非必要不要新增依赖。

## 项目速览

```text
华泰 miniQMT / xtquant
  → SQLite + DuckDB/Parquet 或 ClickHouse
  → FastAPI 后端
  → Vue 3 + TypeScript + Element Plus 前端
  → DataSkill / 因子计算 / 策略回测 / 实盘交易
```

- 后端：Python 3.12+、FastAPI、SQLAlchemy async、SQLite 元数据库。
- 默认行情后端：`MARKET_DATA_BACKEND=parquet`，通过 DuckDB 查询公共目录 `E:\Projects\Data\parquet`。
- 可选行情后端：ClickHouse，主要用于大规模高并发查询。
- 前端：Vue 3、TypeScript、Vite、Element Plus、深色主题。

## 数据访问规则

- 策略和因子开发优先使用 `DataSkill`：`backend/app/services/data_skill.py`。
- 行情数据查询使用 `get_market_data_store()`，不要在业务层直接调用 `get_ch_client()`。
- SQLite 访问使用 `get_async_session()` 依赖注入；写入优先使用 upsert 模式。
- 指数历史成分使用 `backend/app/services/index_components.py`，按 `trade_date <= as_of` 最近快照取 point-in-time 成分。

## xtquant 注意事项

- 不要使用 `download_financial_data`，它可能在 miniQMT 上无限阻塞；使用 `download_financial_data2(callback=None)`。
- 所有 xtquant 调用都是同步阻塞的，异步代码中必须用 `asyncio.get_running_loop().run_in_executor()` 包装。
- `get_financial_data` 返回 DataFrame；`m_timetag` 是字符串，比较时用 `'20240331'` 这样的字符串。
- 不要用 AKShare 替代 xtquant；这是项目明确约束。
- 策略运行时不要反复读 QMT 分钟线，应先同步到 Parquet/ClickHouse，再由回测读取本地列式数据。

## AKQuant / 回测规则

- `engine="akquant"` 是事件驱动回测主路径之一，支持 `bar_type="daily"`、`minute`、`minute_timer`。
- 回测日期、资金、费用、滑点、股票池、bar type、timer times 必须由前端/API payload 控制。
- 策略代码读取 `strategy_params`，不要硬编码日期、资金、股票池或固定 timer。
- 用户选择指数池时传 `index_symbol`，不要把当前自选股静态展开成历史股票池。
- ID=43 小市值策略应使用 `399101.SZ` 的历史成分快照；固定盘中时点策略优先使用 `minute_timer`。

## 常用文档入口

- `README.md`：项目入口、启动方式、AKQuant 回测入口、ID=43 推荐流程。
- `docs/user-manual.md`：平台使用和策略回测操作手册。
- `docs/data-source-cheatsheet.md`：miniQMT / Tushare / AKShare 数据源小抄。
- `docs/akquant-integration-todo.md`：AKQuant 集成状态和验证命令。
- `docs/factor-value-store.md`：Factor Value Store 设计、覆盖率、预计算和 ID=43 接入。
- `docs/small-cap-jq-alignment-notes.md`：ID=43 小市值策略与聚宽对齐记录。

## 开发端口

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

## 验证建议

- 修改 `sync_service.py` 后至少跑语法检查：`python -c "import ast; ast.parse(open('sync_service.py', encoding='utf-8').read())"`。
- 修改 `qmt_gateway.py` 后做语法检查，尤其注意 class 方法 4 空格缩进。
- 修改 AKQuant 集成后至少跑：`.\backend\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q`。
- 修改前端后至少考虑跑：`cd frontend; npm run build`。
- 完成任务后按影响范围重启 dev 模块；只改文档通常无需重启。

## 外部 Agent 开工清单

1. 确认当前仓库是 `E:\Projects\GaoshouPlatform-dev`。
2. 运行 `git status --short`，识别并保护已有用户变更。
3. 阅读 `AGENTS.md` 和任务相关文档/代码。
4. 在 dev 中做最小必要修改，避免触碰 prod。
5. 跑与变更范围匹配的最小验证。
6. 交付时说明改了哪些文件、验证结果、是否需要重启。
