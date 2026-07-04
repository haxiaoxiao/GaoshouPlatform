# GaoshouPlatform AI Native v2 计划与进度

更新时间：2026-07-04

## 当前小任务完成情况

已完成：在 `E:\Projects\GaoshouPlatform-AINative` 内安全重建 AI Native v2 基线。

- 已备份 AI Native v1 草稿到：`E:\Projects\GaoshouPlatform-AINative\.runtime\ainative-v1-backup-20260703-230551`
- 备份内容包含：`git-status-short.txt`、`tracked-diff.patch`、`untracked-files.txt`、`untracked-source/`
- 已从 `E:\Projects\GaoshouPlatform-prod` 本地 `main` 拉取最新基线到 AINative 工作区
- 当前 AINative 分支：`AINative`
- 当前基线提交：`81cc96c Refactor backtest pipeline and factor research flows`
- 当前工作区状态：干净

说明：PROD 本地 `main` 比 `origin/main` ahead 1，因此本轮按用户确认的“PROD 是最新环境”采用 PROD 本地 `main` 作为真实基线，而不是直接采用远端 `origin/main`。

## AI Native v2 总体实施计划

四层架构：

1. 模型网关层：统一 LLM Gateway，第一阶段内嵌 LiteLLM SDK。
2. 工具/数据标准层：typed tool registry，统一服务 Copilot、MCP、LangGraph。
3. 任务/证据层：复用 runtime tasks 和任务队列，为研报、回测、数据补全等长流程保存 artifact。
4. 交互/工作流层：全局 Copilot 抽屉先落地，LangGraph 后续承接复杂流程。

## 下一步实施顺序

1. 新增 LLM Gateway 配置与服务，迁移 `llm_strategy.py`、`report_to_strategy.py`。
2. 新增 AI tool registry，覆盖系统状态、数据总览、同步、股票快照、回测、研报策略等第一批工具。
3. 新增 `/api/ai/status`、`/api/ai/tools`、`/api/ai/chat`、`/api/ai/tools/{name}/execute`。
4. 新增 stdio MCP server：`python -m app.ai.mcp_server`。
5. 新增 AI artifact store，记录输入摘要、工具调用、结果链接和关键输出。
6. 前端新增全局 Copilot 抽屉，支持动作卡片、确认执行、任务入口和页面跳转。
7. 工具层稳定后再引入 LangGraph：`CommandGraph`、`ReportStrategyGraph`、`QuantResearchGraph`。

## 第一阶段落地进度（2026-07-04）

已完成第一阶段主干开发：

- 新增 `backend/app/ai/`：`LLMGateway`、typed tool registry、artifact store、stdio MCP server。
- 新增 AI 配置项：`AI_MODEL`、`AI_API_KEY_ENV`、timeout、temperature、max tokens 等，默认内嵌 LiteLLM SDK。
- 迁移 `llm_strategy.py`、`report_to_strategy.py` 到统一 `LLMGateway`，不再直接绑定 `Anthropic()`。
- 新增 SQLite `ai_artifacts` 表模型，记录 chat/tool 输入摘要、工具调用、结果链接、关键输出和错误。
- 新增 `/api/ai/status`、`/api/ai/tools`、`/api/ai/chat`、`/api/ai/tools/{name}/execute`、`/api/ai/artifacts`。
- 新增首批工具：`system.status`、`system.data_summary`、`runtime.tasks`、`data.stock_snapshot`、`data.sync_submit`、`backtest.submit`、`report.strategy_generate`、`strategy.convert_to_akquant`、`live_trading.status`。
- 写入型工具默认需要确认：数据同步、回测提交；实盘交易第一阶段只提供状态查询。
- 新增 `python -m app.ai.mcp_server`，通过同一个 tool registry 暴露 stdio MCP tools。
- 前端新增 `frontend/src/api/ai.ts` 和全局 `CopilotDrawer.vue`，并挂到 `MainLayout.vue` 顶栏。
- AI Native 版本左上角品牌位已区分为 `GAOSHOU AI`，logo 使用 `GS + AI + V2` 标记。
- 新增 AI Native 专用启动/停止脚本：
  - `tools/start-gaoshouplatform-ai-native.bat`
  - `tools/stop-gaoshouplatform-ai-native.bat`
  - 默认端口：后端 `18880`、同步服务 `18890`、前端 `13580`。
  - 脚本会设置 `VITE_APP_ENV_LABEL=V2`、`VITE_API_PROXY_TARGET=http://127.0.0.1:18880`，并检查 `/api/ai/status`。
  - `.env.local` 中普通 `BACKEND_PORT`/`FRONTEND_PORT` 不覆盖 AI 端口；需要覆盖时使用 `GAOSHOU_AI_BACKEND_PORT`、`GAOSHOU_AI_SYNC_PORT`、`GAOSHOU_AI_FRONTEND_PORT` 或 `AI_BACKEND_PORT`、`AI_SYNC_PORT`、`AI_FRONTEND_PORT`。
- 运维控制台新增 AI Gateway API Key 配置区，后端提供 `/api/ai/config` 读取/保存 `.env.local`，响应只返回 key 配置状态和掩码。

已验证：

- `python3.12 -m pytest tests/ai tests/api/test_ai_routes.py -q`
- `python3.12 -m ruff check app/ai app/api/ai.py app/db/models/ai.py app/services/llm_strategy.py app/services/report_to_strategy.py tests/ai tests/api/test_ai_routes.py`
- `python3.12 -c "from app.ai.mcp_server import create_mcp_server; create_mcp_server()"`
- `python3.12 -m pytest tests/services/test_task_queue.py tests/api/test_system_data_summary.py tests/api/test_factor_research_routes.py -q`
- `python3.12 -m pytest -q`：`418 passed`
- `cd frontend && npm run build`

## 当前风险与决策

- 不直接修改 `E:\Projects\GaoshouPlatform-prod`。
- 第一阶段不引入独立 LiteLLM Proxy、Langfuse、Promptfoo、LangSmith。
- MCP 第一阶段使用 stdio server，HTTP/SSE transport 后续再做。
- 实盘交易只做状态查询和强确认入口，不做自动下单 agent。
- LangGraph 不替代 tool registry，只编排复杂、有状态、需要确认/恢复的研究和回测流程。
