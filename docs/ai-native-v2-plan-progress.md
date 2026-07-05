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
- 新增 `/api/ai/manifest`，输出 Copilot、HTTP tools、stdio MCP 共用的工具契约、传输方式、风险策略和 per-tool schema。
- 新增 `/api/ai/diagnostics` 和 `system.ai_diagnostics` 工具，汇总模型网关、工具 manifest、最近 artifact、路由来源、答案模式和工具失败摘要。
- 新增 `/api/ai/workflows`、`/api/ai/workflows/{workflow_name}/run`，提供 `CommandGraph`、`ReportStrategyGraph`、`QuantResearchGraph` 三张可执行工作流图。
- 新增首批工具：`system.status`、`system.data_summary`、`runtime.tasks`、`data.stock_snapshot`、`data.sync_submit`、`backtest.submit`、`report.strategy_generate`、`strategy.convert_to_akquant`、`live_trading.status`。
- 当前 tool registry 已扩展到 193 个工具，覆盖系统、AI artifact 审计、工作流、数据、同步、行情、自选股、个股复盘上下文、AKShare 显式外部备用数据、Indicator、Compute、Evaluation、legacy/V2 因子、Factor Research、Parquet、Explorer、回测预设、回测报告、因子回测、策略/研报对话、舆情和实盘只读/强确认入口。
- 新增 `system.mcp_manifest` 工具，可由 Copilot/MCP client 读取同一份工具 manifest。
- 新增 `system.ai_diagnostics` 工具，Copilot/MCP client 可直接查询 AI Native 运行诊断。
- 新增 `workflow.catalog`、`workflow.run`、`workflow.command_graph`、`workflow.report_strategy_graph`、`workflow.quant_research_graph`，Graph 层复用同一个 typed tool registry 和 artifact 证据结构。
- 已接入官方 `langgraph` 依赖，`CommandGraph`、`ReportStrategyGraph`、`QuantResearchGraph` 的运行入口均通过 `StateGraph.compile().ainvoke()` 执行；原有 HTTP、MCP tool、artifact 和 confirmation 契约保持不变。
- Copilot 已加入 workflow intent promotion：研报策略请求优先走 `ReportStrategyGraph`，个股研究/走势解读优先走 `QuantResearchGraph`，多节点排查/命令优先走 `CommandGraph`。
- `CommandGraph` 内部会过滤 nested `workflow.*` 工具，避免 Graph 递归调用；写入/确认型工具仍停在 pending confirmation。
- 写入型工具默认需要确认；实盘交易第一阶段只提供状态查询、预检和强确认入口，不做自动下单 agent。
- 新增 `python -m app.ai.mcp_server`，通过同一个 tool registry 暴露 stdio MCP tools。
- Copilot 路由已升级为节点式链路：`ContextNode` 读取完整对话，`RouterNode` 用大模型选择工具，失败时回退本地 deterministic routing。
- 工具执行后的输出已升级为 ReAct-style `AnswerNode`，基于工具 observation 生成面向用户问题的投研/运维解读；前端只展示可审计路由摘要，不展示隐藏思维链。
- Copilot 默认 `auto_execute=true`，只读工具和只读 Graph 会直接执行并输出结果；当请求被显式设置为不自动执行或命中确认型工具时，后端使用最终 action plan 生成稳定的待执行说明，避免模型文本与实际工具计划不一致。
- `QuantResearchGraph` 已接入 Copilot 直接结果路径：个股走势类问题会执行股票快照、日 K、指标/因子目录等只读节点，再由 ReAct-style `AnswerNode` 生成面向客户问题的走势解读。
- async API 中的同步 LLM 调用已通过线程池隔离：Copilot RouterNode、AnswerNode、纯文本兜底和 `CommandGraph` 内部 RouterNode 不再阻塞 FastAPI 事件循环。
- Copilot RouterNode / AnswerNode 已加入分步超时保护：路由超时切本地 deterministic routing，答案生成超过 25 秒则返回本地结构化 observation 摘要；workflow 嵌套行情结果会展开股票快照、区间涨跌幅、高点回撤、最近一日变化、支撑/压力和成交额摘要。
- 前端新增 `frontend/src/api/ai.ts` 和全局 `CopilotDrawer.vue`，并挂到 `MainLayout.vue` 顶栏。
- AI Native 版本左上角品牌位已区分为 `GAOSHOU AI`，logo 使用 `GS + AI + V2` 标记。
- 新增 AI Native 专用启动/停止脚本，集中放在独立目录：
  - `tools/ai-native-startup/start-gaoshouplatform-ai-native.bat`
  - `tools/ai-native-startup/stop-gaoshouplatform-ai-native.bat`
  - `tools/start-gaoshouplatform-ai-native.bat` 和 `tools/stop-gaoshouplatform-ai-native.bat` 保留为兼容转发入口。
  - 默认端口：后端 `18880`、同步服务 `18890`、前端 `13580`。
  - 脚本会设置 `VITE_APP_ENV_LABEL=V2`、`VITE_API_PROXY_TARGET=http://127.0.0.1:18880`，并检查 `/api/ai/status`。
  - `.env.local` 中普通 `BACKEND_PORT`/`FRONTEND_PORT` 不覆盖 AI 端口；需要覆盖时使用 `GAOSHOU_AI_BACKEND_PORT`、`GAOSHOU_AI_SYNC_PORT`、`GAOSHOU_AI_FRONTEND_PORT` 或 `AI_BACKEND_PORT`、`AI_SYNC_PORT`、`AI_FRONTEND_PORT`。
- 运维控制台新增 AI Gateway API Key 配置区，后端提供 `/api/ai/config` 读取/保存 `.env.local`，响应只返回 key 配置状态和掩码。
- 运维控制台新增 AI 诊断读数：工具总数、工作流图数量、路由来源、答案模式、工具执行状态、近期失败和最近对话 artifact。
- 运维控制台新增 AI 工作流入口，展示三张 Graph、节点数、最近 workflow artifact，并提供 dry-run 预演按钮。
- `/api/ai/diagnostics` 现在采样所有 AI artifact（chat、tool、workflow），workflow/tool artifact 不会污染 chat 路由统计，但会计入 kind、工具调用、执行状态和失败摘要。
- 工具覆盖继续补齐平台全量能力：
  - 系统/任务：`runtime.task_detail`、`system.health`、`system.cache`。
  - Explorer：`explorer.table_schema`、`explorer.table_search`、`explorer.distinct_values`。
  - Compute：`compute.validate`、`compute.evaluate`、`compute.screen`。
  - Factor Value Store：`factor_value.param_hashes`、`factor_value.query`、`factor_value.paper_manifest`、`factor_value.paper_experiments`、`factor_value.paper_feature_snapshot`。
  - Backtest 辅助：`backtest.data_coverage`、`backtest.stock_names`。
  - 实盘只读辅助：`live_trading.strategy_profiles`、`live_trading.weekly_trades`。
- Copilot fallback router 已补齐上述新工具的自然语言命中规则，并对用户明确点名的具体工具加优先级，避免被泛化目录工具挤出 6 个 action 上限。
- 工具 wrapper 增加内部 API 错误上浮：当内部 API 返回 `code != 0` 时，AI tool 会明确返回 `status=error`，不再出现“工具 ok 但 result 里失败”的歧义。
- 第二批工具继续补齐“所有服务进 MCP”的平台面：
  - 运维/系统：`system.dev_data_mode`、`system.update_dev_data_mode`、`system.live_trading_guardrails`、`system.update_live_trading_guardrails`。
  - 数据同步：`data.sync_cancel`、`data.sync_cancel_all`。
  - Factor Value Store 长流程：`factor_value.precompute_prepare`、`factor_value.precompute`、`factor_value.group_precompute`。
  - 回测/优化：`backtest.engines`、`backtest.pool_symbols`、`backtest.task_cancel`、`backtest.optimize_grid`、`backtest.optimize_walk_forward`、`backtest.strategy_params_schema`、`backtest.strategy_params_validate`。
  - 舆情：`sentiment.threads`、`sentiment.ingest_run`。
- 第三批工具补齐因子研究与评估平台面：
  - Compute：`compute.precompute`、`compute.batch`。
  - Evaluation：`evaluation.ic_analysis`、`evaluation.quantile_backtest`、`evaluation.full_report`、`evaluation.report`、`evaluation.board`。
  - V2 因子：`factor.templates_v2`、`factor.validate_python`、`factor.validate_v2`、`factor.preview_saved`、`factor.precompute_saved`、`factor.coverage_saved`、`factor.analyze_saved`。
  - Factor Research：`factor_research.prepare`、`factor_research.submit`、`factor_research.batch`、`factor_research.combinations`。
- Copilot fallback router 已补齐第三批工具的自然语言命中规则：IC 分析、分层回测、完整评估、因子看板、V2 因子校验、保存因子预览/覆盖/预计算/分析、表达式预计算/批量计算、因子研究 prepare/submit/batch/combinations 都会优先路由到具体工具。
- 第四批工具补齐策略、传统回测和实盘强确认入口：
  - 策略库：`strategy.create`、`strategy.update`、`strategy.delete`。
  - 策略研究：`strategy.trend_signals_daily`、`strategy.trend_signals_summary`、`strategy.trend_backtest`、`strategy.deep_value_backtest`。
  - 传统回测记录：`backtest.create_record`、`backtest.run_record`、`backtest.delete_record`、`backtest.delete_records_batch`。
  - 实盘/模拟策略控制：`live_trading.account_initialize`、`live_trading.profile_create`、`live_trading.profile_update`、`live_trading.preflight`、`live_trading.signals`、`live_trading.runner_start`、`live_trading.runner_stop`、`live_trading.runner_takeover`。
  - 实盘/模拟订单入口：`live_trading.orders_sync`、`live_trading.orders_submit`、`live_trading.orders_cancel`、`live_trading.orders_cancel_resubmit`、`live_trading.orders_close_local`。
- 实盘订单类工具保留双保险：AI tool 本身需要确认，原 live API 的 `confirm=true` / `confirm_cancel=true` / `confirm_submit=true` 仍需在 payload 中显式给出；默认路由生成的订单参数保持 `confirm=false`。
- `live_trading.preflight` 和 `live_trading.signals` 已增加工具级超时保护，分别超过 25 秒和 60 秒时返回明确错误，避免 Copilot 对话长时间挂起。
- Copilot fallback router 已补齐第四批工具的自然语言命中规则：趋势资金信号/汇总/回测、深度价值回测、传统回测记录创建/运行/删除、实盘预检/信号/runner/profile/order 控制都会优先路由到具体工具。
- 上述写入/长任务/高风险工具默认需要确认；实盘仍只暴露状态和防护开关强确认入口，不增加自动下单 agent。
- 第五批工具补齐数据写入和因子管理平台面：
  - 自选股写入口：`data.watchlist_group_create`、`data.watchlist_group_delete`、`data.watchlist_stock_add`、`data.watchlist_stock_remove`。
  - Indicator 体系：`indicator.categories`、`indicator.description`、`indicator.query`、`indicator.compute`、`indicator.screen`、`indicator.financial`。
  - Legacy 因子：`factor.create_legacy`、`factor.update_legacy`、`factor.delete_legacy`、`factor.analysis_list`、`factor.analysis_detail`。
  - V2 保存因子：`factor.create_v2`、`factor.update_v2`、`factor.delete_v2`、`factor.evaluate_saved`。
- Copilot fallback router 已补齐第五批工具的自然语言命中规则，并修正 `factor_id` / `analysis_id` 解析顺序，避免 legacy/V2 因子更新、删除、分析详情类请求在本地路由阶段报错。
- 自选股、Indicator compute、legacy/V2 因子写入和保存因子评估默认需要确认；读取型 Indicator 查询、筛选和分析详情可直接返回结果。
- 第六批工具补齐剩余高价值平台面：
  - 个股复盘：`data.review_context`，复用 DataSkill review context，为“直接出结果”的投研回答提供更完整上下文。
  - Parquet 数据湖：`parquet.dataset_schema`，读取数据集列名和 DuckDB 推断类型。
  - 回测预设：`backtest.preset_dual_stock_grid` 读取内置双标的底仓网格策略预设；`backtest.create_preset_dual_stock_grid_strategy`、`backtest.create_preset_multi_factor_strategy`、`backtest.create_preset_tech_small_cap_strategy` 将内置策略写入策略库。
- Copilot fallback router 已补齐第六批工具自然语言命中规则：复盘/投研上下文、Parquet schema/数据集字段、双标的网格预设、写入通用多因子/科技小市值内置策略都会优先路由到具体工具。
- 内置策略写入入口默认需要确认；预设读取、复盘上下文和 Parquet schema 为只读工具，可直接执行。
- 第七批工具补齐审计、报告和研报对话缺口：
  - AI artifact 审计：`system.ai_artifacts`、`system.ai_artifact_detail`，用于查看最近对话、工具调用、工作流证据和单条 artifact 明细。
  - 回测补齐：`backtest.factor` 运行 FactorConfig/BtConfig 因子分层回测；`backtest.task_report` 读取异步回测 QuantStats HTML 报告摘要和链接。
  - 研报对话：`report.chat_session_create`、`report.chat_session_send`，复用现有 LLM strategy session 服务，支持基于研报文本创建策略对话并继续追问。
- Copilot fallback router 已补齐第七批工具自然语言命中规则：对话记录/artifact 详情、因子回测、回测报告、创建/继续研报对话都会优先路由到具体工具；artifact id 解析已收紧，避免把 `artifacts` 或“对话记录”误识别为具体 id。
- 回测报告、因子回测和 artifact 审计为只读工具；研报对话工具会调用模型并产生会话状态，但不触发交易或策略库写入。
- 第八批工具补齐 AKShare 独立 API 服务面：
  - `akshare.stock_daily`、`akshare.stock_daily_batch`、`akshare.stock_spot`、`akshare.stock_list`、`akshare.stock_info`、`akshare.stock_hist`。
  - 这些工具只作为“显式外部备用数据源”暴露到 HTTP/MCP；Copilot fallback 只有在用户明确写出 `AKShare` / `akshare` / `AK 数据` 时才路由到 `akshare.*`。
  - 普通行情、K 线、回测数据请求仍优先走本地 DataSkill、Parquet/DuckDB/QMT，不把 AKShare 作为默认替代源。
- 第九批工具补齐原生平台查询缺口：
  - 数据查询：`data.stock_list` 包装 `/api/data/stocks` 分页股票列表，`data.klines_query` 包装 `/api/data/klines` 通用日线/分钟 K 线分页查询。
  - 指标查询：`data.indicator_timeseries_batch` 包装 `/api/skill/indicator/timeseries/batch`，支持多股票多指标时序批量读取。
  - 保存 Python 因子：`factor.run_python_saved` 包装 `/api/factors/{factor_id}/run-python`，返回区间因子值且不写缓存；同时补齐 `FactorPreviewRequest.params`，让 Python 因子运行参数能从 Copilot/MCP 传入。
  - Copilot fallback router 已补齐第九批工具自然语言命中规则，且这批只读工具会直接执行并进入 ReAct-style AnswerNode。
- 第十批工具补齐平台详情口径和只读明细路由：
  - `data.stock_detail` 包装 `/api/data/stocks/{symbol}`，读取前端股票详情页使用的状态、估值、财务和行业字段。
  - `backtest.index_pool_detail` 包装 `/api/backtest/index-pools/{index_symbol}`，按日期区间读取回测指数池成分覆盖、快照数量和股票列表。
  - Copilot fallback router 新增股票详情、回测指数池详情、实盘待处理委托、订单审计和成交记录的自然语言命中规则；后三者复用既有只读工具，不扩大实盘自动执行范围。

已验证：

- `python3.12 -m pytest tests/ai tests/api/test_ai_routes.py -q`
- `python3.12 -m pytest tests/api/test_ai_routes.py tests/ai -q`
- `python3.12 -m ruff check app/ai app/api/ai.py app/db/models/ai.py app/services/llm_strategy.py app/services/report_to_strategy.py tests/ai tests/api/test_ai_routes.py`
- `python3.12 -m ruff check app/ai app/api/ai.py tests/api/test_ai_routes.py tests/ai`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m ruff check app/ai app/api/ai.py tests/api/test_ai_routes.py tests/ai`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m ruff check app/ai app/api/ai.py app/api/indicator.py app/models/factor.py tests/api/test_ai_routes.py tests/ai`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m ruff check app/ai app/api/ai.py app/api/factors.py app/api/indicator.py app/models/factor.py tests/api/test_ai_routes.py tests/ai`：`All checks passed`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest tests/api/test_ai_routes.py tests/ai -q`：`26 passed`
- `python -c "from langgraph.graph import StateGraph"`：官方 LangGraph 依赖可导入；`get_compiled_langgraph_workflow("CommandGraph")` 返回 `CompiledStateGraph`。
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest tests/api/test_ai_routes.py tests/ai -q`：`27 passed`，覆盖 `CompiledStateGraph` 和 workflow `graph_runtime=langgraph`。
- `cd frontend && npm run build`
- 上一轮运行态烟测已确认：`system.health` 和 `compute.validate` 执行成功，`explorer.table_schema(klines_daily)` 返回 10 列，缺失表 `definitely_missing_table` 明确返回 `status=error`。
- 运行态烟测：`GET /api/ai/status` 返回 `tool_count=108`；`system.live_trading_guardrails`、`backtest.engines`、`sentiment.threads` 执行成功；`factor_value.precompute`、`backtest.optimize_grid`、`system.update_live_trading_guardrails` 在未确认时返回 `needs_confirmation`；HTTP manifest 和 `python -m app.ai.mcp_server` 共用 registry，均可看到 108 个工具。
- 第三批运行态烟测：`GET /api/ai/status` 返回 `tool_count=126`；`GET /api/ai/manifest` 返回 126 个 HTTP/MCP 共用工具、18 个确认型工具；`factor.validate_v2` 执行成功；`factor.templates_v2` 执行成功；`compute.precompute` 未确认时返回 `needs_confirmation`；`create_mcp_server().list_tools()` 返回 126 个工具，包含 `factor_research.submit` 和 `evaluation.board`。
- 最新运行态烟测：`GET /api/ai/status` 返回 `tool_count=150`；`GET /api/ai/manifest` 返回 150 个 HTTP/MCP 共用工具、39 个确认型工具，风险分布为 read=110、write=32、danger=8；manifest 包含 `strategy.trend_signals_daily` 和 `live_trading.orders_submit`；`live_trading.orders_submit` 未确认时返回 `needs_confirmation`；`create_mcp_server().list_tools()` 返回 150 个工具，包含 `live_trading.orders_submit` 和 `strategy.deep_value_backtest`。
- 最新本地 registry 检查：`get_ai_tool_registry().list()` 返回 169 个工具；`data.watchlist_stock_add`、`indicator.compute`、`factor.create_v2`、`factor.evaluate_saved` 均为确认型写入入口。
- 最新临时运行态烟测：`GET /api/ai/status` 和 `GET /api/ai/manifest` 均返回 169 个工具，风险分布为 read=117、write=44、danger=8，51 个工具需要确认；`data.watchlist_stock_add` 和 `factor.evaluate_saved` 未确认时返回 `needs_confirmation`；`create_mcp_server().list_tools()` 返回 169 个工具，包含 `data.watchlist_stock_add`、`indicator.query`、`indicator.compute`、`factor.create_v2`、`factor.evaluate_saved`。
- 最新本地 registry 检查：`get_ai_tool_registry().list()` 返回 175 个工具；`data.review_context`、`parquet.dataset_schema`、`backtest.preset_dual_stock_grid` 为只读入口，`backtest.create_preset_multi_factor_strategy` 为确认型写入入口。
- 最新临时运行态烟测：`GET /api/ai/status` 和 `GET /api/ai/manifest` 均返回 175 个工具，风险分布为 read=120、write=47、danger=8，54 个工具需要确认；`backtest.create_preset_multi_factor_strategy` 未确认时返回 `needs_confirmation`；`create_mcp_server().list_tools()` 返回 175 个工具，包含 `data.review_context`、`parquet.dataset_schema`、`backtest.preset_dual_stock_grid`、`backtest.create_preset_multi_factor_strategy`。
- 最新本地 registry 检查：`get_ai_tool_registry().list()` 返回 181 个工具；`system.ai_artifacts`、`system.ai_artifact_detail`、`backtest.factor`、`backtest.task_report` 为只读入口，`report.chat_session_create`、`report.chat_session_send` 为研报对话入口。
- 最新临时运行态烟测：`GET /api/ai/status` 和 `GET /api/ai/manifest` 均返回 181 个工具，风险分布为 read=124、write=49、danger=8，54 个工具需要确认；`create_mcp_server().list_tools()` 返回 181 个工具，包含 `system.ai_artifacts`、`system.ai_artifact_detail`、`backtest.factor`、`backtest.task_report`、`report.chat_session_create`、`report.chat_session_send`；`system.ai_artifacts` HTTP tool 执行返回 `status=ok`。
- 第八批本地 registry 检查：`get_ai_tool_registry().list()` 返回 187 个工具；`akshare.stock_daily`、`akshare.stock_daily_batch`、`akshare.stock_spot`、`akshare.stock_list`、`akshare.stock_info`、`akshare.stock_hist` 均为只读入口。
- 第八批临时运行态烟测：`GET /api/ai/status` 和 `GET /api/ai/manifest` 均返回 187 个工具，风险分布为 read=130、write=49、danger=8，54 个工具需要确认；`create_mcp_server().list_tools()` 返回 187 个工具，包含全部 6 个 `akshare.*` 显式外部数据工具。
- 第九批本地 registry 检查：`get_ai_tool_registry().list()` 返回 191 个工具；`data.stock_list`、`data.klines_query`、`data.indicator_timeseries_batch`、`factor.run_python_saved` 均为只读入口。
- 第九批临时运行态烟测：`GET /api/ai/status`、`GET /api/ai/manifest` 和 `create_mcp_server().list_tools()` 均返回 191 个工具，包含 `data.stock_list`、`data.klines_query`、`data.indicator_timeseries_batch`、`factor.run_python_saved`；烟测结束后 18880 无残留监听。
- 最新本地 registry 检查：`get_ai_tool_registry().list()` 返回 193 个工具；`data.stock_detail`、`backtest.index_pool_detail` 均为只读入口。
- 最新临时运行态烟测：`GET /api/ai/status`、`GET /api/ai/manifest` 和 `create_mcp_server().list_tools()` 均返回 193 个工具，包含 `data.stock_detail`、`backtest.index_pool_detail`；烟测结束后 18880 无残留监听。
- 最新 LangGraph 运行态烟测：临时启动 18880，调用 `POST /api/ai/workflows/CommandGraph/run` dry-run 返回 `graph_runtime=langgraph`、`compiled_graph=CompiledStateGraph`、`workflow_status=planned`；烟测结束后 18880 无残留监听。
- `python3.12 -c "from app.ai.mcp_server import create_mcp_server; create_mcp_server()"`
- `python3.12 -m pytest tests/services/test_task_queue.py tests/api/test_system_data_summary.py tests/api/test_factor_research_routes.py -q`
- `python3.12 -m pytest -q`：`418 passed`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest tests/test_factor_models.py tests/test_factor_validator.py tests/api/test_evaluation_api.py tests/indicators/test_scheduler_gating.py -q`：`26 passed`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest -q`：`438 passed`
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest -q`：`438 passed`（本轮仅保留既有 Pydantic deprecation 和 aiosqlite 线程退出 warning）
- `/Users/albert/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest -q`：`439 passed`
- `cd frontend && npm run build`

## 当前风险与决策

- 不直接修改 `E:\Projects\GaoshouPlatform-prod`。
- 第一阶段不引入独立 LiteLLM Proxy、Langfuse、Promptfoo、LangSmith。
- MCP 第一阶段使用 stdio server，HTTP/SSE transport 后续再做。
- 实盘交易只做状态查询和强确认入口，不做自动下单 agent。
- LangGraph 不替代 tool registry，只编排复杂、有状态、需要确认/恢复的研究和回测流程。
