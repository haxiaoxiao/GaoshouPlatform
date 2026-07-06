# GaoshouPlatform AI Native v2 计划与进度

更新时间：2026-07-03

## 当前小任务完成情况

已完成：在 `E:\Projects\GaoshouPlatform-AINative` 内安全重建 AI Native v2 基线。

- 已备份 AI Native v1 草稿到：`E:\Projects\GaoshouPlatform-AINative\.runtime\ainative-v1-backup-20260703-230551`
- 备份内容包含：`git-status-short.txt`、`tracked-diff.patch`、`untracked-files.txt`、`untracked-source/`
- 已从 `E:\Projects\GaoshouPlatform-prod` 本地 `main` 拉取最新基线到 AINative 工作区
- 当前 AINative 分支：`codex/ainative-v2-from-prod`
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

## 当前风险与决策

- 不直接修改 `E:\Projects\GaoshouPlatform-prod`。
- 第一阶段不引入独立 LiteLLM Proxy、Langfuse、Promptfoo、LangSmith。
- MCP 第一阶段使用 stdio server，HTTP/SSE transport 后续再做。
- 实盘交易只做状态查询和强确认入口，不做自动下单 agent。
- LangGraph 不替代 tool registry，只编排复杂、有状态、需要确认/恢复的研究和回测流程。

