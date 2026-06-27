---
name: sentiment-analysis
description: Use when Codex needs to run, inspect, troubleshoot, or extend GaoshouPlatform's A-share sentiment analysis pipeline and crawlers, including NGA/flocktrader, Eastmoney Guba, Taoguba, Baidu Tieba, Laohu8, Jisilu, WeChat/Sogou, Xueqiu, source status/progress nodes, cookie or login setup, real-run smoke tests, symbol-level sentiment summaries, or new forum/social sentiment data sources.
---

# Sentiment Analysis

GaoshouPlatform 多源 A 股舆情分析与抓取 skill。用于把论坛、股吧、公众号搜索、雪球/NGA 等源接入统一缓存、汇总、状态节点和前端面板。

## Entry Points

- 后端主逻辑：`backend/app/services/sentiment.py`
- API 路由：`backend/app/api/sentiment.py`
- 前端 API：`frontend/src/api/sentiment.ts`
- 前端面板：`frontend/src/views/DataManage/SentimentPanel.vue`
- 测试：`backend/tests/services/test_sentiment_service.py`、`backend/tests/api/test_sentiment_routes.py`
- 配置：`backend/app/core/config.py`、`.env.example`

## Workflow

1. 先看 `git status --short` 和相关 diff；当前 GaoshouPlatform 开发临时在 prod 仓库，保留无关脏改。
2. 处理运行状态或空结果时，先查 `/api/sentiment/overview` 和 `/api/system/tasks/{task_id}`，区分“还在跑”“被验证码/cookie 卡住”“抓到 thread 但未匹配股票”“过滤参数太窄”。
3. 跑现有源时，优先用 `POST /api/sentiment/ingest/run`；需要定位代码问题时再直接调用 `SentimentIngestService`。
4. 新增或重构源时，沿用现有 source -> collect -> normalize -> upsert -> overview/status -> API/frontend/test 的链路。
5. 每个抓取源必须发出可观察状态节点：至少包含 `source`、`current_step`，并在适用时包含日期、页码、帖子 id/标题、query/bar/symbol 计数。
6. 改完按影响面验证：后端服务/API 测试必须跑；改前端源类型或面板时再跑前端 build。

## Guardrails

- 不接入新浪源；用户已经明确说“新浪算了吧”。
- 需要登录、cookie 或注册的源，检测后返回/记录 `verification_required` 或清晰错误，让用户提供账号/cookie；不要绕验证码。
- 优先使用公开 JSON、SSR HTML 或稳定列表页；解析时用结构化 HTML/API 解析，不做脆弱的单字符串拼接。
- 尽量同时保存 source-level threads 和 symbol-expanded posts，便于排查“源有内容但股票汇总为空”。
- 不提交 cookie、账号、浏览器 profile、缓存数据或本地运行产物。

## Reference

Read [references/gaoshou-sentiment.md](references/gaoshou-sentiment.md) when you need the source matrix, environment variables, API/direct-run snippets, empty-result diagnosis, or the full checklist for adding a new sentiment source.
