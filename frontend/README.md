# GaoshouPlatform 前端

Last updated: 2026-06-03.

前端使用 Vue 3 + TypeScript + Vite + Element Plus，默认通过 Vite dev server 访问后端 API。当前 UI 采用低饱和深色量化终端风格，并区分“投研决策页”和“工程运维页”。

## 启动

```powershell
cd E:\Projects\GaoshouPlatform-dev\frontend
npm install
npm run dev -- --host 127.0.0.1 --port 13500 --strictPort
```

环境端口必须严格分开：

| 环境 | 前端 | 后端 API | 同步服务 |
|---|---:|---:|---:|
| dev | `13500` | `18800` | `18810` |
| prod | `3500` | `8800` | `8810` |

实际访问地址以启动脚本输出为准。

## 主要页面

| 路由 | 用途 |
|---|---|
| `/home` | 今日投研工作台：研究就绪度、今日行动建议、投研输入口径和流水线推进 |
| `/data` | 数据查看：最新日线、分钟线、基础股票、财务、指标、概念和舆情口径 |
| `/data/sync` | 数据同步：任务目录、预设方案、执行参数、队列、运行进度和最近同步记录 |
| `/explorer` | 本地 Parquet / ClickHouse 数据浏览 |
| `/watchlist` | 自选股与研究股票池 |
| `/factor` | 因子定义：因子目录、覆盖率、参数版本和预计算入口 |
| `/factor/detail/:factorName` | 因子详情、公式、人话解释和研究结果 |
| `/factor/evaluation` | 因子评估：IC、ICIR、多空收益、回撤、换手和已计算组合 |
| `/research` | 研究实验室：假设、证据链、外部链接、实验记录和复盘 |
| `/backtest` | 策略回测和 AKQuant 入口 |
| `/trade` | 模拟 / 实盘：信号、账户、订单预览和真实下单护栏 |
| `/monitor` | 系统运维控制台：服务拓扑、值班排障、任务表、存储巡检、同步审计 |
| `/docs` | 文档中心 |

## 页面职责

- `/home` 面向投研决策，只回答“今天先做什么”：数据是否可研究、同步是否影响判断、是否需要先跑基准回测、交易护栏是否开启。
- `/monitor` 面向工程运维，只回答“哪里出问题”：后端、同步服务、存储、QMT、订单护栏、队列和任务日志。
- `/data` 只做数据查看，不塞同步表单；同步入口集中到 `/data/sync`。
- `/factor` 与 `/factor/evaluation` 分开：定义页管理因子和预计算，评估页消费已落库缓存。

## 同步页语义

- `GET /api/data/sync/status` 的 `can_trigger=true` 表示同步服务可接受新提交或排队，不表示当前没有任务运行。
- 前端右侧上下文显示为“提交入口 / 运行说明”；当 `status=running` 时会明确提示当前执行任务和排队状态。
- 顶部“一键同步核心数据”会跟随同步状态禁用，避免同步运行中再次误触发核心任务。

## 因子研究注意事项

- 因子值缓存统一使用 `/api/factor-values/*`。
- Alpha101 101 个公式支持宽表批量预计算。
- 看板排序依赖已保存的研究结果；如果公式或缓存口径变更，需要重新预计算并重新跑研究。
- 覆盖率低的因子不要直接和高覆盖率因子比较 IC。
- 因子表达式只在创建或编辑时打开；普通定义/评估流程优先使用表格、覆盖率和报告结果。

## 视觉语义

- 平台状态：红色表示需要关注/异常，绿色表示正常/就绪。
- A 股行情：上涨使用红色，下跌使用绿色。
- 表格和代码区优先高信息密度，避免强装饰光效。

## 验证

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

Vite chunk size warning 是当前已知的非阻塞构建警告。
