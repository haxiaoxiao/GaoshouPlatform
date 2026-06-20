# AKQuant 集成状态

本文只记录当前仍有效的 AKQuant 集成状态和后续事项。历史执行计划已归档到 `docs/archive/`。

Last updated: 2026-06-20.

## 当前状态

| 能力 | 状态 | 入口 |
|---|---|---|
| 事件驱动回测 | 已接入 | `POST /api/backtest/run`, `engine="akquant"` |
| 能力检测 | 已接入 | `GET /api/backtest/capabilities` |
| 日线回测 | 已接入 | `bar_type="daily"` |
| 完整分钟线回测 | 已接入 | `bar_type="minute"` |
| 固定时点分钟线 | 已接入 | `bar_type="minute_timer"` |
| Grid Search | 已接入 | `POST /api/backtest/optimize/grid` |
| Walk-forward Validation | 已接入 | `POST /api/backtest/optimize/walk-forward` |
| 策略参数 schema/validate | 已接入 | `/api/backtest/strategy-params/*` |
| AKQuant Polars 因子计算 | 已接入 | `POST /api/compute/evaluate`, `engine="akquant"` |
| 因子预计算 | 已接入 | `POST /api/compute/precompute` |
| 优化结果持久化 | 已接入 | `backtests.record_type="optimization"` |
| Parquet/DuckDB 默认行情后端 | 已接入 | `MARKET_DATA_BACKEND=parquet` |
| 因子值缓存 | 已接入 | `/api/factor-values/*` |
| Alpha101 宽表预计算 | 已接入 | `group_name=alpha101` |

## 当前原则

- `engine="akquant"` 继续作为事件驱动策略回测的主路径之一。
- 回测日期、股票池、资金、费用、滑点、bar type、timer times 必须由前端控制面板或 API payload 传入，策略代码读取 `strategy_params`。
- 只需要固定盘中时点的策略优先使用 `minute_timer`，避免读取完整分钟线。
- 指数池通过 `index_symbol` 传递，不能把当前成分静态展开成历史股票池。
- `MARKET_DATA_BACKEND=parquet` 是当前支持模式；不要再新增外部列式数据库依赖。
- 因子研究优先使用 Factor Value Store；Alpha101 批量预计算已经走宽表实现，研究结果应在缓存重算后再比较。

## 仍需跟进

| 优先级 | 事项 | 说明 |
|---|---|---|
| P1 | 动态参数表单 | 前端基于 `strategy-params/schema` 自动生成更完整的策略参数表单 |
| P1 | 风控配置 UI | 暴露 AKQuant 风控参数和组合约束 |
| P2 | AKQuant/JQ 语义对齐报告 | 对比成交时点、涨跌停、停牌、ST、退市、行业集中度等规则 |
| P2 | 年度 debug 前端页 | 后端已有 yearly debug API，前端对比页仍需产品化 |
| P3 | ML 工作流入口 | Walk-forward 训练、模型注册、预测缓存 |
| P3 | 实盘 runner 规划 | AKQuant live runner 与 miniQMT 执行网关的边界设计 |

## 验证命令

修改 AKQuant 回测、优化、数据 provider 或策略参数相关代码后至少运行：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
```

修改前端回测页面后运行：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

## 相关文档

| 文档 | 用途 |
|---|---|
| `docs/user-manual.md` | 面向平台使用和策略回测操作 |
| `docs/data-source-cheatsheet.md` | 数据源优先级和适用场景 |
| `docs/local-data-onboarding.md` | 本地 SQLite + Parquet/DuckDB 接入 |
| `docs/factor-value-store.md` | 因子值缓存和预计算 |
