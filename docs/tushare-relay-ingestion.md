# Tushare Relay 数据接入

Last updated: 2026-06-16.

## 当前落地状态

平台已新增 `sync_type="tushare_relay"`，用于把 Indevs Tushare Relay 的高价值结构化数据先落到本地 Parquet。默认限速为 `1 req/s`，遇到超时、429、5xx、`relay_pending` 会指数退避并切换 base URL。

配置项只从环境变量或 `.env.local` 读取：

```powershell
INDEVS_TUSHARE_API_KEY=<your-key>
INDEVS_TUSHARE_BASE_URLS=http://127.0.0.1:8000/tushare/pro,https://ai-tool.indevs.in/tushare/pro,https://tushare.indevs.in/tushare/pro
INDEVS_TUSHARE_RPS=1
INDEVS_TUSHARE_TIMEOUT_SECONDS=30
```

## 第一批数据集

| Relay dataset | API | Parquet dataset | 分区日期列 | 说明 |
|---|---|---|---|---|
| `adj_factor` | `adj_factor` | `adj_factors` | `trade_date` | 复权因子，作为基础数据，不直接作为排序因子 |
| `moneyflow` | `moneyflow` | `moneyflow` | `trade_date` | 个股资金流，并派生 `net_mf_amount`、`net_mf_vol` |
| `ths_index` | `ths_index` | `ths_index` | `snapshot_date` | 同花顺板块字典快照 |
| `ths_member` | `ths_member` | `ths_member` | `snapshot_date` | 同花顺板块成分快照 |
| `block_moneyflow` | `block_moneyflow` | `block_moneyflow` | `trade_date` | 板块资金流，默认小 limit |
| `stk_auction_replay` | `stk_auction_replay` | `auction_replay` | `datetime` | 集合竞价摘要/序列，独立于普通分钟线 |

`dividend` 暂不接入 Relay，因为平台已有 QMT 分红同步，避免口径重复。

## 分析师与研报数据

2026-06-15 已接入分析师/研报 Relay 数据集，仍然先落本地 Parquet，因子计算和回测只读本地数据。

| Relay dataset | API | Parquet dataset | 分区日期列 | 同步方式 |
|---|---|---|---|---|
| `report_rc` | `report_rc` | `analyst_report_forecasts` | `report_date` | 按 `symbols` + `start_date/end_date` 查询卖方盈利预测，单次 `report_rc_limit` 默认 100 |
| `analyst_rank` | `analyst_rank` | `analyst_rank` | `update_date` | 按年份查询，年份来自同步日期区间或 `relay_options.analyst_years` |
| `analyst_detail` | `analyst_detail` | `analyst_detail` | `latest_rating_date` | 先从 `analyst_rank` 取 `analyst_id`，再按 `indicator=最新跟踪成分股` 下钻 |
| `analyst_history` | `analyst_history` | `analyst_history` | `in_date` | 先从 `analyst_rank` 取 `analyst_id`，再按 `indicator=历史跟踪成分股` 下钻 |
| `stock_research_report_em` | `stock_research_report_em` | `research_reports` | `report_date` | 按股票和日期同步研报标题、评级、机构和 PDF 链接 |

示例：

```json
POST /api/data/sync
{
  "sync_type": "tushare_relay",
  "relay_datasets": ["report_rc", "analyst_rank", "analyst_history", "stock_research_report_em"],
  "symbols": ["000001.SZ"],
  "start_date": "2026-04-01",
  "end_date": "2026-04-30",
  "relay_options": {
    "report_rc_limit": 100,
    "analyst_limit": 50,
    "analyst_rank_limit": 50,
    "daily_limit": 200,
    "allow_text_sources": true
  }
}
```

注意：
- `report_rc` 回源慢且可能超时，建议小股票池、低频增量同步，并依赖本地 Parquet 缓存。
- `analyst_detail` / `analyst_history` 必须有 `analyst_id`；平台默认先同步 `analyst_rank` 并抽取前 `analyst_limit` 个分析师下钻。
- 如需指定分析师，可传 `relay_options.analyst_ids`，例如 `"11000213851,11000455635"`。
- `stock_research_report_em` 属于文本源，仍需要 `allow_text_sources=true`。

## 新闻公告护栏

新闻、公告、研报已登记在同步目录中，但默认不进入一键同步。触发这些数据集时必须显式传入：

```json
{
  "sync_type": "tushare_relay",
  "relay_datasets": ["anns_d"],
  "relay_options": {
    "allow_text_sources": true,
    "daily_limit": 200
  }
}
```

默认策略：

- 默认只取最近 7 天。
- 每源每日最多 200 条。
- 不做历史全量回灌，除非显式指定日期范围。
- 过滤空标题、坏链接、明显广告/导航内容。
- 保存原始标题、链接、来源和 `quality_flags`，暂不直接进入因子研究。

## 北向、基金持仓与三表财务

2026-06-16 新增一组从“A股操作指导”审计结果落地的数据集，仍然统一走 `sync_type="tushare_relay"` 并先写入本地 Parquet。

| Relay dataset | API | Parquet dataset | 分区日期列 | 同步方式 |
|---|---|---|---|---|
| `moneyflow_hsgt` | `moneyflow_hsgt` | `hsgt_moneyflow` | `trade_date` | 按交易日同步北向/南向聚合资金流，适合市场状态因子 |
| `hk_hold` | `hk_hold` | `hsgt_holdings` | `trade_date` | 按交易日+交易所，或按股票+交易日同步沪股通/深股通持股明细 |
| `fund_portfolio` | `fund_portfolio` | `fund_portfolio_holdings` | `end_date` | 按报告期+基金代码、报告期+股票，或显式允许后按报告期同步基金持仓 |
| `income` | `income` | `financial_income` | `f_ann_date` | 按股票+实际公告日区间同步利润表 |
| `balancesheet` | `balancesheet` | `financial_balancesheet` | `f_ann_date` | 按股票+实际公告日区间同步资产负债表 |
| `cashflow` | `cashflow` | `financial_cashflow` | `f_ann_date` | 按股票+实际公告日区间同步现金流量表 |

示例：
```json
POST /api/data/sync
{
  "sync_type": "tushare_relay",
  "relay_datasets": ["moneyflow_hsgt", "hk_hold", "fund_portfolio", "income", "balancesheet", "cashflow"],
  "symbols": ["600519.SH"],
  "start_date": "2024-01-01",
  "end_date": "2026-06-16",
  "relay_options": {
    "hk_hold_limit": 3800,
    "fund_portfolio_limit": 5000,
    "fund_period_limit": 8,
    "statement_limit": 5000,
    "rps": 1,
    "timeout_seconds": 45
  }
}
```

注意：
- `moneyflow_hsgt` 是聚合资金流，不提供个股截面持仓；个股北向持仓变化应使用 `hk_hold` / `hsgt_holdings`。
- `fund_portfolio` 默认要求传 `symbols` 或 `relay_options.fund_codes`；只有显式传 `allow_all_symbols=true`、`allow_all_funds=true` 或 `fund_allow_period_only=true` 才会按报告期全量拉取，防止任务过大。
- 三表财务以 `f_ann_date` 作为本地分区日期，避免按 `end_date` 使用未来数据；同时保留 `end_date` 用于报告期对齐。
- 三表规范化会补充便于因子使用的别名：`rd_expense`、`intangible_assets`、`rd_capitalized`、`net_operate_cash_flow`、`capex`。

## API

同步目录：

```text
GET /api/data/sync/catalog
```

触发 Relay 结构化同步：

```json
POST /api/data/sync
{
  "sync_type": "tushare_relay",
  "relay_datasets": ["adj_factor", "moneyflow", "stk_auction_replay"],
  "symbols": ["000001.SZ"],
  "start_date": "2024-05-06",
  "end_date": "2024-05-06",
  "relay_options": {
    "block_moneyflow_limit": 5,
    "ths_member_limit": 50
  }
}
```

全市场同步必须显式确认：

```json
{
  "relay_options": {
    "allow_all_symbols": true
  }
}
```

## 因子入口

已注册轻量结构化因子：

| factor_name | 数据来源 |
|---|---|
| `moneyflow_net_mf_amount` | `moneyflow.net_mf_amount` |
| `moneyflow_net_mf_vol` | `moneyflow.net_mf_vol` |
| `block_moneyflow_net_amount` | `block_moneyflow` + `ths_member` |
| `auction_amount` | `auction_replay.amount` |
| `auction_gap_pct` | `(price - open) / open` |
| `auction_vwap` | `auction_replay.vwap` |

使用前先跑 `tushare_relay` 同步；预计算会从本地 Parquet 读取，不会在因子计算阶段访问外部接口。
