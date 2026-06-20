# Relay 长尾数据目录与替代源查找笔记

Last updated: 2026-06-20.

本文记录 Indevs Tushare Relay 中回源慢、需要按股票/分析师/报告期逐项同步的数据目录，方便继续寻找替代来源。当前公共数据根目录为 `E:\Projects\data\BaiduSyncdisk\parquet`。

## 2026-06-20 本地 JQ/BaiduSyncdisk 数据接入

新接入目录位于：

```text
E:\Projects\data\BaiduSyncdisk\parquet
```

已纳入 `backend/app/services/parquet_dataset_catalog.py`，数据浏览器、系统数据摘要和前端表信息都从这个 catalog 读取标签、分类和日期字段。

| Parquet 目录 | 行数/覆盖 | 日期字段 | 接入状态 |
|---|---:|---|---|
| `jq_money_flow_daily` | `13,424,052` 行，`2010-01-04` 至 `2026-04-17` | `trade_date_1` | 已接入浏览器、系统摘要、资金流因子 |
| `jq_financial_income` | 历史财报，约从 2005 年开始至 2026 年 | `available_date` | 已登记浏览器目录，后续可做 PIT 财务因子 |
| `jq_financial_balance` | 历史财报，约从 2005 年开始至 2026 年 | `available_date` | 已登记浏览器目录 |
| `jq_financial_cash_flow` | 历史财报，约从 2005 年开始至 2026 年 | `available_date` | 已登记浏览器目录 |
| `jq_index_daily_bars` | 指数日线 | `trade_date` | 已登记浏览器目录 |
| `jq_etf_daily_bars` | ETF 日线 | `trade_date` | 已登记浏览器目录 |
| `jq_index_minute_bars` | 指数分钟线 | `datetime` | 已登记浏览器目录 |

`jq_money_flow_daily` 注意事项：

- 不要使用 `trade_date`。该字段来自 Tushare schema 污染，在当前数据中为空。
- 统一使用 `trade_date_1`。它由迁移时合并原始 `date` 和 `time` 规范生成，覆盖全部 `13,424,052` 行。
- 后续如果清洗数据，可以把 `trade_date_1` 重命名为更明确的 `trade_date_dt`，但当前代码和文档统一按 `trade_date_1` 接入。

当前已暴露为 Factor Value Store 预计算因子的资金流字段：

| 因子名 | 源字段 |
|---|---|
| `jq_moneyflow_net_amount_main` | `jq_money_flow_daily.net_amount_main` |
| `jq_moneyflow_net_pct_main` | `jq_money_flow_daily.net_pct_main` |
| `jq_moneyflow_net_amount_xl` | `jq_money_flow_daily.net_amount_xl` |
| `jq_moneyflow_net_pct_xl` | `jq_money_flow_daily.net_pct_xl` |
| `jq_moneyflow_net_amount_l` | `jq_money_flow_daily.net_amount_l` |
| `jq_moneyflow_net_pct_l` | `jq_money_flow_daily.net_pct_l` |
| `jq_moneyflow_net_mf_amount` | `jq_money_flow_daily.net_mf_amount` |

## 当前样本覆盖

| Parquet 目录 | 当前行数 | 日期覆盖 | 状态 |
|---|---:|---|---|
| `financial_income` | 710 | `2024-04-16` 至 `2026-05-15`，按 `f_ann_date` | 已完成本轮 80 只大市值样本 |
| `financial_balancesheet` | 704 | `2024-04-16` 至 `2026-05-15`，按 `f_ann_date` | 已完成本轮 80 只大市值样本 |
| `financial_cashflow` | 702 | `2024-04-16` 至 `2026-05-15`，按 `f_ann_date` | 已完成本轮 80 只大市值样本 |
| `analyst_report_forecasts` | 216 | `2025-11-28` 至 `2026-06-15`，按 `report_date` | 有样本；本轮增量任务失败，需要小批量重试 |
| `analyst_rank` | 20 | `2026-06-17` | 有样本 |
| `analyst_detail` | 36 | `2025-12-30` 至 `2026-06-09`，按 `latest_rating_date` | 有样本 |
| `analyst_history` | 0 | - | 当前未落盘；接口曾超时 |
| `hsgt_moneyflow` | 9 | `2026-06-08` 至 `2026-06-18`，按 `trade_date` | 有样本；本轮增量任务失败，需要重试 |
| `hsgt_holdings` | 0 | - | 当前未落盘；探测返回空 |
| `fund_portfolio_holdings` | 3000 | `2000-12-31` 至 `2023-03-31`，按 `end_date` | 有样本但历史范围偏旧 |
| `ths_member` | 530 | `2026-05-26`，按 `snapshot_date` | 可用 |
| `block_moneyflow` | 5 | `2026-06-06`，按 `trade_date` | 小 limit 可用 |

## 优先找替代源的数据

| 数据 | Relay 接口 | 本地目录 | 为什么慢/不稳定 | 替代源必须满足 |
|---|---|---|---|---|
| 利润表 | `income` | `financial_income` | 按股票和公告日区间逐只同步，历史全量会很慢 | 有股票、报告期、公告日/实际披露日、主要利润表字段，最好有修订标记 |
| 资产负债表 | `balancesheet` | `financial_balancesheet` | 同上，部分股票返回慢 | 有股票、报告期、公告日/实际披露日、资产负债字段，最好有修订标记 |
| 现金流量表 | `cashflow` | `financial_cashflow` | 同上 | 有股票、报告期、公告日/实际披露日、现金流字段，最好有修订标记 |
| 卖方盈利预测 | `report_rc` | `analyst_report_forecasts` | 按股票查，回源慢且容易超时 | 有研报日期、机构、作者、评级、目标价、EPS/PE/净利润/营收预测 |
| 分析师历史跟踪 | `analyst_history` | `analyst_history` | 必须先有 `analyst_id`，再逐分析师下钻 | 有分析师 ID、股票、调入/调出日期、评级、调出原因、区间收益 |
| 陆股通个股持仓 | `hk_hold` | `hsgt_holdings` | 按日期+交易所或股票+日期拉；当前样本返回空 | 有股票、交易日、沪/深股通市场、持股数量、持股比例、持股市值 |
| 基金持仓 | `fund_portfolio` | `fund_portfolio_holdings` | 按基金/股票/报告期组合查，全量很重 | 有基金代码、股票、报告期、公告日、持股数量、持股市值、占比 |

## 重点字段说明

### `financial_income`

利润表。适合构建营收增长、利润增长、费用率、研发费用率、ROE/ROA 的分子等因子。

| 字段 | 说明 |
|---|---|
| `symbol` / `ts_code` | 股票代码 |
| `end_date` | 报告期截止日，例如年报通常是 `YYYY-12-31` |
| `ann_date` | 公告日期 |
| `f_ann_date` | 实际公告/披露日期；PIT 计算优先用它 |
| `report_type` | 报告类型 |
| `comp_type` | 公司类型/报表类型 |
| `total_revenue` | 营业总收入 |
| `revenue` | 营业收入 |
| `total_cogs` / `oper_cost` | 总成本 / 营业成本 |
| `sell_exp` / `admin_exp` / `fin_exp` | 销售、管理、财务费用 |
| `rd_exp` / `rd_expense` | 研发费用 |
| `operate_profit` | 营业利润 |
| `total_profit` | 利润总额 |
| `n_income` | 净利润 |
| `n_income_attr_p` / `net_profit_attributable` | 归母净利润 |
| `ebit` / `ebitda` | EBIT / EBITDA |
| `update_flag` | 更新标记；用于识别是否有修订 |

### `financial_balancesheet`

资产负债表。适合构建资产负债率、商誉占比、无形资产占比、资本化研发、营运资本等因子。

| 字段 | 说明 |
|---|---|
| `symbol` / `ts_code` | 股票代码 |
| `end_date` | 报告期截止日 |
| `ann_date` / `f_ann_date` | 公告日期 / 实际公告日期 |
| `total_assets` | 总资产 |
| `total_liab` | 总负债 |
| `total_hldr_eqy_exc_min_int` | 归母股东权益 |
| `total_hldr_eqy_inc_min_int` / `total_equity` | 股东权益合计 |
| `total_cur_assets` | 流动资产 |
| `total_nca` | 非流动资产 |
| `total_cur_liab` | 流动负债 |
| `total_ncl` | 非流动负债 |
| `intan_assets` / `intangible_assets` | 无形资产 |
| `goodwill` | 商誉 |
| `r_and_d` / `rd_capitalized` | 资本化研发相关字段 |
| `update_flag` | 更新标记 |

### `financial_cashflow`

现金流量表。适合构建经营现金流质量、自由现金流、资本开支、折旧摊销等因子。

| 字段 | 说明 |
|---|---|
| `symbol` / `ts_code` | 股票代码 |
| `end_date` | 报告期截止日 |
| `ann_date` / `f_ann_date` | 公告日期 / 实际公告日期 |
| `net_profit` | 净利润 |
| `n_cashflow_act` / `net_operate_cash_flow` | 经营活动现金流净额 |
| `im_net_cashflow_oper_act` | 间接法经营活动现金流净额 |
| `c_pay_acq_const_fiolta` / `capex` | 购建固定资产等支付现金 / 资本开支 |
| `n_cashflow_inv_act` | 投资活动现金流净额 |
| `free_cashflow` | 自由现金流 |
| `amort_intang_assets` | 无形资产摊销 |
| `depr_fa_coga_dpba` | 固定资产折旧、油气资产折耗、生产性生物资产折旧 |
| `stot_out_inv_act` | 投资活动现金流出小计 |
| `update_flag` | 更新标记 |

### `analyst_report_forecasts`

卖方盈利预测和评级。适合构建一致预期变化、评级上调、目标价空间、预测分歧等因子。

| 字段 | 说明 |
|---|---|
| `symbol` / `ts_code` | 股票代码 |
| `name` | 股票名称 |
| `report_date` | 研报日期 |
| `report_title` / `title_hash` | 研报标题 / 标题哈希 |
| `report_type` / `classify` | 报告类型 / 分类 |
| `org_name` | 机构 |
| `author_name` | 作者 |
| `quarter` | 预测期 |
| `op_rt` | 营收相关预测字段 |
| `op_pr` | 利润相关预测字段 |
| `tp` | 目标价 |
| `np` | 净利润预测 |
| `eps` | EPS 预测 |
| `pe` | PE 预测 |
| `rd` | 股息率或相关预测字段，需结合源端定义确认 |
| `roe` | ROE 预测 |
| `ev_ebitda` | EV/EBITDA 预测 |
| `rating` | 投资评级 |
| `min_price` / `max_price` | 目标价区间 |

### `analyst_rank` / `analyst_detail` / `analyst_history`

分析师能力和跟踪股票。`analyst_rank` 是入口表，`analyst_detail` 与 `analyst_history` 依赖 `analyst_id` 下钻。

| 目录 | 关键字段 |
|---|---|
| `analyst_rank` | `analyst_id`、`analyst_name`、`org_name`、`industry`、`year`、`update_date`、`annual_index`、`return_12m` |
| `analyst_detail` | `analyst_id`、`symbol`、`stock_name`、`in_date`、`latest_rating_date`、`rating`、`latest_price`、`stage_return`、`indicator` |
| `analyst_history` | 预期包含 `analyst_id`、`symbol`、`stock_name`、`in_date`、`out_date`、调入评级、调出原因、累计涨跌幅、`indicator` |

### `hsgt_moneyflow` / `hsgt_holdings`

陆股通相关数据。`hsgt_moneyflow` 是日级聚合资金流，不是个股持仓；个股北向持仓变化需要 `hsgt_holdings`。

| 目录 | 关键字段 |
|---|---|
| `hsgt_moneyflow` | `trade_date`、`hgt`、`sgt`、`ggt_ss`、`ggt_sz`、`north_money`、`south_money`、`north_money_million`、`south_money_million` |
| `hsgt_holdings` | 预期包含 `symbol`、`trade_date`、`exchange`、持股数量、持股比例、持股市值 |

### `fund_portfolio_holdings`

基金持仓。适合构建机构持仓、基金增减持、机构拥挤度等因子。

| 字段 | 说明 |
|---|---|
| `fund_code` / `ts_code` | 基金代码 |
| `symbol` | 股票代码 |
| `ann_date` | 公告日 |
| `end_date` | 报告期 |
| `mkv` | 持股市值 |
| `amount` | 持股数量 |
| `stk_mkv_ratio` | 占基金净值/资产比例 |
| `stk_float_ratio` | 占个股流通股比例 |

### `ths_member` / `block_moneyflow`

同花顺板块成分和板块资金流。可以用于主题/概念暴露、板块资金流暴露。

| 目录 | 关键字段 |
|---|---|
| `ths_member` | `ths_code`、`symbol`、`con_code`、`name`、`weight`、`in_date`、`out_date`、`is_new`、`snapshot_date` |
| `block_moneyflow` | `block_code`、`trade_date`、`name`、`lead_stock`、`close_price`、`pct_change`、`company_num`、`net_buy_amount`、`net_sell_amount`、`net_amount` |

## PIT 财报口径

PIT 是 point-in-time，意思是回测或因子计算只能使用当时市场已经能看到的数据。财报本身有 `end_date`，但报告期结束不等于市场已经知道报表内容。

示例：

| 字段 | 值 | 含义 |
|---|---|---|
| `end_date` | `2024-12-31` | 2024 年年报 |
| `f_ann_date` | `2025-03-28` | 实际披露日 |
| `as_of_date` | `2025-02-10` | 回测中的历史日期 |

在 `2025-02-10` 计算因子时，不能使用 `end_date=2024-12-31` 的年报，因为它到 `2025-03-28` 才披露。正确规则是：

```text
只使用 f_ann_date <= as_of_date 的财报记录
```

如果替代源没有 `f_ann_date`，但有可靠的 `ann_date`，可以退而使用：

```text
只使用 ann_date <= as_of_date 的财报记录
```

## 只知道 `ann_date` 够不够

最低限度上，知道真实公告日就可以避免最常见的未来函数：不要按 `end_date` 直接使用尚未披露的财报。

但如果要做严谨的 PIT 财务因子，不能只保存 `ann_date` 一个字段。至少还需要：

| 字段 | 为什么需要 |
|---|---|
| `symbol` | 定位股票 |
| `end_date` | 区分 2024Q3、2024 年报、2025Q1 等报告期 |
| `ann_date` 或 `f_ann_date` | 判断当时是否已经披露 |
| 财报数值字段 | 用于计算因子 |

最好还要有：

| 字段 | 为什么需要 |
|---|---|
| `f_ann_date` | 如果源端区分公告日和实际披露日，优先用实际披露日 |
| `report_type` | 区分一季报、半年报、三季报、年报、调整报告 |
| `comp_type` | 区分一般工商、银行、保险、证券等报表类型 |
| `update_flag` 或版本字段 | 防止后续修订后的数值被错误当成首次公告时已经知道 |
| `ingested_at` | 本地入库时间，用于审计和排查数据覆盖 |
| `source` / `source_api` | 记录来源，便于多源对账 |

一句话：`ann_date` 是 PIT 的门票，但不是完整数据模型。若只是做日级回测防未来函数，可靠的公告日基本够；若要做可复现、可审计、能处理修订的财务因子，最好同时保留 `end_date`、`f_ann_date`、`report_type`、`update_flag` 和版本/入库信息。

## 替代源筛选清单

找新来源时优先确认：

1. 是否支持 A 股历史批量下载，而不是只能查当前最新。
2. 是否包含公告日或实际披露日。
3. 是否保留报告期 `end_date`。
4. 是否能区分修订/调整版本。
5. 是否有稳定分页、限速和失败重试语义。
6. 是否能按日期或股票分片拉取，方便断点续跑。
7. 字段是否能映射到当前 Parquet 目录，避免后续因子层再改一遍。
