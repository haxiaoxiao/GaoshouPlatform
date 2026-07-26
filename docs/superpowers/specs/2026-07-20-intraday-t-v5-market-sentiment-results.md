# 日内做 T v5 大盘情绪研究结果

日期：2026-07-20

## 1. 结论

本轮仍只研究 `603629.SH`（利通电子）和 `688008.SH`（澜起科技），没有修改页面、
模拟盘、实盘参数或真实下单边界。

在 v4 最稳健的 `volume_return_forecast` 上增加大盘情绪后：

- 10:00 涨跌停广度没有过滤任何一笔，结果与 v4 控制组完全相同。
- 连板晋级在板率门控从 29 对降到 19 对，名义净收益从 `+1,181.44` 提高到
  `+1,799.38` 元，10bp/边压力从 `+43.40` 提高到 `+1,032.18` 元。
- 复合情绪门控为 23 对，名义/5bp/10bp 分别为
  `+1,485.95/+1,150.33/+590.95` 元。

但最强的连板晋级候选在 `603629.SH` 上仍为 `-67.16` 元，全部历史已经参与假设形成，
而且只有 19 对。因此结论固定为 `research_only`：保留为新数据 shadow 观察项，不接入
模拟盘或实盘默认策略。

## 2. 点时情绪口径

每日只使用 canonical JQ 分钟源的 09:31-10:00 共 30 根已完成 bar；10:00 形成快照，
最早在 10:01 开盘成交。当日收盘后的 `limit_list_d`、`limit_step` 和日线结果均未进入
信号。

市场状态基于 SQLite `stock_limit_prices` 的当日精确上下限价：

```text
touched_up = high_09:31_to_10:00 >= up_limit - 0.005
at_limit   = abs(close_10:00 - up_limit) <= 0.005
broken_up  = touched_up and not at_limit
```

一分钟 OHLC 只能说明 10:00 最后价仍在涨停价，不能证明卖盘为空或存在真实封单。因此
报告使用 `at_limit`，不把它描述成真实封板强度。

昨日首板取 `limit_list_d.limit_times=1`，二板以上取 `limit_step.nums>=2`。同股同日板数
冲突时由专门的 `limit_step` 优先且只计一次；历史名称含 ST、北交所 30%、ST 5% 和
新股无涨跌幅日均排除。今日晋级分母先由昨日队列和今日盘前精确限价固定，分钟数据不全
时当日晋级率失败关闭，不能缩小分母后继续计算。

## 3. 数据质量

- 冻结窗口：2024-07-19 至 2026-03-13，共 398 个交易日。
- 两只股票和固定指数：各 `95,520 = 398 * 240` bars。
- 10:00 标准 10%/20% 股票池覆盖：每日 4,965 至 5,056 只。
- 连板晋级分母/可观测：`28,320/28,255`；56 日存在至少一个缺失观测并失败关闭。
- 供应商板数冲突：95 条，全部留痕并按 `limit_step` 优先去重。
- 两股精确涨跌停价：796/796 个股票日。
- 因果标准化：严格使用此前 60 日中位数/MAD，至少 40 个历史有效日。

`1 -> 2`、`2 -> 3`、`3+ -> 更高` 的分母、触板数和 10:00 在板数全部写入
`market_sentiment_daily.csv`；高板分层只作诊断，没有单独搜索阈值。

## 4. 固定回放结果

| 变体 | 名义净收益 | 对数 | 5bp | 10bp | 2.5% 容量 | 容量对数 | 603629.SH | 688008.SH |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 方向位移锚点 | +1,038.90 | 74 | +105.37 | -1,450.49 | +1,195.53 | 71 | -519.12 | +1,558.01 |
| v4 量价预测控制组 | +1,181.44 | 29 | +754.67 | +43.40 | +1,356.44 | 27 | -147.52 | +1,328.96 |
| 量价 + 涨跌停广度 | +1,181.44 | 29 | +754.67 | +43.40 | +1,356.44 | 27 | -147.52 | +1,328.96 |
| 量价 + 连板晋级在板率 | +1,799.38 | 19 | +1,511.68 | +1,032.18 | +1,862.59 | 18 | -67.16 | +1,866.53 |
| 量价 + 复合情绪 | +1,485.95 | 23 | +1,150.33 | +590.95 | +1,670.96 | 21 | -232.35 | +1,718.30 |

### 连板晋级候选

- 相对 v4 控制组，名义/5bp/10bp/容量增量分别为
  `+617.94/+757.00/+988.78/+506.15` 元。
- 三个名义测试折全部为正，2/3 折优于 v4 控制组；10bp 下 2/3 折为正。
- 正 T 为 10 对、`+833.98` 元；倒 T 为 9 对、`+965.39` 元。
- 逐折删除最佳一笔后，名义/5bp/10bp 仍为
  `+769.51/+539.96/+157.37` 元。
- 2.5% participation 保留 18/19 对；603629.SH 保留 5/6 对但仍为 `-3.94` 元。

这些稳健性结果比 v4 控制组改善，但 19 对远不足以建立单股统计证据。603629.SH 在
名义、5bp 和 10bp 下分别为 `-67.16/-124.82/-220.94` 元，仍未解决两股共同有效问题。

### 复合候选

复合情绪主要改善 688008.SH；603629.SH 反而从控制组 `-147.52` 降至 `-232.35` 元。
其 10bp 逐折去最佳一笔后为负，因此不优于连板晋级候选。

## 5. 完整性审计

- 完成 `3 折 * 5 变体 * 4 场景 = 60` 个唯一运行。
- v5 的 `volume_return_forecast` 控制组逐折、逐场景、指标和 signal-ledger SHA 均与
  v4 完全一致。
- 每个折次/变体的 2/5/10bp signal ledger SHA 完全一致。
- 60 个运行均为零期末未平配对、零恢复失败、恢复率 100%。
- `runs.csv` SHA-256：`bcabfe8a564cfd03ac69578acdfd6ab6a28857c51f365bd710f791ad34a2151d`。
- `signal_ledger.csv` SHA-256：`9aa18db6bf9990e29fa78d7c4fe24285c2e4189ef6c44258d8f9a1853f00922e`。
- `market_sentiment_daily.csv` SHA-256：`bf616ce3ccff6ddf0fe88825f490c0ac5c7fab27788e64cc6a92ac5556d32297`。
- 实现指纹：`354685ca39556dfed4f7da37888a7e0068360efc7037bb648dc4853dd479ba8a`。

## 6. 决策与后续证据

当前历史只能用于发现候选，不能用于自动晋级。后续仅 shadow 记录固定的
`volume_board_promotion_alignment`，不再调整阈值；至少等待两股各自积累足够的新配对，
并分别要求 5bp 为正、10bp 非负和恢复率 100%。在此之前：

- 不修改 `/trade/intraday-t` 页面参数；
- 不修改持久化模拟盘默认策略；
- 不新增真实提交入口；
- 不把 688008.SH 的盈利解释为 603629.SH 已通过。

## 7. 产物与复跑

- `.runtime/intraday-t-v5-sentiment-research/2024-07-19_2026-03-13/research.json`
- `.runtime/intraday-t-v5-sentiment-research/2024-07-19_2026-03-13/runs.csv`
- `.runtime/intraday-t-v5-sentiment-research/2024-07-19_2026-03-13/signal_ledger.csv`
- `.runtime/intraday-t-v5-sentiment-research/2024-07-19_2026-03-13/market_sentiment_daily.csv`
- `backend/app/scripts/research_intraday_t_v5.py`

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m app.scripts.research_intraday_t_v5 `
  --start-date 2024-07-19 --end-date 2026-03-13 `
  --limit-price-db E:\Projects\data\BaiduSyncdisk\gaoshou.db `
  --parquet-root E:\Projects\data\BaiduSyncdisk\parquet `
  --output-dir ..\.runtime\intraday-t-v5-sentiment-research\2024-07-19_2026-03-13
```

## 8. 主要来源

- Tushare 涨跌停明细：<https://tushare.pro/document/2?doc_id=298>
- Tushare 连板天梯：<https://tushare.pro/document/2?doc_id=356>
- 深交所交易规则：<https://docs.static.szse.cn/www/lawrules/rule/trade/W020260424690713155663.pdf>
- 深交所精确上下限技术字段：<https://docs.static.szse.cn/www/marketServices/technicalservice/notice/W020180523596999490643.pdf>
- A 股涨跌停高频研究：<https://arxiv.org/abs/1503.03548>

盘中晋级率是本研究自行构造的点时指标，不是交易所或 Tushare 官方盘中统计。文献支持
区分触板与收盘/点时在板状态，但不直接证明该指标能提高这两只股票的做 T 收益。
