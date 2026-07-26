# 日内做T v5：大盘情绪辅助研究设计

## 目标与边界

- 研究对象固定为 `603629.SH` 与 `688008.SH`，不扩大股票池。
- 保留 v4 的 `directional_move_0_100` 和表现最好的 `volume_return_forecast` 两个控制组，不改变交易时段、仓位、成本、成交、涨跌停和恢复规则。
- 大盘情绪只能拦截新的做T开仓，不能阻止已经打开配对的回补/恢复。
- 本轮只做离线、回顾性消融研究，不修改模拟盘或实盘默认参数，不允许自动晋级。

## 因果时间线

策略信号窗口仍为 10:00-10:29。每日情绪快照固定在 10:00，且只使用当日已经完成的 09:31-10:00 共 30 根一分钟 bar。策略在分钟 `t` 收盘后生成信号，最早在 `t+1` 分钟开盘成交，因此 10:00 快照可用于 10:00 及之后的信号。

昨日连板队列来自上一交易日收盘后已经确定的数据；今天是否晋级只按截至 10:00 的最高价和最后价判断。禁止使用今天收盘后的 `limit_list_d`、`limit_step` 或日线收盘结果。

## 数据源

- 全 A 一分钟线：Parquet `klines_minute`，冻结口径只使用 canonical `source='jq'`；不得混入有重复冲突的 QMT bar。
- 精确涨跌停价：SQLite `stock_limit_prices`，不按板块硬编码 10%/20%。
- 昨日首板队列：Tushare `limit_list_d` 中 `limit='U'` 且 `limit_times=1`。
- 昨日二板及以上队列：Tushare `limit_step` 中 `nums>=2`。

只纳入精确涨跌停幅度约为 10%（8%-12%）或 20%（17%-22%）的股票。由此排除 ST 5%、北交所 30% 和新股无涨跌幅限制日。市场广度只统计当日 30 根早盘 bar 完整的股票；连板晋级先按昨日队列与今日盘前精确限价固定分母，再单独记录有完整 30 根 bar 的观测数。观测数小于分母时，晋级指标失败关闭，不能静默缩小分母。

若 `limit_list_d` 与 `limit_step` 对同一 `(trade_date, symbol)` 给出冲突板数，使用专门描述二板以上梯队的 `limit_step` 层级，且在 `board_source_conflicts` 中留痕；同股同日只能进入一个层级，不能双计。`limit_step` 中历史名称含 ST 的股票排除。

## 10:00 原始指标

对每只股票，以半个价格跳动单位 `0.005` 判断是否触及或封住精确涨跌停价：

```text
locked_up     = abs(last_10_00 - up_limit) <= 0.005
locked_down   = abs(last_10_00 - down_limit) <= 0.005
touched_up    = high_09_31_to_10_00 >= up_limit - 0.005
broken_up     = touched_up and not locked_up
```

分别保留全市场标准 10%/20% 股票、10% 匹配池和 20% 匹配池的计数。`603629.SH` 使用 10% 匹配池，`688008.SH` 使用 20% 匹配池。

派生指标：

```text
limit_breadth = log((locked_up + 0.5) / (locked_down + 0.5))
at_limit_quality = (locked_up + 0.5) / (touched_up + 1.0)
```

### 连板晋级率

昨日 `k` 板股票是今天 `k -> k+1` 的分母。对 `k=1`、`k=2`、`k>=3` 分别记录：

```text
touch_rate_k = (touched_count_k + 0.5) / (eligible_count_k + 1.0)
at_limit_rate_k = (at_limit_count_k + 0.5) / (eligible_count_k + 1.0)
```

总晋级率使用所有标准 10%/20% 队列的分母加总：

```text
promotion_at_limit_rate = (promotion_at_limit + 0.5) / (promotion_eligible + 1.0)
```

分层成功率会完整落盘，但不分别拿来调参或门控；高板分母偏小，只作为解释和后续前瞻观察指标。

## 因果标准化

连续指标均使用严格 `shift(1)` 的过去 60 个交易日中位数与 MAD 标准化，最少需要 40 个历史日：

```text
robust_z_t = (x_t - median(x_{t-60:t-1})) / (1.4826 * MAD(x_{t-60:t-1}))
```

MAD 为零、历史不足、分母缺失或非有限值时返回缺失，相关情绪门控失败关闭。未来日期数据变化不得改写过去日期的情绪特征。

## 固定候选

所有候选先通过 v4 方向锚定条件：

```text
0 <= sign(stock_z) * session_return_bps < 100
```

`volume_return_forecast` 控制组再要求过去数据训练的下一分钟预测与当前偏离方向相反：

```text
stock_z * volume_return_forecast_bps < 0
```

三个情绪候选全部叠加在这个固定的 v4 成交量控制组上，以直接检验新增情绪的边际贡献。阈值统一固定为 `1.5` 个因果 robust-z，不根据做T收益选择：

1. `volume_limit_breadth_alignment`

   ```text
   sign(stock_z) * global_limit_breadth_z <= 1.5
   ```

2. `volume_board_promotion_alignment`

   ```text
   sign(stock_z) * promotion_at_limit_rate_z <= 1.5
   ```

3. `volume_composite_market_sentiment`

   先将以下四项截断到 `[-3, 3]` 后等权平均，再对平均值做一次严格历史 robust-z：

   - 全市场 `limit_breadth_z`
   - 股票匹配涨跌幅池的 `segment_limit_breadth_z`
   - 全市场 `at_limit_quality_z`
   - 全市场 `promotion_at_limit_rate_z`

   ```text
   sign(stock_z) * composite_sentiment_z <= 1.5
   ```

方向含义：正 T 的 `stock_z<0`，极冷市场会被拦截；倒 T 的 `stock_z>0`，极热市场会被拦截。三个情绪候选不互相叠加，避免组合搜索。分钟 OHLC 只能证明 10:00 最后价仍在涨停价，`at_limit` 是价格状态代理，不代表卖盘为空或存在真实封单。

## 回测矩阵与硬约束

- 日期冻结：`2024-07-19` 至 `2026-03-13`，398 个共同交易日。
- 折叠：复用 v4 的 3 个连续回顾性测试折。
- 变体：方向锚定、v4 成交量控制组，以及成交量控制组上 3 个情绪候选，共 5 个。
- 场景：2bp、5bp、10bp 以及 2.5% bar participation，共 4 个。
- 完整矩阵：`3 * 5 * 4 = 60` 次运行。
- 2/5/10bp 的 signal ledger 必须完全一致。
- 每个股票、每折、每场景必须恢复到期初数量，`open_pairs_at_end=0`、`restoration_failures=0`、`restoration_rate=1.0`。
- 报告必须保留按股票、方向、折和成本的结果，不允许用总收益掩盖单股负收益。
- 无论结果如何，最终决策固定为 `research_only`。

## 产物与复现

输出目录为 `.runtime/intraday-t-v5-sentiment-research/2024-07-19_2026-03-13/`：

- `research.json`：协议、配置、数据质量、指纹、结果和 research-only 结论。
- `runs.csv`：60 次运行的平铺指标。
- `signal_ledger.csv`：逐笔信号账本。
- `market_sentiment_daily.csv`：398 日原始计数、分层晋级率和因果标准化特征。

实现、配置、股票分钟、指数分钟、精确涨跌停价、情绪日表和最终 CSV 均写入 SHA-256 指纹。

## 来源与口径依据

- Tushare 涨跌停明细 `limit_list_d`：<https://tushare.pro/document/2?doc_id=298>
- Tushare 连板天梯 `limit_step`：<https://tushare.pro/document/2?doc_id=356>
- 上交所科创板交易机制与 20% 涨跌幅：<https://www.sse.com.cn/aboutus/mediacenter/hotandd/c/c_20190719_4866745.shtml>
- A 股涨跌停触板后的价格行为研究：<https://arxiv.org/abs/1503.03548>

“盘中晋级率”不是交易所或 Tushare 的现成字段，而是本协议基于昨日收盘队列、今日点时分钟行情和今日精确涨停价构造的研究指标。报告必须明确这一点，不能把它描述成官方统计。
