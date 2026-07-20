# 日内做 T v3 因果状态研究

日期：2026-07-20

## 1. 结论

本轮验证“增加更多判断”是否能改善做 T，但新增的是正交市场状态，而不是 RSI、MACD、KDJ 等同源价格变换。六个独立候选全部保持 `research_only`，不修改 v2 运行默认值、不自动晋级模拟盘，也不增加真实委托入口。

唯一得到正收益的 `directional_move_0_100` 仍未通过冻结门槛：名义净收益 `+1,038.90` 元、5bp/边压力 `+105.37` 元、10bp/边压力 `-1,450.49` 元；只有 74 对，低于预注册的 80 对要求，而且 603629.SH 为 `-519.12` 元，收益由 688008.SH 的 `+1,558.01` 元覆盖。

## 2. 研究问题

v2 的主要损失来自 `risk_restore`。本轮区分：

```text
暂时性个股偏离
vs
市场共同重估、单边趋势、价格跳跃和不足以覆盖成本的偏离
```

已有成本不交易区继续保留：预估回归边际必须覆盖佣金、最低佣金、印花税、过户费、双边滑点和 12bp 缓冲。本轮只研究入场状态，不修改底仓恢复状态机。

## 3. 数据冻结

- 股票：603629.SH、688008.SH，各 398 日、95,520 根、每日 240 根。
- 基准：603629.SH 固定映射 000001.SH；688008.SH 固定映射 000688.SH。
- 区间：2024-07-19 至 2026-03-13。
- 面板：股票 191,040 bars，基准 191,040 bars；无缺日、缺分钟或重复键。
- 涨跌停：SQLite `stock_limit_prices` 覆盖 796/796 个股票日。
- 固定底仓：603629.SH 2,000 股；688008.SH 1,000 股。

2026-03-13 之后存在指数分钟缺口、个股缺日和重复冲突 bar，不进入参数研究。v2 的后段结果已经被观察，因此本轮没有可声称“未见”的历史 holdout。

## 4. 因果特征

市场残差：

```text
residual[d,t] = log(stock_close[d,t] / stock_open[d])
              - log(index_close[d,t] / index_open[d])
```

同分钟稳健标准化只使用此前 20 个交易日，先 `shift(1)`，最少 15 日：

```text
location = median(past residual at the same minute)
scale = 1.4826 * MAD(past residual at the same minute)
residual_z = (residual - location) / scale
```

残差路径效率使用最近 15 根残差收益的绝对净位移除以绝对路径长度。相对跳跃分数使用当前残差收益除以只含此前收益的双幂变差尺度。指数缺 bar、历史不足、MAD 为零或非有限时均失败关闭；不前向填充。

## 5. 冻结候选

公共基线为 `1.75 <= abs(z) < 2.40`、10:00 至 10:29 入场、下午不开仓、每股每日最多一对。

| 候选 | 独立门控 |
|---|---|
| `baseline_time_window` | 公共基线 |
| `rv_15_25` | `15 <= RV10 < 25bp` |
| `directional_move_0_100` | `0 <= sign(z) * session_return < 100bp` |
| `max_z_2_25` | `abs(z) < 2.25` |
| `market_residual` | 原始 z 与残差 z 同号，且 `abs(residual_z) >= 1` |
| `residual_regime` | 路径效率 `<=0.65` 且相对跳跃分数 `<=4` |

候选先单独消融，不在看到结果后组合。252 日用于因果特征历史，测试块为 42、42、62 个连续交易日。每个候选均运行 2/5/10bp 每边滑点和 2.5% 成交量参与率压力；压力场景固定名义 2bp 的入场决策成本，只改变实际成交滑点，保证 2/5/10bp 使用同一信号集。

## 6. 结果

| 候选 | 名义净收益 | 配对 | 正收益折 | 5bp 压力 | 决策 |
|---|---:|---:|---:|---:|---|
| `baseline_time_window` | -5,372.13 | 193 | 0/3 | -8,200.64 | 拒绝 |
| `rv_15_25` | -201.64 | 94 | 1/3 | -1,670.56 | 拒绝 |
| `directional_move_0_100` | +1,038.90 | 74 | 3/3 | +105.37 | 仅前向观察 |
| `max_z_2_25` | -5,857.88 | 176 | 0/3 | -8,469.99 | 拒绝 |
| `market_residual` | -1,608.76 | 79 | 1/3 | -2,702.36 | 拒绝 |
| `residual_regime` | -5,149.97 | 190 | 0/3 | -7,942.65 | 拒绝 |

`directional_move_0_100` 三折分别为 `+284.06`、`+23.68`、`+731.16` 元；剔除全样本最佳单笔后仍为 `+521.65` 元。正 T 为 `+509.85` 元，倒 T 为 `+529.05` 元；但股票贡献不稳定，且 10bp 压力为 `-1,450.49` 元。8 个有交易月份中 6 个为正、2 个为负。5 次拒单均为 `volume_cap`，没有现金不足拒单。所有运行底仓恢复率为 100%。

严格筛选要求至少 80 对、每只股票至少 20 对、两只股票和两个方向均为正、5bp 压力为正、剔除最佳单笔后为正、`risk_restore` 未吞噬正常退出收益且无未恢复配对。六个候选均未全部满足。

## 7. 能力边界

- 本地没有历史逐笔或连续盘口，不能从分钟 OHLCV 伪造 microprice、queue imbalance 或 OFI。
- 没有可靠的历史公告盘中发布时间，价格跳跃不能称为新闻因子。
- 没有 point-in-time 行业归属和可用行业分钟指数，本轮只做固定市场指数残差。
- 阈值受已观察失败归因启发，历史正收益只能用于提出前向假说。

正式晋级至少需要从 2026-07-20 之后冻结代码和参数，累计 60 个交易日且不少于 40 个完整配对；实际模拟净收益和附加 5bp/边压力均为正，零未恢复配对，并经过人工复核。真实提交继续关闭。

## 8. 依据与产物

- 市场残差均值回归：[Avellaneda and Lee](https://math.nyu.edu/inmemoriam/avellaneda/AvellanedaLeeStatArb20090616.pdf)
- 日内残差反转：[Intraday Residual Reversal](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4731947)
- 跳跃检测：[Lee and Mykland](https://galton.uchicago.edu/~mykland/paperlinks/LeeMykland-2535.pdf)
- A 股日内时段效应：[Chu, Gu and Zhou](https://www.sciencedirect.com/science/article/pii/S1544612318307414)
- 交易成本不交易区：[Mean Reversion Pays, but Costs](https://arxiv.org/abs/1103.4934)

产物：

- `.runtime/intraday-t-v3-research/2024-07-19_2026-03-13/research.json`
- `.runtime/intraday-t-v3-research/2024-07-19_2026-03-13/runs.csv`
- `backend/app/scripts/research_intraday_t_v3.py`
- `backend/tests/scripts/test_research_intraday_t_v3.py`
