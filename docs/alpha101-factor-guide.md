# Alpha101 因子使用说明

Last updated: 2026-05-25.

本文说明平台当前的 Alpha101 因子口径、详情页解释和分组预计算行为。

## 因子定义

平台内置 `alpha101_001` 到 `alpha101_101`。公式来源为 `backend/app/services/alpha101_calculator.py` 中 `Alphas.alpha_N` 的本地实现；批量预计算优先使用 `backend/app/services/alpha101_wide_calculator.py` 的宽表实现。定义接口 `GET /api/factor-values/definitions` 会为每个 Alpha 返回：

| 字段 | 说明 |
|---|---|
| `name` | 平台因子名，例如 `alpha101_002` |
| `display_name` | 展示名，例如 `Alpha101 #002` |
| `formula` | 从 Python docstring 提取的真实公式 |
| `human_description` | 面向使用者的中文解释 |
| `dependencies` | 依赖的本地数据字段 |
| `data_policy` | 面板、回看窗口、横截面排序和行业中性化说明 |

详情页优先展示后端返回的 `human_description`，再展示 `formula`。这样前端不会再只显示一个 Alpha 名字。

## 如何读公式

Alpha101 公式通常嵌套三类操作：

1. 基础序列：`open`、`high`、`low`、`close`、`vwap`、`volume`、`returns`、`cap`。
2. 时序窗口：`delta`、`delay`、`correlation`、`covariance`、`decay_linear`、`Ts_Rank`、`stddev`、`sum`。
3. 截面处理：`rank`、`scale`、`IndNeutralize`。

建议从公式最内层开始读：先看它构造了什么价格/成交量/收益率序列，再看滚动窗口长度，最后看是否做横截面排名、缩放或行业中性化。公式最外层如果带 `-1`，说明方向相对原始关系取反，但真正的多空方向仍要用 IC、分组收益和换手率确认。

## Alpha02 示例

`alpha101_002` 的公式是：

```text
Alpha#2: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
```

人话解释：它衡量“成交量变化”和“日内价格强弱”之间的 6 日滚动相关性并取负。先计算 `log(volume)` 的 2 日变化并做当日横截面排名，再计算 `(close - open) / open` 表示日内收益并做横截面排名；最后对每只股票滚动计算两者 6 日相关系数并乘以 `-1`。它属于短周期价量背离/反转类信号。

## 分组预计算

Alpha101 分组预计算使用 `POST /api/factor-values/groups/precompute`，`group_name=alpha101` 会请求 101 个 Alpha。

当前行为：

- 先按股票池和日期加载 OHLCV、VWAP、市值、行业面板。
- 101 个公式已迁移到宽表计算，批量任务会复用同一个宽表面板。
- 单个 Alpha 公式失败时记录到 `failed_factor_names` 和 `errors`，不会中断整组。
- 成功计算的因子会继续写入 Factor Value Store。
- 返回结果包含 `rows`、`written_factor_count`、`zero_row_factor_names`、`coverage_ranges`，用于前端判断哪些因子已经落库。

如果某些 Alpha 返回 0 行，通常原因是窗口期不足、公式本身在该股票池/日期内全为 `NaN`，或所需字段缺失。优先查看 `coverage_ranges`、`failed_factor_names` 和后端日志。

## 数据口径

| 字段/算子 | 当前口径 |
|---|---|
| `vwap` | `amount / volume`，并按样本自动判断 `volume` 是否需要乘以 100 股/手 |
| `return` | 同一股票的日收盘价 `pct_change()` |
| `cap` / `market_value` | 优先来自日频基础面板，不用当前快照倒填历史 |
| `rank` | 当日横截面百分位排名 |
| `scale` | 当日横截面缩放，目标是 `sum(abs(value)) = 1` |
| `IndNeutralize` | 按行业分组做当日横截面去均值 |

这些口径变化会影响 IC 和分组收益。修改公式或数据单位后，需要重跑 Alpha101 预计算和研究报告，不能直接比较旧缓存结果。

## IC 解读

Alpha101 是候选因子库，不是直接交易信号。推荐先按 Rank IC 过滤：

| 平均 Rank IC | 判断 |
|---:|---|
| `< 0.005` | 大概率是噪声 |
| `0.005 - 0.01` | 偏弱，需要稳定性很好才值得保留 |
| `0.01 - 0.02` | 有研究价值 |
| `0.02 - 0.03` | 可进入组合因子候选 |
| `> 0.03` | 较强，但要排查未来函数和样本偏差 |

同时看 ICIR、分组收益单调性、覆盖率和换手率。稳定负 IC 可以反向使用；覆盖率极低的 IC 不具备横向比较价值。

## 使用建议

- Alpha101 依赖较长历史窗口，平台默认给 370 天 lookback；短区间计算也需要足够前置日线数据。
- 先在中证 500、沪深 300、中证 1000 等指数池做覆盖率和预览，再进入因子分析。
- 先排除覆盖率明显偏低的公式，再比较 IC；例如长链路滚动相关公式在短样本中可能只有很少有效截面。
- 不要只按单日最大/最小值判断因子好坏，优先看 IC、分组收益单调性、换手率和不同市场阶段稳定性。
- 公式方向并不保证“越大越好”，应以后续研究报告中的 IC 和分组表现为准。
