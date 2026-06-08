# 因子值缓存

Last updated: 2026-05-25.

因子值缓存（Factor Value Store）是因子研究模块的持久化层，用于保存可复用、point-in-time 的因子或指标截面值。

完整链路：

```text
因子定义 -> 因子计算 -> 因子值缓存 -> 因子分析报告 -> 因子看板 -> 策略复用
```

## 存储

当前 Parquet 目录：

```text
E:/Projects/Data/parquet/factor_values/year=YYYY/month=MM/part-*.parquet
```

逻辑主键：

```text
symbol, trade_date, as_of_time, factor_name, params_hash
```

核心字段：

```text
symbol, trade_date, as_of_time, factor_name, params_hash, value, source, created_at
```

旧 `E:/Projects/Data/parquet/feature_values` 只作为历史数据源保留，不再被因子值缓存 API 使用。迁移脚本：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\python.exe -m app.scripts.migrate_feature_values_to_factor_values --overwrite
```

## API

| Endpoint | 说明 |
|---|---|
| `GET /api/factor-values/definitions` | 查询可缓存因子/指标定义 |
| `GET /api/factor-values/groups` | 查询预计算集合 |
| `GET /api/factor-values/coverage` | 查询覆盖率 |
| `GET /api/factor-values/preview` | 预览某日截面 |
| `POST /api/factor-values/precompute` | 触发预计算 |
| `POST /api/factor-values/groups/precompute` | 触发集合预计算 |
| `POST /api/factor-values/query` | 查询某日截面 |

旧 `/api/features/*` 已移除。新代码只使用 `/api/factor-values/*` 和 `FactorValueStore`。

## Alpha101 定义与解释

平台内置 `alpha101_001` 到 `alpha101_101`。`GET /api/factor-values/definitions` 会为每个 Alpha101 因子返回真实 `formula` 和中文 `human_description`，详情页优先展示这两个字段，避免只看到因子名却看不到公式逻辑。

Alpha101 的统一依赖为日线 OHLCV、VWAP、收益率、市值和行业字段。当前 101 个公式已接入宽表计算实现，批量预计算会复用同一个 `WideAlphas` 面板，减少重复 groupby 和重复字段展开。公式解释和使用建议见 `docs/alpha101-factor-guide.md`。

当前重要口径：

| 项 | 口径 |
|---|---|
| VWAP | 从 `amount / volume` 计算，并自动识别日线 `volume` 是“手”还是“股” |
| `scale()` | 按交易日横截面缩放，使当日 `sum(abs(value)) = 1` |
| `params_hash` | 内置 Alpha101 使用空参数 hash；研究报告参数 hash 用于匹配研究配置 |
| 容错 | 单个 Alpha 失败只写入 `failed_factor_names/errors`，不会中断整组 |
| 覆盖率 | 长窗口/嵌套相关公式可能覆盖低，必须先看 coverage 再看 IC |

## 集合预计算结果

`POST /api/factor-values/groups/precompute` 支持按集合预计算，例如 `group_name=alpha101` 会请求 101 个 Alpha 因子。集合结果会返回：

| 字段 | 说明 |
|---|---|
| `factor_names` | 请求计算的因子列表 |
| `rows` | 每个因子实际写入行数 |
| `written_factor_count` | 写入行数大于 0 的因子数 |
| `zero_row_factor_names` | 没有写入有效值的因子 |
| `failed_factor_names` | 计算异常的因子 |
| `errors` | 按因子名归集的异常信息 |
| `coverage_ranges` | 每个因子的缓存覆盖范围 |

Alpha101 集合预计算采用逐因子容错：单个公式失败会记录错误并继续计算其他 Alpha，不会让整组任务直接中断。

如果修改了 Alpha101 公式、VWAP 单位或 `scale()` 口径，旧缓存不会自动重算。需要重新运行 Alpha101 集合预计算，再重新生成研究报告。

## 内置通用因子

| 名称 | 类型 | 说明 |
|---|---|---|
| `market_cap` | indicator | 当日可用市值 |
| `market_cap_rank` | factor | 股票池内市值升序排名 |
| `is_st` | state | ST、退市或名称异常 |
| `is_paused` | state | 停牌或无可用行情 |
| `is_limit_up` | state | 当前价格触及涨停 |
| `is_limit_down` | state | 当前价格触及跌停 |
| `yesterday_limit_up` | state | 前一交易日涨停 |
| `cum_volume_at_time` | indicator | 指定时间累计成交量 |
| `rolling_max_volume` | indicator | N 日最大成交量 |
| `high_volume_ratio` | factor | 当前累计量 / N 日最大量 |
| `high_volume_signal` | signal | 放量信号 |
| `v4gv` | indicator | V4GV 技术指标 |
| `v4gv_signal` | indicator | V4GV 信号线 |
| `macd_positive` | signal | MACD 正向信号 |

## Relay 结构化因子

`sync_type="tushare_relay"` 会先把 Relay 数据落到本地 Parquet，因子预计算阶段只读本地数据，不访问外部接口。当前已登记的轻量因子：

| 名称 | 来源 | 说明 |
|---|---|---|
| `moneyflow_net_mf_amount` | `moneyflow.net_mf_amount` | 个股主动资金净流入金额 |
| `moneyflow_net_mf_vol` | `moneyflow.net_mf_vol` | 个股主动买卖量差 |
| `block_moneyflow_net_amount` | `block_moneyflow` + `ths_member` | 所属板块资金流暴露 |
| `auction_amount` | `auction_replay.amount` | 集合竞价成交额 |
| `auction_gap_pct` | `(price - open) / open` | 集合竞价价格偏离 |
| `auction_vwap` | `auction_replay.vwap` | 集合竞价成交均价 |

新闻、公告、研报类 Relay 数据只作为可查、可筛选、可回溯数据源，第一阶段不直接进入因子研究。

## 策略复用

```python
from app.services.factor_value_store import get_factor_value_store

values = get_factor_value_store().load_cross_section(
    factor_name="high_volume_signal",
    trade_date=trade_date,
    symbols=symbols,
    as_of_time="14:30",
    params={
        "time": "14:30",
        "window": 120,
        "threshold": 0.9,
        "daily_volume_to_share_multiplier": 100.0,
    },
)
```

策略参数统一使用：

```json
{
  "enable_factor_value_cache": true,
  "high_volume_factor_name": "high_volume_signal"
}
```

## 表达式算子方向

新因子表达式优先使用语义明确的算子：

```text
ts_mean($close, 20)
ts_delta($close, 5) / ts_delay($close, 5)
cs_rank(ts_mean($amount, 20))
cs_zscore($turnover_rate)
```

旧 `Mean/Std/Rank` 等算子仍在底层注册表中存在，但新模板、文档和页面示例应推广 `ts_*` 与 `cs_*`，避免时序和截面语义混淆。
