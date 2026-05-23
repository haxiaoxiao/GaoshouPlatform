# 因子值缓存

因子值缓存（Factor Value Store）是因子研究模块的持久化层，用于保存可复用、point-in-time 的因子或指标截面值。

完整链路：

```text
因子定义 -> 因子计算 -> 因子值缓存 -> 因子分析报告 -> 因子看板 -> 策略复用
```

## 存储

当前 Parquet 目录：

```text
data/parquet/factor_values/year=YYYY/month=MM/part-*.parquet
```

逻辑主键：

```text
symbol, trade_date, as_of_time, factor_name, params_hash
```

核心字段：

```text
symbol, trade_date, as_of_time, factor_name, params_hash, value, source, created_at
```

旧 `data/parquet/feature_values` 只作为历史数据源保留，不再被因子值缓存 API 使用。迁移脚本：

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

