# GaoshouPlatform 数据技能 (DataSkill)

Last updated: 2026-05-25.

DataSkill 为策略、脚本和 API 提供统一数据访问接口。调用方不需要关心底层数据来自 SQLite、Parquet/DuckDB、ClickHouse 还是 miniQMT/QMT。

## 数据优先级

当前默认行情后端是 Parquet/DuckDB：

```text
MARKET_DATA_BACKEND=parquet
CLICKHOUSE_ENABLED=false
```

数据读取优先级：

| 数据类型 | 首选 | 兜底 |
|---|---|---|
| 股票快照 | SQLite `stocks` | QMT 当前详情 |
| 日 K 线 | MarketDataStore：Parquet/DuckDB 或 ClickHouse | QMT 本地缓存/实时请求 |
| 分钟 K 线 | MarketDataStore：Parquet/DuckDB 或 ClickHouse | QMT 本地缓存/实时请求 |
| 财务数据 | SQLite `financial_data` | QMT 财务接口 |
| 实时行情 | QMT | 无 |
| 指标/因子缓存 | `factor_values` / `stock_indicators` | 重新计算或同步 |

策略运行时不要反复打外部数据源。能落地到 Parquet/SQLite 的数据，应先同步或预计算，再从本地读取。

## 常用接口

```python
from app.services.data_skill import DataSkill
from app.db.sqlite import get_async_session

skill = DataSkill(session)
```

| 方法 | 用途 |
|---|---|
| `get_stock(symbol)` | 单只股票快照 |
| `get_stocks(symbols)` | 批量股票快照 |
| `screen_stocks(...)` | 条件选股 |
| `get_kline_daily(symbol, start_date, end_date, limit)` | 日 K 线 |
| `get_kline_minute(symbol, start_date, end_date, limit)` | 分钟 K 线 |
| `get_financial(symbol, report_count)` | 单只财务数据 |
| `get_financial_batch(symbols, report_count)` | 批量财务数据 |
| `get_realtime_quote(symbol)` | 实时行情 |
| `get_realtime_quotes(symbols)` | 批量实时行情 |
| `get_industries()` | 行业列表 |
| `get_all_symbols()` | 全部股票代码 |
| `get_indicator(symbol, name, trade_date)` | 单指标值 |
| `get_indicators_batch(symbols, trade_date)` | 批量指标 |

## 示例

```python
from datetime import date

snapshot = await skill.get_stock("600051.SH")
bars = await skill.get_kline_daily("600051.SH", start_date=date(2024, 1, 1))
financial = await skill.get_financial("600051.SH", report_count=8)
```

## 使用约束

- xtquant 是同步阻塞 SDK，后端服务中调用 QMT 必须用 executor 或 `asyncio.to_thread()` 包装。
- 不要使用 `download_financial_data`；财务同步只用 `download_financial_data2(callback=None)`。
- 指数池回测应使用历史指数成分快照，不要用当前自选股静态替代。
- 固定时点分钟策略应使用 `minute_timer` 数据，不要在策略运行时加载完整分钟线。
- 因子研究和策略复用优先读 Factor Value Store：`data/parquet/factor_values`。
