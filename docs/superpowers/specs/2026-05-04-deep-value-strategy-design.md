# Deep Value Strategy — 深度价值策略设计

> 2026-05-04 | 状态: 设计完成，待实施

## 策略概要

低估值 + 高股息 + 深度折价策略：

- **入场条件**: 股价低于 250 周均线 10%+ AND 股息率 > 3.5% AND PE < 40 AND PE > 0
- **持仓**: 等权买入，持有 52 周（约 1 年）
- **调仓**: 每 13 周检查一次，符合条件的新标的买入，到期卖出
- **风控**: 过滤 ST/停牌/退市，单票上限 10%
- **回测**: 2015-2025，事件驱动引擎

---

## 第一部分：数据管线

### 1.1 新建 ClickHouse 表 `klines_weekly`

```sql
CREATE TABLE IF NOT EXISTS klines_weekly
(
    symbol LowCardinality(String),
    trade_date Date,
    open Decimal(10, 4),
    high Decimal(10, 4),
    low Decimal(10, 4),
    close Decimal(10, 4),
    volume UInt64,
    amount Decimal(18, 4),
    turnover_rate Decimal(8, 4),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date)
```

位置: `backend/app/db/clickhouse.py` — `init_clickhouse_tables()`

### 1.2 新增 `kline_weekly` 同步

**位置**: `backend/app/services/sync_service.py` — 新增 `sync_kline_weekly()` 方法

**流程**: 下载 → 读取 → 写入 ClickHouse → 清理缓存（同日线同步流程）

**QMT 调用**: `xt.download_history_data(symbol, period="1w", start_time=..., end_time=...)`

**API 端点**: `POST /api/data/sync` — body `{"sync_type": "kline_weekly"}`

**数据范围**: 2015-01-01 ~ 2025-12-31（含预热期）

### 1.3 新增 `ma250_weekly` 指标

**位置**: `backend/app/indicators/technical.py`

- `name = "ma250_weekly"`
- `data_type = "时序"` — 需要从 klines_weekly 读取历史数据
- `is_precomputed = True` — 同步后自动计算
- 计算逻辑: 从 `klines_weekly` 取最近 250 周 close，求均值

### 1.4 新增 `price_to_ma250w` 指标

**位置**: `backend/app/indicators/valuation.py` (或新增一个 crossover 类别)

- `name = "price_to_ma250w"`
- `data_type = "截面"`
- `is_precomputed = True`
- 计算逻辑: 最新收盘价 / ma250_weekly
- `dependencies = ["ma250_weekly"]`

### 1.5 修复 `dividend_yield` 指标

**当前问题**:
1. `symbols_with_data[:1000]` — 只算前 1000 只股票
2. 只存 `date.today()` 一个时间点，回测不可用
3. 对每只股票逐条 DELETE + INSERT，效率低

**修复方案**:
1. 去掉 1000 限制
2. 改为每个月存一个截面值（历史回看 12 个月的累计分红 / 当时价格），写入 `stock_indicators` 的 trade_date 用当时日期
3. 批量写入优化

### 1.6 确认 `pe_ttm` 历史数据

`pe_ttm` 已存在于:
- SQLite `stocks` 表（最新值）
- ClickHouse `stock_indicators`（需确认是否有历史截面数据）

若 stock_indicators 中 pe_ttm 只有最新值，需在 indicator_scheduler 中补充历史计算逻辑。

---

## 第二部分：策略开发

### 2.1 策略文件

**位置**: `backend/app/strategies/deep_value.py`

**类名**: `DeepValueStrategy`

```python
class DeepValueStrategy:
    """深度价值策略 — 低估值 + 高股息 + 深度折价"""
    
    # 参数
    PRICE_TO_MA_THRESHOLD = 0.9      # 低于250周线10%
    DIVIDEND_YIELD_MIN = 3.5          # 股息率 > 3.5%
    PE_MAX = 40                       # PE < 40
    HOLD_WEEKS = 52                   # 持有52周
    REBALANCE_EVERY = 13             # 每13周调仓
    MAX_POSITIONS = 10               # 最多持仓数
    MAX_SINGLE_PCT = 0.10            # 单票上限10%
    
    def screen_candidates(self, as_of_date: date) -> list[str]:
        """筛选候选标的"""
        ...
    
    def run_backtest(self, start: date, end: date) -> dict:
        """运行回测"""
        ...
```

### 2.2 数据流

```
策略回测:
  deep_value_strategy.run_backtest(2015-01-01, 2025-12-31)
    → 每个调仓日 (每13周):
        → DataSkill.screen_stocks(
            price_to_ma250w < 0.9,
            dividend_yield > 3.5,
            pe_ttm > 0, pe_ttm < 40
          )
        → 过滤 ST/停牌/退市
        → 等权分配资金买入
        → 持有满52周卖出
```

### 2.3 回测集成

- 通过事件驱动引擎 `BacktestRunner.run(config)` 或独立回测方法
- 输出: 年化收益、最大回撤、夏普比率、胜率、逐年收益、持仓明细 CSV

---

## 任务列表

| # | 任务 | 文件 | 优先级 |
|---|------|------|--------|
| P1 | 新建 `klines_weekly` 表 | `db/clickhouse.py` | 1 |
| P2 | 新增 `kline_weekly` 同步方法 | `services/sync_service.py` | 1 |
| P3 | 修复 `dividend_yield` 指标 | `services/sync_service.py`, `indicators/valuation.py` | 1 |
| P4 | 新增 `ma250_weekly` 指标 | `indicators/technical.py` | 2 |
| P5 | 新增 `price_to_ma250w` 指标 | `indicators/valuation.py` | 2 |
| P6 | 补充 `pe_ttm` 历史截面数据（如需） | `indicators/scheduler.py` | 2 |
| P7 | 编写 `DeepValueStrategy` 策略类 | `strategies/deep_value.py` | 3 |
| P8 | 策略回测 & 结果验证 | — | 3 |
