# GaoshouPlatform 数据技能 (DataSkill)

> 为策略模块提供统一数据访问接口，策略只需调用 DataSkill 方法，无需关心数据来自 QMT/SQLite/ClickHouse。

---

## 架构

```
策略模块
    │
    ▼
 DataSkill (统一接口)
    │
    ├─ SQLite  (stocks / financial_data / watchlist)  ← 优先
    │
    ├─ ClickHouse (klines_daily / klines_minute / stock_indicators)  ← 优先
    │
    └─ QMT Gateway (xtquant)  ← 兜底，本地无数据时实时请求
```

**数据优先级**：本地数据库 → QMT 实时请求

---

## 数据类

### StockSnapshot — 股票截面快照

```python
@dataclass
class StockSnapshot:
    symbol: str                    # 股票代码 如 '600051.SH'
    name: str                      # 股票名称
    exchange: str | None           # 交易所 SH/SZ/BJ
    industry: str | None           # 申万一级行业
    sector: str | None             # 板块分类
    list_date: date | None         # 上市日期
    is_st: int                     # ST状态 0/1/2
    is_suspend: int                # 是否停牌 0/1
    total_shares: float | None    # 总股本(万股)
    float_shares: float | None    # 流通股本(万股)
    a_float_shares: float | None  # A股流通股本(万股)
    total_mv: float | None        # 总市值(万元)
    circ_mv: float | None         # 流通市值(万元)
    eps: float | None              # 每股收益(元)
    bvps: float | None            # 每股净资产(元)
    roe: float | None             # 净资产收益率(%)
    pe_ttm: float | None          # 市盈率TTM
    pb: float | None              # 市净率
    revenue: float | None          # 营业收入(万元)
    net_profit: float | None      # 净利润(万元)
    total_assets: float | None    # 总资产(万元)
    total_liability: float | None  # 总负债(万元)
    total_equity: float | None    # 股东权益(万元)
    report_date: date | None      # 最新报告期
    report_type: str | None        # Q1/H1/Q3/Annual
```

### KlineBar — 单根K线

```python
@dataclass
class KlineBar:
    symbol: str
    datetime: date | datetime     # 日期或日期时间
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    turnover: float | None       # 换手率
    change_pct: float | None     # 涨跌幅
```

### FinancialReport — 单季度财务报告

```python
@dataclass
class FinancialReport:
    symbol: str
    report_date: date
    report_type: str | None       # Q1/H1/Q3/Annual
    eps: float | None
    bvps: float | None
    roe: float | None
    revenue: float | None
    net_profit: float | None
    revenue_yoy: float | None    # 营收同比增长率(%)
    profit_yoy: float | None    # 净利润同比增长率(%)
    gross_margin: float | None   # 毛利率(%)
    total_assets: float | None
    total_liability: float | None
    total_equity: float | None
    total_shares: float | None
    float_shares: float | None
    pe_ttm: float | None
    pb: float | None
```

### ScreenResult — 选股筛选结果

```python
@dataclass
class ScreenResult:
    stocks: list[StockSnapshot]
    total: int
```

---

## 方法一览

| 方法 | 参数 | 返回 | 数据源 | 说明 |
|------|------|------|--------|------|
| `get_stock` | `symbol` | `StockSnapshot \| None` | SQLite → QMT | 单只股票快照 |
| `get_stocks` | `symbols: list[str]` | `dict[str, StockSnapshot]` | SQLite → QMT | 批量获取 |
| `screen_stocks` | `industry/exchange/is_st/min_mv/max_mv/min_pe/max_pe/min_roe/limit` | `ScreenResult` | SQLite | 条件选股 |
| `get_kline_daily` | `symbol, start_date?, end_date?, limit=500` | `list[KlineBar]` | ClickHouse → QMT | 日K线 |
| `get_kline_minute` | `symbol, start_date?, end_date?, limit=500` | `list[KlineBar]` | ClickHouse → QMT | 分钟K线 |
| `get_financial` | `symbol, report_count=8` | `list[FinancialReport]` | SQLite → QMT | 多季度财务 |
| `get_financial_batch` | `symbols, report_count=1` | `dict[str, list[FinancialReport]]` | SQLite → QMT | 批量财务 |
| `get_realtime_quote` | `symbol` | `dict \| None` | QMT | 单只实时行情 |
| `get_realtime_quotes` | `symbols` | `list[dict]` | QMT | 批量实时行情 |
| `get_industries` | — | `list[IndustryInfo]` | SQLite | 行业统计 |
| `get_all_symbols` | — | `list[str]` | SQLite → QMT | 全部A股代码 |
| `get_symbols_by_industry` | `industry` | `list[str]` | SQLite | 按行业筛选代码 |
| `get_indicator` | `symbol, name, trade_date?` | `float \| None` | ClickHouse | 单指标查询 |
| `get_indicators_batch` | `symbols, trade_date?` | `list[dict]` | ClickHouse | 批量指标查询 |

---

## 使用示例

```python
from app.services.data_skill import DataSkill, StockSnapshot, KlineBar, FinancialReport

# 在 API 端点中（通过依赖注入获取 session）
from app.db.sqlite import get_async_session
from sqlalchemy.ext.asyncio import AsyncSession

skill = DataSkill(session)  # session: AsyncSession

# 1. 获取股票快照
snapshot = await skill.get_stock("600051.SH")
print(snapshot.name, snapshot.pe_ttm, snapshot.roe)

# 2. 批量获取
snapshots = await skill.get_stocks(["600051.SH", "000001.SZ"])

# 3. 条件选股：PE 5-30，ROE > 10%，市值 > 100亿
result = await skill.screen_stocks(
    min_pe=5, max_pe=30, min_roe=10,
    min_mv=1_000_000, limit=50  # 万元，100亿 = 1,000,000万
)

# 4. 日K线
from datetime import date
bars = await skill.get_kline_daily("600051.SH", start_date=date(2024, 1, 1))

# 5. 财务数据（最近4个季度）
reports = await skill.get_financial("600051.SH", report_count=4)
for r in reports:
    print(f"{r.report_date} EPS={r.eps} ROE={r.roe}%")

# 6. 实时行情
quote = await skill.get_realtime_quote("600051.SH")

# 7. 行业列表
industries = await skill.get_industries()

# 8. ClickHouse 指标
pe = skill.get_indicator("600051.SH", "pe_ttm", date(2025, 4, 1))
```

---

## API 端点 (`/api/skill/*`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/skill/stock/{symbol}` | 获取股票快照 |
| POST | `/skill/stocks/batch` | 批量获取股票快照 |
| GET | `/skill/screen` | 条件选股筛选 |
| GET | `/skill/kline/daily/{symbol}` | 日K线数据 |
| GET | `/skill/kline/minute/{symbol}` | 分钟K线数据 |
| GET | `/skill/financial/{symbol}` | 财务数据(?report_count=8) |
| POST | `/skill/financial/batch` | 批量财务数据 |
| GET | `/skill/quote/{symbol}` | 实时行情 |
| POST | `/skill/quote/batch` | 批量实时行情 |
| GET | `/skill/industries` | 行业列表及统计 |
| GET | `/skill/symbols` | 全部股票代码(?industry=行业名) |
| GET | `/skill/indicator/{symbol}` | 指标值查询(?name=指标名&trade_date=日期) |

所有端点返回统一格式：`{"code": 0, "data": ...}`

---

## 单位约定

- 市值：**万元**（total_mv, circ_mv）
- 股数：**万股**（total_shares, float_shares）
- 金额：**万元**（revenue, net_profit, total_assets 等）
- 比率：**百分比**（roe, pe_ttm, pb, revenue_yoy 等）

---

## 设计注意事项

1. **所有方法为 async**，QMT 同步调用内部自动通过 `run_in_executor` 包装
2. **返回 dataclass**，不暴露 ORM 模型或 QMT 内部结构
3. **ClickHouse 查询为同步方法**（`get_indicator` / `get_indicators_batch` / `_query_ch_klines`），因为 `clickhouse-driver` 是同步库
4. **本地无数据时自动从 QMT 获取**，但不会回写到数据库（回写由 SyncService 负责）
5. **策略模块直接使用 DataSkill**，不需要了解 SQLite/ClickHouse/QMT 的细节