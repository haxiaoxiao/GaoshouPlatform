# 指标库模块设计

## 目标

构建通用指标引擎，支持股票筛选和因子研究。指标库是纯粹的"定义+计算+存储"层，选股筛选器和因子分析模块作为消费方按需取用。

## 架构方案：轻量级指标引擎

类注册式指标定义 + 混合计算模式 + ClickHouse统一存储 + 调度器集成到SyncService。

## 1. 核心抽象

### IndicatorBase（指标基类）

```python
class IndicatorBase(ABC):
    name: str               # 唯一标识，如 "pe_ttm"
    display_name: str       # 中文显示名，如 "市盈率TTM"
    category: str           # 金融语义分类，如 "valuation"
    tags: list[str]         # 自由标签，如 ["基本面", "估值"]
    data_type: str          # "截面" | "时序"
    is_precomputed: bool    # True=同步时预计算, False=查询时实时计算
    dependencies: list[str] # 依赖的其他指标name

    @abstractmethod
    def compute(self, context: IndicatorContext) -> float | None: ...

    def compute_batch(self, symbols: list[str], context: IndicatorContext) -> dict[str, float | None]:
        # 默认逐个调用compute，可覆写优化
        return {s: self.compute(context.with_symbol(s)) for s in symbols}
```

### IndicatorContext（计算上下文）

```python
class IndicatorContext:
    symbol: str
    date: date
    stock_info: StockInfo | None
    kline_data: list[KlineData] | None
    indicator_values: dict[str, float | None]  # 已计算的依赖指标值

    def with_symbol(self, symbol: str) -> 'IndicatorContext': ...
```

### IndicatorRegistry（注册表）

```python
class IndicatorRegistry:
    _indicators: dict[str, type[IndicatorBase]]

    def register(self, cls: type[IndicatorBase]): ...
    def get(self, name: str) -> type[IndicatorBase]: ...
    def by_category(self, category: str) -> list[type[IndicatorBase]]: ...
    def by_data_type(self, data_type: str) -> list[type[IndicatorBase]]: ...
    def all(self) -> list[type[IndicatorBase]]: ...
    # 启动时自动扫描 app/indicators/ 目录
```

## 2. 指标分类体系与初始指标集

### 分类

| category key | 中文名 | 说明 |
|---|---|---|
| valuation | 估值 | PE/PB/PS/股息率 |
| growth | 成长 | 营收增速/利润增速 |
| quality | 质量 | ROE/资产负债率/毛利率 |
| momentum | 动量 | N日涨幅/均线趋势 |
| volatility | 波动 | 波动率/振幅均值 |
| liquidity | 流动性 | 换手率/成交额/自由流通市值 |
| technical | 技术 | MA/MACD/RSI/KDJ |
| theme | 主题 | 业务纯度/产业链定位/营收占比 |
| custom | 自定义 | 用户扩展区 |

### 初始指标集（V1，约20个）

| 分类 | name | display_name | data_type | 计算来源 |
|---|---|---|---|---|
| valuation | pe_ttm | 市盈率TTM | 截面 | total_mv / net_profit |
| valuation | pb | 市净率 | 截面 | total_mv / total_equity |
| valuation | ps_ttm | 市销率TTM | 截面 | total_mv / revenue |
| valuation | dividend_yield | 股息率 | 截面 | 外部/估算 |
| growth | revenue_growth | 营收增速 | 截面 | 同比财务数据 |
| growth | profit_growth | 净利润增速 | 截面 | 同比财务数据 |
| quality | roe | 净资产收益率 | 截面 | Stock.roe |
| quality | debt_ratio | 资产负债率 | 截面 | total_liability / total_assets |
| quality | gross_margin | 毛利率 | 截面 | 毛利数据 |
| momentum | return_5d | 5日涨幅 | 时序 | K线close计算 |
| momentum | return_20d | 20日涨幅 | 时序 | K线close计算 |
| momentum | return_60d | 60日涨幅 | 时序 | K线close计算 |
| momentum | ma5_slope | 5日均线斜率 | 时序 | MA5线性回归斜率 |
| volatility | volatility_20d | 20日波动率 | 时序 | 日收益率标准差 |
| volatility | avg_amplitude | 平均振幅 | 时序 | amplitude均值 |
| liquidity | turnover_rate | 换手率 | 截面 | volume / a_float_shares |
| liquidity | avg_amount_20d | 20日均成交额 | 时序 | amount均值 |
| liquidity | free_float_mv | 自由流通市值 | 截面 | circ_mv |
| theme | business_purity | 业务纯度 | 截面 | SQLite theme_annotations表(人工标注) |
| theme | chain_position | 产业链定位 | 截面 | SQLite theme_annotations表(人工标注) |
| theme | revenue_ratio | 主题营收占比 | 截面 | SQLite theme_annotations表(人工标注) |

> **theme类指标特殊处理**：V1阶段在SQLite新增theme_annotations表(symbol, theme_name, business_purity, chain_position, revenue_ratio)，通过前端UI人工标注。指标compute时从该表读取标注值。

## 3. 存储层

全部指标存ClickHouse，SQLite只保留基础Stock信息。

### stock_indicators（截面指标）

```sql
CREATE TABLE stock_indicators (
    symbol          String,
    indicator_name  String,
    trade_date      Date,
    value           Nullable(Float64),
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, indicator_name, trade_date);
```

### indicator_timeseries（时序指标）

```sql
CREATE TABLE indicator_timeseries (
    symbol          String,
    indicator_name  String,
    trade_date      Date,
    value           Nullable(Float64),
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, indicator_name, trade_date);
```

### EAV反透视查询

查询时动态拼接SQL将纵表转为宽表，前端选择哪些指标就拼哪些列。

## 4. 计算调度

### IndicatorScheduler

```python
class IndicatorScheduler:
    def run_after_sync(self, sync_type: str, symbols: list[str] | None = None):
        """数据同步后自动触发相关指标计算"""
        if sync_type in ("stock_info", "stock_full", "realtime_mv"):
            self._compute_by_data_type("截面", symbols)
        elif sync_type in ("kline_daily", "kline_minute"):
            self._compute_by_data_type("时序", symbols)

    def _compute_by_data_type(self, data_type: str, symbols: list[str] | None):
        indicators = registry.by_data_type(data_type)
        ordered = self._topo_sort(indicators)  # 按dependencies排序
        for indicator in ordered:
            context = self._build_context(indicator, symbols)
            values = indicator.compute_batch(symbols, context)
            self._save_to_clickhouse(indicator, values)

    def compute_single(self, indicator_name: str, symbols: list[str] | None = None, full_compute: bool = False):
        """手动触发单个/全部指标计算"""

    def _topo_sort(self, indicators) -> list[IndicatorBase]:
        """简单拓扑排序：无依赖的先算，有依赖的后算"""
```

### 集成到SyncService

在 sync_stock_info / sync_kline_daily / sync_kline_minute 完成后调用 scheduler.run_after_sync()。

### 增量 vs 全量

- 增量（默认）：截面指标只算 trade_date=today，时序指标只算最近N天
- 全量：重算所有历史日期

## 5. API

```
GET  /api/indicators/categories           → 分类列表(含指标数)
GET  /api/indicators/list?category=估值    → 指标定义列表
GET  /api/indicators/{name}/description   → 单个指标详情

GET  /api/indicators/query                → 指标值查询
     params: symbols, indicator_names, trade_date, category
     响应: { trade_date, items: [{ symbol, name, indicators: { pe_ttm: 5.2, ... } }] }

POST /api/indicators/compute              → 手动触发计算
     body: { indicator_names, symbols, full_compute }

GET  /api/indicators/screen               → 选股筛选
     params: filters=[{indicator_name, op, value}], trade_date, sort_by, limit
     op: gt/gte/lt/lte/eq/between
     响应: { items: [{ symbol, name, indicator_values }], total }
```

## 6. 前端

### 导航结构

```
侧边栏:
├── 数据管理 (已有)
├── 因子投研    ← 合并入口(替换原"因子研究")
├── 策略回测 (已有)
└── 实盘交易 (待开发)
```

### 因子投研页面（4个Tab）

**Tab1: 指标总览**
- 分类筛选栏
- 指标卡片列表(卡片=display_name/category/tags/类型/计算方式)
- 点击卡片→详情弹窗(描述/计算逻辑/依赖/分布图)

**Tab2: 选股筛选**
- 筛选条件构建器: [指标下拉] [操作符] [值] [删除] × N行
- 排序设置
- 结果表格(symbol + name + 各指标值)

**Tab3: 因子分析**
- 因子选择(从指标库选取)
- 分析设置(回测区间/分组数/IC方式)
- 分析结果(IC/IR/分组收益)

**Tab4: 因子合成**
- 已选因子 + 权重调节
- 合成方式(等权/IC加权/最优化)
- 合成因子表现预览

### 核心关系

指标 = 单个数值定义 + 计算逻辑
因子 = 指标的时间序列化 + 标准化(去极值/中性化/Z-score)

## 7. 文件结构

```
backend/app/
├── indicators/
│   ├── __init__.py          # 注册表初始化+自动发现
│   ├── base.py              # IndicatorBase, IndicatorContext, IndicatorRegistry
│   ├── scheduler.py         # IndicatorScheduler
│   ├── valuation.py         # 估值类指标
│   ├── growth.py            # 成长类指标
│   ├── quality.py           # 质量类指标
│   ├── momentum.py          # 动量类指标
│   ├── volatility.py        # 波动类指标
│   ├── liquidity.py         # 流动性类指标
│   ├── technical.py         # 技术类指标
│   └── theme.py             # 主题类指标
├── api/
│   └── indicator.py         # 指标API路由(新增)
├── services/
│   └── sync_service.py      # 集成scheduler调用(修改)
└── db/
    └── clickhouse.py         # 新增两张指标表DDL(修改)

frontend/src/
├── api/
│   └── indicator.ts         # 指标API封装(新增)
├── views/
│   └── FactorResearch/
│       ├── index.vue         # 主页面改造为4 Tab(修改)
│       ├── IndicatorOverview.vue  # Tab1(新增)
│       ├── StockScreen.vue        # Tab2(新增)
│       ├── FactorAnalysis.vue     # Tab3(改造)
│       └── FactorCompose.vue      # Tab4(改造)
└── router/
    └── index.ts              # 因子投研路由更新(修改)
```
