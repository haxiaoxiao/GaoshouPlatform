# 指标库模块 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建通用指标引擎，支持股票筛选和因子研究，指标和因子合并为"因子投研"模块。

**Architecture:** 类注册式指标定义(IndicatorBase + Registry) + 混合计算模式(预计算+实时) + ClickHouse EAV存储 + IndicatorScheduler集成到SyncService

**Tech Stack:** Python 3.12 / FastAPI / SQLAlchemy / ClickHouse / xtquant (后端), Vue 3 / TypeScript / Element Plus / ECharts (前端)

---

## File Structure

### Backend — New Files

| File | Responsibility |
|------|---------------|
| `backend/app/indicators/__init__.py` | 自动发现并注册所有指标类 |
| `backend/app/indicators/base.py` | IndicatorBase抽象基类, IndicatorContext, IndicatorRegistry |
| `backend/app/indicators/scheduler.py` | IndicatorScheduler调度器，集成到SyncService |
| `backend/app/indicators/valuation.py` | 估值类指标: pe_ttm, pb, ps_ttm, dividend_yield |
| `backend/app/indicators/growth.py` | 成长类指标: revenue_growth, profit_growth |
| `backend/app/indicators/quality.py` | 质量类指标: roe, debt_ratio, gross_margin |
| `backend/app/indicators/momentum.py` | 动量类指标: return_5d, return_20d, return_60d, ma5_slope |
| `backend/app/indicators/volatility.py` | 波动类指标: volatility_20d, avg_amplitude |
| `backend/app/indicators/liquidity.py` | 流动性类指标: turnover_rate, avg_amount_20d, free_float_mv |
| `backend/app/indicators/technical.py` | 技术类指标: ma5, ma10, ma20, rsi_14 |
| `backend/app/indicators/theme.py` | 主题类指标: business_purity, chain_position, revenue_ratio |
| `backend/app/api/indicator.py` | 指标API路由: categories/list/query/compute/screen |

### Backend — Modified Files

| File | Change |
|------|--------|
| `backend/app/db/clickhouse.py` | 新增 stock_indicators + indicator_timeseries 表DDL |
| `backend/app/db/models/stock.py` | 新增 ThemeAnnotation SQLAlchemy模型 |
| `backend/app/main.py` | 注册 indicator router |
| `backend/app/services/sync_service.py` | sync完成后调用scheduler.run_after_sync() |

### Frontend — New Files

| File | Responsibility |
|------|---------------|
| `frontend/src/api/indicator.ts` | 指标API封装 |
| `frontend/src/views/FactorResearch/IndicatorOverview.vue` | Tab1: 指标总览 |
| `frontend/src/views/FactorResearch/StockScreen.vue` | Tab2: 选股筛选 |

### Frontend — Modified Files

| File | Change |
|------|--------|
| `frontend/src/views/FactorResearch/index.vue` | 改造为4 Tab结构 |
| `frontend/src/router/index.ts` | 路由label从"因子研究"改为"因子投研" |

---

### Task 1: IndicatorBase + IndicatorContext + IndicatorRegistry

**Files:**
- Create: `backend/app/indicators/__init__.py`
- Create: `backend/app/indicators/base.py`

- [ ] **Step 1: Create indicators package with base.py**

```python
# backend/app/indicators/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class IndicatorContext:
    """指标计算上下文 - 提供数据访问"""
    symbol: str = ""
    trade_date: date = date.today()
    stock_info: dict[str, Any] = field(default_factory=dict)
    kline_data: list[dict[str, Any]] = field(default_factory=list)
    indicator_values: dict[str, float | None] = field(default_factory=dict)

    def with_symbol(self, symbol: str) -> IndicatorContext:
        return IndicatorContext(
            symbol=symbol,
            trade_date=self.trade_date,
            stock_info=self.stock_info,
            kline_data=self.kline_data,
            indicator_values=self.indicator_values,
        )


class IndicatorBase(ABC):
    """指标基类 - 所有指标必须继承"""

    name: str = ""
    display_name: str = ""
    category: str = ""
    tags: list[str] = []
    data_type: str = "截面"  # "截面" | "时序"
    is_precomputed: bool = True
    dependencies: list[str] = []
    description: str = ""

    @abstractmethod
    def compute(self, context: IndicatorContext) -> float | None:
        """计算单个股票的指标值"""

    def compute_batch(
        self, symbols: list[str], context: IndicatorContext
    ) -> dict[str, float | None]:
        """批量计算，默认逐个调用compute"""
        results: dict[str, float | None] = {}
        for symbol in symbols:
            ctx = context.with_symbol(symbol)
            results[symbol] = self.compute(ctx)
        return results

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.name:  # 跳过name为空的中间子类
            IndicatorRegistry.register(cls)


class IndicatorRegistry:
    """指标注册表"""
    _indicators: dict[str, type[IndicatorBase]] = {}

    @classmethod
    def register(cls, indicator_cls: type[IndicatorBase]) -> None:
        name = indicator_cls.name
        if name in cls._indicators:
            raise ValueError(f"指标 '{name}' 已注册，不允许重复")
        cls._indicators[name] = indicator_cls

    @classmethod
    def get(cls, name: str) -> type[IndicatorBase] | None:
        return cls._indicators.get(name)

    @classmethod
    def by_category(cls, category: str) -> list[type[IndicatorBase]]:
        return [v for v in cls._indicators.values() if v.category == category]

    @classmethod
    def by_data_type(cls, data_type: str) -> list[type[IndicatorBase]]:
        return [v for v in cls._indicators.values() if v.data_type == data_type]

    @classmethod
    def all(cls) -> list[type[IndicatorBase]]:
        return list(cls._indicators.values())

    @classmethod
    def categories(cls) -> dict[str, int]:
        result: dict[str, int] = {}
        for ind in cls._indicators.values():
            result[ind.category] = result.get(ind.category, 0) + 1
        return result

    @classmethod
    def clear(cls) -> None:
        cls._indicators.clear()
```

- [ ] **Step 2: Create __init__.py with auto-discovery**

```python
# backend/app/indicators/__init__.py
"""指标库 - 自动发现并注册所有指标"""
import importlib
import pkgutil


def auto_discover() -> None:
    """扫描indicators包下所有模块，触发IndicatorBase.__init_subclass__注册"""
    package_dir = __path__
    for _, module_name, _ in pkgutil.iter_modules(package_dir):
        if module_name.startswith("_"):
            continue
        importlib.import_module(f".{module_name}", __package__)


# 首次import时自动发现
auto_discover()
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/indicators/
git commit -m "feat: add indicator base classes and registry"
```

---

### Task 2: ClickHouse指标表 + SQLite主题标注表

**Files:**
- Modify: `backend/app/db/clickhouse.py` (新增2张表DDL)
- Modify: `backend/app/db/models/stock.py` (新增ThemeAnnotation模型)

- [ ] **Step 1: Add ClickHouse indicator tables**

在 `backend/app/db/clickhouse.py` 的 `_ensure_tables` 方法中追加：

```python
# stock_indicators (截面指标 EAV)
client.execute("""
    CREATE TABLE IF NOT EXISTS stock_indicators (
        symbol String,
        indicator_name String,
        trade_date Date,
        value Nullable(Float64),
        updated_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(trade_date)
    ORDER BY (symbol, indicator_name, trade_date)
""")

# indicator_timeseries (时序指标 EAV)
client.execute("""
    CREATE TABLE IF NOT EXISTS indicator_timeseries (
        symbol String,
        indicator_name String,
        trade_date Date,
        value Nullable(Float64),
        updated_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(trade_date)
    ORDER BY (symbol, indicator_name, trade_date)
""")
```

- [ ] **Step 2: Add ThemeAnnotation SQLAlchemy model**

在 `backend/app/db/models/stock.py` 文件末尾追加：

```python
class ThemeAnnotation(Base):
    """主题标注 - 人工标注的股票主题属性"""
    __tablename__ = "theme_annotations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    theme_name = Column(String(100), nullable=False)
    business_purity = Column(Float, nullable=True)     # 业务纯度 0~1
    chain_position = Column(String(20), nullable=True)  # 上游/中游/下游
    revenue_ratio = Column(Float, nullable=True)        # 主题营收占比 0~1
    notes = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("symbol", "theme_name", name="uq_symbol_theme"),
    )
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/db/clickhouse.py backend/app/db/models/stock.py
git commit -m "feat: add indicator ClickHouse tables and ThemeAnnotation model"
```

---

### Task 3: IndicatorScheduler

**Files:**
- Create: `backend/app/indicators/scheduler.py`
- Modify: `backend/app/services/sync_service.py` (集成scheduler调用)

- [ ] **Step 1: Create scheduler.py**

```python
# backend/app/indicators/scheduler.py
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.db.clickhouse import get_ch_client
from app.indicators.base import IndicatorContext, IndicatorRegistry


class IndicatorScheduler:
    """指标计算调度器"""

    def run_after_sync(
        self,
        sync_type: str,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> None:
        """数据同步后自动触发相关指标计算"""
        if sync_type in ("stock_info", "stock_full", "realtime_mv"):
            self._compute_by_data_type("截面", symbols, trade_date)
        elif sync_type in ("kline_daily", "kline_minute"):
            self._compute_by_data_type("时序", symbols, trade_date)

    def compute_indicators(
        self,
        indicator_names: list[str] | None = None,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
        full_compute: bool = False,
    ) -> dict[str, int]:
        """手动触发指标计算，返回 {indicator_name: computed_count}"""
        if indicator_names:
            indicators = [
                IndicatorRegistry.get(n) for n in indicator_names
            ]
            indicators = [i for i in indicators if i is not None]
        else:
            indicators = IndicatorRegistry.all()

        if not indicators:
            return {}

        results: dict[str, int] = {}
        ordered = self._topo_sort(indicators)

        for indicator_cls in ordered:
            indicator = indicator_cls()
            context = self._build_context(indicator, symbols, trade_date)
            computed = indicator.compute_batch(
                symbols or self._get_all_symbols(), context
            )
            self._save_results(indicator, computed, trade_date)
            results[indicator.name] = len([v for v in computed.values() if v is not None])

        return results

    def _compute_by_data_type(
        self,
        data_type: str,
        symbols: list[str] | None,
        trade_date: date | None,
    ) -> None:
        indicators = IndicatorRegistry.by_data_type(data_type)
        if not indicators:
            return
        ordered = self._topo_sort(indicators)
        target_symbols = symbols or self._get_all_symbols()
        target_date = trade_date or date.today()

        for indicator_cls in ordered:
            try:
                indicator = indicator_cls()
                context = self._build_context(indicator, target_symbols, target_date)
                computed = indicator.compute_batch(target_symbols, context)
                self._save_results(indicator, computed, target_date)
            except Exception as e:
                print(f"Indicator {indicator_cls.name} compute failed: {e}")

    def _topo_sort(
        self, indicators: list[type]
    ) -> list[type]:
        """简单拓扑排序：无依赖的先算，有依赖的后算"""
        name_set = {i.name for i in indicators}
        name_to_cls = {i.name: i for i in indicators}

        ordered: list[type] = []
        remaining = list(indicators)

        while remaining:
            ready = [
                i for i in remaining
                if not any(d in name_set and d != i.name for d in i.dependencies)
                or all(d not in name_set for d in i.dependencies)
            ]
            if not ready:
                ordered.extend(remaining)
                break
            ordered.extend(ready)
            remaining = [i for i in remaining if i not in ready]

        return ordered

    def _build_context(
        self,
        indicator_cls: type,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> IndicatorContext:
        """构建计算上下文"""
        return IndicatorContext(trade_date=trade_date or date.today())

    def _get_all_symbols(self) -> list[str]:
        """从SQLite获取所有股票代码"""
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings

        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        with engine.connect() as conn:
            result = conn.execute(select(Stock.symbol))
            symbols = [row[0] for row in result.all()]
        engine.dispose()
        return symbols

    def _save_results(
        self,
        indicator: Any,
        results: dict[str, float | None],
        trade_date: date | None = None,
    ) -> None:
        """保存计算结果到ClickHouse"""
        ch = get_ch_client()
        table = "stock_indicators" if indicator.data_type == "截面" else "indicator_timeseries"
        target_date = trade_date or date.today()

        rows = [
            {
                "symbol": symbol,
                "indicator_name": indicator.name,
                "trade_date": target_date,
                "value": value,
                "updated_at": datetime.now(),
            }
            for symbol, value in results.items()
            if value is not None
        ]

        if rows:
            ch.insert(table, rows)


indicator_scheduler = IndicatorScheduler()
```

- [ ] **Step 2: Integrate scheduler into sync_service.py**

在 `backend/app/services/sync_service.py` 顶部导入区追加：

```python
from app.indicators.scheduler import indicator_scheduler
```

在每个sync方法的完成处（`progress.status = "completed"` 之前）追加调度调用。找到 `sync_stock_info` 方法中 `progress.status = "completed"` 行前加：

```python
indicator_scheduler.run_after_sync("stock_info", symbols=symbols, trade_date=date.today())
```

找到 `sync_kline_daily` 方法中 `progress.status = "completed"` 行前加：

```python
indicator_scheduler.run_after_sync("kline_daily", symbols=symbols, trade_date=end_date)
```

找到 `sync_kline_minute` 方法中 `progress.status = "completed"` 行前加：

```python
indicator_scheduler.run_after_sync("kline_minute", symbols=symbols, trade_date=end_date)
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/indicators/scheduler.py backend/app/services/sync_service.py
git commit -m "feat: add indicator scheduler and integrate with sync service"
```

---

### Task 4: 具体指标实现 — 估值+成长+质量

**Files:**
- Create: `backend/app/indicators/valuation.py`
- Create: `backend/app/indicators/growth.py`
- Create: `backend/app/indicators/quality.py`

- [ ] **Step 1: Create valuation.py**

```python
# backend/app/indicators/valuation.py
"""估值类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class PETTM(IndicatorBase):
    name = "pe_ttm"
    display_name = "市盈率TTM"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 净利润(TTM)"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_mv = info.get("total_mv") or info.get("totalValue")
        net_profit = info.get("net_profit")
        if total_mv and net_profit and net_profit != 0:
            return round(total_mv / net_profit, 4)
        return None


class PB(IndicatorBase):
    name = "pb"
    display_name = "市净率"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 净资产"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_mv = info.get("total_mv") or info.get("totalValue")
        total_equity = info.get("total_equity")
        if total_mv and total_equity and total_equity != 0:
            return round(total_mv / total_equity, 4)
        return None


class PSTTM(IndicatorBase):
    name = "ps_ttm"
    display_name = "市销率TTM"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 营业收入(TTM)"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_mv = info.get("total_mv") or info.get("totalValue")
        revenue = info.get("revenue")
        if total_mv and revenue and revenue != 0:
            return round(total_mv / revenue, 4)
        return None


class DividendYield(IndicatorBase):
    name = "dividend_yield"
    display_name = "股息率"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "近12个月分红 / 总市值"

    def compute(self, context: IndicatorContext) -> float | None:
        # V1: 暂无分红数据源，返回None
        return None
```

- [ ] **Step 2: Create growth.py**

```python
# backend/app/indicators/growth.py
"""成长类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class RevenueGrowth(IndicatorBase):
    name = "revenue_growth"
    display_name = "营收增速"
    category = "growth"
    tags = ["成长", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "营业收入同比增长率"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        revenue_growth = info.get("revenue_growth")
        if revenue_growth is not None:
            return round(float(revenue_growth), 4)
        return None


class ProfitGrowth(IndicatorBase):
    name = "profit_growth"
    display_name = "净利润增速"
    category = "growth"
    tags = ["成长", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "净利润同比增长率"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        profit_growth = info.get("profit_growth")
        if profit_growth is not None:
            return round(float(profit_growth), 4)
        return None
```

- [ ] **Step 3: Create quality.py**

```python
# backend/app/indicators/quality.py
"""质量类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class ROE(IndicatorBase):
    name = "roe"
    display_name = "净资产收益率"
    category = "quality"
    tags = ["质量", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "净利润 / 净资产"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        roe = info.get("roe")
        if roe is not None:
            return round(float(roe), 4)
        return None


class DebtRatio(IndicatorBase):
    name = "debt_ratio"
    display_name = "资产负债率"
    category = "quality"
    tags = ["质量", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总负债 / 总资产"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_liability = info.get("total_liability")
        total_assets = info.get("total_assets")
        if total_liability is not None and total_assets and total_assets != 0:
            return round(float(total_liability) / float(total_assets), 4)
        return None


class GrossMargin(IndicatorBase):
    name = "gross_margin"
    display_name = "毛利率"
    category = "quality"
    tags = ["质量", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "(营业收入-营业成本) / 营业收入"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        gross_margin = info.get("gross_margin")
        if gross_margin is not None:
            return round(float(gross_margin), 4)
        return None
```

- [ ] **Step 4: Commit**

```bash
git add backend/app/indicators/valuation.py backend/app/indicators/growth.py backend/app/indicators/quality.py
git commit -m "feat: add valuation, growth, quality indicators"
```

---

### Task 5: 具体指标实现 — 动量+波动+流动性

**Files:**
- Create: `backend/app/indicators/momentum.py`
- Create: `backend/app/indicators/volatility.py`
- Create: `backend/app/indicators/liquidity.py`

- [ ] **Step 1: Create momentum.py**

```python
# backend/app/indicators/momentum.py
"""动量类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


def _calc_return(kline_data: list[dict], n: int) -> float | None:
    """计算N日涨幅"""
    if not kline_data or len(kline_data) < 2:
        return None
    # kline_data按日期降序(最新在前)
    if len(kline_data) <= n:
        latest = kline_data[0].get("close", 0)
        oldest = kline_data[-1].get("close", 0)
    else:
        latest = kline_data[0].get("close", 0)
        oldest = kline_data[n].get("close", 0)
    if oldest == 0:
        return None
    return round((latest - oldest) / oldest, 4)


class Return5d(IndicatorBase):
    name = "return_5d"
    display_name = "5日涨幅"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近5个交易日涨幅"

    def compute(self, context: IndicatorContext) -> float | None:
        return _calc_return(context.kline_data, 5)


class Return20d(IndicatorBase):
    name = "return_20d"
    display_name = "20日涨幅"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20个交易日涨幅"

    def compute(self, context: IndicatorContext) -> float | None:
        return _calc_return(context.kline_data, 20)


class Return60d(IndicatorBase):
    name = "return_60d"
    display_name = "60日涨幅"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近60个交易日涨幅"

    def compute(self, context: IndicatorContext) -> float | None:
        return _calc_return(context.kline_data, 60)


class MA5Slope(IndicatorBase):
    name = "ma5_slope"
    display_name = "5日均线斜率"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "MA5线性回归斜率"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:5] if len(context.kline_data) >= 5 else context.kline_data
        if len(data) < 3:
            return None
        closes = [d.get("close", 0) for d in reversed(data)]
        if any(c == 0 for c in closes):
            return None
        # 简单线性回归斜率
        n = len(closes)
        x_mean = (n - 1) / 2
        y_mean = sum(closes) / n
        numerator = sum((i - x_mean) * (c - y_mean) for i, c in enumerate(closes))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        if denominator == 0:
            return None
        slope = numerator / denominator
        # 归一化为百分比
        return round(slope / y_mean * 100, 4)
```

- [ ] **Step 2: Create volatility.py**

```python
# backend/app/indicators/volatility.py
"""波动类指标"""
import math

from app.indicators.base import IndicatorBase, IndicatorContext


class Volatility20d(IndicatorBase):
    name = "volatility_20d"
    display_name = "20日波动率"
    category = "volatility"
    tags = ["波动", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20日日收益率标准差(年化)"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if len(data) < 5:
            return None
        closes = [d.get("close", 0) for d in reversed(data)]
        if any(c == 0 for c in closes):
            return None
        # 计算日收益率
        returns = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
        ]
        if not returns:
            return None
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        # 年化
        return round(math.sqrt(variance) * math.sqrt(252), 4)


class AvgAmplitude(IndicatorBase):
    name = "avg_amplitude"
    display_name = "平均振幅"
    category = "volatility"
    tags = ["波动", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20日振幅均值"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if not data:
            return None
        amplitudes = []
        for d in data:
            high = d.get("high", 0)
            low = d.get("low", 0)
            prev_close = d.get("prev_close", 0) or d.get("open", 0)
            if prev_close > 0:
                amplitudes.append((high - low) / prev_close)
        if not amplitudes:
            return None
        return round(sum(amplitudes) / len(amplitudes), 4)
```

- [ ] **Step 3: Create liquidity.py**

```python
# backend/app/indicators/liquidity.py
"""流动性类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class TurnoverRate(IndicatorBase):
    name = "turnover_rate"
    display_name = "换手率"
    category = "liquidity"
    tags = ["流动性", "行情"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "成交量 / 流通股本"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        # 优先用QMT提供的换手率
        turnover = info.get("turnover")
        if turnover is not None:
            return round(float(turnover), 4)
        # 否则从K线和股本计算
        volume = info.get("volume")
        float_shares = info.get("a_float_shares") or info.get("float_shares")
        if volume and float_shares and float_shares != 0:
            return round(float(volume) / float(float_shares), 4)
        return None


class AvgAmount20d(IndicatorBase):
    name = "avg_amount_20d"
    display_name = "20日均成交额"
    category = "liquidity"
    tags = ["流动性", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20日成交额均值(万元)"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if not data:
            return None
        amounts = [d.get("amount", 0) for d in data]
        valid = [a for a in amounts if a > 0]
        if not valid:
            return None
        return round(sum(valid) / len(valid), 2)


class FreeFloatMV(IndicatorBase):
    name = "free_float_mv"
    display_name = "自由流通市值"
    category = "liquidity"
    tags = ["流动性", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "流通市值(万元)"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        circ_mv = info.get("circ_mv") or info.get("floatValue")
        if circ_mv is not None:
            return round(float(circ_mv), 2)
        return None
```

- [ ] **Step 4: Commit**

```bash
git add backend/app/indicators/momentum.py backend/app/indicators/volatility.py backend/app/indicators/liquidity.py
git commit -m "feat: add momentum, volatility, liquidity indicators"
```

---

### Task 6: 具体指标实现 — 技术+主题

**Files:**
- Create: `backend/app/indicators/technical.py`
- Create: `backend/app/indicators/theme.py`

- [ ] **Step 1: Create technical.py**

```python
# backend/app/indicators/technical.py
"""技术类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class MA5(IndicatorBase):
    name = "ma5"
    display_name = "5日均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False  # 实时计算
    dependencies = []
    description = "5日移动平均线"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:5]
        if len(data) < 5:
            return None
        closes = [d.get("close", 0) for d in data]
        if any(c == 0 for c in closes):
            return None
        return round(sum(closes) / len(closes), 4)


class MA10(IndicatorBase):
    name = "ma10"
    display_name = "10日均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "10日移动平均线"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:10]
        if len(data) < 10:
            return None
        closes = [d.get("close", 0) for d in data]
        if any(c == 0 for c in closes):
            return None
        return round(sum(closes) / len(closes), 4)


class MA20(IndicatorBase):
    name = "ma20"
    display_name = "20日均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "20日移动平均线"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if len(data) < 20:
            return None
        closes = [d.get("close", 0) for d in data]
        if any(c == 0 for c in closes):
            return None
        return round(sum(closes) / len(closes), 4)


class RSI14(IndicatorBase):
    name = "rsi_14"
    display_name = "14日RSI"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "14日相对强弱指标"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:15]  # 需要15条数据算14个变化
        if len(data) < 15:
            return None
        closes = [d.get("close", 0) for d in reversed(data)]
        if any(c == 0 for c in closes):
            return None
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [c for c in changes if c > 0]
        losses = [-c for c in changes if c < 0]
        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 4)
```

- [ ] **Step 2: Create theme.py**

```python
# backend/app/indicators/theme.py
"""主题类指标 - 从SQLite theme_annotations表读取人工标注"""
from app.indicators.base import IndicatorBase, IndicatorContext


def _get_theme_annotation(symbol: str, theme_name: str | None = None) -> dict | None:
    """从SQLite读取主题标注"""
    from sqlalchemy import create_engine, select
    from app.db.models.stock import ThemeAnnotation
    from app.core.config import settings

    sync_url = settings.database_url.replace("+aiosqlite", "")
    engine = create_engine(sync_url)
    try:
        with engine.connect() as conn:
            query = select(ThemeAnnotation).where(ThemeAnnotation.symbol == symbol)
            if theme_name:
                query = query.where(ThemeAnnotation.theme_name == theme_name)
            result = conn.execute(query)
            row = result.first()
            if row:
                return {
                    "business_purity": row.business_purity,
                    "chain_position": row.chain_position,
                    "revenue_ratio": row.revenue_ratio,
                }
    finally:
        engine.dispose()
    return None


class BusinessPurity(IndicatorBase):
    name = "business_purity"
    display_name = "业务纯度"
    category = "theme"
    tags = ["主题", "人工标注"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "股票与主题的相关程度(0~1)，人工标注"

    def compute(self, context: IndicatorContext) -> float | None:
        annotation = _get_theme_annotation(context.symbol)
        if annotation and annotation["business_purity"] is not None:
            return round(annotation["business_purity"], 4)
        return None


class ChainPosition(IndicatorBase):
    name = "chain_position"
    display_name = "产业链定位"
    category = "theme"
    tags = ["主题", "人工标注"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "股票在产业链中的位置(上游/中游/下游)，人工标注"

    def compute(self, context: IndicatorContext) -> float | None:
        annotation = _get_theme_annotation(context.symbol)
        if annotation and annotation["chain_position"]:
            mapping = {"上游": 1, "中游": 2, "下游": 3}
            return float(mapping.get(annotation["chain_position"], 0))
        return None


class RevenueRatio(IndicatorBase):
    name = "revenue_ratio"
    display_name = "主题营收占比"
    category = "theme"
    tags = ["主题", "人工标注"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "主题相关业务营收占总营收比重(0~1)，人工标注"

    def compute(self, context: IndicatorContext) -> float | None:
        annotation = _get_theme_annotation(context.symbol)
        if annotation and annotation["revenue_ratio"] is not None:
            return round(annotation["revenue_ratio"], 4)
        return None
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/indicators/technical.py backend/app/indicators/theme.py
git commit -m "feat: add technical and theme indicators"
```

---

### Task 7: 指标API路由

**Files:**
- Create: `backend/app/api/indicator.py`
- Modify: `backend/app/main.py` (注册路由)

- [ ] **Step 1: Create indicator.py**

```python
# backend/app/api/indicator.py
"""指标库API路由"""
from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.db.clickhouse import get_ch_client
from app.indicators.base import IndicatorRegistry

router = APIRouter(prefix="/api/indicators", tags=["indicators"])

# === 分类中文映射 ===
CATEGORY_LABELS: dict[str, str] = {
    "valuation": "估值",
    "growth": "成长",
    "quality": "质量",
    "momentum": "动量",
    "volatility": "波动",
    "liquidity": "流动性",
    "technical": "技术",
    "theme": "主题",
    "custom": "自定义",
}


# === 响应模型 ===

class CategoryInfo(BaseModel):
    key: str
    label: str
    count: int


class IndicatorInfo(BaseModel):
    name: str
    display_name: str
    category: str
    category_label: str
    tags: list[str]
    data_type: str
    is_precomputed: bool
    dependencies: list[str]
    description: str


class IndicatorValueItem(BaseModel):
    symbol: str
    name: str | None = None
    indicators: dict[str, float | None]


class QueryResponse(BaseModel):
    trade_date: str
    items: list[IndicatorValueItem]


class ComputeRequest(BaseModel):
    indicator_names: list[str] | None = None
    symbols: list[str] | None = None
    full_compute: bool = False


class ComputeResponse(BaseModel):
    results: dict[str, int]
    message: str


class ScreenFilter(BaseModel):
    indicator_name: str
    op: str  # gt/gte/lt/lte/eq/between
    value: float | list[float]


class ScreenRequest(BaseModel):
    filters: list[ScreenFilter]
    trade_date: str | None = None
    sort_by: str | None = None
    sort_order: str = "desc"
    limit: int = 100


# === API端点 ===

@router.get("/categories", response_model=list[CategoryInfo])
async def get_categories():
    """获取指标分类列表"""
    counts = IndicatorRegistry.categories()
    result = []
    for key, label in CATEGORY_LABELS.items():
        if key in counts:
            result.append(CategoryInfo(key=key, label=label, count=counts[key]))
    # 追加未在映射中的分类
    for key, count in counts.items():
        if key not in CATEGORY_LABELS:
            result.append(CategoryInfo(key=key, label=key, count=count))
    return result


@router.get("/list", response_model=list[IndicatorInfo])
async def list_indicators(category: str | None = None):
    """获取指标定义列表"""
    if category:
        indicators = IndicatorRegistry.by_category(category)
    else:
        indicators = IndicatorRegistry.all()
    return [
        IndicatorInfo(
            name=i.name,
            display_name=i.display_name,
            category=i.category,
            category_label=CATEGORY_LABELS.get(i.category, i.category),
            tags=i.tags,
            data_type=i.data_type,
            is_precomputed=i.is_precomputed,
            dependencies=i.dependencies,
            description=i.description,
        )
        for i in indicators
    ]


@router.get("/{name}/description", response_model=IndicatorInfo)
async def get_indicator_description(name: str):
    """获取单个指标详情"""
    cls = IndicatorRegistry.get(name)
    if not cls:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"指标 '{name}' 不存在")
    return IndicatorInfo(
        name=cls.name,
        display_name=cls.display_name,
        category=cls.category,
        category_label=CATEGORY_LABELS.get(cls.category, cls.category),
        tags=cls.tags,
        data_type=cls.data_type,
        is_precomputed=cls.is_precomputed,
        dependencies=cls.dependencies,
        description=cls.description,
    )


@router.get("/query", response_model=QueryResponse)
async def query_indicators(
    symbols: str = Query(..., description="逗号分隔的股票代码"),
    indicator_names: str = Query(..., description="逗号分隔的指标名"),
    trade_date: str | None = None,
):
    """查询指标值"""
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    name_list = [n.strip() for n in indicator_names.split(",") if n.strip()]

    target_date = trade_date or date.today().isoformat()
    ch = get_ch_client()

    # 从stock_indicators和indicator_timeseries分别查询
    all_values: dict[str, dict[str, float | None]] = {
        s: {} for s in symbol_list
    }

    for table in ["stock_indicators", "indicator_timeseries"]:
        names_str = ", ".join(f"'{n}'" for n in name_list)
        symbols_str = ", ".join(f"'{s}'" for s in symbol_list)
        query = f"""
            SELECT symbol, indicator_name, value
            FROM {table}
            WHERE symbol IN ({symbols_str})
              AND indicator_name IN ({names_str})
              AND trade_date = %(trade_date)s
            ORDER BY symbol, indicator_name
        """
        try:
            rows = ch.execute(query, {"trade_date": target_date})
            for row in rows:
                sym, ind_name, val = row[0], row[1], row[2]
                if sym in all_values:
                    all_values[sym][ind_name] = val
        except Exception:
            pass

    # 获取股票名称
    stock_names: dict[str, str] = {}
    try:
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings
        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        with engine.connect() as conn:
            result = conn.execute(
                select(Stock.symbol, Stock.name).where(Stock.symbol.in_(symbol_list))
            )
            for row in result.all():
                stock_names[row[0]] = row[1]
        engine.dispose()
    except Exception:
        pass

    items = [
        IndicatorValueItem(
            symbol=s,
            name=stock_names.get(s),
            indicators={n: all_values.get(s, {}).get(n) for n in name_list},
        )
        for s in symbol_list
    ]

    return QueryResponse(trade_date=target_date, items=items)


@router.post("/compute", response_model=ComputeResponse)
async def compute_indicators(request: ComputeRequest):
    """手动触发指标计算"""
    from app.indicators.scheduler import indicator_scheduler
    try:
        results = indicator_scheduler.compute_indicators(
            indicator_names=request.indicator_names,
            symbols=request.symbols,
            full_compute=request.full_compute,
        )
        return ComputeResponse(
            results=results,
            message=f"计算完成，共处理 {len(results)} 个指标",
        )
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"计算失败: {str(e)}")


@router.post("/screen")
async def screen_stocks(request: ScreenRequest):
    """选股筛选"""
    target_date = request.trade_date or date.today().isoformat()
    ch = get_ch_client()

    # 收集所有涉及的指标名
    all_names = list({f.indicator_name for f in request.filters})
    if request.sort_by:
        all_names.append(request.sort_by)
    names_str = ", ".join(f"'{n}'" for n in all_names)

    # 构建WHERE条件
    where_parts: list[str] = []
    params: dict[str, Any] = {"trade_date": target_date}

    for i, f in enumerate(request.filters):
        param_val = f"val_{i}"
        if f.op == "between" and isinstance(f.value, list) and len(f.value) == 2:
            where_parts.append(
                f"indicator_name = '{f.indicator_name}' AND value >= %(val_{i}_low)s AND value <= %(val_{i}_high)s"
            )
            params[f"val_{i}_low"] = f.value[0]
            params[f"val_{i}_high"] = f.value[1]
        elif f.op == "eq":
            where_parts.append(
                f"indicator_name = '{f.indicator_name}' AND value = %({param_val})s"
            )
            params[param_val] = f.value[0] if isinstance(f.value, list) else f.value
        elif f.op in ("gt", "gte", "lt", "lte"):
            op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
            where_parts.append(
                f"indicator_name = '{f.indicator_name}' AND value {op_map[f.op]} %({param_val})s"
            )
            params[param_val] = f.value[0] if isinstance(f.value, list) else f.value

    # 查询两个表，找出满足所有筛选条件的symbol
    matching_symbols: set[str] = set()

    for table in ["stock_indicators", "indicator_timeseries"]:
        for i, f in enumerate(request.filters):
            param_val = f"val_{i}"
            query_params: dict[str, Any] = {"trade_date": target_date}

            if f.op == "between" and isinstance(f.value, list) and len(f.value) == 2:
                cond = f"indicator_name = '{f.indicator_name}' AND value >= %(val_low)s AND value <= %(val_high)s"
                query_params["val_low"] = f.value[0]
                query_params["val_high"] = f.value[1]
            elif f.op == "eq":
                cond = f"indicator_name = '{f.indicator_name}' AND value = %({param_val})s"
                query_params[param_val] = f.value[0] if isinstance(f.value, list) else f.value
            elif f.op in ("gt", "gte", "lt", "lte"):
                op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
                cond = f"indicator_name = '{f.indicator_name}' AND value {op_map[f.op]} %({param_val})s"
                query_params[param_val] = f.value[0] if isinstance(f.value, list) else f.value
            else:
                continue

            query = f"""
                SELECT DISTINCT symbol FROM {table}
                WHERE {cond} AND trade_date = %(trade_date)s
            """
            try:
                rows = ch.execute(query, query_params)
                syms = {row[0] for row in rows}
                if i == 0 and table == "stock_indicators":
                    matching_symbols = syms
                else:
                    matching_symbols &= syms
            except Exception:
                matching_symbols = set()

    if not matching_symbols:
        return {"items": [], "total": 0, "trade_date": target_date}

    # 查询匹配股票的指标值
    symbols_str = ", ".join(f"'{s}'" for s in matching_symbols)
    values: dict[str, dict[str, float | None]] = {s: {} for s in matching_symbols}

    for table in ["stock_indicators", "indicator_timeseries"]:
        query = f"""
            SELECT symbol, indicator_name, value
            FROM {table}
            WHERE symbol IN ({symbols_str})
              AND indicator_name IN ({names_str})
              AND trade_date = %(trade_date)s
        """
        try:
            rows = ch.execute(query, {"trade_date": target_date})
            for row in rows:
                sym, ind_name, val = row[0], row[1], row[2]
                if sym in values:
                    values[sym][ind_name] = val
        except Exception:
            pass

    # 排序
    items_list = [
        {"symbol": s, "indicators": values.get(s, {})}
        for s in matching_symbols
    ]
    if request.sort_by:
        reverse = request.sort_order == "desc"
        items_list.sort(
            key=lambda x: x["indicators"].get(request.sort_by) or 0,
            reverse=reverse,
        )

    # 限制数量
    items_list = items_list[: request.limit]

    # 获取股票名称
    stock_names: dict[str, str] = {}
    try:
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings
        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        with engine.connect() as conn:
            result = conn.execute(
                select(Stock.symbol, Stock.name).where(
                    Stock.symbol.in_([i["symbol"] for i in items_list])
                )
            )
            for row in result.all():
                stock_names[row[0]] = row[1]
        engine.dispose()
    except Exception:
        pass

    for item in items_list:
        item["name"] = stock_names.get(item["symbol"])

    return {"items": items_list, "total": len(items_list), "trade_date": target_date}
```

- [ ] **Step 2: Register router in main.py**

在 `backend/app/main.py` 中，找到其他router注册的位置，追加：

```python
from app.api.indicator import router as indicator_router
app.include_router(indicator_router)
```

同时在 `app/lifespan` 或启动逻辑中确保指标自动发现：

```python
# 在lifespan startup中追加
import app.indicators  # 触发auto_discover
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/api/indicator.py backend/app/main.py
git commit -m "feat: add indicator API routes and register in app"
```

---

### Task 8: 前端API封装

**Files:**
- Create: `frontend/src/api/indicator.ts`

- [ ] **Step 1: Create indicator.ts**

```typescript
// frontend/src/api/indicator.ts
import request from './request'

// === 类型定义 ===

export interface CategoryInfo {
  key: string
  label: string
  count: number
}

export interface IndicatorInfo {
  name: string
  display_name: string
  category: string
  category_label: string
  tags: string[]
  data_type: string
  is_precomputed: boolean
  dependencies: string[]
  description: string
}

export interface IndicatorValueItem {
  symbol: string
  name: string | null
  indicators: Record<string, number | null>
}

export interface QueryResponse {
  trade_date: string
  items: IndicatorValueItem[]
}

export interface ComputeRequest {
  indicator_names?: string[]
  symbols?: string[]
  full_compute?: boolean
}

export interface ComputeResponse {
  results: Record<string, number>
  message: string
}

export interface ScreenFilter {
  indicator_name: string
  op: string
  value: number | number[]
}

export interface ScreenRequest {
  filters: ScreenFilter[]
  trade_date?: string
  sort_by?: string
  sort_order?: 'asc' | 'desc'
  limit?: number
}

export interface ScreenResult {
  items: Array<{
    symbol: string
    name: string | null
    indicators: Record<string, number | null>
  }>
  total: number
  trade_date: string
}

// === API 调用 ===

export const indicatorApi = {
  getCategories() {
    return request.get<CategoryInfo[]>('/indicators/categories')
  },

  listIndicators(category?: string) {
    return request.get<IndicatorInfo[]>('/indicators/list', {
      params: category ? { category } : {},
    })
  },

  getIndicatorDescription(name: string) {
    return request.get<IndicatorInfo>(`/indicators/${name}/description`)
  },

  queryIndicators(params: {
    symbols: string[]
    indicator_names: string[]
    trade_date?: string
  }) {
    return request.get<QueryResponse>('/indicators/query', {
      params: {
        symbols: params.symbols.join(','),
        indicator_names: params.indicator_names.join(','),
        trade_date: params.trade_date,
      },
    })
  },

  computeIndicators(data: ComputeRequest) {
    return request.post<ComputeResponse>('/indicators/compute', data)
  },

  screenStocks(data: ScreenRequest) {
    return request.post<ScreenResult>('/indicators/screen', data)
  },
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/api/indicator.ts
git commit -m "feat: add frontend indicator API client"
```

---

### Task 9: 前端指标总览页面 (Tab1)

**Files:**
- Create: `frontend/src/views/FactorResearch/IndicatorOverview.vue`

- [ ] **Step 1: Create IndicatorOverview.vue**

```vue
<template>
  <div class="indicator-overview">
    <!-- 分类筛选 -->
    <div class="category-filter">
      <el-radio-group v-model="activeCategory" @change="loadIndicators">
        <el-radio-button value="">全部</el-radio-button>
        <el-radio-button
          v-for="cat in categories"
          :key="cat.key"
          :value="cat.key"
        >
          {{ cat.label }} ({{ cat.count }})
        </el-radio-button>
      </el-radio-group>
    </div>

    <!-- 指标卡片网格 -->
    <div v-loading="loading" class="indicator-grid">
      <el-card
        v-for="ind in indicators"
        :key="ind.name"
        class="indicator-card"
        shadow="hover"
        @click="showDetail(ind)"
      >
        <div class="card-title">{{ ind.display_name }}</div>
        <div class="card-meta">
          <el-tag size="small" :type="categoryTagType(ind.category)">
            {{ ind.category_label }}
          </el-tag>
          <el-tag size="small" type="info" style="margin-left: 6px">
            {{ ind.data_type }}
          </el-tag>
          <el-tag
            v-if="!ind.is_precomputed"
            size="small"
            type="warning"
            style="margin-left: 6px"
          >
            实时
          </el-tag>
        </div>
        <div class="card-desc">{{ ind.description }}</div>
        <div class="card-tags" v-if="ind.tags.length">
          <el-tag
            v-for="tag in ind.tags"
            :key="tag"
            size="small"
            effect="plain"
            style="margin: 2px"
          >
            {{ tag }}
          </el-tag>
        </div>
      </el-card>
    </div>

    <!-- 指标详情弹窗 -->
    <el-dialog
      v-model="detailVisible"
      :title="detailData?.display_name"
      width="500px"
    >
      <el-descriptions :column="1" border v-if="detailData">
        <el-descriptions-item label="标识">{{ detailData.name }}</el-descriptions-item>
        <el-descriptions-item label="分类">{{ detailData.category_label }}</el-descriptions-item>
        <el-descriptions-item label="数据类型">{{ detailData.data_type }}</el-descriptions-item>
        <el-descriptions-item label="计算方式">
          {{ detailData.is_precomputed ? '预计算' : '实时计算' }}
        </el-descriptions-item>
        <el-descriptions-item label="描述">{{ detailData.description }}</el-descriptions-item>
        <el-descriptions-item label="依赖" v-if="detailData.dependencies.length">
          {{ detailData.dependencies.join(', ') }}
        </el-descriptions-item>
        <el-descriptions-item label="标签">
          <el-tag v-for="tag in detailData.tags" :key="tag" size="small" style="margin: 2px">{{ tag }}</el-tag>
        </el-descriptions-item>
      </el-descriptions>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { indicatorApi, type CategoryInfo, type IndicatorInfo } from '@/api/indicator'

const loading = ref(false)
const categories = ref<CategoryInfo[]>([])
const indicators = ref<IndicatorInfo[]>([])
const activeCategory = ref('')
const detailVisible = ref(false)
const detailData = ref<IndicatorInfo | null>(null)

const categoryTagType = (category: string): '' | 'success' | 'warning' | 'danger' | 'info' => {
  const map: Record<string, '' | 'success' | 'warning' | 'danger' | 'info'> = {
    valuation: 'danger',
    growth: 'success',
    quality: '',
    momentum: 'warning',
    volatility: 'info',
    liquidity: '',
    technical: 'warning',
    theme: 'success',
  }
  return map[category] || 'info'
}

const loadCategories = async () => {
  try {
    categories.value = await indicatorApi.getCategories()
  } catch (e) {
    console.error('Load categories failed:', e)
  }
}

const loadIndicators = async () => {
  loading.value = true
  try {
    indicators.value = await indicatorApi.listIndicators(activeCategory.value || undefined)
  } catch (e) {
    console.error('Load indicators failed:', e)
    indicators.value = []
  } finally {
    loading.value = false
  }
}

const showDetail = (ind: IndicatorInfo) => {
  detailData.value = ind
  detailVisible.value = true
}

onMounted(async () => {
  await loadCategories()
  await loadIndicators()
})
</script>

<style scoped>
.indicator-overview {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.category-filter {
  flex-shrink: 0;
}

.category-filter :deep(.el-radio-group) {
  flex-wrap: wrap;
}

.indicator-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
  gap: 12px;
  flex: 1;
  align-content: start;
}

.indicator-card {
  cursor: pointer;
  transition: transform 0.15s;
}

.indicator-card:hover {
  transform: translateY(-2px);
}

.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 8px;
}

.card-meta {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
}

.card-desc {
  font-size: 12px;
  color: #909399;
  margin-bottom: 8px;
  line-height: 1.5;
}

.card-tags {
  display: flex;
  flex-wrap: wrap;
}
</style>
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/views/FactorResearch/IndicatorOverview.vue
git commit -m "feat: add indicator overview page (Tab1)"
```

---

### Task 10: 前端选股筛选页面 (Tab2)

**Files:**
- Create: `frontend/src/views/FactorResearch/StockScreen.vue`

- [ ] **Step 1: Create StockScreen.vue**

```vue
<template>
  <div class="stock-screen">
    <!-- 筛选条件构建器 -->
    <el-card shadow="never" class="filter-card">
      <template #header>
        <div class="card-header">
          <span>筛选条件</span>
          <el-button type="primary" link @click="addFilter">
            <el-icon><Plus /></el-icon>
            添加条件
          </el-button>
        </div>
      </template>

      <div class="filter-list">
        <div v-for="(filter, index) in filters" :key="index" class="filter-row">
          <el-select
            v-model="filter.indicator_name"
            placeholder="选择指标"
            filterable
            style="width: 180px"
          >
            <el-option-group
              v-for="cat in categories"
              :key="cat.key"
              :label="cat.label"
            >
              <el-option
                v-for="ind in getIndicatorsByCategory(cat.key)"
                :key="ind.name"
                :label="ind.display_name"
                :value="ind.name"
              />
            </el-option-group>
          </el-select>

          <el-select v-model="filter.op" style="width: 80px; margin-left: 8px">
            <el-option label=">" value="gt" />
            <el-option label=">=" value="gte" />
            <el-option label="<" value="lt" />
            <el-option label="<=" value="lte" />
            <el-option label="=" value="eq" />
            <el-option label="区间" value="between" />
          </el-select>

          <template v-if="filter.op === 'between'">
            <el-input-number
              v-model="filter.valueLow"
              style="width: 120px; margin-left: 8px"
              :controls="false"
              placeholder="最小值"
            />
            <span style="margin: 0 4px">~</span>
            <el-input-number
              v-model="filter.valueHigh"
              style="width: 120px"
              :controls="false"
              placeholder="最大值"
            />
          </template>
          <template v-else>
            <el-input-number
              v-model="filter.value"
              style="width: 140px; margin-left: 8px"
              :controls="false"
              placeholder="数值"
            />
          </template>

          <el-button
            type="danger"
            link
            style="margin-left: 8px"
            @click="removeFilter(index)"
          >
            <el-icon><Delete /></el-icon>
          </el-button>
        </div>

        <div v-if="filters.length === 0" class="empty-filter">
          点击"添加条件"设置筛选规则
        </div>
      </div>

      <!-- 排序和操作 -->
      <div class="filter-actions">
        <el-select v-model="sortBy" placeholder="排序指标" clearable filterable style="width: 180px; margin-right: 8px">
          <el-option-group
            v-for="cat in categories"
            :key="cat.key"
            :label="cat.label"
          >
            <el-option
              v-for="ind in getIndicatorsByCategory(cat.key)"
              :key="ind.name"
              :label="ind.display_name"
              :value="ind.name"
            />
          </el-option-group>
        </el-select>
        <el-select v-model="sortOrder" style="width: 80px; margin-right: 16px" v-if="sortBy">
          <el-option label="降序" value="desc" />
          <el-option label="升序" value="asc" />
        </el-select>

        <el-button type="primary" @click="handleScreen" :loading="loading">
          筛选
        </el-button>
        <el-button @click="resetFilters">重置</el-button>
      </div>
    </el-card>

    <!-- 筛选结果 -->
    <el-card v-if="results.length > 0" shadow="never" class="result-card">
      <template #header>
        <div class="card-header">
          <span>筛选结果 ({{ total }})</span>
          <el-button link @click="handleExport">
            <el-icon><Download /></el-icon>
            导出
          </el-button>
        </div>
      </template>

      <el-table :data="results" stripe border max-height="500" size="small">
        <el-table-column prop="symbol" label="代码" width="120" fixed />
        <el-table-column prop="name" label="名称" width="100" />
        <el-table-column
          v-for="ind in selectedIndicatorNames"
          :key="ind"
          :label="getIndicatorLabel(ind)"
          width="120"
          align="right"
        >
          <template #default="{ row }">
            <span v-if="row.indicators[ind] != null">
              {{ formatValue(row.indicators[ind]) }}
            </span>
            <span v-else class="text-muted">-</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Plus, Delete, Download } from '@element-plus/icons-vue'
import { indicatorApi, type CategoryInfo, type IndicatorInfo, type ScreenFilter } from '@/api/indicator'

interface FilterRow {
  indicator_name: string
  op: string
  value: number | null
  valueLow: number | null
  valueHigh: number | null
}

const loading = ref(false)
const categories = ref<CategoryInfo[]>([])
const allIndicators = ref<IndicatorInfo[]>([])
const filters = ref<FilterRow[]>([])
const sortBy = ref('')
const sortOrder = ref<'asc' | 'desc'>('desc')
const results = ref<Array<{ symbol: string; name: string | null; indicators: Record<string, number | null> }>>([])
const total = ref(0)

const nameToInfo = computed(() => {
  const map: Record<string, IndicatorInfo> = {}
  allIndicators.value.forEach(i => { map[i.name] = i })
  return map
})

const selectedIndicatorNames = computed(() => {
  const names = new Set(filters.value.map(f => f.indicator_name))
  if (sortBy.value) names.add(sortBy.value)
  return Array.from(names)
})

const getIndicatorsByCategory = (category: string) => {
  return allIndicators.value.filter(i => i.category === category)
}

const getIndicatorLabel = (name: string) => {
  return nameToInfo.value[name]?.display_name || name
}

const formatValue = (val: number | null) => {
  if (val == null) return '-'
  if (Math.abs(val) >= 1000) return val.toFixed(0)
  if (Math.abs(val) >= 1) return val.toFixed(2)
  return val.toFixed(4)
}

const addFilter = () => {
  filters.value.push({
    indicator_name: '',
    op: 'gt',
    value: null,
    valueLow: null,
    valueHigh: null,
  })
}

const removeFilter = (index: number) => {
  filters.value.splice(index, 1)
}

const resetFilters = () => {
  filters.value = []
  sortBy.value = ''
  results.value = []
  total.value = 0
}

const handleScreen = async () => {
  const validFilters = filters.value.filter(f => f.indicator_name)
  if (validFilters.length === 0) {
    ElMessage.warning('请至少添加一个筛选条件')
    return
  }

  const screenFilters: ScreenFilter[] = validFilters.map(f => {
    if (f.op === 'between') {
      return {
        indicator_name: f.indicator_name,
        op: 'between',
        value: [f.valueLow || 0, f.valueHigh || 0],
      }
    }
    return {
      indicator_name: f.indicator_name,
      op: f.op,
      value: f.value || 0,
    }
  })

  loading.value = true
  try {
    const response = await indicatorApi.screenStocks({
      filters: screenFilters,
      sort_by: sortBy.value || undefined,
      sort_order: sortOrder.value,
      limit: 100,
    })
    results.value = response.items
    total.value = response.total
  } catch (e) {
    console.error('Screen failed:', e)
    ElMessage.error('筛选失败')
    results.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const handleExport = () => {
  if (!results.value.length) return
  const headers = ['代码', '名称', ...selectedIndicatorNames.value.map(n => getIndicatorLabel(n))]
  const rows = results.value.map(row => [
    row.symbol,
    row.name || '',
    ...selectedIndicatorNames.value.map(n => row.indicators[n] != null ? String(row.indicators[n]) : ''),
  ])
  const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n')
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `screen_result_${new Date().toISOString().slice(0, 10)}.csv`
  link.click()
  URL.revokeObjectURL(url)
}

onMounted(async () => {
  try {
    const [cats, inds] = await Promise.all([
      indicatorApi.getCategories(),
      indicatorApi.listIndicators(),
    ])
    categories.value = cats
    allIndicators.value = inds
  } catch (e) {
    console.error('Load data failed:', e)
  }
})
</script>

<style scoped>
.stock-screen {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.filter-card,
.result-card {
  flex-shrink: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 12px;
}

.filter-row {
  display: flex;
  align-items: center;
}

.empty-filter {
  color: #909399;
  font-size: 13px;
  padding: 12px 0;
}

.filter-actions {
  display: flex;
  align-items: center;
  padding-top: 8px;
  border-top: 1px solid #ebeef5;
}

.text-muted {
  color: #909399;
}
</style>
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/views/FactorResearch/StockScreen.vue
git commit -m "feat: add stock screening page (Tab2)"
```

---

### Task 11: 前端因子投研主页面改造 + 路由更新

**Files:**
- Modify: `frontend/src/views/FactorResearch/index.vue`
- Modify: `frontend/src/router/index.ts`

- [ ] **Step 1: Rewrite FactorResearch/index.vue as 4-Tab layout**

Replace entire content of `frontend/src/views/FactorResearch/index.vue` with:

```vue
<template>
  <div class="factor-research">
    <el-tabs v-model="activeTab" type="border-card" class="main-tabs">
      <el-tab-pane label="指标总览" name="overview" lazy>
        <IndicatorOverview />
      </el-tab-pane>
      <el-tab-pane label="选股筛选" name="screen" lazy>
        <StockScreen />
      </el-tab-pane>
      <el-tab-pane label="因子分析" name="analysis" lazy>
        <div class="placeholder">
          <el-empty description="因子分析模块开发中..." />
        </div>
      </el-tab-pane>
      <el-tab-pane label="因子合成" name="compose" lazy>
        <div class="placeholder">
          <el-empty description="因子合成模块开发中..." />
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import IndicatorOverview from './IndicatorOverview.vue'
import StockScreen from './StockScreen.vue'

const activeTab = ref('overview')
</script>

<style scoped>
.factor-research {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.main-tabs {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.main-tabs :deep(.el-tabs__content) {
  flex: 1;
  overflow: auto;
}

.main-tabs :deep(.el-tab-pane) {
  height: 100%;
}

.placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}
</style>
```

- [ ] **Step 2: Update router label**

在 `frontend/src/router/index.ts` 中，找到因子研究路由的 label/name，将"因子研究"改为"因子投研"。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/views/FactorResearch/index.vue frontend/src/router/index.ts
git commit -m "feat: restructure FactorResearch as 4-tab layout with indicator overview and stock screen"
```

---

### Task 12: 启动验证

- [ ] **Step 1: Restart backend and verify API**

```bash
# 重启后端
cd E:/Projects/GaoshouPlatform/backend && .venv/Scripts/activate && uvicorn app.main:app --reload --port 8001
```

验证：
- 访问 http://localhost:8001/docs 确认 indicators API 出现
- 调用 GET /api/indicators/categories 确认返回分类列表
- 调用 GET /api/indicators/list 确认返回指标列表

- [ ] **Step 2: Restart frontend and verify pages**

```bash
cd E:/Projects/GaoshouPlatform/frontend && npm run dev
```

验证：
- 访问因子投研页面
- Tab1 指标总览：分类筛选、卡片展示
- Tab2 选股筛选：条件构建器交互

- [ ] **Step 3: Final commit if any fixes needed**
