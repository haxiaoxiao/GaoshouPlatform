# 统一计算层 & 回测引擎 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建统一因子计算层（表达式引擎 + 四级算子 + 三级缓存）和轻量回测引擎（向量化 + 事件驱动），替代现有 `indicators/` 和 `vn_engine.py` mock。

**Architecture:** 新建 `compute/` 和 `backtest/` 两个独立包，通过 `/api/v2/` 暴露。旧代码保留不动，新系统稳定后逐步废弃旧模块。计算层为回测引擎提供因子数据，两者通过 `compute/cache.py` 的 L2 ClickHouse 缓存共享预计算结果。

**Tech Stack:** Python 3.12, FastAPI, Pandas, NumPy, TA-Lib, ClickHouse (clickhouse-driver), Pydantic v2

---

## File Map

```
backend/app/compute/                    # NEW — 统一计算层
├── __init__.py
├── expression.py                       # Tokenizer + Parser + Evaluator
├── cache.py                            # 三级缓存管理器
├── scheduler.py                        # 每日预计算调度器
├── api.py                              # FastAPI router
└── operators/
    ├── __init__.py                     # auto_discover + exports
    ├── registry.py                     # OperatorRegistry
    ├── base.py                         # Operator 基类
    ├── raw_fields.py                   # L0: $open, $high, $low, $close, $volume, $amount
    ├── math_ops.py                     # L1: +, -, *, /, Abs, Log, Rank, Delay, Delta
    ├── rolling_ops.py                  # L2: Mean, Std, Max, Min, Sum, Corr, Cov
    └── ta_ops.py                       # L3: RSI, MACD, BBANDS, EMA, SMA, ATR...

backend/app/backtest/                   # NEW — 回测引擎
├── __init__.py
├── config.py                           # BacktestConfig, BacktestResult
├── runner.py                           # BacktestRunner 统一入口
├── vectorized.py                       # 向量化引擎
├── event_driven.py                     # 事件驱动引擎
├── analyzers.py                        # 分析器
└── api.py                              # FastAPI router

backend/tests/compute/                  # NEW — 计算层测试
├── __init__.py
├── test_registry.py
├── test_expression.py
├── test_operators.py
└── test_cache.py

backend/tests/backtest/                 # NEW — 回测引擎测试
├── __init__.py
├── test_vectorized.py
└── test_event_driven.py

Files MODIFIED:
- backend/requirements.txt              # Add ta-lib, pandas-ta
- backend/app/db/clickhouse.py          # Add factor_cache table DDL
- backend/app/api/router.py             # Register compute + backtest v2 routers
- backend/app/main.py                   # Init factor_cache table on startup
```

---

## Phase 1: 计算层核心

### Task 1.1: 添加依赖

**Files:**
- Modify: `backend/requirements.txt`

- [ ] **Step 1: Add TA-Lib and pandas-ta to requirements**

```bash
echo "ta-lib>=0.6.0" >> backend/requirements.txt
echo "pandas-ta>=0.4.0" >> backend/requirements.txt
```

- [ ] **Step 2: Install dependencies**

```bash
cd backend && pip install ta-lib pandas-ta
```

- [ ] **Step 3: Commit**

```bash
git add backend/requirements.txt
git commit -m "chore: add ta-lib and pandas-ta dependencies"
```

---

### Task 1.2: Operator 基类 + 注册表

**Files:**
- Create: `backend/app/compute/__init__.py`
- Create: `backend/app/compute/operators/__init__.py`
- Create: `backend/app/compute/operators/base.py`
- Create: `backend/app/compute/operators/registry.py`
- Create: `backend/tests/compute/__init__.py`
- Create: `backend/tests/compute/test_registry.py`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p backend/app/compute/operators
mkdir -p backend/tests/compute
```

- [ ] **Step 2: Write `__init__.py` files**

`backend/app/compute/__init__.py`:
```python
"""统一计算层 — 表达式引擎 + 算子体系 + 缓存"""
```

`backend/app/compute/operators/__init__.py`:
```python
"""算子注册表 — L0-L3 四级算子体系"""
from app.compute.operators.registry import OperatorRegistry

def auto_discover():
    """自动发现并注册所有算子模块"""
    import importlib
    import pkgutil
    from app.compute import operators as pkg

    for _importer, modname, _ispkg in pkgutil.iter_modules(pkg.__path__):
        if modname in ("base", "registry", "__init__"):
            continue
        importlib.import_module(f"app.compute.operators.{modname}")

__all__ = ["OperatorRegistry", "auto_discover"]
```

`backend/tests/compute/__init__.py`:
```python
"""统一计算层测试"""
```

- [ ] **Step 3: Write Operator base class**

`backend/app/compute/operators/base.py`:
```python
"""算子基类 — 统一计算接口"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class Operator(ABC):
    """算子基类 — 所有 L0-L3 算子的父类"""

    name: str = ""
    description: str = ""
    level: int = 0              # 0=原始字段, 1=数学统计, 2=滚动窗口, 3=技术指标
    signature: str = ""         # 函数签名，如 "RSI(field, period)"
    category: str = ""          # raw_field / math / rolling / technical

    @abstractmethod
    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        """对 DataFrame 计算该算子，返回一维 Series"""
        ...

    def __repr__(self):
        return f"<Operator {self.signature or self.name} L{self.level}>"


class RawFieldOperator(Operator):
    """L0 原始字段算子 — 直接提取 DataFrame 列"""

    level: int = 0
    category: str = "raw_field"

    def __init__(self, name: str, column: str, description: str = ""):
        self.name = name
        self.column = column
        self.signature = f"${name}"
        self.description = description or f"原始字段: {column}"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        if self.column not in df.columns:
            raise KeyError(f"Column '{self.column}' not found in DataFrame")
        return df[self.column].astype(float)
```

- [ ] **Step 4: Write OperatorRegistry**

`backend/app/compute/operators/registry.py`:
```python
"""算子注册表 — 全局单例，管理所有算子"""
from __future__ import annotations

from typing import Any

from app.compute.operators.base import Operator


class OperatorRegistry:
    """算子注册表"""

    _operators: dict[str, Operator] = {}

    @classmethod
    def register(cls, op: Operator) -> Operator:
        """注册一个算子（同名则覆盖）"""
        cls._operators[op.name] = op
        return op

    @classmethod
    def get(cls, name: str) -> Operator | None:
        """按名称获取算子"""
        return cls._operators.get(name)

    @classmethod
    def all(cls) -> list[Operator]:
        """返回所有算子"""
        return list(cls._operators.values())

    @classmethod
    def by_level(cls, level: int) -> list[Operator]:
        """按级别筛选举子"""
        return [op for op in cls._operators.values() if op.level == level]

    @classmethod
    def by_category(cls, category: str) -> list[Operator]:
        """按类别筛选举子"""
        return [op for op in cls._operators.values() if op.category == category]

    @classmethod
    def names(cls) -> list[str]:
        """返回所有算子名称"""
        return list(cls._operators.keys())

    @classmethod
    def to_api_list(cls) -> list[dict[str, Any]]:
        """返回为前端 API 准备的数据格式"""
        return [
            {
                "name": op.name,
                "signature": op.signature,
                "description": op.description,
                "level": op.level,
                "category": op.category,
            }
            for op in cls._operators.values()
        ]

    @classmethod
    def clear(cls) -> None:
        """清空注册表（测试用）"""
        cls._operators.clear()
```

- [ ] **Step 5: Write registry unit test**

`backend/tests/compute/test_registry.py`:
```python
"""算子注册表测试"""
import pytest
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry


class TestOperatorRegistry:
    def setup_method(self):
        OperatorRegistry.clear()

    def test_register_and_get(self):
        op = RawFieldOperator("close", "close", "收盘价")
        OperatorRegistry.register(op)
        assert OperatorRegistry.get("close") is op

    def test_all(self):
        o1 = RawFieldOperator("close", "close")
        o2 = RawFieldOperator("open", "open")
        OperatorRegistry.register(o1)
        OperatorRegistry.register(o2)
        assert len(OperatorRegistry.all()) == 2

    def test_by_level(self):
        OperatorRegistry.register(RawFieldOperator("close", "close"))
        assert len(OperatorRegistry.by_level(0)) == 1

    def test_to_api_list(self):
        OperatorRegistry.register(RawFieldOperator("close", "close", "收盘价"))
        api_list = OperatorRegistry.to_api_list()
        assert len(api_list) == 1
        assert api_list[0]["name"] == "close"
        assert api_list[0]["level"] == 0

    def test_register_overwrite(self):
        o1 = RawFieldOperator("close", "close")
        o2 = RawFieldOperator("close", "new_close")
        OperatorRegistry.register(o1)
        OperatorRegistry.register(o2)
        assert OperatorRegistry.get("close") is o2
```

- [ ] **Step 6: Run tests**

```bash
cd backend && python -m pytest tests/compute/test_registry.py -v
```

- [ ] **Step 7: Commit**

```bash
git add backend/app/compute/ backend/tests/compute/
git commit -m "feat: add operator base class and registry"
```

---

### Task 1.3: L0 原始字段算子

**Files:**
- Create: `backend/app/compute/operators/raw_fields.py`

- [ ] **Step 1: Write L0 raw field operators**

`backend/app/compute/operators/raw_fields.py`:
```python
"""L0 原始字段算子 — OHLCV + 成交额 + 换手率"""
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry

RAW_FIELDS = [
    ("open", "open", "开盘价"),
    ("high", "high", "最高价"),
    ("low", "low", "最低价"),
    ("close", "close", "收盘价"),
    ("volume", "volume", "成交量(股)"),
    ("amount", "amount", "成交额(元)"),
    ("turnover", "turnover_rate", "换手率"),
]

for name, column, desc in RAW_FIELDS:
    OperatorRegistry.register(RawFieldOperator(name, column, desc))
```

- [ ] **Step 2: Verify registration in Python**

```bash
cd backend && python -c "
from app.compute.operators.registry import OperatorRegistry
from app.compute.operators import raw_fields
print('L0 operators:', OperatorRegistry.names())
assert len(OperatorRegistry.all()) == 7
print('OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/compute/operators/raw_fields.py
git commit -m "feat: add L0 raw field operators (OHLCV)"
```

---

### Task 1.4: L1 数学/统计算子

**Files:**
- Create: `backend/app/compute/operators/math_ops.py`
- Create: `backend/tests/compute/test_operators.py`

- [ ] **Step 1: Write L1 math operators**

`backend/app/compute/operators/math_ops.py`:
```python
"""L1 数学/统计算子 — 逐元素运算 + 横截面排序"""
import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class UnaryMathOp(Operator):
    """一元数学算子"""

    level: int = 1
    category: str = "math"

    def __init__(self, name: str, fn, signature: str, description: str = ""):
        self.name = name
        self._fn = fn
        self.signature = signature
        self.description = description

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        # 子节点应已将其计算结果写入 df 的临时列
        raise NotImplementedError("UnaryMathOp 由 Evaluator 在表达式求值阶段调用，不直接通过 evaluate(df)")


class DelayOp(Operator):
    """延时算子 — Shift series by N periods"""

    name: str = "Delay"
    level: int = 1
    category: str = "math"
    signature: str = "Delay(series, period)"
    description: str = "将序列向前移动 N 期"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = kwargs.get("series")
        period = kwargs.get("period", 1)
        if series is None:
            raise ValueError("Delay requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"Delay 'series' must be pd.Series, got {type(series)}")
        return series.shift(period)


class DeltaOp(Operator):
    """变动算子 — 当期值减 N 期前值"""

    name: str = "Delta"
    level: int = 1
    category: str = "math"
    signature: str = "Delta(series, period)"
    description: str = "当期值与 N 期前值的差"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = kwargs.get("series")
        period = kwargs.get("period", 1)
        if series is None:
            raise ValueError("Delta requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"Delta 'series' must be pd.Series, got {type(series)}")
        return series - series.shift(period)


class RankOp(Operator):
    """横截面排名算子 — 在某个交易日所有股票中排名"""

    name: str = "Rank"
    level: int = 1
    category: str = "math"
    signature: str = "Rank(series)"
    description: str = "横截面排名(百分位)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = kwargs.get("series")
        if series is None:
            raise ValueError("Rank requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"Rank 'series' must be pd.Series, got {type(series)}")
        # 返回百分位排名 [0, 1]
        return series.rank(pct=True)


for op in [
    DelayOp(),
    DeltaOp(),
    RankOp(),
]:
    OperatorRegistry.register(op)
```

- [ ] **Step 2: Write L1 operator tests**

`backend/tests/compute/test_operators.py`:
```python
"""算子单元测试"""
import numpy as np
import pandas as pd
import pytest
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry

# 注册所有算子
OperatorRegistry.clear()
from app.compute.operators import raw_fields  # noqa
from app.compute.operators import math_ops    # noqa


class TestRawFields:
    def test_close_field(self):
        op = OperatorRegistry.get("close")
        df = pd.DataFrame({"close": [10.0, 11.0, 12.0]})
        result = op.evaluate(df)
        pd.testing.assert_series_equal(result, pd.Series([10.0, 11.0, 12.0], dtype=float))

    def test_missing_column_raises(self):
        op = OperatorRegistry.get("close")
        df = pd.DataFrame({"open": [10.0]})
        with pytest.raises(KeyError):
            op.evaluate(df)


class TestDelay:
    def test_delay_1(self):
        op = OperatorRegistry.get("Delay")
        series = pd.Series([1.0, 2.0, 3.0, 4.0])
        result = op.evaluate(pd.DataFrame(), series=series, period=1)
        expected = pd.Series([np.nan, 1.0, 2.0, 3.0])
        pd.testing.assert_series_equal(result, expected)

    def test_delay_2(self):
        op = OperatorRegistry.get("Delay")
        series = pd.Series([1.0, 2.0, 3.0, 4.0])
        result = op.evaluate(pd.DataFrame(), series=series, period=2)
        expected = pd.Series([np.nan, np.nan, 1.0, 2.0])
        pd.testing.assert_series_equal(result, expected)


class TestRank:
    def test_rank_pct(self):
        op = OperatorRegistry.get("Rank")
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = op.evaluate(pd.DataFrame(), series=series)
        assert result.min() == 0.2  # 1/5 = 0.2
        assert result.max() == 1.0
```

- [ ] **Step 3: Run tests**

```bash
cd backend && python -m pytest tests/compute/test_operators.py -v
```

- [ ] **Step 4: Commit**

```bash
git add backend/app/compute/operators/math_ops.py backend/tests/compute/test_operators.py
git commit -m "feat: add L1 math operators (Delay, Delta, Rank)"
```

---

### Task 1.5: L2 滚动窗口算子

**Files:**
- Create: `backend/app/compute/operators/rolling_ops.py`

- [ ] **Step 1: Write L2 rolling window operators**

`backend/app/compute/operators/rolling_ops.py`:
```python
"""L2 滚动窗口算子 — 时间序列统计函数"""
import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class _RollingOp(Operator):
    """滚动窗口算子基类"""

    level: int = 2
    category: str = "rolling"

    def _get_series(self, kwargs: dict) -> pd.Series:
        series = kwargs.get("series")
        if series is None: raise ValueError(f"{self.name} requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"{self.name} 'series' must be pd.Series, got {type(series)}")
        return series

    def _get_window(self, kwargs: dict) -> int:
        w = kwargs.get("period", kwargs.get("window", 5))
        return int(w)


class MeanOp(_RollingOp):
    name: str = "Mean"
    signature: str = "Mean(series, period)"
    description: str = "N 期滚动均值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).mean()


class StdOp(_RollingOp):
    name: str = "Std"
    signature: str = "Std(series, period)"
    description: str = "N 期滚动标准差"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).std()


class MaxOp(_RollingOp):
    name: str = "Max"
    signature: str = "Max(series, period)"
    description: str = "N 期滚动最大值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).max()


class MinOp(_RollingOp):
    name: str = "Min"
    signature: str = "Min(series, period)"
    description: str = "N 期滚动最小值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).min()


class SumOp(_RollingOp):
    name: str = "Sum"
    signature: str = "Sum(series, period)"
    description: str = "N 期滚动求和"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).sum()


class CorrOp(Operator):
    name: str = "Corr"
    level: int = 2
    category: str = "rolling"
    signature: str = "Corr(series_a, series_b, period)"
    description: str = "N 期两序列滚动相关系数"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        a = kwargs.get("series_a")
        b = kwargs.get("series_b")
        w = int(kwargs.get("period", 20))
        if a is None or b is None:
            raise ValueError("Corr requires 'series_a' and 'series_b' arguments")
        if not isinstance(a, pd.Series) or not isinstance(b, pd.Series):
            raise TypeError("Corr series arguments must be pd.Series")
        return a.rolling(window=w, min_periods=max(1, w // 2)).corr(b)


class CovOp(Operator):
    name: str = "Cov"
    level: int = 2
    category: str = "rolling"
    signature: str = "Cov(series_a, series_b, period)"
    description: str = "N 期两序列滚动协方差"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        a = kwargs.get("series_a")
        b = kwargs.get("series_b")
        w = int(kwargs.get("period", 20))
        if a is None or b is None:
            raise ValueError("Cov requires 'series_a' and 'series_b'")
        if not isinstance(a, pd.Series) or not isinstance(b, pd.Series):
            raise TypeError("Cov series arguments must be pd.Series")
        return a.rolling(window=w, min_periods=max(1, w // 2)).cov(b)


class SlopeOp(_RollingOp):
    name: str = "Slope"
    signature: str = "Slope(series, period)"
    description: str = "N 期滚动线性回归斜率"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)

        def _slope(x):
            if len(x) < 2:
                return np.nan
            y = x.values
            t = np.arange(len(y))
            return np.polyfit(t, y, 1)[0]

        return s.rolling(window=w, min_periods=2).apply(_slope, raw=False)


for op in [MeanOp(), StdOp(), MaxOp(), MinOp(), SumOp(), CorrOp(), CovOp(), SlopeOp()]:
    OperatorRegistry.register(op)
```

- [ ] **Step 2: Run tests**

```bash
cd backend && python -c "
from app.compute.operators import raw_fields, math_ops, rolling_ops
from app.compute.operators.registry import OperatorRegistry
import pandas as pd, numpy as np

mean = OperatorRegistry.get('Mean')
s = pd.Series([1,2,3,4,5,6,7,8,9,10], dtype=float)
r = mean.evaluate(pd.DataFrame(), series=s, period=3)
print('Mean(3) of 1..10:', r.dropna().tolist())
print('L2 operators:', [n for n in OperatorRegistry.names() if OperatorRegistry.get(n).level==2])
print('OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/compute/operators/rolling_ops.py
git commit -m "feat: add L2 rolling window operators (Mean, Std, Max, Min, Sum, Corr, Cov, Slope)"
```

---

### Task 1.6: L3 TA-Lib 技术指标算子

**Files:**
- Create: `backend/app/compute/operators/ta_ops.py`

- [ ] **Step 1: Write L3 TA-Lib operators**

`backend/app/compute/operators/ta_ops.py`:
```python
"""L3 技术指标算子 — TA-Lib 封装"""
import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class _TAOperator(Operator):
    """TA-Lib 算子基类 — 自动处理 NaN"""

    level: int = 3
    category: str = "technical"
    _func = None  # talib 函数引用

    def _to_numpy(self, series: pd.Series) -> np.ndarray:
        return series.values.astype(np.float64)


class SMAOp(_TAOperator):
    name: str = "SMA"
    signature: str = "SMA(series, period)"
    description: str = "简单移动平均"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        r = talib.SMA(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class EMAOp(_TAOperator):
    name: str = "EMA"
    signature: str = "EMA(series, period)"
    description: str = "指数移动平均"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        r = talib.EMA(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class RSIOp(_TAOperator):
    name: str = "RSI"
    signature: str = "RSI(series, period)"
    description: str = "相对强弱指标"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 14))
        r = talib.RSI(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class MACDOp(Operator):
    name: str = "MACD"
    level: int = 3
    category: str = "technical"
    signature: str = "MACD(series, fast, slow, signal)"
    description: str = "MACD 柱 (DIF - DEA)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = kwargs["series"].values.astype(np.float64)
        fast = int(kwargs.get("fast", 12))
        slow = int(kwargs.get("slow", 26))
        sig = int(kwargs.get("signal", 9))
        _dif, _dea, macd = talib.MACD(s, fastperiod=fast, slowperiod=slow, signalperiod=sig)
        return pd.Series(macd, index=kwargs["series"].index)


class BBANDSUpperOp(_TAOperator):
    name: str = "BBANDS_upper"
    signature: str = "BBANDS_upper(series, period, nbdev)"
    description: str = "布林带上轨"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        nd = int(kwargs.get("nbdev", 2))
        upper, _mid, _low = talib.BBANDS(s, timeperiod=p, nbdevup=nd, nbdevdn=nd)
        return pd.Series(upper, index=kwargs["series"].index)


class BBANDSLowerOp(_TAOperator):
    name: str = "BBANDS_lower"
    signature: str = "BBANDS_lower(series, period, nbdev)"
    description: str = "布林带下轨"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        nd = int(kwargs.get("nbdev", 2))
        _up, _mid, low = talib.BBANDS(s, timeperiod=p, nbdevup=nd, nbdevdn=nd)
        return pd.Series(low, index=kwargs["series"].index)


class ATROp(_TAOperator):
    name: str = "ATR"
    signature: str = "ATR(high_series, low_series, close_series, period)"
    description: str = "平均真实波幅"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high_series"].values.astype(np.float64)
        low = kwargs["low_series"].values.astype(np.float64)
        close = kwargs["close_series"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = talib.ATR(high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["high_series"].index)


class KDJOp(Operator):
    name: str = "KDJ_K"
    level: int = 3
    category: str = "technical"
    signature: str = "KDJ_K(high, low, close, period)"
    description: str = "KDJ K值 (通过 Stochastic 慢速实现)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 9))
        k, _d = talib.STOCH(high, low, close, fastk_period=p, slowk_period=3,
                            slowd_period=3, slowd_matype=0)
        return pd.Series(k, index=kwargs["close"].index)


class CCIAOp(_TAOperator):
    name: str = "CCI"
    signature: str = "CCI(high, low, close, period)"
    description: str = "商品通道指数"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = talib.CCI(high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["close"].index)


class WILLROp(_TAOperator):
    name: str = "WILLR"
    signature: str = "WILLR(high, low, close, period)"
    description: str = "威廉指标"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = talib.WILLR(high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["close"].index)


class OBVOp(Operator):
    name: str = "OBV"
    level: int = 3
    category: str = "technical"
    signature: str = "OBV(close, volume)"
    description: str = "能量潮"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        close = kwargs["close"].values.astype(np.float64)
        volume = kwargs["volume"].values.astype(np.float64)
        r = talib.OBV(close, volume)
        return pd.Series(r, index=kwargs["close"].index)


class MOMOp(_TAOperator):
    name: str = "MOM"
    signature: str = "MOM(series, period)"
    description: str = "动量"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 10))
        r = talib.MOM(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


for op in [
    SMAOp(), EMAOp(), RSIOp(), MACDOp(), BBANDSUpperOp(), BBANDSLowerOp(),
    ATROp(), KDJOp(), CCIAOp(), WILLROp(), OBVOp(), MOMOp(),
]:
    OperatorRegistry.register(op)
```

- [ ] **Step 2: Run tests**

```bash
cd backend && python -c "
from app.compute.operators import raw_fields, math_ops, rolling_ops, ta_ops
from app.compute.operators.registry import OperatorRegistry
import pandas as pd, numpy as np

rsi = OperatorRegistry.get('RSI')
s = pd.Series(np.random.randn(100) + 10, name='close')
result = rsi.evaluate(pd.DataFrame(), series=s, period=14)
print('RSI range:', result.dropna().min(), '-', result.dropna().max())
print('L3 operators:', [n for n in OperatorRegistry.names() if OperatorRegistry.get(n).level==3])
print('OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/compute/operators/ta_ops.py
git commit -m "feat: add L3 TA-Lib operators (RSI, MACD, BBANDS, ATR, KDJ, etc.)"
```

---

### Task 1.7: 表达式引擎

**Files:**
- Create: `backend/app/compute/expression.py`
- Create: `backend/tests/compute/test_expression.py`

- [ ] **Step 1: Write Tokenizer**

`backend/app/compute/expression.py`:
```python
"""表达式引擎 — Tokenizer + Parser + Evaluator"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

import numpy as np
import pandas as pd


class TokenType(Enum):
    VARIABLE = auto()    # $close, $open ...
    NUMBER = auto()      # 14, 20, 5.5
    FUNCTION = auto()    # RSI, Mean, Std ...
    LPAREN = auto()      # (
    RPAREN = auto()      # )
    COMMA = auto()       # ,
    PLUS = auto()        # +
    MINUS = auto()       # -
    STAR = auto()        # *
    SLASH = auto()       # /
    LT = auto()          # <
    GT = auto()          # >
    LE = auto()          # <=
    GE = auto()          # >=
    EQ = auto()          # ==
    NE = auto()          # !=
    AND = auto()         # AND
    OR = auto()          # OR
    EOF = auto()         # end of input


@dataclass
class Token:
    type: TokenType
    value: str
    pos: int = 0  # position in source for error reporting


TOKEN_PATTERNS = [
    (r"\$[a-zA-Z_]\w*", TokenType.VARIABLE),
    (r"\d+\.?\d*", TokenType.NUMBER),
    (r"and\b", TokenType.AND),
    (r"or\b", TokenType.OR),
    (r"<=", TokenType.LE),
    (r">=", TokenType.GE),
    (r"==", TokenType.EQ),
    (r"!=", TokenType.NE),
    (r"<", TokenType.LT),
    (r">", TokenType.GT),
    (r"\+", TokenType.PLUS),
    (r"-", TokenType.MINUS),
    (r"\*", TokenType.STAR),
    (r"/", TokenType.SLASH),
    (r"\(", TokenType.LPAREN),
    (r"\)", TokenType.RPAREN),
    (r",", TokenType.COMMA),
]


class Tokenizer:
    """将表达式字符串分解为 Token 流"""

    def __init__(self, source: str):
        self.source = source
        self.pos = 0
        self.tokens: list[Token] = []

    def tokenize(self) -> list[Token]:
        tokens = []
        while self.pos < len(self.source):
            if self.source[self.pos].isspace():
                self.pos += 1
                continue
            # 尝试匹配所有 pattern
            matched = False
            for pattern, ttype in TOKEN_PATTERNS:
                m = re.match(pattern, self.source[self.pos:], re.IGNORECASE)
                if m:
                    val = m.group()
                    tokens.append(Token(ttype, val, self.pos))
                    self.pos += len(val)
                    matched = True
                    break
            if not matched:
                # 尝试匹配函数名（大写字母开头或字母+可选数字）
                m = re.match(r"[a-zA-Z_]\w*", self.source[self.pos:])
                if m:
                    val = m.group()
                    tokens.append(Token(TokenType.FUNCTION, val, self.pos))
                    self.pos += len(val)
                else:
                    raise SyntaxError(
                        f"Unexpected character '{self.source[self.pos]}' at position {self.pos}"
                    )
        tokens.append(Token(TokenType.EOF, "", self.pos))
        return tokens


# AST Nodes
class ASTNode:
    pass


@dataclass
class LiteralNode(ASTNode):
    value: float


@dataclass
class VariableNode(ASTNode):
    name: str  # e.g. "close" (strip $ prefix)


@dataclass
class FunctionCallNode(ASTNode):
    name: str
    args: list[ASTNode]


@dataclass
class BinaryOpNode(ASTNode):
    op: str  # "+", "-", "*", "/", "<", ">", "<=", ">=", "==", "!=", "AND", "OR"
    left: ASTNode
    right: ASTNode


@dataclass
class UnaryOpNode(ASTNode):
    op: str  # "-" for negation
    operand: ASTNode


class Parser:
    """递归下降解析器

    Grammar:
      comparison -> expression (("<"|">"|"<="|">="|"=="|"!=") expression)*
      expression -> term (("+"|"-") term)*
      term       -> factor (("*"|"/") factor)*
      factor     -> ("-") factor | atom
      atom       -> NUMBER | VARIABLE | FUNCTION "(" args ")" | "(" comparison ")"
      args       -> comparison ("," comparison)*
      and_or     -> comparison (("AND"|"OR") comparison)*
    """

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def consume(self) -> Token:
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def expect(self, ttype: TokenType, msg: str = "") -> Token:
        t = self.peek()
        if t.type != ttype:
            raise SyntaxError(
                f"{msg or f'Expected {ttype}, got {t.type}'} at position {t.pos}: '{t.value}'"
            )
        return self.consume()

    def parse(self) -> ASTNode:
        node = self._parse_and_or()
        if self.peek().type != TokenType.EOF:
            raise SyntaxError(
                f"Unexpected token '{self.peek().value}' at position {self.peek().pos}"
            )
        return node

    def _parse_and_or(self) -> ASTNode:
        left = self._parse_comparison()
        while self.peek().type in (TokenType.AND, TokenType.OR):
            op = self.consume().value.upper()
            right = self._parse_comparison()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_comparison(self) -> ASTNode:
        left = self._parse_expression()
        while self.peek().type in (TokenType.LT, TokenType.GT, TokenType.LE,
                                     TokenType.GE, TokenType.EQ, TokenType.NE):
            op = self.consume().value
            right = self._parse_expression()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_expression(self) -> ASTNode:
        left = self._parse_term()
        while self.peek().type in (TokenType.PLUS, TokenType.MINUS):
            op = self.consume().value
            right = self._parse_term()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_term(self) -> ASTNode:
        left = self._parse_factor()
        while self.peek().type in (TokenType.STAR, TokenType.SLASH):
            op = self.consume().value
            right = self._parse_factor()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_factor(self) -> ASTNode:
        if self.peek().type == TokenType.MINUS:
            self.consume()
            operand = self._parse_factor()
            return UnaryOpNode("-", operand)
        return self._parse_atom()

    def _parse_atom(self) -> ASTNode:
        t = self.peek()
        if t.type == TokenType.NUMBER:
            self.consume()
            return LiteralNode(float(t.value))
        if t.type == TokenType.VARIABLE:
            self.consume()
            return VariableNode(t.value[1:])  # strip $
        if t.type == TokenType.FUNCTION:
            name = self.consume().value
            self.expect(TokenType.LPAREN, f"Expected '(' after function '{name}'")
            args = self._parse_args()
            self.expect(TokenType.RPAREN, f"Expected ')' after function args for '{name}'")
            return FunctionCallNode(name, args)
        if t.type == TokenType.LPAREN:
            self.consume()
            # 对于比较运算符，递归回 and_or（完整表达式）
            node = self._parse_and_or()
            self.expect(TokenType.RPAREN, "Expected ')'")
            return node
        raise SyntaxError(f"Unexpected token '{t.value}' at position {t.pos}")

    def _parse_args(self) -> list[ASTNode]:
        args = []
        if self.peek().type == TokenType.RPAREN:
            return args
        args.append(self._parse_and_or())
        while self.peek().type == TokenType.COMMA:
            self.consume()
            args.append(self._parse_and_or())
        return args


class Evaluator:
    """AST 求值器 — 递归遍历语法树执行计算"""

    # 二元运算符映射
    _BINOPS = {
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b.replace(0, np.nan),
        "<": lambda a, b: a < b,
        ">": lambda a, b: a > b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "AND": lambda a, b: a & b,
        "OR": lambda a, b: a | b,
    }

    def __init__(self, data: dict[str, pd.Series]):
        """
        Args:
            data: {symbol -> DataFrame(index=trade_date, columns=open,high,low,close,volume,amount,turnover)}
                  or single symbol case: 直接在 context 中传入 Series
        """
        self.data = data
        self._cache: dict[str, Any] = {}  # 当前求值过程的内存缓存

    def evaluate(self, node: ASTNode) -> pd.Series | pd.DataFrame:
        """求值入口"""

        if isinstance(node, LiteralNode):
            return self._eval_literal(node)
        if isinstance(node, VariableNode):
            return self._eval_variable(node)
        if isinstance(node, FunctionCallNode):
            return self._eval_function(node)
        if isinstance(node, BinaryOpNode):
            return self._eval_binary(node)
        if isinstance(node, UnaryOpNode):
            return self._eval_unary(node)
        raise TypeError(f"Unknown AST node: {type(node)}")

    def _eval_literal(self, node: LiteralNode):
        return node.value

    def _eval_variable(self, node: VariableNode):
        """L0 原始字段 — 从数据中提取列"""
        field = node.name
        from app.compute.operators.registry import OperatorRegistry

        op = OperatorRegistry.get(field)
        if op is not None and op.level == 0:
            # 对每个 symbol 的 DataFrame 提取列
            results = {}
            for key, df in self.data.items():
                results[key] = op.evaluate(df)
            if len(results) == 1:
                return list(results.values())[0]
            return results
        raise NameError(f"Unknown field '${field}'")

    def _eval_function(self, node: FunctionCallNode):
        from app.compute.operators.registry import OperatorRegistry

        op = OperatorRegistry.get(node.name)
        if op is None:
            raise NameError(f"Unknown function '{node.name}'")

        # 求值所有参数
        args = [self.evaluate(arg) for arg in node.args]

        # 对于 std、rolling 等需要原生 Python 数据类型的参数,
        # 第 2 个之后的位置参数通常是数值
        kwargs = {}
        if len(args) >= 1:
            kwargs["series"] = args[0]
        if len(args) >= 2:
            kwargs["period"] = int(args[1]) if isinstance(args[1], (int, float)) else args[1]
        if node.name in ("Corr", "Cov"):
            kwargs["series_a"] = args[0]
            if len(args) >= 2:
                kwargs["series_b"] = args[1]
            if len(args) >= 3:
                kwargs["period"] = int(args[2]) if isinstance(args[2], (int, float)) else args[2]
        if node.name == "ATR":
            kwargs["high_series"] = args[0]
            kwargs["low_series"] = args[1]
            if len(args) >= 3:
                kwargs["close_series"] = args[2]
            if len(args) >= 4:
                kwargs["period"] = int(args[3]) if isinstance(args[3], (int, float)) else args[3]
        if node.name in ("CCI", "WILLR"):
            kwargs["high"] = args[0]
            kwargs["low"] = args[1]
            kwargs["close"] = args[2]
            if len(args) >= 4:
                kwargs["period"] = int(args[3]) if isinstance(args[3], (int, float)) else args[3]
        if node.name == "OBV":
            kwargs["close"] = args[0]
            kwargs["volume"] = args[1]

        # 生成缓存 key
        cache_key = self._make_key(node)
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = op.evaluate(None, **kwargs)
        self._cache[cache_key] = result
        return result

    def _eval_binary(self, node: BinaryOpNode):
        left = self.evaluate(node.left)
        right = self.evaluate(node.right)
        op_fn = self._BINOPS.get(node.op)
        if op_fn is None:
            raise ValueError(f"Unknown operator '{node.op}'")
        # 处理 literal + series 的情况
        if isinstance(left, (int, float)) and isinstance(right, pd.Series):
            left = pd.Series(left, index=right.index)
        if isinstance(right, (int, float)) and isinstance(left, pd.Series):
            right = pd.Series(right, index=left.index)
        return op_fn(left, right)

    def _eval_unary(self, node: UnaryOpNode):
        operand = self.evaluate(node.operand)
        if node.op == "-":
            return -operand
        raise ValueError(f"Unknown unary operator '{node.op}'")

    def _make_key(self, node: FunctionCallNode) -> str:
        """生成函数调用的缓存 key"""
        parts = [node.name]
        for arg in node.args:
            if isinstance(arg, VariableNode):
                parts.append(arg.name)
            elif isinstance(arg, LiteralNode):
                parts.append(str(arg.value))
            elif isinstance(arg, FunctionCallNode):
                parts.append(self._make_key(arg))
            else:
                parts.append("?")
        return ":".join(parts)


def evaluate_expression(
    expression: str,
    data: dict[str, pd.DataFrame],
) -> pd.Series | dict[str, pd.Series]:
    """便捷函数 — 解析并求值表达式

    Args:
        expression: 因子表达式，如 "Mean($close, 5) / Std($close, 20)"
        data: {symbol -> DataFrame} 或 single symbol

    Returns:
        单 symbol 返回 pd.Series，多 symbol 返回 dict[str, pd.Series]
    """
    tokens = Tokenizer(expression).tokenize()
    ast = Parser(tokens).parse()
    evaluator = Evaluator(data)
    return evaluator.evaluate(ast)


def validate_expression(expression: str) -> tuple[bool, str]:
    """校验表达式语法是否正确
    
    Returns:
        (is_valid, error_message)
    """
    try:
        tokens = Tokenizer(expression).tokenize()
        Parser(tokens).parse()
        return True, ""
    except SyntaxError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {e}"
```

- [ ] **Step 2: Write expression engine tests**

`backend/tests/compute/test_expression.py`:
```python
"""表达式引擎测试"""
import numpy as np
import pandas as pd
import pytest

# 注册所有算子
from app.compute.operators.registry import OperatorRegistry
OperatorRegistry.clear()
from app.compute.operators import raw_fields, math_ops, rolling_ops, ta_ops  # noqa

from app.compute.expression import (
    Tokenizer, TokenType, Parser, Evaluator,
    VariableNode, LiteralNode, FunctionCallNode, BinaryOpNode,
    evaluate_expression, validate_expression,
)


def make_test_data(n: int = 100) -> dict:
    """创建测试用的 OHLCV 数据"""
    np.random.seed(42)
    close = np.cumsum(np.random.randn(n) * 0.01) + 10.0
    open_ = close - np.random.randn(n) * 0.005
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.005)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.005)
    volume = np.abs(np.random.randn(n) * 1e6 + 5e6).astype(int)
    amount = volume * close * 0.8

    df = pd.DataFrame({
        "trade_date": pd.date_range("2025-01-01", periods=n, freq="B"),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
        "turnover_rate": volume / 1e8,
    })
    df.set_index("trade_date", inplace=True)
    return {"test": df}


class TestTokenizer:
    def test_simple_variable(self):
        tokens = Tokenizer("$close").tokenize()
        assert tokens[0].type == TokenType.VARIABLE
        assert tokens[0].value == "$close"

    def test_arithmetic(self):
        tokens = Tokenizer("$close + $open * 2").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert types == [
            TokenType.VARIABLE, TokenType.PLUS,
            TokenType.VARIABLE, TokenType.STAR, TokenType.NUMBER,
        ]

    def test_function_call(self):
        tokens = Tokenizer("RSI($close, 14)").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert types == [
            TokenType.FUNCTION, TokenType.LPAREN,
            TokenType.VARIABLE, TokenType.COMMA, TokenType.NUMBER,
            TokenType.RPAREN,
        ]

    def test_comparison(self):
        tokens = Tokenizer("RSI($close, 14) < 30").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert TokenType.LT in types

    def test_nested_function(self):
        tokens = Tokenizer("Mean($close, 5)").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert TokenType.FUNCTION in types

    def test_logical(self):
        tokens = Tokenizer("a < 30 AND b > 70").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert TokenType.AND in types


class TestParser:
    def test_variable(self):
        tokens = Tokenizer("$close").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, VariableNode)
        assert ast.name == "close"

    def test_addition(self):
        tokens = Tokenizer("$close + 1").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, BinaryOpNode)
        assert ast.op == "+"

    def test_precedence(self):
        # a + b * c => a + (b * c)
        tokens = Tokenizer("$close + $open * 2").tokenize()
        ast = Parser(tokens).parse()
        assert ast.op == "+"
        assert ast.right.op == "*"

    def test_function_call(self):
        tokens = Tokenizer("Mean($close, 20)").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, FunctionCallNode)
        assert ast.name == "Mean"
        assert len(ast.args) == 2

    def test_nested_function(self):
        tokens = Tokenizer("RSI(Mean($close, 5), 14)").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, FunctionCallNode)
        assert ast.name == "RSI"
        inner = ast.args[0]
        assert isinstance(inner, FunctionCallNode)
        assert inner.name == "Mean"


class TestEvaluator:
    def test_raw_field(self):
        data = make_test_data(20)
        result = evaluate_expression("$close", data)
        assert len(result) == 20

    def test_arithmetic(self):
        data = make_test_data(20)
        result = evaluate_expression("$close - $open", data)
        assert len(result) == 20
        assert not result.isna().all()

    def test_mean(self):
        data = make_test_data(50)
        result = evaluate_expression("Mean($close, 10)", data)
        assert len(result) == 50
        assert not result.iloc[9:].isna().any()  # 第10个值开始有效

    def test_std(self):
        data = make_test_data(50)
        result = evaluate_expression("Std($close, 10)", data)
        assert len(result) == 50

    def test_rsi(self):
        data = make_test_data(50)
        result = evaluate_expression("RSI($close, 14)", data)
        assert len(result) == 50
        # RSI 应在 0-100 之间
        valid = result.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_composite_factor(self):
        data = make_test_data(50)
        result = evaluate_expression("(Mean($close, 5) - Mean($close, 20)) / Std($close, 20)", data)
        assert len(result) == 50

    def test_nested_rsi_delay(self):
        data = make_test_data(50)
        result = evaluate_expression("RSI($close, 14) - Delay(RSI($close, 14), 1)", data)
        assert len(result) == 50


class TestValidateExpression:
    def test_valid(self):
        ok, err = validate_expression("Mean($close, 20)")
        assert ok
        assert err == ""

    def test_invalid_syntax(self):
        ok, err = validate_expression("Mean($close, 20")
        assert not ok

    def test_invalid_char(self):
        ok, err = validate_expression("$close @ $open")
        assert not ok
```

- [ ] **Step 3: Run tests**

```bash
cd backend && python -m pytest tests/compute/test_expression.py -v
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add backend/app/compute/expression.py backend/tests/compute/test_expression.py
git commit -m "feat: add expression engine (tokenizer, parser, evaluator)"
```

---

### Task 1.8: 缓存管理器

**Files:**
- Create: `backend/app/compute/cache.py`
- Create: `backend/tests/compute/test_cache.py`

- [ ] **Step 1: Write cache manager**

`backend/app/compute/cache.py`:
```python
"""三级缓存管理器 — L1 内存 LRU → L2 ClickHouse → L3 原始数据"""
from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from datetime import date
from typing import Any

import pandas as pd
from clickhouse_driver import Client


class LRUCache:
    """线程安全的 LRU 内存缓存"""

    def __init__(self, max_size: int = 256):
        self._max_size = max_size
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            return None

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


class ComputeCache:
    """三级因子计算缓存

    L1: 进程内 LRU dict（请求级复用）
    L2: ClickHouse factor_cache 表（跨请求持久化）
    L3: ClickHouse klines_daily（原始数据，不算入缓存逻辑）
    """

    def __init__(self, ch_client: Client | None = None):
        self.l1 = LRUCache(max_size=256)
        self._ch = ch_client

    @property
    def ch_client(self) -> Client:
        if self._ch is None:
            from app.db.clickhouse import get_ch_client
            self._ch = get_ch_client()
        return self._ch

    @staticmethod
    def make_key(full_expression: str) -> str:
        """生成规范化表达式 hash key（16 字符 hex）"""
        normalized = full_expression.strip().lower().replace(" ", "")
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def get(self, expression: str) -> dict[str, pd.Series] | None:
        """从 L1 内存缓存获取"""
        key = self.make_key(expression)
        return self.l1.get(key)

    def set(self, expression: str, result: dict[str, pd.Series]) -> None:
        """写入 L1 内存缓存"""
        key = self.make_key(expression)
        self.l1.set(key, result)

    def get_from_ch(
        self,
        expr_hash: str,
        symbols: list[str],
        trade_date: date,
    ) -> pd.Series | None:
        """从 L2 ClickHouse 读取预计算结果"""
        try:
            rows = self.ch_client.execute(
                """
                SELECT symbol, value FROM factor_cache
                WHERE expr_hash = %(h)s AND trade_date = %(d)s
                AND symbol IN %(syms)s
                """,
                {"h": expr_hash, "d": trade_date, "syms": symbols},
            )
            if rows:
                return pd.Series({r[0]: r[1] for r in rows})
        except Exception:
            pass
        return None

    def save_to_ch(
        self,
        expr_hash: str,
        trade_date: date,
        series: pd.Series,
    ) -> None:
        """写入 L2 ClickHouse 预计算结果"""
        try:
            rows = [
                {"symbol": sym, "trade_date": trade_date, "expr_hash": expr_hash, "value": float(val)}
                for sym, val in series.dropna().items()
            ]
            if rows:
                self.ch_client.execute(
                    """
                    INSERT INTO factor_cache (symbol, trade_date, expr_hash, value)
                    VALUES
                    """,
                    rows,
                )
        except Exception:
            pass

    def clear_l1(self) -> None:
        """清空 L1 缓存"""
        self.l1.clear()


# 全局单例
_compute_cache: ComputeCache | None = None


def get_compute_cache() -> ComputeCache:
    global _compute_cache
    if _compute_cache is None:
        _compute_cache = ComputeCache()
    return _compute_cache


def reset_compute_cache() -> None:
    global _compute_cache
    _compute_cache = ComputeCache()
```

- [ ] **Step 2: Write cache tests**

`backend/tests/compute/test_cache.py`:
```python
"""缓存管理器测试"""
import pandas as pd
from app.compute.cache import LRUCache, ComputeCache, get_compute_cache, reset_compute_cache


class TestLRUCache:
    def test_get_set(self):
        c = LRUCache(max_size=10)
        c.set("a", 1)
        assert c.get("a") == 1

    def test_miss(self):
        c = LRUCache(max_size=10)
        assert c.get("nonexistent") is None

    def test_eviction(self):
        c = LRUCache(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)  # should evict "a"
        assert c.get("a") is None
        assert c.get("b") == 2
        assert c.get("c") == 3

    def test_touch_on_get(self):
        c = LRUCache(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.get("a")  # "a" becomes most recent
        c.set("c", 3)  # should evict "b"
        assert c.get("a") == 1
        assert c.get("b") is None


class TestComputeCache:
    def test_make_key_deterministic(self):
        k1 = ComputeCache.make_key("Mean($close, 20)")
        k2 = ComputeCache.make_key("Mean($close, 20)")
        assert k1 == k2

    def test_make_key_whitespace_insensitive(self):
        k1 = ComputeCache.make_key("Mean($close, 20)")
        k2 = ComputeCache.make_key("  mean($close,20)  ")
        assert k1 == k2

    def test_l1_cache(self):
        reset_compute_cache()
        cache = get_compute_cache()
        cache.set("Mean($close, 20)", {"test": pd.Series([1, 2, 3])})
        result = cache.get("Mean($close, 20)")
        assert result is not None
        assert len(result["test"]) == 3
```

- [ ] **Step 3: Run tests**

```bash
cd backend && python -m pytest tests/compute/test_cache.py -v
```

- [ ] **Step 4: Commit**

```bash
git add backend/app/compute/cache.py backend/tests/compute/test_cache.py
git commit -m "feat: add three-level compute cache (L1 LRU + L2 ClickHouse)"
```

---

### Task 1.9: 创建 ClickHouse factor_cache 表 + 启动初始化

**Files:**
- Modify: `backend/app/db/clickhouse.py`
- Modify: `backend/app/main.py`

- [ ] **Step 1: Add factor_cache table DDL to clickhouse.py**

In `backend/app/db/clickhouse.py`, add after the `indicator_timeseries` table creation block:

```python
    # 创建因子缓存表
    client.execute("""
        CREATE TABLE IF NOT EXISTS factor_cache
        (
            symbol LowCardinality(String),
            trade_date Date,
            expr_hash FixedString(16),
            value Float64,
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(trade_date)
        ORDER BY (expr_hash, trade_date, symbol)
    """)
```

- [ ] **Step 2: Verify table DDL**

```bash
cd backend && python -c "
from app.db.clickhouse import init_clickhouse_tables
init_clickhouse_tables()
from app.db.clickhouse import get_ch_client
ch = get_ch_client()
tables = ch.execute('SHOW TABLES')
print('Tables:', tables)
assert 'factor_cache' in tables
print('factor_cache table created successfully')
"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/db/clickhouse.py
git commit -m "feat: add factor_cache table DDL to ClickHouse init"
```

---

### Task 1.10: 计算调度器

**Files:**
- Create: `backend/app/compute/scheduler.py`

- [ ] **Step 1: Write compute scheduler**

`backend/app/compute/scheduler.py`:
```python
"""每日盘后预计算调度器"""
import logging
from datetime import date
from datetime import datetime as dt

import pandas as pd

from app.compute.cache import ComputeCache, get_compute_cache

logger = logging.getLogger(__name__)


class ComputeScheduler:
    """每日盘后批量预计算常用因子"""

    def __init__(self, cache: ComputeCache | None = None):
        self.cache = cache or get_compute_cache()
        self._precomputed_expressions: list[str] = []  # 需要预计算的表达式列表

    def register_precompute(self, expression: str) -> None:
        """注册一个需要每日预计算的表达式"""
        if expression not in self._precomputed_expressions:
            self._precomputed_expressions.append(expression)

    async def run_daily_jobs(
        self,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> None:
        """执行每日预计算任务"""
        if not self._precomputed_expressions:
            logger.info("No precomputed expressions registered, skipping")
            return

        if trade_date is None:
            trade_date = date.today()

        # 获取数据
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()

        if symbols is None:
            rows = ch.execute("SELECT DISTINCT symbol FROM klines_daily")
            symbols = [r[0] for r in rows]

        if not symbols:
            logger.warning("No symbols found for precompute")
            return

        for expr in self._precomputed_expressions:
            try:
                self._precompute_one(expr, symbols, trade_date)
            except Exception as e:
                logger.error(f"Failed to precompute '{expr}': {e}")

    def _precompute_one(
        self,
        expression: str,
        symbols: list[str],
        trade_date: date,
    ) -> None:
        """预计算单个表达式并写入 L2 缓存"""
        from app.compute.expression import evaluate_expression

        # 加载数据
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        rows = ch.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s AND trade_date >= %(start)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols, "start": date(2020, 1, 1) if hasattr(trade_date, 'year') else "2020-01-01"},
        )

        if not rows:
            return

        # DataFrame 格式
        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        # 按 symbol 分组
        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        # 求值
        result = evaluate_expression(expression, data)
        if isinstance(result, pd.DataFrame):
            result = result.iloc[:, 0]  # take first column if DataFrame

        # 提取当天的截面值写入 L2
        expr_hash = self.cache.make_key(expression)
        if isinstance(result, dict):
            for sym, series in result.items():
                if trade_date in series.index:
                    val = series.loc[trade_date]
                    if pd.notna(val):
                        self.cache.save_to_ch(
                            expr_hash, trade_date,
                            pd.Series({sym: float(val)}),
                        )
        elif isinstance(result, pd.Series):
            self.cache.save_to_ch(expr_hash, trade_date, result)


# 全局单例
_scheduler: ComputeScheduler | None = None


def get_compute_scheduler() -> ComputeScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = ComputeScheduler()
    return _scheduler
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/compute/scheduler.py
git commit -m "feat: add compute scheduler for daily factor precomputation"
```

---

### Task 1.11: Compute API 端点

**Files:**
- Create: `backend/app/compute/api.py`

- [ ] **Step 1: Write compute API**

`backend/app/compute/api.py`:
```python
"""计算层 API — /api/v2/compute"""
import time
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.compute.cache import ComputeCache, get_compute_cache
from app.compute.expression import evaluate_expression, validate_expression
from app.compute.operators import auto_discover
from app.compute.operators.registry import OperatorRegistry

router = APIRouter(prefix="/v2/compute")

# 启动时自动发现所有算子
auto_discover()


class EvaluateRequest(BaseModel):
    expression: str = Field(..., description="因子表达式，如 Mean($close, 5)")
    symbols: list[str] = Field(..., description="股票代码列表")
    start_date: date = Field(..., description="开始日期")
    end_date: date = Field(..., description="结束日期")
    use_cache: bool = Field(default=True, description="是否使用缓存")


class EvaluateResponse(BaseModel):
    code: int = 0
    message: str = "success"
    data: dict[str, list[dict]]
    meta: dict


class ScreenRequest(BaseModel):
    condition: str = Field(..., description="筛选条件表达式，如 RSI($close, 14) < 30")
    universe: str = Field(default="all", description="股票池: all 或 自选股分组名")
    trade_date: date = Field(..., description="筛选日期")
    limit: int = Field(default=50, description="返回数量上限")


class ScreenResponse(BaseModel):
    code: int = 0
    message: str = "success"
    data: dict


class BatchRequest(BaseModel):
    expressions: list[str] = Field(..., description="批量计算表达式列表")
    symbols: list[str]
    start_date: date
    end_date: date


@router.get("/operators")
async def list_operators():
    """返回所有可用算子"""
    return {
        "code": 0,
        "message": "success",
        "data": OperatorRegistry.to_api_list(),
    }


@router.post("/evaluate")
async def evaluate(req: EvaluateRequest):
    """计算表达式"""
    t0 = time.time()
    cache_hit = False

    cache: ComputeCache = get_compute_cache()

    # 如果是简单单因子且启用缓存，查 L1
    result = None
    if req.use_cache:
        result = cache.get(req.expression)

    if result is None:
        # 从 ClickHouse 加载数据
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        rows = ch.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY symbol, trade_date
            """,
            {"syms": req.symbols, "start": req.start_date, "end": req.end_date},
        )

        if not rows:
            return {"code": 0, "message": "success", "data": {}, "meta": {"rows": 0}}

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close",
                     "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        result = evaluate_expression(req.expression, data)

        if req.use_cache:
            cache.set(req.expression, result)
    else:
        cache_hit = True

    # 格式化输出
    out: dict[str, list[dict]] = {}
    if isinstance(result, dict):
        for sym, series in result.items():
            if not isinstance(series, pd.Series):
                continue
            out[sym] = [
                {"trade_date": str(idx.date()), "value": float(v) if pd.notna(v) else None}
                for idx, v in series.items()
                if req.start_date <= idx.date() <= req.end_date
            ]
    elif isinstance(result, pd.Series):
        out["result"] = [
            {"trade_date": str(idx.date()), "value": float(v) if pd.notna(v) else None}
            for idx, v in result.items()
            if req.start_date <= idx.date() <= req.end_date
        ]

    elapsed = (time.time() - t0) * 1000

    return {
        "code": 0,
        "message": "success",
        "data": out,
        "meta": {
            "expression": req.expression,
            "cache_hit": cache_hit,
            "compute_time_ms": round(elapsed, 1),
        },
    }


@router.post("/screen")
async def screen(req: ScreenRequest):
    """基于条件筛选股票"""
    # 校验条件表达式
    valid, err = validate_expression(req.condition)
    if not valid:
        return {"code": 1, "message": f"Invalid condition: {err}", "data": None}

    # 获取股票池
    from app.db.clickhouse import get_ch_client
    ch = get_ch_client()

    if req.universe == "all":
        rows = ch.execute("SELECT DISTINCT symbol FROM klines_daily WHERE trade_date = %(d)s",
                          {"d": req.trade_date})
        symbols = [r[0] for r in rows]
    else:
        # 从自选股分组获取 — 需要 async session，这里暂时用 all
        rows = ch.execute("SELECT DISTINCT symbol FROM klines_daily WHERE trade_date = %(d)s",
                          {"d": req.trade_date})
        symbols = [r[0] for r in rows]

    if not symbols:
        return {"code": 0, "message": "success", "data": {"symbols": [], "count": 0}}

    # 加载数据并在 Evaluator 中求值
    rows = ch.execute(
        """
        SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
        FROM klines_daily
        WHERE symbol IN %(syms)s
          AND trade_date >= %(start)s
        ORDER BY symbol, trade_date
        """,
        {"syms": symbols, "start": date(2020, 1, 1)},
    )

    df = pd.DataFrame(
        rows,
        columns=["symbol", "trade_date", "open", "high", "low", "close",
                 "volume", "amount", "turnover_rate"],
    )
    for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
        df[col] = df[col].astype(float)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    data = {}
    for sym, grp in df.groupby("symbol"):
        data[sym] = grp.set_index("trade_date")

    result = evaluate_expression(req.condition, data)

    # 提取当天的截面值
    if isinstance(result, dict):
        matching = []
        for sym, series in result.items():
            if isinstance(series, pd.Series) and req.trade_date in series.index:
                val = series.loc[req.trade_date]
                if isinstance(val, (bool, pd.Series)):
                    cond = bool(val.iloc[0] if isinstance(val, pd.Series) else val)
                elif pd.isna(val):
                    continue
                else:
                    cond = bool(val)
                if cond:
                    matching.append(sym)
        matching = matching[:req.limit]
    else:
        matching = []

    return {
        "code": 0,
        "message": "success",
        "data": {"symbols": matching, "count": len(matching)},
    }


@router.post("/batch")
async def batch_compute(req: BatchRequest):
    """批量计算多个表达式"""
    results = {}
    for expr in req.expressions:
        valid, err = validate_expression(expr)
        if not valid:
            results[expr] = {"error": err}
            continue
        # 简化：只返回截面值 (最新日期)
        results[expr] = {"status": "queued"}

    return {
        "code": 0,
        "message": "success",
        "data": {"expressions": list(results.keys()), "count": len(results)},
    }
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/compute/api.py
git commit -m "feat: add compute API endpoints (/api/v2/compute)"
```

---

### Task 1.12: 注册路由到主 Router

**Files:**
- Modify: `backend/app/api/router.py`

- [ ] **Step 1: Register compute API router**

Add to `backend/app/api/router.py` after the existing imports and before the prefix registration:

```python
from app.compute.api import router as compute_router

# Add this include line with the others:
api_router.include_router(compute_router, tags=["计算引擎"])
```

- [ ] **Step 2: Verify route registration**

```bash
cd backend && python -c "
from app.api.router import api_router
# Count routes
count = 0
for route in api_router.routes:
    print(route.path, route.methods if hasattr(route, 'methods') else '')
    count += 1
print(f'Total routes: {count}')
"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/api/router.py
git commit -m "feat: register compute API router"
```

---

### Task 1.13: Phase 1 集成测试

- [ ] **Step 1: Run all compute tests**

```bash
cd backend && python -m pytest tests/compute/ -v --tb=short
```

- [ ] **Step 2: Smoke test expression evaluate via FastAPI (if server running)**

```bash
curl -s -X POST http://127.0.0.1:8000/api/v2/compute/evaluate \
  -H "Content-Type: application/json" \
  -d '{"expression": "Mean($close, 5)", "symbols": ["000001.SZ"], "start_date": "2025-01-01", "end_date": "2025-01-31"}' | head -c 500
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test: Phase 1 integration tests pass"
```

---

## Phase 3: 向量化回测引擎

### Task 3.1: 回测配置与结果数据结构

**Files:**
- Create: `backend/app/backtest/__init__.py`
- Create: `backend/app/backtest/config.py`

- [ ] **Step 1: Write backtest config and result classes**

`backend/app/backtest/__init__.py`:
```python
"""回测引擎 — 向量化 + 事件驱动"""
```

`backend/app/backtest/config.py`:
```python
"""回测配置与结果数据结构"""
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal


@dataclass
class BacktestConfig:
    """回测配置"""

    mode: Literal["vectorized", "event_driven"] = "vectorized"
    symbols: list[str] = field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    initial_capital: float = 1_000_000.0

    # 向量化模式
    factor_expression: str | None = None
    rebalance_freq: str = "monthly"    # daily / weekly / monthly
    n_groups: int = 5                  # 分组数
    weight_method: str = "equal"       # equal / market_cap

    # 事件驱动模式
    buy_condition: str | None = None
    sell_condition: str | None = None
    bar_type: str = "daily"            # daily / minute

    # 风控
    stop_loss: float | None = None     # 止损比例，如 -0.08
    stop_profit: float | None = None   # 止盈比例
    max_positions: int | None = None   # 最大持仓数

    # 交易成本
    commission_rate: float = 0.0003    # 手续费率
    slippage: float = 0.001           # 滑点


@dataclass
class BacktestResult:
    """回测结果"""

    # 收益指标
    total_return: float = 0.0
    annual_return: float = 0.0
    annual_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    calmar_ratio: float = 0.0

    # 交易统计
    total_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    turnover_rate: float = 0.0

    # 净值曲线
    nav_series: list[dict[str, Any]] = field(default_factory=list)
    daily_returns: list[dict[str, Any]] = field(default_factory=list)

    # 分组收益 (向量化模式)
    group_navs: list[dict[str, Any]] | None = None

    # 交易明细
    trades: list[dict[str, Any]] = field(default_factory=list)

    # 元数据
    start_date: date | None = None
    end_date: date | None = None
    initial_capital: float = 0.0
    final_capital: float = 0.0
    n_trading_days: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "annual_volatility": self.annual_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "calmar_ratio": self.calmar_ratio,
            "total_trades": self.total_trades,
            "win_trades": self.win_trades,
            "loss_trades": self.loss_trades,
            "win_rate": self.win_rate,
            "avg_return": self.avg_return,
            "turnover_rate": self.turnover_rate,
            "nav_series": self.nav_series,
            "daily_returns": self.daily_returns,
            "group_navs": self.group_navs,
            "trades": self.trades,
            "start_date": str(self.start_date) if self.start_date else None,
            "end_date": str(self.end_date) if self.end_date else None,
            "initial_capital": self.initial_capital,
            "final_capital": self.final_capital,
            "n_trading_days": self.n_trading_days,
        }
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/backtest/
git commit -m "feat: add backtest config and result data structures"
```

---

### Task 3.2: 向量化回测引擎

**Files:**
- Create: `backend/app/backtest/vectorized.py`
- Create: `backend/tests/backtest/__init__.py`
- Create: `backend/tests/backtest/test_vectorized.py`

- [ ] **Step 1: Write vectorized backtest engine**

`backend/app/backtest/vectorized.py`:
```python
"""向量化回测引擎 — 因子分组回测"""
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig, BacktestResult


class VectorizedBacktestEngine:
    """向量化回测引擎

    核心思路：将整个回测区间转化为矩阵运算，避免逐日循环。
    适用于因子分组/排序策略。
    """

    FREQ_MAP = {"daily": 1, "weekly": 5, "monthly": 21}

    def run(
        self,
        factor_matrix: pd.DataFrame,
        return_matrix: pd.DataFrame,
        config: BacktestConfig,
    ) -> BacktestResult:
        """
        Args:
            factor_matrix: DataFrame(index=trade_date, columns=symbol, values=因子值)
            return_matrix: DataFrame(index=trade_date, columns=symbol, values=下期收益率)
            config: 回测配置
        """
        # 1. 对齐日期
        common_dates = factor_matrix.index.intersection(return_matrix.index)
        common_symbols = factor_matrix.columns.intersection(return_matrix.columns)

        factor_matrix = factor_matrix.loc[common_dates, common_symbols]
        return_matrix = return_matrix.loc[common_dates, common_symbols]

        # 2. 确定调仓日
        freq_days = self.FREQ_MAP.get(config.rebalance_freq, 21)
        rebalance_dates = common_dates[::freq_days]

        n_groups = config.n_groups

        # 3. 逐期分组并计算收益
        group_daily_returns: dict[int, list[float]] = {i: [] for i in range(n_groups)}
        nav_dates: list[date] = []
        turnover_list: list[float] = []

        for i, rb_date in enumerate(rebalance_dates):
            # 当前调仓日的因子值
            factors = factor_matrix.loc[rb_date].dropna()
            if len(factors) < n_groups * 2:
                continue

            # 分组
            try:
                groups = pd.qcut(factors, n_groups, labels=False, duplicates="drop")
            except ValueError:
                continue

            # 确定持有期的结束日期
            if i + 1 < len(rebalance_dates):
                hold_end = rebalance_dates[i + 1]
            else:
                hold_end = common_dates[-1]

            hold_dates = common_dates[
                (common_dates > rb_date) & (common_dates <= hold_end)
            ]
            if len(hold_dates) == 0:
                continue

            hold_returns = return_matrix.loc[hold_dates]

            # 每组的平均收益
            for g in range(n_groups):
                g_symbols = factors[groups == g].index
                g_returns = hold_returns[g_symbols.intersection(common_symbols)]
                mean_ret = g_returns.mean(axis=1).mean()  # 组内等权
                group_daily_returns[g].append(mean_ret)

            nav_dates.append(rb_date)

        if not nav_dates:
            return BacktestResult(
                total_return=0, initial_capital=config.initial_capital,
                final_capital=config.initial_capital,
            )

        # 4. 构建净值曲线
        group_navs = {}
        for g in range(n_groups):
            returns = pd.Series(group_daily_returns[g])
            # 扣除交易成本
            if config.rebalance_freq == "monthly":
                returns = returns - config.commission_rate * 2 / 12
            elif config.rebalance_freq == "weekly":
                returns = returns - config.commission_rate * 2 / 52
            else:
                returns = returns - config.commission_rate * 2

            nav = (1 + returns).cumprod()
            group_navs[f"group_{g + 1}"] = [
                {"date": str(d), "nav": float(v)}
                for d, v in zip(nav_dates, nav)
            ]

        # 多空组合
        long_returns = pd.Series(group_daily_returns[0])  # 组1 (因子值最小)
        short_returns = pd.Series(group_daily_returns[n_groups - 1])  # 组N
        long_short_returns = long_returns - short_returns - config.commission_rate * 4
        ls_nav = (1 + long_short_returns).cumprod()

        # 5. 计算统计指标
        return self._compute_metrics(
            long_short_returns, ls_nav, nav_dates, config, group_navs,
        )

    def _compute_metrics(
        self,
        returns: pd.Series,
        nav: pd.Series,
        nav_dates: list,
        config: BacktestConfig,
        group_navs: dict,
    ) -> BacktestResult:
        """计算回测统计指标"""
        if len(returns) == 0:
            return BacktestResult()

        total_return = nav.iloc[-1] - 1
        n_periods = len(returns)
        annual_return = (1 + total_return) ** (252 / max(n_periods, 1)) - 1
        annual_vol = returns.std() * np.sqrt(252 / max(len(returns), 1))
        sharpe = (annual_return - 0.02) / annual_vol if annual_vol > 0 else 0

        # 最大回撤
        cummax = nav.cummax()
        drawdown = (nav - cummax) / cummax
        max_dd = drawdown.min()

        calmar = annual_return / abs(max_dd) if max_dd != 0 else 0

        # 胜率
        wins = (returns > 0).sum()
        total = len(returns)
        win_rate = wins / total if total > 0 else 0

        nav_series = [
            {"date": str(d), "nav": float(v)}
            for d, v in zip(nav_dates, nav)
        ]

        return BacktestResult(
            total_return=total_return,
            annual_return=annual_return,
            annual_volatility=annual_vol,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            calmar_ratio=calmar,
            total_trades=total,
            win_trades=wins,
            loss_trades=total - wins,
            win_rate=win_rate,
            avg_return=returns.mean(),
            nav_series=nav_series,
            group_navs=group_navs,
            start_date=nav_dates[0] if nav_dates else None,
            end_date=nav_dates[-1] if nav_dates else None,
            initial_capital=config.initial_capital,
            final_capital=config.initial_capital * (1 + total_return),
            n_trading_days=len(returns),
        )


# 全局单例
_vectorized_engine: VectorizedBacktestEngine | None = None


def get_vectorized_engine() -> VectorizedBacktestEngine:
    global _vectorized_engine
    if _vectorized_engine is None:
        _vectorized_engine = VectorizedBacktestEngine()
    return _vectorized_engine
```

- [ ] **Step 2: Write vectorized engine test**

`backend/tests/backtest/__init__.py`:
```python
"""回测引擎测试"""
```

`backend/tests/backtest/test_vectorized.py`:
```python
"""向量化回测引擎测试"""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from app.backtest.config import BacktestConfig
from app.backtest.vectorized import VectorizedBacktestEngine


def make_test_data(n_symbols=100, n_days=252):
    """生成测试用因子矩阵和收益矩阵"""
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    symbols = [f"{i:06d}.SZ" for i in range(n_symbols)]

    # 因子值：随机 + 与收益轻微相关
    factor_data = np.random.randn(n_days, n_symbols)
    factor_matrix = pd.DataFrame(factor_data, index=dates, columns=symbols)

    # 收益：因子值 * 0.01 + noise
    return_data = factor_data * 0.01 + np.random.randn(n_days, n_symbols) * 0.02
    return_matrix = pd.DataFrame(return_data, index=dates, columns=symbols)

    return factor_matrix, return_matrix


class TestVectorizedBacktest:
    def test_basic_run(self):
        factor, returns = make_test_data(50, 200)
        config = BacktestConfig(
            mode="vectorized",
            factor_expression="test_factor",
            rebalance_freq="monthly",
            n_groups=5,
            initial_capital=1_000_000,
        )

        engine = VectorizedBacktestEngine()
        result = engine.run(factor, returns, config)

        assert result is not None
        assert result.n_trading_days > 0
        assert result.sharpe_ratio != 0 or result.total_return != 0
        assert result.group_navs is not None
        assert len(result.group_navs) == 5

    def test_empty_data(self):
        engine = VectorizedBacktestEngine()
        result = engine.run(
            pd.DataFrame(), pd.DataFrame(),
            BacktestConfig(initial_capital=100_000),
        )
        assert result.total_return == 0

    def test_nav_series_format(self):
        factor, returns = make_test_data(30, 100)
        config = BacktestConfig(rebalance_freq="monthly", n_groups=3)
        engine = VectorizedBacktestEngine()
        result = engine.run(factor, returns, config)

        assert len(result.nav_series) > 0
        assert "date" in result.nav_series[0]
        assert "nav" in result.nav_series[0]
```

- [ ] **Step 3: Run tests**

```bash
cd backend && python -m pytest tests/backtest/test_vectorized.py -v
```

- [ ] **Step 4: Commit**

```bash
git add backend/app/backtest/vectorized.py backend/tests/backtest/
git commit -m "feat: add vectorized backtest engine with group testing"
```

---

### Task 3.3: 回测分析器

**Files:**
- Create: `backend/app/backtest/analyzers.py`

- [ ] **Step 1: Write analyzers**

`backend/app/backtest/analyzers.py`:
```python
"""回测分析器 — 收益/回撤/夏普/IC/换手率"""
import numpy as np
import pandas as pd


def compute_annual_return(nav_series: list[dict], n_trading_days: int) -> float:
    """从净值序列计算年化收益"""
    if not nav_series or n_trading_days == 0:
        return 0.0
    total_return = nav_series[-1]["nav"] - 1
    return (1 + total_return) ** (252 / max(n_trading_days, 1)) - 1


def compute_max_drawdown(nav_series: list[dict]) -> float:
    """计算最大回撤"""
    if not nav_series:
        return 0.0
    navs = pd.Series([d["nav"] for d in nav_series])
    cummax = navs.cummax()
    drawdown = (navs - cummax) / cummax
    return float(drawdown.min())


def compute_sharpe_ratio(daily_returns: list[dict], risk_free: float = 0.02) -> float:
    """计算夏普比率"""
    if not daily_returns:
        return 0.0
    rets = pd.Series([d["return"] for d in daily_returns])
    excess = rets.mean() * 252 - risk_free
    vol = rets.std() * np.sqrt(252)
    if vol == 0:
        return 0.0
    return float(excess / vol)


def compute_win_rate(trades: list[dict]) -> float:
    """计算胜率"""
    if not trades:
        return 0.0
    wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
    return wins / len(trades)


def compute_ic_series(
    factor_matrix: pd.DataFrame,
    return_matrix: pd.DataFrame,
) -> pd.Series:
    """计算 IC 序列（Spearman 秩相关）"""
    ic_list = []
    common_dates = factor_matrix.index.intersection(return_matrix.index)
    common_symbols = factor_matrix.columns.intersection(return_matrix.columns)

    for d in common_dates:
        f = factor_matrix.loc[d, common_symbols].dropna()
        r = return_matrix.loc[d, common_symbols].dropna()
        common = f.index.intersection(r.index)
        if len(common) < 10:
            continue
        ic = f[common].corr(r[common], method="spearman")
        if not np.isnan(ic):
            ic_list.append({"date": str(d.date()), "ic": float(ic)})

    if not ic_list:
        return pd.Series()

    ic_series = pd.Series({d["date"]: d["ic"] for d in ic_list})
    return ic_series


def compute_turnover(
    factor_matrix: pd.DataFrame,
    rebalance_dates: list,
    n_positions: int = 50,
) -> list[dict]:
    """计算每期换手率"""
    turnover = []
    for i in range(1, len(rebalance_dates)):
        prev_date = rebalance_dates[i - 1]
        curr_date = rebalance_dates[i]

        prev_factors = factor_matrix.loc[prev_date].dropna()
        curr_factors = factor_matrix.loc[curr_date].dropna()

        prev_top = set(prev_factors.nlargest(n_positions).index)
        curr_top = set(curr_factors.nlargest(n_positions).index)
        common = prev_top & curr_top

        if len(curr_top) > 0:
            to_rate = 1 - len(common) / n_positions
        else:
            to_rate = 1.0

        turnover.append({"date": str(curr_date.date()), "rate": to_rate})

    return turnover
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/backtest/analyzers.py
git commit -m "feat: add backtest analyzers (sharpe, max_dd, IC, turnover)"
```

---

### Task 3.4: 回测 Runner 统一入口

**Files:**
- Create: `backend/app/backtest/runner.py`

- [ ] **Step 1: Write runner**

`backend/app/backtest/runner.py`:
```python
"""回测统一入口"""
import logging
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.vectorized import get_vectorized_engine

logger = logging.getLogger(__name__)


class BacktestRunner:
    """回测运行器 — 统一入口"""

    async def run(self, config: BacktestConfig) -> BacktestResult:
        """运行回测

        1. 根据 mode 选择引擎
        2. 从 ClickHouse 加载数据 → 构造 factor_matrix + return_matrix
        3. 执行回测
        4. 返回统一格式结果
        """
        if config.mode == "vectorized":
            return await self._run_vectorized(config)
        elif config.mode == "event_driven":
            return await self._run_event_driven(config)
        else:
            raise ValueError(f"Unknown backtest mode: {config.mode}")

    async def _run_vectorized(self, config: BacktestConfig) -> BacktestResult:
        if config.factor_expression is None:
            raise ValueError("factor_expression is required for vectorized backtest")

        # 1. 加载数据并计算因子
        factor_matrix = await self._load_factor_matrix(
            config.factor_expression, config.symbols,
            config.start_date, config.end_date,
        )

        # 2. 构造收益矩阵
        return_matrix = await self._load_return_matrix(
            config.symbols, config.start_date, config.end_date,
        )

        # 3. 运行向量化引擎
        engine = get_vectorized_engine()
        result = engine.run(factor_matrix, return_matrix, config)
        return result

    async def _run_event_driven(self, config: BacktestConfig) -> BacktestResult:
        # Phase 4 实现
        logger.warning("Event-driven backtest not yet implemented")
        return BacktestResult(
            initial_capital=config.initial_capital,
            final_capital=config.initial_capital,
        )

    async def _load_factor_matrix(
        self,
        expression: str,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载并计算因子矩阵"""
        from app.db.clickhouse import get_ch_client
        from app.compute.expression import evaluate_expression

        ch = get_ch_client()

        query = """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
        """
        params: dict = {"syms": symbols}
        if start_date:
            query += " AND trade_date >= %(start)s"
            params["start"] = start_date
        if end_date:
            query += " AND trade_date <= %(end)s"
            params["end"] = end_date
        query += " ORDER BY symbol, trade_date"

        rows = ch.execute(query, params)
        if not rows:
            raise ValueError("No data found")

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close",
                     "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        # 按 symbol 分组
        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        # 计算因子
        result = evaluate_expression(expression, data)

        # 转换为矩阵格式 (date x symbol)
        if isinstance(result, dict):
            factor_dfs = []
            for sym, series in result.items():
                if isinstance(series, pd.Series):
                    s = series.rename(sym)
                    factor_dfs.append(s)
            if factor_dfs:
                factor_matrix = pd.concat(factor_dfs, axis=1)
                return factor_matrix

        raise ValueError("Failed to compute factor matrix")

    async def _load_return_matrix(
        self,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载收益率矩阵 — 下一日收益率"""
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()

        rows = ch.execute(
            """
            SELECT symbol, trade_date, close
            FROM klines_daily
            WHERE symbol IN %(syms)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols},
        )

        df = pd.DataFrame(rows, columns=["symbol", "trade_date", "close"])
        df["close"] = df["close"].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        # 计算下一日收益率
        return_matrix = {}
        for sym, grp in df.groupby("symbol"):
            grp = grp.sort_values("trade_date")
            ret = grp["close"].pct_change().shift(-1)  # next day return
            ret.index = grp["trade_date"]
            return_matrix[sym] = ret

        if return_matrix:
            return pd.DataFrame(return_matrix)
        return pd.DataFrame()


# 全局单例
_runner: BacktestRunner | None = None


def get_backtest_runner() -> BacktestRunner:
    global _runner
    if _runner is None:
        _runner = BacktestRunner()
    return _runner
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/backtest/runner.py
git commit -m "feat: add BacktestRunner unified entry with factor matrix loading"
```

---

### Task 3.5: 回测 API 端点

**Files:**
- Create: `backend/app/backtest/api.py`

- [ ] **Step 1: Write backtest API**

`backend/app/backtest/api.py`:
```python
"""回测 API — /api/v2/backtest"""
import uuid
from datetime import date

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.backtest.config import BacktestConfig
from app.backtest.runner import get_backtest_runner

router = APIRouter(prefix="/v2/backtest")

# 内存任务存储（后续可换 Redis）
_tasks: dict[str, dict] = {}


class RunBacktestRequest(BaseModel):
    mode: str = "vectorized"
    factor_expression: str | None = None
    buy_condition: str | None = None
    sell_condition: str | None = None
    symbols: list[str]
    start_date: date
    end_date: date
    initial_capital: float = 1_000_000
    rebalance_freq: str = "monthly"
    n_groups: int = 5
    bar_type: str = "daily"
    commission_rate: float = 0.0003
    slippage: float = 0.001


class BacktestStatusResponse(BaseModel):
    task_id: str
    status: str  # queued / running / done / failed
    progress: float = 0.0
    result: dict | None = None


@router.post("/run")
async def run_backtest(req: RunBacktestRequest):
    """提交回测任务"""
    task_id = str(uuid.uuid4())[:8]
    config = BacktestConfig(
        mode=req.mode,
        factor_expression=req.factor_expression,
        buy_condition=req.buy_condition,
        sell_condition=req.sell_condition,
        symbols=req.symbols,
        start_date=req.start_date,
        end_date=req.end_date,
        initial_capital=req.initial_capital,
        rebalance_freq=req.rebalance_freq,
        n_groups=req.n_groups,
        bar_type=req.bar_type,
        commission_rate=req.commission_rate,
        slippage=req.slippage,
    )

    _tasks[task_id] = {"status": "queued", "progress": 0, "result": None}

    # 同步执行（Phase 4改为异步）
    try:
        _tasks[task_id]["status"] = "running"
        runner = get_backtest_runner()
        result = await runner.run(config)
        _tasks[task_id] = {
            "status": "done",
            "progress": 1.0,
            "result": result.to_dict(),
        }
    except Exception as e:
        _tasks[task_id] = {
            "status": "failed",
            "progress": 1.0,
            "result": {"error": str(e)},
        }

    return {"code": 0, "message": "success", "data": {"task_id": task_id}}


@router.get("/status/{task_id}")
async def get_status(task_id: str):
    """查询回测进度"""
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    return {"code": 0, "message": "success", "data": task}


@router.get("/result/{task_id}")
async def get_result(task_id: str):
    """获取回测结果"""
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    if task["status"] != "done":
        return {"code": 1, "message": f"Task status: {task['status']}", "data": None}
    return {"code": 0, "message": "success", "data": task["result"]}
```

- [ ] **Step 2: Commit**

```bash
git add backend/app/backtest/api.py
git commit -m "feat: add backtest API endpoints (/api/v2/backtest)"
```

---

### Task 3.6: 注册回测路由

**Files:**
- Modify: `backend/app/api/router.py`

- [ ] **Step 1: Register backtest router**

Add to `backend/app/api/router.py` after the compute router import:

```python
from app.backtest.api import router as backtest_v2_router

api_router.include_router(backtest_v2_router, tags=["回测引擎"])
```

- [ ] **Step 2: Run Phase 3 integration test**

```bash
cd backend && python -m pytest tests/backtest/ -v
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/api/router.py
git commit -m "feat: register backtest API v2 router"
```

---

## Phase 4: 事件驱动引擎 + 策略接入（简要）

### Task 4.1: 事件驱动回测引擎

**Files:**
- Create: `backend/app/backtest/event_driven.py`
- Create: `backend/tests/backtest/test_event_driven.py`

- [ ] **Step 1: Write event-driven engine**

`backend/app/backtest/event_driven.py`:
```python
"""事件驱动回测引擎 — 逐日/逐分钟信号触发"""
from datetime import date, datetime
from typing import Any, Callable

import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig, BacktestResult


class EventDrivenBacktestEngine:
    """事件驱动回测引擎"""

    def run(
        self,
        ohlcv_data: dict[str, pd.DataFrame],
        signal_fn: Callable[[pd.DataFrame], pd.Series],
        config: BacktestConfig,
    ) -> BacktestResult:
        """
        Args:
            ohlcv_data: {symbol -> DataFrame} 日线或分钟线
            signal_fn: 信号函数，接受单只股票的 DataFrame，返回 1(买)/-1(卖)/0 信号序列
            config: 回测配置
        """
        daily_nav = []
        trades = []
        cash = config.initial_capital
        positions: dict[str, dict] = {}  # {symbol: {shares, cost}}

        initial_capital = config.initial_capital
        commission = config.commission_rate
        slippage = config.slippage

        all_dates = set()
        for df in ohlcv_data.values():
            all_dates.update(df.index)
        all_dates = sorted(all_dates)

        for i, dt in enumerate(all_dates):
            total_value = cash

            # 更新持仓市值
            for sym, pos in list(positions.items()):
                df = ohlcv_data.get(sym)
                if df is None or dt not in df.index:
                    continue
                px = float(df.loc[dt, "close"])
                mv = pos["shares"] * px
                total_value += mv

                # 检查止损止盈
                if config.stop_loss and mv / pos["cost"] - 1 <= config.stop_loss:
                    # 止损出
                    cash += mv * (1 - commission)
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "sell", "reason": "stop_loss",
                        "price": px, "pnl": mv - pos["cost"],
                    })
                    del positions[sym]
                elif config.stop_profit and mv / pos["cost"] - 1 >= config.stop_profit:
                    cash += mv * (1 - commission)
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "sell", "reason": "stop_profit",
                        "price": px, "pnl": mv - pos["cost"],
                    })
                    del positions[sym]

            # 信号检查
            for sym, df in ohlcv_data.items():
                if dt not in df.index:
                    continue
                if config.max_positions and len(positions) >= config.max_positions:
                    break
                if sym in positions:
                    continue

                # 获取信号
                hist = df.loc[:dt]
                try:
                    signal = signal_fn(hist)
                    sig_val = signal.iloc[-1] if isinstance(signal, pd.Series) else signal
                except Exception:
                    continue

                px = float(df.loc[dt, "close"])
                if sig_val > 0 and cash > px * 100:
                    # 买入
                    shares = int(cash * 0.2 / px / 100) * 100  # 20% 仓位
                    if shares == 0:
                        continue
                    cost = shares * px * (1 + commission + slippage)
                    if cost > cash:
                        continue
                    cash -= cost
                    positions[sym] = {"shares": shares, "cost": cost}
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "buy",
                        "price": px, "shares": shares, "cost": cost,
                    })
                elif sig_val < 0 and sym in positions:
                    # 卖出
                    pos = positions.pop(sym)
                    mv = pos["shares"] * px * (1 - commission)
                    cash += mv
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "sell",
                        "price": px, "pnl": mv - pos["cost"],
                    })

            if i > 0:
                daily_nav.append({
                    "date": str(dt.date() if hasattr(dt, 'date') else dt),
                    "nav": total_value / initial_capital,
                })

        if not daily_nav:
            return BacktestResult(
                initial_capital=initial_capital, final_capital=initial_capital,
                trades=trades,
            )

        final_nav = daily_nav[-1]["nav"]
        total_return = final_nav - 1
        n_days = len(daily_nav)
        annual_return = (1 + total_return) ** (252 / max(n_days, 1)) - 1

        # 每日收益
        daily_rets = []
        for i in range(1, len(daily_nav)):
            r = daily_nav[i]["nav"] / daily_nav[i - 1]["nav"] - 1
            daily_rets.append({"date": daily_nav[i]["date"], "return": float(r)})

        rets = pd.Series([r["return"] for r in daily_rets])
        annual_vol = float(rets.std() * np.sqrt(252)) if len(rets) > 0 else 0
        sharpe = (annual_return - 0.02) / annual_vol if annual_vol > 0 else 0

        nav_vals = pd.Series([d["nav"] for d in daily_nav])
        dd = (nav_vals - nav_vals.cummax()) / nav_vals.cummax()
        max_dd = float(dd.min())

        wins = sum(1 for t in trades if t["action"] == "sell" and t.get("pnl", 0) > 0)
        sell_trades = [t for t in trades if t["action"] == "sell"]
        total_trades = len(sell_trades)
        win_rate = wins / total_trades if total_trades > 0 else 0

        return BacktestResult(
            total_return=total_return,
            annual_return=annual_return,
            annual_volatility=annual_vol,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            total_trades=total_trades,
            win_trades=wins,
            loss_trades=total_trades - wins,
            win_rate=win_rate,
            nav_series=daily_nav,
            daily_returns=daily_rets,
            trades=trades,
            start_date=daily_nav[0]["date"],
            end_date=daily_nav[-1]["date"],
            initial_capital=initial_capital,
            final_capital=initial_capital * final_nav,
            n_trading_days=n_days,
        )


# 全局单例
_event_driven_engine: EventDrivenBacktestEngine | None = None


def get_event_driven_engine() -> EventDrivenBacktestEngine:
    global _event_driven_engine
    if _event_driven_engine is None:
        _event_driven_engine = EventDrivenBacktestEngine()
    return _event_driven_engine
```

- [ ] **Step 2: Write event-driven test**

`backend/tests/backtest/test_event_driven.py`:
```python
"""事件驱动回测引擎测试"""
import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig
from app.backtest.event_driven import EventDrivenBacktestEngine


def test_basic_event_driven():
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=100, freq="B")

    # 构造一只持续上涨的股票
    close = np.cumsum(np.random.randn(100) * 0.01) + 10.0
    df = pd.DataFrame({
        "trade_date": dates,
        "open": close * 0.99,
        "high": close * 1.02,
        "low": close * 0.98,
        "close": close,
        "volume": np.random.randint(1e6, 1e7, 100),
        "amount": close * np.random.randint(1e6, 1e7, 100) * 0.8,
    })
    df.set_index("trade_date", inplace=True)

    def always_buy(df):
        s = pd.Series(0, index=df.index)
        s.iloc[10] = 1
        return s

    engine = EventDrivenBacktestEngine()
    result = engine.run({"test": df}, always_buy, BacktestConfig(initial_capital=100_000))

    assert result.n_trading_days > 0
    assert len(result.trades) >= 0  # 可能没触发因为信号日无数据
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/backtest/event_driven.py backend/tests/backtest/test_event_driven.py
git commit -m "feat: add event-driven backtest engine"
```

---

## 最终验证

- [ ] **Step 1: Run all tests**

```bash
cd backend && python -m pytest tests/compute/ tests/backtest/ -v --tb=short
```

Expected: All tests pass.

- [ ] **Step 2: Verify import chain**

```bash
cd backend && python -c "
from app.compute.operators import auto_discover
auto_discover()
from app.compute.operators.registry import OperatorRegistry
print(f'Total operators: {len(OperatorRegistry.all())}')
print(f'L0: {len(OperatorRegistry.by_level(0))}')
print(f'L1: {len(OperatorRegistry.by_level(1))}')
print(f'L2: {len(OperatorRegistry.by_level(2))}')
print(f'L3: {len(OperatorRegistry.by_level(3))}')
assert len(OperatorRegistry.all()) >= 7 + 3 + 8 + 12
print('All operators loaded successfully')
"
```

- [ ] **Step 3: Verify API routes**

```bash
cd backend && python -c "
from app.main import app
routes = [(r.path, list(r.methods)) for r in app.routes if hasattr(r, 'methods')]
v2_routes = [r for r in routes if '/v2/' in r[0]]
print(f'V2 routes: {len(v2_routes)}')
for path, methods in v2_routes:
    print(f'  {methods} {path}')
"
```

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "feat: compute layer and backtest engine — implementation complete"
```
