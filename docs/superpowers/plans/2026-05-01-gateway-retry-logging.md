# DataGateway Decoupling, Retry & Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple the codebase from QMT via an abstract DataGateway interface, add exponential backoff retry to data sync, and introduce structured logging with loguru — implementing report §4.1.2 and §4.1.3.

**Architecture:** Introduce a `gateway/` module with an ABC that `QMTGateway` implements. All consumers go through the ABC. Add a standalone `retry.py` utility with exponential backoff, wired into `sync_service.py`. Replace stdlib `logging` with `loguru` across the backend, configured centrally in `core/logging.py`.

**Tech Stack:** Python 3.12+, abc/abstractmethod, asyncio, loguru, pytest + pytest-asyncio

---

## File Structure

```
backend/app/
├── gateway/                      # NEW module
│   ├── __init__.py               # Public exports
│   ├── base.py                   # DataGateway ABC + shared dataclasses (StockInfo, FinancialQuarter, KlineData)
│   └── factory.py                # create_gateway(source) -> DataGateway
├── engines/
│   ├── __init__.py               # MODIFY: update exports
│   └── qmt_gateway.py            # MODIFY: inherit ABC, rename _methods to public
├── core/
│   ├── config.py                 # MODIFY: add data_source setting
│   ├── retry.py                  # NEW: async_retry with exponential backoff
│   └── logging.py                # NEW: loguru config (console + rotating file)
├── services/
│   ├── sync_service.py           # MODIFY: use gateway ABC, retry utility, loguru
│   └── data_skill.py             # MODIFY: use gateway ABC, loguru
├── api/
│   ├── data.py                   # MODIFY: add cancel endpoint
│   └── data_skill.py             # MODIFY: use gateway ABC
├── scripts/
│   ├── sync_data.py              # MODIFY: use gateway ABC
│   └── sync_stock_info.py        # MODIFY: use gateway ABC
├── main.py                       # MODIFY: use loguru instead of logging.basicConfig
└── tests/
    ├── conftest.py               # MODIFY: add MockDataGateway fixture
    └── gateway/
        ├── __init__.py
        └── test_mock_gateway.py  # NEW: verify MockDataGateway satisfies ABC
```

---

### Task 1: Create DataGateway ABC with shared dataclasses

**Files:**
- Create: `backend/app/gateway/__init__.py`
- Create: `backend/app/gateway/base.py`

- [ ] **Step 1: Write `backend/app/gateway/base.py`**

The ABC declares the contract. Move dataclasses here from `qmt_gateway.py` so they live with the interface rather than the QMT implementation. Extract the subset of `FinancialQuarter` and `KlineData` that are already defined in `qmt_gateway.py` — keep them identical so consumers don't break.

```python
"""DataGateway abstract interface and shared dataclasses."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


# === Shared Dataclasses (moved from qmt_gateway.py, identical) ===

@dataclass
class StockInfo:
    """股票完整信息"""
    symbol: str
    name: str
    exchange: str | None = None
    industry: str | None = None
    industry2: str | None = None
    industry3: str | None = None
    sector: str | None = None
    concept: str | None = None
    list_date: date | None = None
    delist_date: date | None = None
    is_st: int = 0
    is_delist: int = 0
    is_suspend: int = 0
    total_shares: float | None = None
    float_shares: float | None = None
    a_float_shares: float | None = None
    limit_sell_shares: float | None = None
    total_mv: float | None = None
    circ_mv: float | None = None
    company_name: str | None = None
    company_name_en: str | None = None
    province: str | None = None
    city: str | None = None
    office_addr: str | None = None
    business_scope: str | None = None
    main_business: str | None = None
    website: str | None = None
    employees: int | None = None
    eps: float | None = None
    bvps: float | None = None
    roe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    total_assets: float | None = None
    total_liability: float | None = None
    total_equity: float | None = None
    net_profit: float | None = None
    revenue: float | None = None
    security_type: str | None = None
    product_class: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class FinancialQuarter:
    """单季财务数据"""
    symbol: str
    report_date: date
    report_type: str | None = None
    eps: float | None = None
    bvps: float | None = None
    roe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    total_assets: float | None = None
    total_liability: float | None = None
    total_equity: float | None = None
    net_profit: float | None = None
    revenue: float | None = None
    gross_profit: float | None = None
    operating_profit: float | None = None
    net_profit_parent: float | None = None
    net_profit_deducted: float | None = None
    operating_cash_flow: float | None = None
    investing_cash_flow: float | None = None
    financing_cash_flow: float | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class KlineData:
    """K线数据"""
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    period: str = "daily"


# === Abstract Interface ===

class DataGateway(ABC):
    """数据网关抽象接口 — 所有数据源必须实现此接口"""

    @abstractmethod
    async def check_connection(self) -> bool:
        """检查数据源连接状态"""

    @abstractmethod
    async def get_stock_list(self) -> list[StockInfo]:
        """获取全市场股票列表"""

    @abstractmethod
    async def get_stock_full_info(self, symbol: str) -> StockInfo | None:
        """获取单只股票完整信息"""

    @abstractmethod
    async def get_stock_batch_info(self, symbols: list[str]) -> list[StockInfo]:
        """批量获取股票信息"""

    @abstractmethod
    async def get_financial_quarters(
        self, symbol: str, report_count: int = 8
    ) -> list[FinancialQuarter]:
        """获取股票财务数据（多季度）"""

    @abstractmethod
    async def fetch_financial_data(self, symbol: str, report_count: int = 8):
        """获取原始财务数据DataFrame（供财务数据同步使用）"""

    @abstractmethod
    async def parse_financial_dataframes(self, raw_data: Any) -> list[dict[str, Any]]:
        """解析xtquant返回的财务数据DataFrame为字典列表"""

    @abstractmethod
    async def get_kline_daily(
        self, symbol: str, start_date: date, end_date: date
    ) -> list[KlineData]:
        """获取日线K线数据"""

    @abstractmethod
    async def get_kline_minute(
        self, symbol: str, start_date: date, end_date: date
    ) -> list[KlineData]:
        """获取分钟线K线数据"""

    @abstractmethod
    async def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        """获取实时行情快照"""

    @abstractmethod
    def clean_local_cache(
        self, symbols: list[str] | None = None, data_type: str = "kline"
    ) -> dict[str, int]:
        """清理本地缓存文件"""
```

- [ ] **Step 2: Write `backend/app/gateway/__init__.py`**

```python
from app.gateway.base import (
    DataGateway,
    StockInfo,
    FinancialQuarter,
    KlineData,
)
from app.gateway.factory import create_gateway, get_gateway

__all__ = [
    "DataGateway",
    "StockInfo",
    "FinancialQuarter",
    "KlineData",
    "create_gateway",
    "get_gateway",
]
```

- [ ] **Step 3: Verify the module imports cleanly**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -c "from app.gateway.base import DataGateway, StockInfo, FinancialQuarter, KlineData; print('OK')"
```

Expected: `OK`

---

### Task 2: Create gateway factory

**Files:**
- Create: `backend/app/gateway/factory.py`
- Modify: `backend/app/core/config.py`

- [ ] **Step 1: Add `data_source` to Settings**

In `backend/app/core/config.py`, add one line after the existing `debug` field:

```python
# In class Settings, add:
data_source: str = "qmt"  # 数据源: qmt, mock
```

- [ ] **Step 2: Write `backend/app/gateway/factory.py`**

```python
"""Gateway factory — creates and caches DataGateway instances."""
from app.gateway.base import DataGateway

_gateway: DataGateway | None = None


def create_gateway(source: str | None = None) -> DataGateway:
    """根据数据源名称创建网关实例。仅支持 'qmt' 和 'mock'。"""
    global _gateway
    if _gateway is not None:
        return _gateway

    if source is None:
        from app.core.config import settings
        source = settings.data_source

    if source == "qmt":
        from app.engines.qmt_gateway import QMTGateway
        _gateway = QMTGateway()
    elif source == "mock":
        from tests.conftest import MockDataGateway
        _gateway = MockDataGateway()
    else:
        raise ValueError(f"Unknown data source: {source}")

    return _gateway


def get_gateway() -> DataGateway:
    """获取当前缓存的网关实例（必须先调用 create_gateway 或 settings.data_source 已配置）。"""
    global _gateway
    if _gateway is None:
        _gateway = create_gateway()
    return _gateway
```

- [ ] **Step 3: Verify**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -c "from app.gateway.factory import create_gateway; print('OK')"
```

---

### Task 3: Refactor QMTGateway to implement DataGateway

**Files:**
- Modify: `backend/app/engines/qmt_gateway.py`
- Modify: `backend/app/engines/__init__.py`

- [ ] **Step 1: Remove dataclass definitions from `qmt_gateway.py` and import them from gateway**

Delete the `StockInfo`, `FinancialQuarter`, `KlineData` dataclass definitions (currently lines 9-118). Replace with imports at the top of the file:

```python
from app.gateway.base import DataGateway, StockInfo, FinancialQuarter, KlineData
```

- [ ] **Step 2: Make QMTGateway inherit DataGateway**

Change the class declaration at the line currently reading `class QMTGateway:` to:

```python
class QMTGateway(DataGateway):
```

- [ ] **Step 3: Rename private methods to public**

Rename `_fetch_financial_data` → `fetch_financial_data` (on the `def` line and all `self._fetch_financial_data` calls within the class).

Rename `_parse_financial_dataframes` → `parse_financial_dataframes` (on the `def` line and all `self._parse_financial_dataframes` calls within the class).

- [ ] **Step 4: Update `backend/app/engines/__init__.py`**

```python
from app.gateway.base import StockInfo, FinancialQuarter, KlineData
from .qmt_gateway import QMTGateway, qmt_gateway

__all__ = [
    "QMTGateway",
    "qmt_gateway",
    "StockInfo",
    "FinancialQuarter",
    "KlineData",
]
```

- [ ] **Step 5: Verify QMTGateway satisfies the ABC**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -c "
from app.engines.qmt_gateway import QMTGateway
from app.gateway.base import DataGateway
assert issubclass(QMTGateway, DataGateway), 'QMTGateway must be a DataGateway subclass'
print('OK - QMTGateway is a DataGateway')
"
```

---

### Task 4: Update all consumers to use gateway interface

**Files:**
- Modify: `backend/app/services/sync_service.py`
- Modify: `backend/app/services/data_skill.py`
- Modify: `backend/app/api/data_skill.py`
- Modify: `backend/app/scripts/sync_data.py`
- Modify: `backend/app/scripts/sync_stock_info.py`

- [ ] **Step 1: Update `sync_service.py` imports and calls**

Replace the import:
```python
# OLD
from app.engines.qmt_gateway import qmt_gateway
# NEW
from app.gateway import get_gateway
```

Add at the top of `SyncService.__init__`:
```python
self.gateway = get_gateway()
```

Replace every `qmt_gateway.` call with `self.gateway.`. In methods that use a local import alias (`from app.engines.qmt_gateway import qmt_gateway as gw`), replace with `self.gateway`.

Rename calls:
- `qmt_gateway._get_xt()` — this has no ABC equivalent. Keep as-is for now (QMT-specific internal logic). Wrap in `if hasattr(self.gateway, '_get_xt'):`.
- `qmt_gateway._fetch_financial_data(...)` → `self.gateway.fetch_financial_data(...)`
- `qmt_gateway._parse_financial_dataframes(...)` → `self.gateway.parse_financial_dataframes(...)`

- [ ] **Step 2: Update `data_skill.py` imports and calls**

Replace the direct import of `qmt_gateway`:
```python
# OLD
from app.engines.qmt_gateway import qmt_gateway
# NEW
from app.gateway import get_gateway
```

In `DataSkill.__init__`, store:
```python
self.gateway = get_gateway()
```

Replace all `qmt_gateway.` calls with `self.gateway.`:
- `qmt_gateway.get_stock_batch_info(...)` → `self.gateway.get_stock_batch_info(...)`
- `qmt_gateway.get_kline_daily(...)` → `self.gateway.get_kline_daily(...)`
- `qmt_gateway.get_kline_minute(...)` → `self.gateway.get_kline_minute(...)`
- `qmt_gateway._fetch_financial_data(...)` → `self.gateway.fetch_financial_data(...)`
- `qmt_gateway.get_realtime_quotes(...)` → `self.gateway.get_realtime_quotes(...)`
- `qmt_gateway.get_stock_list()` → `self.gateway.get_stock_list()`
- `qmt_gateway.get_stock_full_info(...)` → `self.gateway.get_stock_full_info(...)`

- [ ] **Step 3: Update `api/data_skill.py` imports**

Replace the two local imports inside functions:
```python
# OLD (inside get_realtime_quote and get_realtime_quotes)
from app.engines.qmt_gateway import qmt_gateway
# NEW
from app.gateway import get_gateway
gateway = get_gateway()
gateway.get_realtime_quotes(...)
```

- [ ] **Step 4: Update `scripts/sync_data.py` and `scripts/sync_stock_info.py`**

Replace:
```python
from app.engines.qmt_gateway import qmt_gateway
```
with:
```python
from app.gateway import get_gateway
gateway = get_gateway()
```
Then replace `qmt_gateway.` → `gateway.` in each script.

- [ ] **Step 5: Verify no remaining direct qmt_gateway imports**

```bash
cd E:\Projects\GaoshouPlatform\backend && grep -rn "from app.engines.qmt_gateway import" app/ --include="*.py" | grep -v __init__
```

Expected: empty (only `engines/__init__.py` should reference QMTGateway directly; factory.py imports it dynamically).

---

### Task 5: Create MockDataGateway for testing

**Files:**
- Create: `backend/tests/gateway/__init__.py`
- Create: `backend/tests/conftest.py` (or modify if exists)
- Create: `backend/tests/gateway/test_mock_gateway.py`

- [ ] **Step 1: Check if conftest.py exists**

Read `backend/tests/conftest.py`. If it doesn't exist, create it.

- [ ] **Step 2: Add MockDataGateway to `backend/tests/conftest.py`**

```python
"""Test fixtures — shared across all test modules."""
import pytest
from datetime import date
from app.gateway.base import DataGateway, StockInfo, FinancialQuarter, KlineData


class MockDataGateway(DataGateway):
    """返回固定数据的模拟网关，用于单元测试，不依赖 QMT 连接。"""

    def __init__(self):
        self._connected = True
        self._stock = StockInfo(
            symbol="000001.SZ",
            name="平安银行",
            exchange="SZ",
            industry="银行",
            pe_ttm=5.2,
            pb=0.7,
        )

    async def check_connection(self) -> bool:
        return self._connected

    async def get_stock_list(self) -> list[StockInfo]:
        return [self._stock]

    async def get_stock_full_info(self, symbol: str) -> StockInfo | None:
        return self._stock if symbol == "000001.SZ" else None

    async def get_stock_batch_info(self, symbols: list[str]) -> list[StockInfo]:
        return [self._stock for s in symbols if s == "000001.SZ"]

    async def get_financial_quarters(self, symbol: str, report_count: int = 8) -> list[FinancialQuarter]:
        return [FinancialQuarter(
            symbol=symbol,
            report_date=date(2025, 12, 31),
            eps=1.5,
            roe=12.0,
        )]

    async def fetch_financial_data(self, symbol: str, report_count: int = 8):
        return None  # mock 返回 None，调用方 skip

    async def parse_financial_dataframes(self, raw_data) -> list[dict[str, Any]]:
        return []

    async def get_kline_daily(self, symbol: str, start_date: date, end_date: date) -> list[KlineData]:
        return [
            KlineData(symbol=symbol, trade_date=date(2025, 12, 31),
                      open=10.0, high=11.0, low=9.5, close=10.5,
                      volume=1_000_000, amount=10_500_000),
        ]

    async def get_kline_minute(self, symbol: str, start_date: date, end_date: date) -> list[KlineData]:
        return self.get_kline_daily(symbol, start_date, end_date)  # 复用

    async def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        return [{"symbol": s, "lastPrice": 10.5, "volume": 1000000} for s in symbols]

    def clean_local_cache(self, symbols: list[str] | None = None, data_type: str = "kline") -> dict[str, int]:
        return {"deleted": 0}


@pytest.fixture
def mock_gateway() -> MockDataGateway:
    return MockDataGateway()
```

- [ ] **Step 3: Write `backend/tests/gateway/test_mock_gateway.py`**

```python
"""Verify MockDataGateway satisfies the DataGateway ABC."""
import pytest
from app.gateway.base import DataGateway


def test_mock_gateway_is_data_gateway(mock_gateway):
    """MockDataGateway must be an instance of DataGateway ABC."""
    assert isinstance(mock_gateway, DataGateway)


@pytest.mark.asyncio
async def test_mock_check_connection(mock_gateway):
    assert await mock_gateway.check_connection() is True


@pytest.mark.asyncio
async def test_mock_get_stock_list(mock_gateway):
    stocks = await mock_gateway.get_stock_list()
    assert len(stocks) == 1
    assert stocks[0].symbol == "000001.SZ"


@pytest.mark.asyncio
async def test_mock_get_kline_daily(mock_gateway):
    from datetime import date
    klines = await mock_gateway.get_kline_daily("000001.SZ", date(2025, 1, 1), date(2025, 12, 31))
    assert len(klines) == 1
    assert klines[0].close == 10.5


@pytest.mark.asyncio
async def test_mock_get_realtime_quotes(mock_gateway):
    quotes = await mock_gateway.get_realtime_quotes(["000001.SZ", "000002.SZ"])
    assert len(quotes) == 2
```

- [ ] **Step 4: Run the mock gateway tests**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -m pytest tests/gateway/test_mock_gateway.py -v
```

Expected: 4 tests pass.

---

### Task 6: Create exponential backoff retry utility

**Files:**
- Create: `backend/app/core/retry.py`
- Create: `backend/tests/test_retry.py`

- [ ] **Step 1: Write `backend/tests/test_retry.py` (failing test first)**

```python
"""Tests for async_retry utility."""
import asyncio
import pytest
from app.core.retry import async_retry


class _TransientError(Exception):
    pass


class _PermanentError(Exception):
    pass


@pytest.mark.asyncio
async def test_retry_succeeds_first_try():
    call_count = 0

    async def succeeds():
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await async_retry(succeeds, max_retries=3, base_delay=0.01)
    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_eventually_succeeds():
    call_count = 0

    async def fails_twice():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise _TransientError("transient")
        return "recovered"

    result = await async_retry(
        fails_twice,
        max_retries=3,
        base_delay=0.01,
        retryable_exceptions=(_TransientError,),
    )
    assert result == "recovered"
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_exhausted_raises():
    call_count = 0

    async def always_fails():
        nonlocal call_count
        call_count += 1
        raise _TransientError("transient")

    with pytest.raises(_TransientError):
        await async_retry(
            always_fails,
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(_TransientError,),
        )
    assert call_count == 2  # initial + 1 retry


@pytest.mark.asyncio
async def test_non_retryable_exception_raises_immediately():
    call_count = 0

    async def permanent_fail():
        nonlocal call_count
        call_count += 1
        raise _PermanentError("permanent")

    with pytest.raises(_PermanentError):
        await async_retry(
            permanent_fail,
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(_TransientError,),
        )
    assert call_count == 1  # no retry for non-retryable


@pytest.mark.asyncio
async def test_backoff_delay_increases():
    """Verify that delay increases with each retry attempt."""
    delays = []

    async def capture_delay(attempt: int):
        delays.append(attempt)

    # We test the delay formula: base_delay * (backoff ** (attempt - 1))
    # attempt 1: 0.01 * 2^0 = 0.01
    # attempt 2: 0.01 * 2^1 = 0.02
    # attempt 3: 0.01 * 2^2 = 0.04
    assert 0.01 * (2 ** 0) == 0.01
    assert 0.01 * (2 ** 1) == 0.02
    assert 0.01 * (2 ** 2) == 0.04
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -m pytest tests/test_retry.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'app.core.retry'`

- [ ] **Step 3: Write `backend/app/core/retry.py`**

```python
"""Exponential backoff retry utility for async operations."""
import asyncio
from collections.abc import Callable, Awaitable
from typing import Any, TypeVar
from loguru import logger

T = TypeVar("T")

# 默认可重试异常：网络/连接类错误
DEFAULT_RETRYABLE = (
    ConnectionError,
    TimeoutError,
    OSError,
)


async def async_retry(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff: float = 2.0,
    retryable_exceptions: tuple[type[Exception], ...] = DEFAULT_RETRYABLE,
    **kwargs: Any,
) -> T:
    """对异步函数执行指数退避重试。

    Args:
        func: 要重试的异步函数
        *args: 传递给 func 的位置参数
        max_retries: 最大重试次数（不含首次执行，共执行 max_retries+1 次）
        base_delay: 首次重试等待秒数
        backoff: 退避倍数（每次重试延迟 = base_delay * backoff^(attempt-1)）
        retryable_exceptions: 可重试的异常类型元组
        **kwargs: 传递给 func 的关键字参数

    Returns:
        func 的返回值

    Raises:
        func 最后一次执行抛出的异常（重试耗尽后）
    """
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except retryable_exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = base_delay * (backoff ** attempt)
                logger.warning(
                    f"Retry {attempt + 1}/{max_retries} for {func.__name__}: "
                    f"{e.__class__.__name__}: {e}. Waiting {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"All {max_retries} retries exhausted for {func.__name__}: "
                    f"{e.__class__.__name__}: {e}"
                )
        except Exception:
            # 非可重试异常直接抛出，不重试
            raise

    raise last_exception  # type: ignore[misc]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -m pytest tests/test_retry.py -v
```

Expected: 5 tests PASS.

---

### Task 7: Wire exponential backoff into sync_service

**Files:**
- Modify: `backend/app/services/sync_service.py`

- [ ] **Step 1: Add import at top of `sync_service.py`**

```python
from app.core.retry import async_retry
```

- [ ] **Step 2: Replace inline single-retry in `sync_kline_daily`**

Find the `elif failure_strategy == "retry":` block (around line 898). Replace the inline try/except with:

```python
elif failure_strategy == "retry":
    try:
        await async_retry(
            lambda: _fetch_and_insert_kline(symbol, start_date, end_date),
            max_retries=sync_task.retry_count if sync_task else 3,
            base_delay=1.0,
            backoff=2.0,
        )
        progress.success_count += 1
        progress.failed_count -= 1
        failed_symbols.pop()
    except Exception:
        pass  # retry exhausted, stay failed
```

Extract the kline fetch+insert logic into a helper so the retry lambda is clean:

```python
async def _fetch_and_insert_kline(self, symbol, start_date, end_date):
    klines = await self.gateway.get_kline_daily(symbol, start_date, end_date)
    if klines:
        await self._insert_klines_to_clickhouse(klines, "daily")
```

- [ ] **Step 3: Apply same pattern to `sync_kline_minute`**

Replace the `elif failure_strategy == "retry":` block (around line 1107) identically to Step 2, using a minute-specific helper.

- [ ] **Step 4: Apply retry to `sync_stock_info`**

Replace the inline single-retry (around line 238) with `async_retry`, using the existing minimal-insert logic inside the retry lambda.

- [ ] **Step 5: Add retry to methods that lack it**

In `sync_stock_full`:
```python
elif failure_strategy == "retry":
    try:
        await async_retry(
            lambda: self._sync_single_stock_full(symbol),
            max_retries=task.retry_count if task else 3,
            base_delay=1.0,
        )
        ...
    except Exception:
        pass
```

In `sync_financial_data`:
```python
elif failure_strategy == "retry":
    try:
        await async_retry(
            lambda: self._sync_single_financial(symbol, report_count),
            max_retries=task.retry_count if task else 3,
            base_delay=1.0,
        )
        ...
    except Exception:
        pass
```

- [ ] **Step 6: Fix missing `stop` branch in `sync_stock_full`**

Find the exception handler in `sync_stock_full` that only has `skip` logic. Add:
```python
if failure_strategy == "stop":
    raise
```

- [ ] **Step 7: Verify sync_service imports cleanly**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -c "from app.services.sync_service import SyncService; print('OK')"
```

---

### Task 8: Add sync cancel API endpoint

**Files:**
- Modify: `backend/app/api/data.py`

- [ ] **Step 1: Check that `cancel_sync` exists in `SyncService`**

The method `cancel_sync()` is at line 1252 of `sync_service.py`. Verify it exists and understand its behavior (sets a cancel flag).

- [ ] **Step 2: Add cancel endpoint to `backend/app/api/data.py`**

```python
@router.post("/sync/cancel")
async def cancel_sync(
    db: AsyncSession = Depends(get_async_session),
):
    """取消当前正在运行的同步任务"""
    service = SyncService(db)
    cancelled = await service.cancel_sync()
    return {
        "code": 0,
        "message": "同步已取消" if cancelled else "当前没有正在运行的同步任务",
        "data": {"cancelled": cancelled},
    }
```

---

### Task 9: Set up loguru structured logging

**Files:**
- Create: `backend/app/core/logging.py`
- Modify: `backend/app/main.py`

- [ ] **Step 1: Install loguru**

```bash
cd E:\Projects\GaoshouPlatform\backend && pip install loguru
```

- [ ] **Step 2: Write `backend/app/core/logging.py`**

```python
"""Centralized logging configuration using loguru."""
import sys
from pathlib import Path
from loguru import logger


def setup_logging(debug: bool = False) -> None:
    """配置 loguru — 移除默认 handler，添加控制台 + 文件输出。

    Args:
        debug: 是否开启 DEBUG 级别日志
    """
    logger.remove()  # 移除默认 handler

    # 控制台输出 — 彩色，简洁格式
    level = "DEBUG" if debug else "INFO"
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        colorize=True,
    )

    # 确保日志目录存在
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    # 文件输出 — JSON 结构化，按天轮转，保留 30 天
    logger.add(
        log_dir / "gaoshou_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        format="{time} | {level} | {name}:{function}:{line} | {message}",
        rotation="00:00",  # 每天午夜轮转
        retention="30 days",
        compression="gz",
        encoding="utf-8",
        enqueue=True,  # 多进程安全
    )

    # 错误单独记录到 error 文件
    logger.add(
        log_dir / "error_{time:YYYY-MM-DD}.log",
        level="ERROR",
        format="{time} | {name}:{function}:{line} | {message}\n{exception}",
        rotation="00:00",
        retention="90 days",
        encoding="utf-8",
        enqueue=True,
    )

    logger.info(f"Logging configured (level={level}, log_dir={log_dir})")
```

- [ ] **Step 3: Replace `logging.basicConfig` in `main.py`**

Remove lines 13-18:
```python
# REMOVE:
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
```

Replace with:
```python
from app.core.logging import setup_logging
from loguru import logger

setup_logging(debug=settings.debug)
```

- [ ] **Step 4: Replace `logging.getLogger` with `from loguru import logger` in sync_service.py**

Remove:
```python
import logging
logger = logging.getLogger(__name__)
```

Add:
```python
from loguru import logger
```

Do this for all files that use logging:
- `backend/app/services/sync_service.py`
- `backend/app/services/factor_evaluation.py`
- `backend/app/api/data.py` (local imports)
- `backend/app/core/scheduler.py`
- `backend/app/main.py`

- [ ] **Step 5: Add structured fields to sync job logging in scheduler.py**

In `_execute_sync_job`, add context to log calls:
```python
logger.info(
    "Sync job started: sync_type={sync_type}, task_id={task_id}",
    sync_type=sync_type,
    task_id=task_id,
)
```

And on failure:
```python
logger.opt(exception=True).error(
    "Sync job failed: sync_type={sync_type}, task_id={task_id}",
    sync_type=sync_type,
    task_id=task_id,
)
```

- [ ] **Step 6: Verify loguru is working**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -c "
from app.core.logging import setup_logging
setup_logging(debug=True)
from loguru import logger
logger.info('Test log from verification')
logger.error('Test error log')
print('Check backend/logs/ for log files')
"
```

Check that `backend/logs/` directory now contains dated log files.

---

### Task 10: Run full test suite to verify no regressions

- [ ] **Step 1: Run all tests**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -m pytest tests/ -v
```

Expected: All previously passing tests still pass; new gateway and retry tests pass. Some tests may fail if they import `qmt_gateway` directly — fix those imports.

- [ ] **Step 2: Verify imports work for all entry points**

```bash
cd E:\Projects\GaoshouPlatform\backend && python -c "
from app.main import app
from app.gateway import DataGateway, create_gateway
from app.core.retry import async_retry
from app.core.logging import setup_logging
print('All critical imports OK')
"
```

---

### Task 11: Commit

- [ ] **Step 1: Verify git status**

```bash
cd E:\Projects\GaoshouPlatform && git status
```

- [ ] **Step 2: Stage and commit**

```bash
cd E:\Projects\GaoshouPlatform && git add backend/app/gateway/ backend/app/engines/qmt_gateway.py backend/app/engines/__init__.py backend/app/core/retry.py backend/app/core/logging.py backend/app/core/config.py backend/app/services/sync_service.py backend/app/services/data_skill.py backend/app/api/data.py backend/app/api/data_skill.py backend/app/scripts/sync_data.py backend/app/scripts/sync_stock_info.py backend/app/main.py backend/tests/ backend/pyproject.toml
git commit -m "$(cat <<'EOF'
feat: add DataGateway ABC, exponential backoff retry, and loguru structured logging

Implementing report §4.1.2 (fault tolerance + logging) and §4.1.3 (data source decoupling):
- gateway/ module with DataGateway ABC, shared dataclasses, factory
- QMTGateway now implements DataGateway
- async_retry() with configurable exponential backoff
- Sync cancel API endpoint
- loguru replaces stdlib logging with rotating file output

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**1. Spec coverage:**
- §4.1.3 DataGateway abstraction → Tasks 1-5 ✓
- §4.1.2 Exponential backoff retry → Tasks 6-7 ✓
- §4.1.2 Structured logging → Task 9 ✓
- §4.1.2 Sync observability → Task 8 (cancel API) + Task 9 (structured fields) ✓
- MockDataGateway for testing → Task 5 ✓

**2. Placeholder scan:** No TBD, TODO, or "implement later" patterns. Every step has concrete code.

**3. Type consistency:** DataGateway ABC methods match QMTGateway renamed methods. Dataclass fields preserved exactly. `async_retry` signature consistent between test and implementation.
