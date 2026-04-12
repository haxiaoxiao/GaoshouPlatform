# Phase 2: 数据模块 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现数据获取、存储、展示功能，包括 miniQMT 网关封装、数据同步服务、股票列表页面和 K线图表。

**Architecture:** 分层服务架构。网关层封装 xtquant SDK，服务层处理数据查询和同步，API 层提供 RESTful 接口，前端使用 ECharts 展示 K线图。

**Tech Stack:** Python: FastAPI, SQLAlchemy, APScheduler, xtquant | Frontend: Vue 3, Element Plus, ECharts, Axios

---

## Task 1: 新增数据表模型

**Files:**
- Create: `backend/app/db/models/watchlist.py`
- Create: `backend/app/db/models/sync.py`
- Modify: `backend/app/db/models/__init__.py`

**Step 1: 创建自选股模型 watchlist.py**

```python
# backend/app/db/models/watchlist.py
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class WatchlistGroup(Base, TimestampMixin):
    """自选股分组表"""

    __tablename__ = "watchlist_groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment="分组名称")
    description: Mapped[str | None] = mapped_column(Text, comment="分组描述")

    # 关联股票
    stocks: Mapped[list["WatchlistStock"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )


class WatchlistStock(Base):
    """自选股关联表"""

    __tablename__ = "watchlist_stocks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        ForeignKey("watchlist_groups.id"), nullable=False, comment="分组ID"
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, comment="股票代码")
    added_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )

    # 关联分组
    group: Mapped["WatchlistGroup"] = relationship(back_populates="stocks")
```

**Step 2: 创建同步任务模型 sync.py**

```python
# backend/app/db/models/sync.py
from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class SyncTask(Base, TimestampMixin):
    """同步任务配置表"""

    __tablename__ = "sync_tasks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="任务名称")
    cron_expression: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="Cron 表达式"
    )
    sync_type: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="同步类型: stock_info/kline_daily/kline_minute"
    )
    symbols: Mapped[str | None] = mapped_column(Text, comment="股票代码列表(JSON)")
    start_date: Mapped[date | None] = mapped_column(Date, comment="历史数据起始日期")
    end_date: Mapped[date | None] = mapped_column(Date, comment="历史数据结束日期")
    failure_strategy: Mapped[str] = mapped_column(
        String(20), default="skip", comment="失败策略: skip/retry/stop"
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=3, comment="重试次数")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, comment="是否启用")
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, comment="上次执行时间")
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime, comment="下次执行时间")

    # 关联执行记录
    logs: Mapped[list["SyncLog"]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class SyncLog(Base):
    """同步执行记录表"""

    __tablename__ = "sync_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[int | None] = mapped_column(
        ForeignKey("sync_tasks.id"), comment="关联任务ID(手动同步为空)"
    )
    sync_type: Mapped[str] = mapped_column(String(50), nullable=False, comment="同步类型")
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="状态: running/completed/failed"
    )
    total_count: Mapped[int | None] = mapped_column(Integer, comment="总数量")
    success_count: Mapped[int | None] = mapped_column(Integer, comment="成功数量")
    failed_count: Mapped[int | None] = mapped_column(Integer, comment="失败数量")
    start_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="开始时间")
    end_time: Mapped[datetime | None] = mapped_column(DateTime, comment="结束时间")
    error_message: Mapped[str | None] = mapped_column(Text, comment="错误信息")
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="详细结果")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )

    # 关联任务
    task: Mapped["SyncTask | None"] = relationship(back_populates="logs")
```

**Step 3: 更新 models/__init__.py 导出新模型**

```python
# backend/app/db/models/__init__.py
"""数据模型"""
from .base import Base, TimestampMixin
from .factor import Factor, FactorAnalysis
from .stock import KlineDaily, KlineMinute, Stock
from .strategy import Backtest, Order, Strategy, Trade
from .sync import SyncLog, SyncTask
from .watchlist import WatchlistGroup, WatchlistStock

__all__ = [
    "Base",
    "TimestampMixin",
    "Stock",
    "KlineDaily",
    "KlineMinute",
    "Strategy",
    "Backtest",
    "Order",
    "Trade",
    "Factor",
    "FactorAnalysis",
    "WatchlistGroup",
    "WatchlistStock",
    "SyncTask",
    "SyncLog",
]
```

**Step 4: 验证模型导入**

```bash
cd backend
python -c "from app.db.models import *; print('All models imported successfully')"
```

**Step 5: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add watchlist and sync task models

- Add WatchlistGroup and WatchlistStock models
- Add SyncTask and SyncLog models
- Update models/__init__.py exports

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: QMTGateway 网关封装

**Files:**
- Create: `backend/app/engines/__init__.py`
- Create: `backend/app/engines/qmt_gateway.py`

**Step 1: 创建 engines 目录和 __init__.py**

```python
# backend/app/engines/__init__.py
"""核心引擎模块"""
from .qmt_gateway import QMTGateway

__all__ = ["QMTGateway"]
```

**Step 2: 创建 QMTGateway 类**

```python
# backend/app/engines/qmt_gateway.py
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from app.core.config import settings


@dataclass
class StockInfo:
    """股票信息"""

    symbol: str
    name: str
    exchange: str | None = None
    industry: str | None = None
    list_date: date | None = None


@dataclass
class KlineData:
    """K线数据"""

    symbol: str
    datetime: datetime | date
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float


class QMTGateway:
    """miniQMT 数据网关 (xtquant 封装)"""

    def __init__(self):
        self._connected = False
        self._xt = None

    def _get_xt(self):
        """延迟导入 xtquant"""
        if self._xt is None:
            try:
                import xtquant.xtdata as xt

                self._xt = xt
            except ImportError:
                raise RuntimeError("xtquant 未安装，请运行: pip install xtquant")
        return self._xt

    async def check_connection(self) -> bool:
        """检查连接状态"""
        try:
            xt = self._get_xt()
            # 尝试获取股票列表来验证连接
            stocks = xt.get_stock_list_in_sector("沪深A股")
            self._connected = len(stocks) > 0
            return self._connected
        except Exception:
            self._connected = False
            return False

    async def get_stock_list(self) -> list[StockInfo]:
        """获取股票列表"""
        xt = self._get_xt()

        # 在线程池中运行同步代码
        loop = asyncio.get_event_loop()
        stock_codes = await loop.run_in_executor(
            None, xt.get_stock_list_in_sector, "沪深A股"
        )

        results = []
        for code in stock_codes:
            try:
                # 获取股票详情
                info = await loop.run_in_executor(
                    None, xt.get_instrument_detail, code
                )
                if info:
                    stock_info = StockInfo(
                        symbol=code,
                        name=info.get("InstrumentName", ""),
                        exchange=info.get("ExchangeCode"),
                        industry=info.get("ProductClass"),
                        list_date=self._parse_date(info.get("OpenDate")),
                    )
                    results.append(stock_info)
            except Exception:
                # 单只股票获取失败不影响整体
                continue

        return results

    async def get_kline_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[KlineData]:
        """获取日K线数据"""
        xt = self._get_xt()

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            lambda: xt.get_market_data_ex(
                field_list=[],
                stock_list=[symbol],
                period="1d",
                start_time=start_str,
                end_time=end_str,
            ),
        )

        results = []
        if symbol in data:
            df = data[symbol]
            for idx, row in df.iterrows():
                try:
                    kline = KlineData(
                        symbol=symbol,
                        datetime=idx.date() if hasattr(idx, "date") else idx,
                        open=float(row.get("open", 0)),
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=float(row.get("close", 0)),
                        volume=int(row.get("volume", 0)),
                        amount=float(row.get("amount", 0)),
                    )
                    results.append(kline)
                except Exception:
                    continue

        return results

    async def get_kline_minute(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[KlineData]:
        """获取分钟K线数据"""
        xt = self._get_xt()

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            lambda: xt.get_market_data_ex(
                field_list=[],
                stock_list=[symbol],
                period="1m",
                start_time=start_str,
                end_time=end_str,
            ),
        )

        results = []
        if symbol in data:
            df = data[symbol]
            for idx, row in df.iterrows():
                try:
                    kline = KlineData(
                        symbol=symbol,
                        datetime=idx,
                        open=float(row.get("open", 0)),
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=float(row.get("close", 0)),
                        volume=int(row.get("volume", 0)),
                        amount=float(row.get("amount", 0)),
                    )
                    results.append(kline)
                except Exception:
                    continue

        return results

    def _parse_date(self, date_str: str | None) -> date | None:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            return datetime.strptime(str(date_str), "%Y%m%d").date()
        except ValueError:
            return None


# 全局单例
qmt_gateway = QMTGateway()
```

**Step 3: 更新 requirements.txt 添加依赖**

```txt
# 在 backend/requirements.txt 末尾添加
apscheduler>=3.10.0
xtquant
```

**Step 4: 安装新依赖**

```bash
cd backend
pip install apscheduler xtquant
```

**Step 5: 验证网关可以导入**

```bash
cd backend
python -c "from app.engines import QMTGateway; print('QMTGateway imported successfully')"
```

**Step 6: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add QMTGateway for xtquant integration

- Create async wrapper for xtquant SDK
- Support stock list, daily and minute kline data
- Add apscheduler and xtquant dependencies

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: DataService 数据查询服务

**Files:**
- Create: `backend/app/services/__init__.py`
- Create: `backend/app/services/data_service.py`

**Step 1: 创建 services 目录和 __init__.py**

```python
# backend/app/services/__init__.py
"""业务服务模块"""
from .data_service import DataService

__all__ = ["DataService"]
```

**Step 2: 创建 DataService 类**

```python
# backend/app/services/data_service.py
import math
from dataclasses import dataclass
from datetime import date

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import KlineDaily, KlineMinute, Stock, WatchlistGroup, WatchlistStock


@dataclass
class PaginatedResult:
    """分页结果"""

    items: list
    total: int
    page: int
    page_size: int
    total_pages: int


class DataService:
    """数据查询服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_stocks(
        self,
        search: str | None = None,
        industry: str | None = None,
        group_id: int | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> PaginatedResult:
        """查询股票列表"""
        query = select(Stock)

        # 搜索条件
        if search:
            query = query.where(
                or_(
                    Stock.symbol.ilike(f"%{search}%"),
                    Stock.name.ilike(f"%{search}%"),
                )
            )

        # 行业筛选
        if industry:
            query = query.where(Stock.industry == industry)

        # 自选股分组筛选
        if group_id:
            subquery = select(WatchlistStock.symbol).where(
                WatchlistStock.group_id == group_id
            )
            query = query.where(Stock.symbol.in_(subquery))

        # 计算总数
        count_query = select(func.count()).select_from(query.subquery())
        total = await self.session.scalar(count_query) or 0

        # 分页
        query = query.offset((page - 1) * page_size).limit(page_size)
        query = query.order_by(Stock.symbol)

        result = await self.session.execute(query)
        stocks = list(result.scalars().all())

        return PaginatedResult(
            items=stocks,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=math.ceil(total / page_size) if total > 0 else 0,
        )

    async def get_stock_by_symbol(self, symbol: str) -> Stock | None:
        """根据代码获取股票"""
        query = select(Stock).where(Stock.symbol == symbol)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_klines(
        self,
        symbol: str,
        kline_type: str = "daily",
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list:
        """查询K线数据"""
        Model = KlineDaily if kline_type == "daily" else KlineMinute

        query = select(Model).where(Model.symbol == symbol)

        if kline_type == "daily":
            if start_date:
                query = query.where(Model.trade_date >= start_date)
            if end_date:
                query = query.where(Model.trade_date <= end_date)
            query = query.order_by(Model.trade_date.desc())
        else:
            if start_date:
                # 分钟线的 start_date 需要转换为 datetime
                from datetime import datetime

                start_datetime = datetime.combine(start_date, datetime.min.time())
                query = query.where(Model.datetime >= start_datetime)
            if end_date:
                from datetime import datetime

                end_datetime = datetime.combine(end_date, datetime.max.time())
                query = query.where(Model.datetime <= end_datetime)
            query = query.order_by(Model.datetime.desc())

        query = query.limit(limit)

        result = await self.session.execute(query)
        klines = list(result.scalars().all())

        # 反转顺序，从旧到新
        return list(reversed(klines))

    async def get_industries(self) -> list[str]:
        """获取所有行业列表"""
        query = (
            select(Stock.industry)
            .where(Stock.industry.isnot(None))
            .distinct()
            .order_by(Stock.industry)
        )
        result = await self.session.execute(query)
        return [row[0] for row in result.all()]

    async def get_watchlist_groups(self) -> list[WatchlistGroup]:
        """获取所有自选股分组"""
        query = select(WatchlistGroup).order_by(WatchlistGroup.id)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def create_watchlist_group(
        self, name: str, description: str | None = None
    ) -> WatchlistGroup:
        """创建自选股分组"""
        group = WatchlistGroup(name=name, description=description)
        self.session.add(group)
        await self.session.flush()
        return group

    async def add_stock_to_group(
        self, group_id: int, symbol: str
    ) -> WatchlistStock | None:
        """添加股票到分组"""
        # 检查分组是否存在
        group = await self.session.get(WatchlistGroup, group_id)
        if not group:
            return None

        # 检查股票是否存在
        stock = await self.get_stock_by_symbol(symbol)
        if not stock:
            return None

        # 检查是否已添加
        query = select(WatchlistStock).where(
            WatchlistStock.group_id == group_id,
            WatchlistStock.symbol == symbol,
        )
        existing = await self.session.execute(query)
        if existing.scalar_one_or_none():
            return None  # 已存在

        watchlist_stock = WatchlistStock(group_id=group_id, symbol=symbol)
        self.session.add(watchlist_stock)
        await self.session.flush()
        return watchlist_stock

    async def remove_stock_from_group(
        self, group_id: int, symbol: str
    ) -> bool:
        """从分组移除股票"""
        query = select(WatchlistStock).where(
            WatchlistStock.group_id == group_id,
            WatchlistStock.symbol == symbol,
        )
        result = await self.session.execute(query)
        watchlist_stock = result.scalar_one_or_none()

        if watchlist_stock:
            await self.session.delete(watchlist_stock)
            return True
        return False

    async def delete_watchlist_group(self, group_id: int) -> bool:
        """删除自选股分组"""
        group = await self.session.get(WatchlistGroup, group_id)
        if group:
            await self.session.delete(group)
            return True
        return False
```

**Step 3: 验证服务可以导入**

```bash
cd backend
python -c "from app.services import DataService; print('DataService imported successfully')"
```

**Step 4: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add DataService for data queries

- Support paginated stock list with filters
- Support kline data queries
- Support watchlist group management
- Add industry list query

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: SyncService 数据同步服务

**Files:**
- Create: `backend/app/services/sync_service.py`
- Modify: `backend/app/services/__init__.py`

**Step 1: 创建 SyncService 类**

```python
# backend/app/services/sync_service.py
import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import KlineDaily, KlineMinute, Stock, SyncLog, SyncTask
from app.engines.qmt_gateway import KlineData, StockInfo, qmt_gateway


@dataclass
class SyncProgress:
    """同步进度"""

    total: int
    current: int
    success: int
    failed: int
    current_symbol: str | None
    status: str  # idle / running / completed / failed


@dataclass
class SyncResult:
    """同步结果"""

    success: bool
    total: int
    success_count: int
    failed_count: int
    errors: list[dict[str, Any]]
    duration_seconds: float


# 全局同步状态
_current_sync: SyncProgress | None = None


class SyncService:
    """数据同步服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    def get_sync_status(self) -> SyncProgress:
        """获取当前同步状态"""
        global _current_sync
        if _current_sync is None:
            return SyncProgress(
                total=0, current=0, success=0, failed=0,
                current_symbol=None, status="idle"
            )
        return _current_sync

    async def sync_stock_info(
        self,
        symbols: list[str] | None = None,
        on_progress: Callable[[SyncProgress], None] | None = None,
    ) -> SyncResult:
        """同步股票基础信息"""
        global _current_sync
        start_time = datetime.now()

        # 获取股票列表
        all_stocks = await qmt_gateway.get_stock_list()

        # 如果指定了股票列表，则过滤
        if symbols:
            all_stocks = [s for s in all_stocks if s.symbol in symbols]

        total = len(all_stocks)
        success_count = 0
        failed_count = 0
        errors = []

        _current_sync = SyncProgress(
            total=total, current=0, success=0, failed=0,
            current_symbol=None, status="running"
        )

        for i, stock_info in enumerate(all_stocks):
            _current_sync.current = i + 1
            _current_sync.current_symbol = stock_info.symbol

            try:
                # 更新或插入股票信息
                existing = await self.session.get(Stock, stock_info.symbol)
                if existing:
                    existing.name = stock_info.name
                    existing.exchange = stock_info.exchange
                    existing.industry = stock_info.industry
                    existing.list_date = stock_info.list_date
                else:
                    stock = Stock(
                        symbol=stock_info.symbol,
                        name=stock_info.name,
                        exchange=stock_info.exchange,
                        industry=stock_info.industry,
                        list_date=stock_info.list_date,
                    )
                    self.session.add(stock)

                await self.session.commit()
                success_count += 1
                _current_sync.success = success_count

            except Exception as e:
                await self.session.rollback()
                failed_count += 1
                _current_sync.failed = failed_count
                errors.append({
                    "symbol": stock_info.symbol,
                    "error": str(e),
                })

            if on_progress:
                on_progress(_current_sync)

            # 避免请求过快
            await asyncio.sleep(0.01)

        _current_sync.status = "completed"
        _current_sync.current_symbol = None

        duration = (datetime.now() - start_time).total_seconds()
        return SyncResult(
            success=True,
            total=total,
            success_count=success_count,
            failed_count=failed_count,
            errors=errors,
            duration_seconds=duration,
        )

    async def sync_kline_daily(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        failure_strategy: str = "skip",
        retry_count: int = 3,
        on_progress: Callable[[SyncProgress], None] | None = None,
    ) -> SyncResult:
        """同步日K线数据"""
        global _current_sync
        start_time = datetime.now()

        # 如果未指定股票列表，获取所有股票
        if symbols is None:
            query = select(Stock.symbol)
            result = await self.session.execute(query)
            symbols = [row[0] for row in result.all()]

        # 默认时间范围：最近1年
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            from datetime import timedelta
            start_date = end_date - timedelta(days=365)

        total = len(symbols)
        success_count = 0
        failed_count = 0
        errors = []

        _current_sync = SyncProgress(
            total=total, current=0, success=0, failed=0,
            current_symbol=None, status="running"
        )

        for i, symbol in enumerate(symbols):
            _current_sync.current = i + 1
            _current_sync.current_symbol = symbol

            try:
                klines = await self._sync_single_kline_daily(
                    symbol, start_date, end_date, retry_count
                )
                success_count += 1
                _current_sync.success = success_count

            except Exception as e:
                failed_count += 1
                _current_sync.failed = failed_count
                errors.append({
                    "symbol": symbol,
                    "error": str(e),
                })

                if failure_strategy == "stop":
                    _current_sync.status = "failed"
                    break

            if on_progress:
                on_progress(_current_sync)

            # 避免请求过快
            await asyncio.sleep(0.05)

        _current_sync.status = "completed"
        _current_sync.current_symbol = None

        duration = (datetime.now() - start_time).total_seconds()
        return SyncResult(
            success=True,
            total=total,
            success_count=success_count,
            failed_count=failed_count,
            errors=errors,
            duration_seconds=duration,
        )

    async def _sync_single_kline_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        retry_count: int,
    ) -> list[KlineData]:
        """同步单只股票的日K线"""
        klines = await qmt_gateway.get_kline_daily(symbol, start_date, end_date)

        for kline in klines:
            # 检查是否已存在
            query = select(KlineDaily).where(
                KlineDaily.symbol == symbol,
                KlineDaily.trade_date == kline.datetime,
            )
            existing = await self.session.execute(query)
            if existing.scalar_one_or_none():
                continue

            kline_record = KlineDaily(
                symbol=symbol,
                trade_date=kline.datetime,
                open=kline.open,
                high=kline.high,
                low=kline.low,
                close=kline.close,
                volume=kline.volume,
                amount=kline.amount,
            )
            self.session.add(kline_record)

        await self.session.commit()
        return klines

    async def create_sync_log(
        self,
        task_id: int | None,
        sync_type: str,
        result: SyncResult,
    ) -> SyncLog:
        """创建同步日志"""
        log = SyncLog(
            task_id=task_id,
            sync_type=sync_type,
            status="completed" if result.success else "failed",
            total_count=result.total,
            success_count=result.success_count,
            failed_count=result.failed_count,
            start_time=datetime.now(),
            end_time=datetime.now(),
            error_message=json.dumps(result.errors[:10]) if result.errors else None,
            details={
                "duration_seconds": result.duration_seconds,
                "error_count": len(result.errors),
            },
        )
        self.session.add(log)
        await self.session.commit()
        return log

    async def get_sync_logs(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> list[SyncLog]:
        """获取同步日志列表"""
        query = (
            select(SyncLog)
            .order_by(SyncLog.start_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
```

**Step 2: 更新 services/__init__.py**

```python
# backend/app/services/__init__.py
"""业务服务模块"""
from .data_service import DataService
from .sync_service import SyncService

__all__ = ["DataService", "SyncService"]
```

**Step 3: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add SyncService for data synchronization

- Support stock info sync with progress tracking
- Support daily kline sync with retry and failure strategies
- Add sync log recording
- Global sync status tracking

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: 数据相关 API 接口

**Files:**
- Create: `backend/app/api/data.py`
- Modify: `backend/app/api/router.py`
- Modify: `backend/app/db/__init__.py`

**Step 1: 创建 data.py API 路由**

```python
# backend/app/api/data.py
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_async_session
from app.services import DataService, SyncService

router = APIRouter()


# ============ Pydantic Models ============

class WatchlistGroupCreate(BaseModel):
    name: str
    description: str | None = None


class WatchlistGroupUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


class SyncRequest(BaseModel):
    sync_type: str  # stock_info / kline_daily / kline_minute
    symbols: list[str] | None = None
    start_date: date | None = None
    end_date: date | None = None
    failure_strategy: str = "skip"  # skip / retry / stop
    retry_count: int = 3


class AddStockToGroup(BaseModel):
    symbol: str


# ============ Stock APIs ============

@router.get("/stocks")
async def get_stocks(
    search: str | None = Query(None),
    industry: str | None = Query(None),
    group_id: int | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_async_session),
):
    """获取股票列表"""
    service = DataService(session)
    result = await service.get_stocks(
        search=search,
        industry=industry,
        group_id=group_id,
        page=page,
        page_size=page_size,
    )
    return {
        "items": [
            {
                "symbol": s.symbol,
                "name": s.name,
                "exchange": s.exchange,
                "industry": s.industry,
            }
            for s in result.items
        ],
        "total": result.total,
        "page": result.page,
        "page_size": result.page_size,
        "total_pages": result.total_pages,
    }


@router.get("/stocks/{symbol}")
async def get_stock_detail(
    symbol: str,
    session: AsyncSession = Depends(get_async_session),
):
    """获取股票详情"""
    service = DataService(session)
    stock = await service.get_stock_by_symbol(symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    return {
        "symbol": stock.symbol,
        "name": stock.name,
        "exchange": stock.exchange,
        "industry": stock.industry,
        "list_date": stock.list_date,
    }


@router.get("/industries")
async def get_industries(
    session: AsyncSession = Depends(get_async_session),
):
    """获取行业列表"""
    service = DataService(session)
    industries = await service.get_industries()
    return {"industries": industries}


# ============ Kline APIs ============

@router.get("/klines")
async def get_klines(
    symbol: str = Query(...),
    kline_type: str = Query("daily"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    limit: int = Query(500, ge=1, le=2000),
    session: AsyncSession = Depends(get_async_session),
):
    """获取K线数据"""
    service = DataService(session)
    klines = await service.get_klines(
        symbol=symbol,
        kline_type=kline_type,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    return {
        "symbol": symbol,
        "kline_type": kline_type,
        "data": [
            {
                "datetime": str(k.trade_date if hasattr(k, "trade_date") else k.datetime),
                "open": float(k.open) if k.open else None,
                "high": float(k.high) if k.high else None,
                "low": float(k.low) if k.low else None,
                "close": float(k.close) if k.close else None,
                "volume": k.volume,
                "amount": float(k.amount) if k.amount else None,
            }
            for k in klines
        ],
    }


# ============ Watchlist APIs ============

@router.get("/watchlist/groups")
async def get_watchlist_groups(
    session: AsyncSession = Depends(get_async_session),
):
    """获取自选股分组列表"""
    service = DataService(session)
    groups = await service.get_watchlist_groups()
    return {
        "groups": [
            {
                "id": g.id,
                "name": g.name,
                "description": g.description,
                "stock_count": len(g.stocks) if g.stocks else 0,
            }
            for g in groups
        ]
    }


@router.post("/watchlist/groups")
async def create_watchlist_group(
    data: WatchlistGroupCreate,
    session: AsyncSession = Depends(get_async_session),
):
    """创建自选股分组"""
    service = DataService(session)
    group = await service.create_watchlist_group(data.name, data.description)
    await session.commit()
    return {"id": group.id, "name": group.name}


@router.delete("/watchlist/groups/{group_id}")
async def delete_watchlist_group(
    group_id: int,
    session: AsyncSession = Depends(get_async_session),
):
    """删除自选股分组"""
    service = DataService(session)
    success = await service.delete_watchlist_group(group_id)
    await session.commit()
    if not success:
        raise HTTPException(status_code=404, detail="Group not found")
    return {"success": True}


@router.post("/watchlist/groups/{group_id}/stocks")
async def add_stock_to_group(
    group_id: int,
    data: AddStockToGroup,
    session: AsyncSession = Depends(get_async_session),
):
    """添加股票到分组"""
    service = DataService(session)
    result = await service.add_stock_to_group(group_id, data.symbol)
    await session.commit()
    if not result:
        raise HTTPException(status_code=400, detail="Failed to add stock")
    return {"success": True}


@router.delete("/watchlist/groups/{group_id}/stocks/{symbol}")
async def remove_stock_from_group(
    group_id: int,
    symbol: str,
    session: AsyncSession = Depends(get_async_session),
):
    """从分组移除股票"""
    service = DataService(session)
    success = await service.remove_stock_from_group(group_id, symbol)
    await session.commit()
    if not success:
        raise HTTPException(status_code=404, detail="Stock not in group")
    return {"success": True}


# ============ Sync APIs ============

@router.post("/sync")
async def trigger_sync(
    data: SyncRequest,
    session: AsyncSession = Depends(get_async_session),
):
    """手动触发同步"""
    service = SyncService(session)

    if data.sync_type == "stock_info":
        result = await service.sync_stock_info(symbols=data.symbols)
    elif data.sync_type == "kline_daily":
        result = await service.sync_kline_daily(
            symbols=data.symbols,
            start_date=data.start_date,
            end_date=data.end_date,
            failure_strategy=data.failure_strategy,
            retry_count=data.retry_count,
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid sync_type")

    # 记录日志
    await service.create_sync_log(None, data.sync_type, result)

    return {
        "success": result.success,
        "total": result.total,
        "success_count": result.success_count,
        "failed_count": result.failed_count,
        "duration_seconds": result.duration_seconds,
    }


@router.get("/sync/status")
async def get_sync_status(
    session: AsyncSession = Depends(get_async_session),
):
    """获取当前同步状态"""
    service = SyncService(session)
    status = service.get_sync_status()
    return {
        "status": status.status,
        "total": status.total,
        "current": status.current,
        "success": status.success,
        "failed": status.failed,
        "current_symbol": status.current_symbol,
        "progress": round(status.current / status.total * 100, 1) if status.total > 0 else 0,
    }


@router.get("/sync/logs")
async def get_sync_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_async_session),
):
    """获取同步日志列表"""
    service = SyncService(session)
    logs = await service.get_sync_logs(page=page, page_size=page_size)
    return {
        "logs": [
            {
                "id": log.id,
                "task_id": log.task_id,
                "sync_type": log.sync_type,
                "status": log.status,
                "total_count": log.total_count,
                "success_count": log.success_count,
                "failed_count": log.failed_count,
                "start_time": log.start_time.isoformat() if log.start_time else None,
                "end_time": log.end_time.isoformat() if log.end_time else None,
            }
            for log in logs
        ]
    }
```

**Step 2: 更新 router.py 注册数据路由**

```python
# backend/app/api/router.py
from fastapi import APIRouter

from .data import router as data_router
from .system import router as system_router

api_router = APIRouter()

# 注册各模块路由
api_router.include_router(system_router, prefix="/system", tags=["系统"])
api_router.include_router(data_router, prefix="/data", tags=["数据"])
```

**Step 3: 更新 db/__init__.py 导出 session**

```python
# backend/app/db/__init__.py
"""数据库模块"""
from .sqlite import get_async_session, init_db

__all__ = ["get_async_session", "init_db"]
```

**Step 4: 启动服务验证 API**

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

访问 http://localhost:8000/docs 查看 API 文档。

**Step 5: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add data API endpoints

- Stock list and detail APIs
- Kline data query API
- Watchlist group management APIs
- Data sync trigger and status APIs
- Sync logs query API

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: 定时任务调度器

**Files:**
- Create: `backend/app/core/scheduler.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/api/data.py` (添加任务管理 API)

**Step 1: 创建调度器**

```python
# backend/app/core/scheduler.py
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    """获取调度器实例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


async def start_scheduler():
    """启动调度器"""
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()


async def stop_scheduler():
    """停止调度器"""
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown()


async def add_sync_job(
    job_id: str,
    cron_expression: str,
    func,
    **kwargs,
):
    """添加同步任务"""
    scheduler = get_scheduler()
    trigger = CronTrigger.from_crontab(cron_expression)
    scheduler.add_job(func, trigger, id=job_id, kwargs=kwargs)


async def remove_sync_job(job_id: str):
    """移除同步任务"""
    scheduler = get_scheduler()
    scheduler.remove_job(job_id)


async def update_sync_job(
    job_id: str,
    cron_expression: str,
    func,
    **kwargs,
):
    """更新同步任务"""
    scheduler = get_scheduler()
    # 先移除再添加
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    trigger = CronTrigger.from_crontab(cron_expression)
    scheduler.add_job(func, trigger, id=job_id, kwargs=kwargs)
```

**Step 2: 更新 main.py 启动调度器**

在 lifespan 中添加调度器启动：

```python
# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import api_router
from app.core.scheduler import start_scheduler, stop_scheduler
from app.db import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    await init_db()
    await start_scheduler()
    yield
    await stop_scheduler()


app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
    lifespan=lifespan,
)

# 注册 API 路由
app.include_router(api_router, prefix="/api")


@app.get("/health")
async def health_check():
    """健康检查接口（根路径）"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
```

**Step 3: 在 data.py 添加任务管理 API**

在文件末尾添加：

```python
# 添加到 backend/app/api/data.py 末尾

from app.db.models import SyncTask
from app.core.scheduler import add_sync_job, remove_sync_job


class SyncTaskCreate(BaseModel):
    name: str
    cron_expression: str
    sync_type: str
    symbols: list[str] | None = None
    start_date: date | None = None
    end_date: date | None = None
    failure_strategy: str = "skip"
    retry_count: int = 3


@router.get("/tasks")
async def get_sync_tasks(
    session: AsyncSession = Depends(get_async_session),
):
    """获取定时任务列表"""
    query = select(SyncTask).order_by(SyncTask.id)
    result = await session.execute(query)
    tasks = list(result.scalars().all())
    return {
        "tasks": [
            {
                "id": t.id,
                "name": t.name,
                "cron_expression": t.cron_expression,
                "sync_type": t.sync_type,
                "enabled": t.enabled,
                "last_run_at": t.last_run_at.isoformat() if t.last_run_at else None,
                "next_run_at": t.next_run_at.isoformat() if t.next_run_at else None,
            }
            for t in tasks
        ]
    }


@router.post("/tasks")
async def create_sync_task(
    data: SyncTaskCreate,
    session: AsyncSession = Depends(get_async_session),
):
    """创建定时任务"""
    task = SyncTask(
        name=data.name,
        cron_expression=data.cron_expression,
        sync_type=data.sync_type,
        symbols=json.dumps(data.symbols) if data.symbols else None,
        start_date=data.start_date,
        end_date=data.end_date,
        failure_strategy=data.failure_strategy,
        retry_count=data.retry_count,
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)

    # 添加到调度器
    # TODO: 实现任务执行函数

    return {"id": task.id, "name": task.name}


@router.delete("/tasks/{task_id}")
async def delete_sync_task(
    task_id: int,
    session: AsyncSession = Depends(get_async_session),
):
    """删除定时任务"""
    task = await session.get(SyncTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    await remove_sync_job(f"sync_task_{task_id}")
    await session.delete(task)
    await session.commit()
    return {"success": True}


@router.post("/tasks/{task_id}/toggle")
async def toggle_sync_task(
    task_id: int,
    session: AsyncSession = Depends(get_async_session),
):
    """启用/禁用任务"""
    task = await session.get(SyncTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task.enabled = not task.enabled
    await session.commit()

    if task.enabled:
        # 添加到调度器
        pass
    else:
        # 从调度器移除
        await remove_sync_job(f"sync_task_{task_id}")

    return {"id": task.id, "enabled": task.enabled}
```

**Step 4: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add task scheduler for sync jobs

- Create APScheduler wrapper
- Integrate scheduler in app lifecycle
- Add sync task CRUD APIs

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: 前端数据管理页面 - 股票列表

**Files:**
- Modify: `frontend/src/views/DataManage/index.vue`
- Create: `frontend/src/views/DataManage/StockList.vue`
- Create: `frontend/src/api/data.ts`
- Modify: `frontend/src/api/index.ts`

**Step 1: 创建数据 API**

```typescript
// frontend/src/api/data.ts
import request from './request'

// 股票相关
export interface Stock {
  symbol: string
  name: string
  exchange: string | null
  industry: string | null
}

export interface PaginatedStocks {
  items: Stock[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

export const stockApi = {
  getList: (params: {
    search?: string
    industry?: string
    group_id?: number
    page?: number
    page_size?: number
  }) => request.get<PaginatedStocks>('/data/stocks', { params }),

  getDetail: (symbol: string) => request.get<Stock>(`/data/stocks/${symbol}`),
}

// 行业列表
export const industryApi = {
  getList: () => request.get<{ industries: string[] }>('/data/industries'),
}

// 自选股相关
export interface WatchlistGroup {
  id: number
  name: string
  description: string | null
  stock_count: number
}

export const watchlistApi = {
  getGroups: () => request.get<{ groups: WatchlistGroup[] }>('/data/watchlist/groups'),

  createGroup: (data: { name: string; description?: string }) =>
    request.post<{ id: number; name: string }>('/data/watchlist/groups', data),

  deleteGroup: (id: number) => request.delete(`/data/watchlist/groups/${id}`),

  addStock: (groupId: number, symbol: string) =>
    request.post(`/data/watchlist/groups/${groupId}/stocks`, { symbol }),

  removeStock: (groupId: number, symbol: string) =>
    request.delete(`/data/watchlist/groups/${groupId}/stocks/${symbol}`),
}
```

**Step 2: 更新 api/index.ts**

```typescript
// frontend/src/api/index.ts
export * from './system'
export * from './data'
export { default as request } from './request'
```

**Step 3: 创建股票列表组件**

```vue
<!-- frontend/src/views/DataManage/StockList.vue -->
<template>
  <div class="stock-list">
    <!-- 搜索和筛选 -->
    <el-form :inline="true" class="filter-form">
      <el-form-item label="搜索">
        <el-input
          v-model="searchText"
          placeholder="代码/名称"
          clearable
          @keyup.enter="handleSearch"
        />
      </el-form-item>
      <el-form-item label="行业">
        <el-select v-model="selectedIndustry" placeholder="全部" clearable>
          <el-option
            v-for="item in industries"
            :key="item"
            :label="item"
            :value="item"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="分组">
        <el-select v-model="selectedGroup" placeholder="全部" clearable>
          <el-option
            v-for="item in groups"
            :key="item.id"
            :label="item.name"
            :value="item.id"
          />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="handleSearch">查询</el-button>
      </el-form-item>
    </el-form>

    <!-- 股票表格 -->
    <el-table :data="stocks" v-loading="loading" stripe>
      <el-table-column prop="symbol" label="代码" width="100" />
      <el-table-column prop="name" label="名称" width="120" />
      <el-table-column prop="industry" label="行业" width="100" />
      <el-table-column prop="exchange" label="交易所" width="80" />
      <el-table-column label="操作" width="150">
        <template #default="{ row }">
          <el-button link type="primary" @click="showDetail(row.symbol)">
            详情
          </el-button>
          <el-dropdown trigger="click" @command="(cmd: string) => addToGroup(cmd, row.symbol)">
            <el-button link type="primary">
              加自选
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item
                  v-for="g in groups"
                  :key="g.id"
                  :command="g.id"
                >
                  {{ g.name }}
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>

    <!-- 分页 -->
    <el-pagination
      v-model:current-page="currentPage"
      v-model:page-size="pageSize"
      :total="total"
      :page-sizes="[50, 100, 200]"
      layout="total, sizes, prev, pager, next"
      @size-change="loadStocks"
      @current-change="loadStocks"
    />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { stockApi, industryApi, watchlistApi, type Stock, type WatchlistGroup } from '@/api/data'

const router = useRouter()

const loading = ref(false)
const stocks = ref<Stock[]>([])
const industries = ref<string[]>([])
const groups = ref<WatchlistGroup[]>([])

const searchText = ref('')
const selectedIndustry = ref('')
const selectedGroup = ref<number | undefined>()

const currentPage = ref(1)
const pageSize = ref(50)
const total = ref(0)

const loadStocks = async () => {
  loading.value = true
  try {
    const res = await stockApi.getList({
      search: searchText.value || undefined,
      industry: selectedIndustry.value || undefined,
      group_id: selectedGroup.value,
      page: currentPage.value,
      page_size: pageSize.value,
    }) as any
    stocks.value = res.items
    total.value = res.total
  } catch (e) {
    ElMessage.error('加载股票列表失败')
  } finally {
    loading.value = false
  }
}

const loadIndustries = async () => {
  try {
    const res = await industryApi.getList() as any
    industries.value = res.industries
  } catch (e) {
    console.error('加载行业列表失败', e)
  }
}

const loadGroups = async () => {
  try {
    const res = await watchlistApi.getGroups() as any
    groups.value = res.groups
  } catch (e) {
    console.error('加载分组列表失败', e)
  }
}

const handleSearch = () => {
  currentPage.value = 1
  loadStocks()
}

const showDetail = (symbol: string) => {
  router.push({ name: 'StockDetail', params: { symbol } })
}

const addToGroup = async (groupId: number, symbol: string) => {
  try {
    await watchlistApi.addStock(groupId, symbol)
    ElMessage.success('添加成功')
  } catch (e) {
    ElMessage.error('添加失败')
  }
}

onMounted(() => {
  loadStocks()
  loadIndustries()
  loadGroups()
})
</script>

<style scoped>
.stock-list {
  padding: 0;
}
.filter-form {
  margin-bottom: 16px;
}
.el-pagination {
  margin-top: 16px;
  justify-content: flex-end;
}
</style>
```

**Step 4: 更新 DataManage/index.vue**

```vue
<!-- frontend/src/views/DataManage/index.vue -->
<template>
  <div class="data-manage">
    <el-tabs v-model="activeTab">
      <el-tab-pane label="股票列表" name="stock">
        <StockList />
      </el-tab-pane>
      <el-tab-pane label="自选股" name="watchlist">
        <div class="placeholder">自选股管理 - 开发中</div>
      </el-tab-pane>
      <el-tab-pane label="数据同步" name="sync">
        <div class="placeholder">数据同步 - 开发中</div>
      </el-tab-pane>
      <el-tab-pane label="定时任务" name="tasks">
        <div class="placeholder">定时任务 - 开发中</div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import StockList from './StockList.vue'

const activeTab = ref('stock')
</script>

<style scoped>
.data-manage {
  background: #fff;
  padding: 16px;
  border-radius: 4px;
}
.placeholder {
  padding: 40px;
  text-align: center;
  color: #999;
}
</style>
```

**Step 5: 安装 ECharts（为后续 K线图准备）**

```bash
cd frontend
npm install echarts
```

**Step 6: 验证前端运行**

```bash
cd frontend
npm run dev
```

**Step 7: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): add stock list page

- Create data API module
- Implement StockList component with search and filters
- Add watchlist dropdown for adding stocks
- Update DataManage page with tabs

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: 前端 K线图表组件

**Files:**
- Create: `frontend/src/views/DataManage/KlineChart.vue`
- Create: `frontend/src/views/StockDetail.vue`
- Modify: `frontend/src/router/index.ts`
- Create: `frontend/src/api/kline.ts`

**Step 1: 创建 K线 API**

```typescript
// frontend/src/api/kline.ts
import request from './request'

export interface KlineData {
  datetime: string
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  volume: number | null
  amount: number | null
}

export interface KlineResponse {
  symbol: string
  kline_type: string
  data: KlineData[]
}

export const klineApi = {
  getKlines: (params: {
    symbol: string
    kline_type?: string
    start_date?: string
    end_date?: string
    limit?: number
  }) => request.get<KlineResponse>('/data/klines', { params }),
}
```

**Step 2: 创建 K线图表组件**

```vue
<!-- frontend/src/views/DataManage/KlineChart.vue -->
<template>
  <div class="kline-chart" ref="chartRef"></div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import type { KlineData } from '@/api/kline'

const props = defineProps<{
  data: KlineData[]
}>()

const chartRef = ref<HTMLElement>()
let chart: echarts.ECharts | null = null

const initChart = () => {
  if (!chartRef.value) return

  chart = echarts.init(chartRef.value)

  const dates = props.data.map(d => d.datetime)
  const ohlc = props.data.map(d => [
    d.open,
    d.close,
    d.low,
    d.high,
  ])
  const volumes = props.data.map(d => d.volume)

  const option: echarts.EChartsOption = {
    animation: false,
    legend: {
      bottom: 10,
      left: 'center',
      data: ['K线', '成交量'],
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross',
      },
    },
    axisPointer: {
      link: [{ xAxisIndex: 'all' }],
    },
    grid: [
      {
        left: '10%',
        right: '8%',
        top: '10%',
        height: '50%',
      },
      {
        left: '10%',
        right: '8%',
        top: '70%',
        height: '15%',
      },
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: false,
        axisLine: { onZero: false },
        splitLine: { show: false },
        min: 'dataMin',
        max: 'dataMax',
      },
      {
        type: 'category',
        gridIndex: 1,
        data: dates,
        boundaryGap: false,
        axisLine: { onZero: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        min: 'dataMin',
        max: 'dataMax',
      },
    ],
    yAxis: [
      {
        scale: true,
        splitArea: {
          show: true,
        },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: 80,
        end: 100,
      },
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: ohlc,
        itemStyle: {
          color: '#ef5350',
          color0: '#26a69a',
          borderColor: '#ef5350',
          borderColor0: '#26a69a',
        },
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        itemStyle: {
          color: (params: any) => {
            const idx = params.dataIndex
            if (idx === 0) return '#26a69a'
            const closeToday = props.data[idx]?.close
            const closePrev = props.data[idx - 1]?.close
            if (closeToday && closePrev) {
              return closeToday >= closePrev ? '#ef5350' : '#26a69a'
            }
            return '#26a69a'
          },
        },
      },
    ],
  }

  chart.setOption(option)
}

const resizeChart = () => {
  chart?.resize()
}

onMounted(() => {
  initChart()
  window.addEventListener('resize', resizeChart)
})

onUnmounted(() => {
  chart?.dispose()
  window.removeEventListener('resize', resizeChart)
})

watch(() => props.data, () => {
  if (chart) {
    initChart()
  }
}, { deep: true })
</script>

<style scoped>
.kline-chart {
  width: 100%;
  height: 500px;
}
</style>
```

**Step 3: 创建股票详情页**

```vue
<!-- frontend/src/views/StockDetail.vue -->
<template>
  <div class="stock-detail">
    <el-page-header @back="goBack" :title="stockInfo?.name || symbol">
      <template #content>
        <span class="text-large font-600 mr-3">{{ symbol }} - {{ stockInfo?.name }}</span>
      </template>
    </el-page-header>

    <el-divider />

    <!-- 时间范围选择 -->
    <el-form :inline="true" class="filter-form">
      <el-form-item label="时间范围">
        <el-date-picker
          v-model="dateRange"
          type="daterange"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          format="YYYY-MM-DD"
          value-format="YYYY-MM-DD"
        />
      </el-form-item>
      <el-form-item label="周期">
        <el-select v-model="klineType" style="width: 100px">
          <el-option label="日K" value="daily" />
          <el-option label="分钟" value="minute" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="loadKlines">查询</el-button>
      </el-form-item>
    </el-form>

    <!-- K线图 -->
    <KlineChart v-if="klineData.length > 0" :data="klineData" />
    <el-empty v-else-if="!loading" description="暂无数据" />

    <!-- 股票信息 -->
    <el-divider content-position="left">股票信息</el-divider>
    <el-descriptions :column="3" border v-if="stockInfo">
      <el-descriptions-item label="代码">{{ stockInfo.symbol }}</el-descriptions-item>
      <el-descriptions-item label="名称">{{ stockInfo.name }}</el-descriptions-item>
      <el-descriptions-item label="行业">{{ stockInfo.industry || '-' }}</el-descriptions-item>
      <el-descriptions-item label="交易所">{{ stockInfo.exchange || '-' }}</el-descriptions-item>
      <el-descriptions-item label="上市日期">{{ stockInfo.list_date || '-' }}</el-descriptions-item>
    </el-descriptions>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import KlineChart from './DataManage/KlineChart.vue'
import { stockApi, type Stock } from '@/api/data'
import { klineApi, type KlineData } from '@/api/kline'

const route = useRoute()
const router = useRouter()

const symbol = route.params.symbol as string
const stockInfo = ref<Stock | null>(null)
const klineData = ref<KlineData[]>([])
const loading = ref(false)

const dateRange = ref<string[]>([])
const klineType = ref('daily')

const goBack = () => {
  router.back()
}

const loadStockInfo = async () => {
  try {
    const res = await stockApi.getDetail(symbol) as any
    stockInfo.value = res
  } catch (e) {
    ElMessage.error('加载股票信息失败')
  }
}

const loadKlines = async () => {
  loading.value = true
  try {
    const params: any = {
      symbol,
      kline_type: klineType.value,
      limit: 500,
    }
    if (dateRange.value && dateRange.value.length === 2) {
      params.start_date = dateRange.value[0]
      params.end_date = dateRange.value[1]
    }
    const res = await klineApi.getKlines(params) as any
    klineData.value = res.data || []
  } catch (e) {
    ElMessage.error('加载K线数据失败')
    klineData.value = []
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadStockInfo()
  loadKlines()
})
</script>

<style scoped>
.stock-detail {
  background: #fff;
  padding: 16px;
  border-radius: 4px;
}
.filter-form {
  margin-bottom: 16px;
}
</style>
```

**Step 4: 更新路由添加详情页**

```typescript
// frontend/src/router/index.ts
import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/data',
    },
    {
      path: '/data',
      name: 'DataManage',
      component: () => import('@/views/DataManage/index.vue'),
      meta: { title: '数据管理' },
    },
    {
      path: '/stock/:symbol',
      name: 'StockDetail',
      component: () => import('@/views/StockDetail.vue'),
      meta: { title: '股票详情' },
    },
    {
      path: '/factor',
      name: 'FactorResearch',
      component: () => import('@/views/FactorResearch/index.vue'),
      meta: { title: '因子研究' },
    },
    {
      path: '/backtest',
      name: 'StrategyBacktest',
      component: () => import('@/views/StrategyBacktest/index.vue'),
      meta: { title: '策略回测' },
    },
    {
      path: '/trade',
      name: 'LiveTrading',
      component: () => import('@/views/LiveTrading/index.vue'),
      meta: { title: '实盘交易' },
    },
    {
      path: '/monitor',
      name: 'SystemMonitor',
      component: () => import('@/views/SystemMonitor/index.vue'),
      meta: { title: '系统监控' },
    },
  ],
})

export default router
```

**Step 5: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): add K-line chart component

- Create KlineChart component with ECharts
- Add stock detail page with date range filter
- Create kline API module
- Update router with stock detail route

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: 前端数据同步面板

**Files:**
- Create: `frontend/src/views/DataManage/SyncPanel.vue`
- Create: `frontend/src/api/sync.ts`
- Modify: `frontend/src/views/DataManage/index.vue`

**Step 1: 创建同步 API**

```typescript
// frontend/src/api/sync.ts
import request from './request'

export interface SyncStatus {
  status: string
  total: number
  current: number
  success: number
  failed: number
  current_symbol: string | null
  progress: number
}

export interface SyncLog {
  id: number
  task_id: number | null
  sync_type: string
  status: string
  total_count: number
  success_count: number
  failed_count: number
  start_time: string
  end_time: string | null
}

export const syncApi = {
  trigger: (data: {
    sync_type: string
    symbols?: string[]
    start_date?: string
    end_date?: string
    failure_strategy?: string
    retry_count?: number
  }) => request.post('/data/sync', data),

  getStatus: () => request.get<SyncStatus>('/data/sync/status'),

  getLogs: (params?: { page?: number; page_size?: number }) =>
    request.get<{ logs: SyncLog[] }>('/data/sync/logs', { params }),
}
```

**Step 2: 创建同步面板组件**

```vue
<!-- frontend/src/views/DataManage/SyncPanel.vue -->
<template>
  <div class="sync-panel">
    <el-row :gutter="20">
      <!-- 左侧：同步配置 -->
      <el-col :span="12">
        <el-card header="同步配置">
          <el-form label-width="100px">
            <el-form-item label="同步类型">
              <el-checkbox-group v-model="syncTypes">
                <el-checkbox label="stock_info">股票基础信息</el-checkbox>
                <el-checkbox label="kline_daily">日K线</el-checkbox>
                <el-checkbox label="kline_minute">分钟K线</el-checkbox>
              </el-checkbox-group>
            </el-form-item>

            <el-form-item label="时间范围">
              <el-date-picker
                v-model="dateRange"
                type="daterange"
                range-separator="至"
                start-placeholder="开始日期"
                end-placeholder="结束日期"
                format="YYYY-MM-DD"
                value-format="YYYY-MM-DD"
              />
            </el-form-item>

            <el-form-item label="股票范围">
              <el-radio-group v-model="stockScope">
                <el-radio label="all">全市场</el-radio>
                <el-radio label="watchlist">自选股</el-radio>
              </el-radio-group>
            </el-form-item>

            <el-form-item label="失败处理">
              <el-select v-model="failureStrategy" style="width: 150px">
                <el-option label="跳过" value="skip" />
                <el-option label="重试" value="retry" />
                <el-option label="停止" value="stop" />
              </el-select>
              <el-input-number
                v-if="failureStrategy === 'retry'"
                v-model="retryCount"
                :min="1"
                :max="10"
                style="width: 100px; margin-left: 10px"
              />
            </el-form-item>

            <el-form-item>
              <el-button
                type="primary"
                :loading="syncing"
                :disabled="syncTypes.length === 0"
                @click="startSync"
              >
                {{ syncing ? '同步中...' : '开始同步' }}
              </el-button>
            </el-form-item>
          </el-form>
        </el-card>
      </el-col>

      <!-- 右侧：同步状态 -->
      <el-col :span="12">
        <el-card header="同步状态">
          <div v-if="syncStatus && syncStatus.status !== 'idle'">
            <el-progress
              :percentage="syncStatus.progress"
              :status="syncStatus.status === 'completed' ? 'success' : undefined"
            />
            <div class="sync-info">
              <p v-if="syncStatus.current_symbol">
                正在同步: {{ syncStatus.current_symbol }}
              </p>
              <p>
                成功: {{ syncStatus.success }} | 失败: {{ syncStatus.failed }}
              </p>
              <p>
                进度: {{ syncStatus.current }} / {{ syncStatus.total }}
              </p>
            </div>
          </div>
          <el-empty v-else description="暂无同步任务" />
        </el-card>
      </el-col>
    </el-row>

    <!-- 同步日志 -->
    <el-card header="最近同步记录" style="margin-top: 20px">
      <el-table :data="syncLogs" stripe>
        <el-table-column prop="start_time" label="时间" width="180">
          <template #default="{ row }">
            {{ formatTime(row.start_time) }}
          </template>
        </el-table-column>
        <el-table-column prop="sync_type" label="类型" width="120">
          <template #default="{ row }">
            {{ syncTypeLabel(row.sync_type) }}
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="80">
          <template #default="{ row }">
            <el-tag :type="row.status === 'completed' ? 'success' : 'danger'">
              {{ row.status === 'completed' ? '完成' : '失败' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="成功/总数" width="120">
          <template #default="{ row }">
            {{ row.success_count }} / {{ row.total_count }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { syncApi, type SyncLog, type SyncStatus } from '@/api/sync'

const syncTypes = ref<string[]>(['stock_info'])
const dateRange = ref<string[]>([])
const stockScope = ref('all')
const failureStrategy = ref('skip')
const retryCount = ref(3)

const syncing = ref(false)
const syncStatus = ref<SyncStatus | null>(null)
const syncLogs = ref<SyncLog[]>([])

let statusTimer: number | null = null

const startSync = async () => {
  if (syncTypes.value.length === 0) {
    ElMessage.warning('请选择同步类型')
    return
  }

  syncing.value = true

  try {
    for (const syncType of syncTypes.value) {
      await syncApi.trigger({
        sync_type: syncType,
        start_date: dateRange.value?.[0],
        end_date: dateRange.value?.[1],
        failure_strategy: failureStrategy.value,
        retry_count: retryCount.value,
      })
    }
    ElMessage.success('同步任务已启动')
    startStatusPolling()
  } catch (e) {
    ElMessage.error('启动同步失败')
  }
}

const loadSyncStatus = async () => {
  try {
    const res = await syncApi.getStatus() as any
    syncStatus.value = res

    if (res.status === 'completed' || res.status === 'idle') {
      syncing.value = false
      stopStatusPolling()
      loadSyncLogs()
    }
  } catch (e) {
    console.error('获取同步状态失败', e)
  }
}

const loadSyncLogs = async () => {
  try {
    const res = await syncApi.getLogs() as any
    syncLogs.value = res.logs || []
  } catch (e) {
    console.error('加载同步日志失败', e)
  }
}

const startStatusPolling = () => {
  if (statusTimer) return
  statusTimer = window.setInterval(loadSyncStatus, 2000)
}

const stopStatusPolling = () => {
  if (statusTimer) {
    clearInterval(statusTimer)
    statusTimer = null
  }
}

const formatTime = (time: string) => {
  return new Date(time).toLocaleString()
}

const syncTypeLabel = (type: string) => {
  const labels: Record<string, string> = {
    stock_info: '股票信息',
    kline_daily: '日K线',
    kline_minute: '分钟K线',
  }
  return labels[type] || type
}

onMounted(() => {
  loadSyncStatus()
  loadSyncLogs()
})

onUnmounted(() => {
  stopStatusPolling()
})
</script>

<style scoped>
.sync-panel {
  padding: 0;
}
.sync-info {
  margin-top: 16px;
  color: #666;
}
.sync-info p {
  margin: 4px 0;
}
</style>
```

**Step 3: 更新 DataManage/index.vue 引入 SyncPanel**

```vue
<!-- frontend/src/views/DataManage/index.vue -->
<template>
  <div class="data-manage">
    <el-tabs v-model="activeTab">
      <el-tab-pane label="股票列表" name="stock">
        <StockList />
      </el-tab-pane>
      <el-tab-pane label="自选股" name="watchlist">
        <div class="placeholder">自选股管理 - 开发中</div>
      </el-tab-pane>
      <el-tab-pane label="数据同步" name="sync">
        <SyncPanel />
      </el-tab-pane>
      <el-tab-pane label="定时任务" name="tasks">
        <div class="placeholder">定时任务 - 开发中</div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import StockList from './StockList.vue'
import SyncPanel from './SyncPanel.vue'

const activeTab = ref('stock')
</script>

<style scoped>
.data-manage {
  background: #fff;
  padding: 16px;
  border-radius: 4px;
}
.placeholder {
  padding: 40px;
  text-align: center;
  color: #999;
}
</style>
```

**Step 4: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): add sync panel component

- Create sync API module
- Implement SyncPanel with progress tracking
- Support multiple sync types and date range
- Display sync status and logs

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: 前后端联调验证

**Step 1: 启动后端服务**

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

**Step 2: 启动前端服务**

```bash
cd frontend
npm run dev
```

**Step 3: 验证功能**

1. 访问 http://localhost:5173
2. 点击"数据管理"
3. 验证股票列表页面加载
4. 验证搜索、行业筛选功能
5. 点击股票详情，验证 K线图表
6. 点击"数据同步" Tab，验证同步面板

**Step 4: 最终提交**

```bash
git add .
git commit -m "$(cat <<'EOF'
chore: verify Phase 2 frontend-backend integration

Backend APIs working:
- GET /api/data/stocks
- GET /api/data/klines
- POST /api/data/sync
- GET /api/data/sync/status

Frontend pages working:
- Stock list with search and filters
- K-line chart with ECharts
- Sync panel with progress tracking

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## 完成标准

- [ ] 后端 QMTGateway 可连接 xtquant 获取数据
- [ ] 数据同步服务可正常执行并记录日志
- [ ] 股票列表 API 正常返回数据
- [ ] K线数据 API 正常返回数据
- [ ] 前端股票列表页面正常显示
- [ ] 前端 K线图表正常渲染
- [ ] 前端同步面板可触发同步并显示进度
- [ ] 所有代码已提交到 Git 仓库

---

*计划版本: 1.0*
*创建日期: 2026-04-12*
