# Phase 3: 回测模块 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现策略回测功能，用户可以编写策略、配置参数、运行回测、查看回测报告。

**Architecture:** 后端使用 VeighNa 作为回测引擎核心，封装为 BacktestService；前端提供策略编辑器、参数配置、回测报告展示。回测使用 ClickHouse 中的 K线数据。

**Tech Stack:** Python 3.12, VeighNa 3.x, FastAPI, SQLAlchemy | Vue 3, TypeScript, Element Plus, Monaco Editor, ECharts

---

## Task 1: 安装 VeighNa 依赖并创建回测引擎封装

**Files:**
- Modify: `backend/requirements.txt`
- Create: `backend/app/engines/vn_engine.py`

**Step 1: 添加 VeighNa 依赖**

```txt
# backend/requirements.txt 末尾添加
veighna>=3.9.0
```

**Step 2: 安装依赖**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\pip install veighna
```

**Step 3: 创建 VeighNa 引擎封装**

```python
# backend/app/engines/vn_engine.py
"""
VeighNa 回测引擎封装

提供简洁的回测接口，从 ClickHouse 加载数据
"""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass
class BacktestConfig:
    """回测配置"""

    strategy_code: str  # 策略代码
    strategy_params: dict[str, Any]  # 策略参数
    symbols: list[str]  # 交易标的
    start_date: date  # 开始日期
    end_date: date  # 结束日期
    initial_capital: Decimal  # 初始资金
    commission_rate: Decimal = Decimal("0.0003")  # 手续费率
    slippage: Decimal = Decimal("0")  # 滑点


@dataclass
class BacktestResult:
    """回测结果"""

    # 收益指标
    total_return: Decimal  # 总收益率
    annual_return: Decimal  # 年化收益率
    max_drawdown: Decimal  # 最大回撤
    sharpe_ratio: Decimal  # 夏普比率

    # 交易统计
    total_trades: int  # 总交易次数
    win_trades: int  # 盈利次数
    loss_trades: int  # 亏损次数
    win_rate: Decimal  # 胜率

    # 详细数据
    daily_values: list[dict]  # 每日净值
    trades: list[dict]  # 交易记录

    # 元数据
    start_date: date
    end_date: date
    initial_capital: Decimal
    final_capital: Decimal


class VnEngine:
    """VeighNa 回测引擎封装"""

    def __init__(self):
        self._initialized = False

    async def run_backtest(self, config: BacktestConfig) -> BacktestResult:
        """
        运行回测

        Args:
            config: 回测配置

        Returns:
            BacktestResult: 回测结果
        """
        # TODO: 实现 VeighNa 回测逻辑
        # 1. 从 ClickHouse 加载 K线数据
        # 2. 动态加载策略代码
        # 3. 运行回测
        # 4. 统计结果

        # 暂时返回模拟结果
        return BacktestResult(
            total_return=Decimal("0.15"),
            annual_return=Decimal("0.12"),
            max_drawdown=Decimal("0.08"),
            sharpe_ratio=Decimal("1.5"),
            total_trades=100,
            win_trades=55,
            loss_trades=45,
            win_rate=Decimal("0.55"),
            daily_values=[],
            trades=[],
            start_date=config.start_date,
            end_date=config.end_date,
            initial_capital=config.initial_capital,
            final_capital=config.initial_capital * Decimal("1.15"),
        )

    async def get_backtest_progress(self, backtest_id: int) -> dict:
        """获取回测进度"""
        return {"backtest_id": backtest_id, "progress": 0, "status": "pending"}


# 全局单例
vn_engine = VnEngine()
```

**Step 4: 更新 engines/__init__.py**

```python
# backend/app/engines/__init__.py
"""核心引擎模块"""
from .qmt_gateway import QMTGateway, qmt_gateway
from .vn_engine import VnEngine, vn_engine, BacktestConfig, BacktestResult

__all__ = [
    "QMTGateway",
    "qmt_gateway",
    "VnEngine",
    "vn_engine",
    "BacktestConfig",
    "BacktestResult",
]
```

**Step 5: 验证导入**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\python -c "from app.engines import vn_engine, BacktestConfig; print('OK')"
```

**Step 6: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add VeighNa backtest engine wrapper

- Add veighna dependency
- Create VnEngine class with BacktestConfig/BacktestResult
- Prepare interface for backtest execution

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: 创建回测服务 (BacktestService)

**Files:**
- Create: `backend/app/services/backtest_service.py`
- Modify: `backend/app/services/__init__.py`

**Step 1: 创建回测服务**

```python
# backend/app/services/backtest_service.py
"""回测服务"""
import asyncio
import logging
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Backtest, Strategy
from app.engines.vn_engine import BacktestConfig, vn_engine

logger = logging.getLogger(__name__)


class BacktestService:
    """回测服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_strategy(
        self,
        name: str,
        code: str,
        parameters: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> Strategy:
        """
        创建策略

        Args:
            name: 策略名称
            code: 策略代码
            parameters: 默认参数
            description: 策略描述

        Returns:
            Strategy: 策略对象
        """
        strategy = Strategy(
            name=name,
            code=code,
            parameters=parameters,
            description=description,
        )
        self.session.add(strategy)
        await self.session.flush()
        return strategy

    async def get_strategies(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Strategy], int]:
        """
        获取策略列表

        Returns:
            tuple: (策略列表, 总数)
        """
        # 统计总数
        count_query = select(Strategy)
        result = await self.session.execute(count_query)
        all_strategies = result.scalars().all()
        total = len(all_strategies)

        # 分页
        offset = (page - 1) * page_size
        query = select(Strategy).order_by(Strategy.id.desc()).offset(offset).limit(page_size)
        result = await self.session.execute(query)
        strategies = result.scalars().all()

        return list(strategies), total

    async def get_strategy(self, strategy_id: int) -> Strategy | None:
        """获取单个策略"""
        query = select(Strategy).where(Strategy.id == strategy_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def update_strategy(
        self,
        strategy_id: int,
        name: str | None = None,
        code: str | None = None,
        parameters: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> Strategy | None:
        """更新策略"""
        strategy = await self.get_strategy(strategy_id)
        if not strategy:
            return None

        if name is not None:
            strategy.name = name
        if code is not None:
            strategy.code = code
        if parameters is not None:
            strategy.parameters = parameters
        if description is not None:
            strategy.description = description

        await self.session.flush()
        return strategy

    async def delete_strategy(self, strategy_id: int) -> bool:
        """删除策略"""
        strategy = await self.get_strategy(strategy_id)
        if not strategy:
            return False

        await self.session.delete(strategy)
        return True

    async def create_backtest(
        self,
        strategy_id: int,
        start_date: date,
        end_date: date,
        initial_capital: Decimal,
        parameters: dict[str, Any] | None = None,
    ) -> Backtest:
        """
        创建回测任务

        Args:
            strategy_id: 策略ID
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            parameters: 回测参数

        Returns:
            Backtest: 回测记录
        """
        backtest = Backtest(
            strategy_id=strategy_id,
            status="pending",
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            parameters=parameters,
        )
        self.session.add(backtest)
        await self.session.flush()
        return backtest

    async def get_backtests(
        self,
        strategy_id: int | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Backtest], int]:
        """获取回测列表"""
        query = select(Backtest)

        if strategy_id:
            query = query.where(Backtest.strategy_id == strategy_id)
        if status:
            query = query.where(Backtest.status == status)

        # 统计总数
        count_query = select(Backtest)
        if strategy_id:
            count_query = count_query.where(Backtest.strategy_id == strategy_id)
        if status:
            count_query = count_query.where(Backtest.status == status)

        result = await self.session.execute(count_query)
        total = len(result.scalars().all())

        # 分页
        offset = (page - 1) * page_size
        query = query.order_by(Backtest.id.desc()).offset(offset).limit(page_size)
        result = await self.session.execute(query)
        backtests = result.scalars().all()

        return list(backtests), total

    async def get_backtest(self, backtest_id: int) -> Backtest | None:
        """获取单个回测"""
        query = select(Backtest).where(Backtest.id == backtest_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def run_backtest(self, backtest_id: int) -> dict[str, Any]:
        """
        运行回测

        Args:
            backtest_id: 回测ID

        Returns:
            dict: 回测结果
        """
        backtest = await self.get_backtest(backtest_id)
        if not backtest:
            raise ValueError(f"回测 {backtest_id} 不存在")

        strategy = await self.get_strategy(backtest.strategy_id)
        if not strategy:
            raise ValueError(f"策略 {backtest.strategy_id} 不存在")

        # 更新状态为运行中
        backtest.status = "running"
        await self.session.flush()

        try:
            # 构建回测配置
            config = BacktestConfig(
                strategy_code=strategy.code,
                strategy_params=backtest.parameters or {},
                symbols=backtest.parameters.get("symbols", []) if backtest.parameters else [],
                start_date=backtest.start_date,
                end_date=backtest.end_date,
                initial_capital=backtest.initial_capital or Decimal("1000000"),
            )

            # 运行回测
            result = await vn_engine.run_backtest(config)

            # 保存结果
            backtest.status = "completed"
            backtest.result = {
                "total_return": float(result.total_return),
                "annual_return": float(result.annual_return),
                "max_drawdown": float(result.max_drawdown),
                "sharpe_ratio": float(result.sharpe_ratio),
                "total_trades": result.total_trades,
                "win_trades": result.win_trades,
                "loss_trades": result.loss_trades,
                "win_rate": float(result.win_rate),
                "final_capital": float(result.final_capital),
            }

            await self.session.flush()

            return backtest.result

        except Exception as e:
            logger.error(f"回测失败: {e}")
            backtest.status = "failed"
            backtest.result = {"error": str(e)}
            await self.session.flush()
            raise

    async def get_backtest_report(self, backtest_id: int) -> dict[str, Any]:
        """
        获取回测详细报告

        Args:
            backtest_id: 回测ID

        Returns:
            dict: 详细报告数据
        """
        backtest = await self.get_backtest(backtest_id)
        if not backtest:
            raise ValueError(f"回测 {backtest_id} 不存在")

        strategy = await self.get_strategy(backtest.strategy_id)

        return {
            "backtest": {
                "id": backtest.id,
                "strategy_id": backtest.strategy_id,
                "strategy_name": strategy.name if strategy else None,
                "status": backtest.status,
                "start_date": backtest.start_date.isoformat(),
                "end_date": backtest.end_date.isoformat(),
                "initial_capital": float(backtest.initial_capital) if backtest.initial_capital else None,
                "parameters": backtest.parameters,
                "result": backtest.result,
                "created_at": backtest.created_at.isoformat() if backtest.created_at else None,
            },
            "summary": backtest.result or {},
        }
```

**Step 2: 更新 services/__init__.py**

```python
# backend/app/services/__init__.py
"""业务服务模块"""
from .backtest_service import BacktestService
from .data_service import DataService
from .sync_service import SyncService

__all__ = ["DataService", "SyncService", "BacktestService"]
```

**Step 3: 验证导入**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\python -c "from app.services import BacktestService; print('OK')"
```

**Step 4: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add BacktestService

- Create/Read/Update/Delete strategies
- Create/Run backtests
- Store backtest results

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: 创建回测 API 接口

**Files:**
- Create: `backend/app/api/backtest.py`
- Modify: `backend/app/api/router.py`

**Step 1: 创建回测 API**

```python
# backend/app/api/backtest.py
"""回测相关 API 接口"""
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services import BacktestService

router = APIRouter()


# ============== Pydantic Models ==============


class StrategyCreate(BaseModel):
    """创建策略请求"""

    name: str = Field(min_length=1, max_length=100, description="策略名称")
    code: str = Field(min_length=1, description="策略代码")
    parameters: dict[str, Any] | None = Field(default=None, description="默认参数")
    description: str | None = Field(default=None, description="策略描述")


class StrategyUpdate(BaseModel):
    """更新策略请求"""

    name: str | None = Field(default=None, max_length=100, description="策略名称")
    code: str | None = Field(default=None, description="策略代码")
    parameters: dict[str, Any] | None = Field(default=None, description="默认参数")
    description: str | None = Field(default=None, description="策略描述")


class StrategyResponse(BaseModel):
    """策略响应"""

    id: int = Field(description="策略ID")
    name: str = Field(description="策略名称")
    code: str = Field(description="策略代码")
    parameters: dict[str, Any] | None = Field(default=None, description="默认参数")
    description: str | None = Field(default=None, description="策略描述")
    created_at: str | None = Field(default=None, description="创建时间")
    updated_at: str | None = Field(default=None, description="更新时间")


class BacktestCreate(BaseModel):
    """创建回测请求"""

    strategy_id: int = Field(description="策略ID")
    start_date: date = Field(description="开始日期")
    end_date: date = Field(description="结束日期")
    initial_capital: Decimal = Field(default=Decimal("1000000"), description="初始资金")
    parameters: dict[str, Any] | None = Field(default=None, description="回测参数")


class BacktestResponse(BaseModel):
    """回测响应"""

    id: int = Field(description="回测ID")
    strategy_id: int = Field(description="策略ID")
    status: str = Field(description="状态")
    start_date: str = Field(description="开始日期")
    end_date: str = Field(description="结束日期")
    initial_capital: str | None = Field(default=None, description="初始资金")
    result: dict[str, Any] | None = Field(default=None, description="回测结果")
    created_at: str | None = Field(default=None, description="创建时间")


# ============== Strategy Endpoints ==============


@router.get("/strategies", summary="获取策略列表")
async def get_strategies(
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """获取策略列表"""
    service = BacktestService(session)
    strategies, total = await service.get_strategies(page=page, page_size=page_size)

    items = [
        {
            "id": s.id,
            "name": s.name,
            "code": s.code,
            "parameters": s.parameters,
            "description": s.description,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        }
        for s in strategies
    ]

    return {
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        },
    }


@router.post("/strategies", summary="创建策略")
async def create_strategy(
    request: StrategyCreate = Body(description="策略信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """创建新策略"""
    service = BacktestService(session)
    strategy = await service.create_strategy(
        name=request.name,
        code=request.code,
        parameters=request.parameters,
        description=request.description,
    )
    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": strategy.id,
            "name": strategy.name,
            "code": strategy.code,
            "parameters": strategy.parameters,
            "description": strategy.description,
            "created_at": strategy.created_at.isoformat() if strategy.created_at else None,
        },
    }


@router.get("/strategies/{strategy_id}", summary="获取策略详情")
async def get_strategy(
    strategy_id: int = Path(description="策略ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """获取单个策略详情"""
    service = BacktestService(session)
    strategy = await service.get_strategy(strategy_id)

    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": strategy.id,
            "name": strategy.name,
            "code": strategy.code,
            "parameters": strategy.parameters,
            "description": strategy.description,
            "created_at": strategy.created_at.isoformat() if strategy.created_at else None,
            "updated_at": strategy.updated_at.isoformat() if strategy.updated_at else None,
        },
    }


@router.put("/strategies/{strategy_id}", summary="更新策略")
async def update_strategy(
    strategy_id: int = Path(description="策略ID"),
    request: StrategyUpdate = Body(description="更新信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """更新策略"""
    service = BacktestService(session)
    strategy = await service.update_strategy(
        strategy_id=strategy_id,
        name=request.name,
        code=request.code,
        parameters=request.parameters,
        description=request.description,
    )

    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": strategy.id,
            "name": strategy.name,
            "code": strategy.code,
            "parameters": strategy.parameters,
            "description": strategy.description,
        },
    }


@router.delete("/strategies/{strategy_id}", summary="删除策略")
async def delete_strategy(
    strategy_id: int = Path(description="策略ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """删除策略"""
    service = BacktestService(session)
    success = await service.delete_strategy(strategy_id)

    if not success:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {"deleted": True},
    }


# ============== Backtest Endpoints ==============


@router.get("/backtests", summary="获取回测列表")
async def get_backtests(
    strategy_id: int | None = Query(default=None, description="策略ID筛选"),
    status: str | None = Query(default=None, description="状态筛选"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """获取回测列表"""
    service = BacktestService(session)
    backtests, total = await service.get_backtests(
        strategy_id=strategy_id,
        status=status,
        page=page,
        page_size=page_size,
    )

    items = [
        {
            "id": b.id,
            "strategy_id": b.strategy_id,
            "status": b.status,
            "start_date": b.start_date.isoformat(),
            "end_date": b.end_date.isoformat(),
            "initial_capital": str(b.initial_capital) if b.initial_capital else None,
            "result": b.result,
            "created_at": b.created_at.isoformat() if b.created_at else None,
        }
        for b in backtests
    ]

    return {
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        },
    }


@router.post("/backtests", summary="创建回测")
async def create_backtest(
    request: BacktestCreate = Body(description="回测参数"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """创建回测任务"""
    service = BacktestService(session)

    # 验证策略存在
    strategy = await service.get_strategy(request.strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略 {request.strategy_id} 不存在")

    backtest = await service.create_backtest(
        strategy_id=request.strategy_id,
        start_date=request.start_date,
        end_date=request.end_date,
        initial_capital=request.initial_capital,
        parameters=request.parameters,
    )
    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": backtest.id,
            "strategy_id": backtest.strategy_id,
            "status": backtest.status,
            "start_date": backtest.start_date.isoformat(),
            "end_date": backtest.end_date.isoformat(),
        },
    }


@router.post("/backtests/{backtest_id}/run", summary="运行回测")
async def run_backtest(
    backtest_id: int = Path(description="回测ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """运行回测"""
    service = BacktestService(session)

    try:
        result = await service.run_backtest(backtest_id)
        await session.commit()

        return {
            "code": 0,
            "message": "success",
            "data": result,
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        await session.commit()
        raise HTTPException(status_code=500, detail=f"回测失败: {str(e)}")


@router.get("/backtests/{backtest_id}", summary="获取回测详情")
async def get_backtest(
    backtest_id: int = Path(description="回测ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """获取回测详情"""
    service = BacktestService(session)

    try:
        report = await service.get_backtest_report(backtest_id)
        return {
            "code": 0,
            "message": "success",
            "data": report,
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
```

**Step 2: 更新路由器**

```python
# backend/app/api/router.py
from fastapi import APIRouter

from .backtest import router as backtest_router
from .data import router as data_router
from .system import router as system_router

api_router = APIRouter()

# 注册各模块路由
api_router.include_router(system_router, prefix="/system", tags=["系统"])
api_router.include_router(data_router, prefix="/data", tags=["数据"])
api_router.include_router(backtest_router, prefix="/backtest", tags=["回测"])
```

**Step 3: 启动后端验证 API**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\uvicorn app.main:app --reload --port 8000
```

访问 http://localhost:8000/docs 查看新增的回测 API。

**Step 4: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add backtest API endpoints

- Strategy CRUD: create, read, update, delete
- Backtest CRUD and run endpoint
- Full REST API for backtest module

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: 前端 - 添加回测 API 调用

**Files:**
- Create: `frontend/src/api/backtest.ts`
- Modify: `frontend/src/api/index.ts`

**Step 1: 创建回测 API 模块**

```typescript
// frontend/src/api/backtest.ts
import request from './request'

// ============== Types ==============

export interface Strategy {
  id: number
  name: string
  code: string
  parameters: Record<string, unknown> | null
  description: string | null
  created_at: string | null
  updated_at: string | null
}

export interface StrategyCreate {
  name: string
  code: string
  parameters?: Record<string, unknown>
  description?: string
}

export interface StrategyUpdate {
  name?: string
  code?: string
  parameters?: Record<string, unknown>
  description?: string
}

export interface Backtest {
  id: number
  strategy_id: number
  status: 'pending' | 'running' | 'completed' | 'failed'
  start_date: string
  end_date: string
  initial_capital: string | null
  result: BacktestResult | null
  created_at: string | null
}

export interface BacktestResult {
  total_return: number
  annual_return: number
  max_drawdown: number
  sharpe_ratio: number
  total_trades: number
  win_trades: number
  loss_trades: number
  win_rate: number
  final_capital: number
}

export interface BacktestCreate {
  strategy_id: number
  start_date: string
  end_date: string
  initial_capital?: number
  parameters?: Record<string, unknown>
}

export interface BacktestReport {
  backtest: Backtest & { strategy_name: string | null }
  summary: BacktestResult
}

// ============== Strategy API ==============

export const strategyApi = {
  list: (page = 1, pageSize = 20) =>
    request.get<{ items: Strategy[]; total: number; page: number; page_size: number }>(
      `/backtest/strategies?page=${page}&page_size=${pageSize}`
    ),

  get: (id: number) =>
    request.get<Strategy>(`/backtest/strategies/${id}`),

  create: (data: StrategyCreate) =>
    request.post<Strategy>('/backtest/strategies', data),

  update: (id: number, data: StrategyUpdate) =>
    request.put<Strategy>(`/backtest/strategies/${id}`, data),

  delete: (id: number) =>
    request.delete<{ deleted: boolean }>(`/backtest/strategies/${id}`),
}

// ============== Backtest API ==============

export const backtestApi = {
  list: (params?: { strategy_id?: number; status?: string; page?: number; page_size?: number }) => {
    const query = new URLSearchParams()
    if (params?.strategy_id) query.set('strategy_id', String(params.strategy_id))
    if (params?.status) query.set('status', params.status)
    if (params?.page) query.set('page', String(params.page))
    if (params?.page_size) query.set('page_size', String(params.page_size))
    return request.get<{ items: Backtest[]; total: number }>(`/backtest/backtests?${query}`)
  },

  get: (id: number) =>
    request.get<BacktestReport>(`/backtest/backtests/${id}`),

  create: (data: BacktestCreate) =>
    request.post<Backtest>('/backtest/backtests', data),

  run: (id: number) =>
    request.post<BacktestResult>(`/backtest/backtests/${id}/run`),
}
```

**Step 2: 更新 API 导出**

```typescript
// frontend/src/api/index.ts
export * from './system'
export * from './backtest'
export { default as request } from './request'
```

**Step 3: 验证编译**

```bash
cd E:/Projects/GaoshouPlatform/frontend
npm run build
```

**Step 4: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): add backtest API module

- Add strategy and backtest TypeScript interfaces
- Add API functions for strategy CRUD
- Add API functions for backtest operations

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: 前端 - 策略列表页面

**Files:**
- Modify: `frontend/src/views/StrategyBacktest/index.vue`

**Step 1: 实现策略列表页面**

```vue
<!-- frontend/src/views/StrategyBacktest/index.vue -->
<template>
  <div class="page-container">
    <div class="header">
      <h2>策略回测</h2>
      <el-button type="primary" @click="showCreateDialog = true">
        <el-icon><Plus /></el-icon>
        新建策略
      </el-button>
    </div>

    <el-tabs v-model="activeTab">
      <el-tab-pane label="策略列表" name="strategies">
        <el-table :data="strategies" v-loading="loading" stripe>
          <el-table-column prop="id" label="ID" width="80" />
          <el-table-column prop="name" label="策略名称" min-width="150" />
          <el-table-column prop="description" label="描述" min-width="200" />
          <el-table-column prop="created_at" label="创建时间" width="180">
            <template #default="{ row }">
              {{ formatDate(row.created_at) }}
            </template>
          </el-table-column>
          <el-table-column label="操作" width="200" fixed="right">
            <template #default="{ row }">
              <el-button size="small" @click="editStrategy(row)">编辑</el-button>
              <el-button size="small" type="primary" @click="goToBacktest(row)">回测</el-button>
              <el-button size="small" type="danger" @click="deleteStrategy(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-pagination
          v-model:current-page="currentPage"
          :page-size="pageSize"
          :total="total"
          layout="total, prev, pager, next"
          @current-change="loadStrategies"
          class="pagination"
        />
      </el-tab-pane>

      <el-tab-pane label="回测记录" name="backtests">
        <BacktestList :strategy-id="selectedStrategyId" />
      </el-tab-pane>
    </el-tabs>

    <!-- 创建/编辑策略对话框 -->
    <el-dialog
      v-model="showCreateDialog"
      :title="editingStrategy ? '编辑策略' : '新建策略'"
      width="800px"
    >
      <el-form :model="strategyForm" label-width="100px">
        <el-form-item label="策略名称" required>
          <el-input v-model="strategyForm.name" placeholder="请输入策略名称" />
        </el-form-item>
        <el-form-item label="策略描述">
          <el-input
            v-model="strategyForm.description"
            type="textarea"
            :rows="2"
            placeholder="请输入策略描述"
          />
        </el-form-item>
        <el-form-item label="策略代码" required>
          <el-input
            v-model="strategyForm.code"
            type="textarea"
            :rows="15"
            placeholder="请输入策略代码"
            class="code-editor"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCreateDialog = false">取消</el-button>
        <el-button type="primary" @click="saveStrategy" :loading="saving">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus } from '@element-plus/icons-vue'
import { strategyApi, type Strategy } from '@/api/backtest'
import BacktestList from './BacktestList.vue'

const router = useRouter()

const activeTab = ref('strategies')
const loading = ref(false)
const strategies = ref<Strategy[]>([])
const currentPage = ref(1)
const pageSize = ref(20)
const total = ref(0)

const showCreateDialog = ref(false)
const editingStrategy = ref<Strategy | null>(null)
const saving = ref(false)
const selectedStrategyId = ref<number | null>(null)

const strategyForm = ref({
  name: '',
  code: '',
  description: '',
})

const loadStrategies = async () => {
  loading.value = true
  try {
    const response = await strategyApi.list(currentPage.value, pageSize.value)
    strategies.value = response.items
    total.value = response.total
  } catch (error) {
    console.error('加载策略列表失败:', error)
    ElMessage.error('加载策略列表失败')
  } finally {
    loading.value = false
  }
}

const editStrategy = (strategy: Strategy) => {
  editingStrategy.value = strategy
  strategyForm.value = {
    name: strategy.name,
    code: strategy.code,
    description: strategy.description || '',
  }
  showCreateDialog.value = true
}

const saveStrategy = async () => {
  if (!strategyForm.value.name || !strategyForm.value.code) {
    ElMessage.warning('请填写策略名称和代码')
    return
  }

  saving.value = true
  try {
    if (editingStrategy.value) {
      await strategyApi.update(editingStrategy.value.id, strategyForm.value)
      ElMessage.success('策略更新成功')
    } else {
      await strategyApi.create(strategyForm.value)
      ElMessage.success('策略创建成功')
    }
    showCreateDialog.value = false
    loadStrategies()
  } catch (error) {
    console.error('保存策略失败:', error)
    ElMessage.error('保存策略失败')
  } finally {
    saving.value = false
  }
}

const deleteStrategy = async (strategy: Strategy) => {
  try {
    await ElMessageBox.confirm(`确定删除策略 "${strategy.name}" 吗？`, '确认删除', {
      type: 'warning',
    })
    await strategyApi.delete(strategy.id)
    ElMessage.success('删除成功')
    loadStrategies()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('删除策略失败:', error)
      ElMessage.error('删除策略失败')
    }
  }
}

const goToBacktest = (strategy: Strategy) => {
  selectedStrategyId.value = strategy.id
  activeTab.value = 'backtests'
}

const formatDate = (dateStr: string | null) => {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleString('zh-CN')
}

onMounted(() => {
  loadStrategies()
})
</script>

<style scoped>
.page-container {
  padding: 20px;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.header h2 {
  margin: 0;
}

.pagination {
  margin-top: 20px;
  justify-content: flex-end;
}

.code-editor :deep(textarea) {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 13px;
}
</style>
```

**Step 2: 创建回测记录子组件**

```vue
<!-- frontend/src/views/StrategyBacktest/BacktestList.vue -->
<template>
  <div class="backtest-list">
    <div class="toolbar">
      <el-button type="primary" @click="showCreateDialog = true">
        <el-icon><VideoPlay /></el-icon>
        新建回测
      </el-button>
    </div>

    <el-table :data="backtests" v-loading="loading" stripe>
      <el-table-column prop="id" label="ID" width="80" />
      <el-table-column prop="strategy_id" label="策略ID" width="100" />
      <el-table-column prop="status" label="状态" width="100">
        <template #default="{ row }">
          <el-tag :type="getStatusType(row.status)">{{ getStatusText(row.status) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="start_date" label="开始日期" width="120" />
      <el-table-column prop="end_date" label="结束日期" width="120" />
      <el-table-column prop="initial_capital" label="初始资金" width="120" />
      <el-table-column prop="created_at" label="创建时间" width="180">
        <template #default="{ row }">
          {{ formatDate(row.created_at) }}
        </template>
      </el-table-column>
      <el-table-column label="操作" width="200" fixed="right">
        <template #default="{ row }">
          <el-button
            size="small"
            type="success"
            @click="runBacktest(row)"
            :loading="row.status === 'running'"
            :disabled="row.status === 'running'"
          >
            运行
          </el-button>
          <el-button
            size="small"
            type="primary"
            @click="viewReport(row)"
            :disabled="row.status !== 'completed'"
          >
            查看报告
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      v-model:current-page="currentPage"
      :page-size="pageSize"
      :total="total"
      layout="total, prev, pager, next"
      @current-change="loadBacktests"
      class="pagination"
    />

    <!-- 创建回测对话框 -->
    <el-dialog v-model="showCreateDialog" title="新建回测" width="500px">
      <el-form :model="backtestForm" label-width="100px">
        <el-form-item label="策略">
          <el-select v-model="backtestForm.strategy_id" placeholder="选择策略">
            <el-option
              v-for="s in strategies"
              :key="s.id"
              :label="s.name"
              :value="s.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="开始日期">
          <el-date-picker
            v-model="backtestForm.start_date"
            type="date"
            placeholder="选择开始日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        <el-form-item label="结束日期">
          <el-date-picker
            v-model="backtestForm.end_date"
            type="date"
            placeholder="选择结束日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        <el-form-item label="初始资金">
          <el-input-number v-model="backtestForm.initial_capital" :min="10000" :step="10000" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCreateDialog = false">取消</el-button>
        <el-button type="primary" @click="createBacktest" :loading="creating">创建</el-button>
      </template>
    </el-dialog>

    <!-- 回测报告对话框 -->
    <el-dialog v-model="showReportDialog" title="回测报告" width="800px">
      <BacktestReport :backtest-id="selectedBacktestId" v-if="selectedBacktestId" />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { VideoPlay } from '@element-plus/icons-vue'
import { backtestApi, strategyApi, type Backtest, type Strategy } from '@/api/backtest'
import BacktestReport from './BacktestReport.vue'

const props = defineProps<{
  strategyId?: number | null
}>()

const loading = ref(false)
const backtests = ref<Backtest[]>([])
const strategies = ref<Strategy[]>([])
const currentPage = ref(1)
const pageSize = ref(20)
const total = ref(0)

const showCreateDialog = ref(false)
const creating = ref(false)
const backtestForm = ref({
  strategy_id: 0,
  start_date: '',
  end_date: '',
  initial_capital: 1000000,
})

const showReportDialog = ref(false)
const selectedBacktestId = ref<number | null>(null)

const loadBacktests = async () => {
  loading.value = true
  try {
    const params: Record<string, unknown> = {
      page: currentPage.value,
      page_size: pageSize.value,
    }
    if (props.strategyId) {
      params.strategy_id = props.strategyId
    }
    const response = await backtestApi.list(params)
    backtests.value = response.items
    total.value = response.total
  } catch (error) {
    console.error('加载回测列表失败:', error)
    ElMessage.error('加载回测列表失败')
  } finally {
    loading.value = false
  }
}

const loadStrategies = async () => {
  try {
    const response = await strategyApi.list(1, 100)
    strategies.value = response.items
    if (strategies.value.length > 0 && !backtestForm.value.strategy_id) {
      backtestForm.value.strategy_id = props.strategyId || strategies.value[0].id
    }
  } catch (error) {
    console.error('加载策略列表失败:', error)
  }
}

const createBacktest = async () => {
  if (!backtestForm.value.strategy_id || !backtestForm.value.start_date || !backtestForm.value.end_date) {
    ElMessage.warning('请填写完整的回测参数')
    return
  }

  creating.value = true
  try {
    await backtestApi.create(backtestForm.value)
    ElMessage.success('回测创建成功')
    showCreateDialog.value = false
    loadBacktests()
  } catch (error) {
    console.error('创建回测失败:', error)
    ElMessage.error('创建回测失败')
  } finally {
    creating.value = false
  }
}

const runBacktest = async (backtest: Backtest) => {
  try {
    ElMessage.info('回测运行中...')
    await backtestApi.run(backtest.id)
    ElMessage.success('回测运行成功')
    loadBacktests()
  } catch (error) {
    console.error('运行回测失败:', error)
    ElMessage.error('运行回测失败')
  }
}

const viewReport = (backtest: Backtest) => {
  selectedBacktestId.value = backtest.id
  showReportDialog.value = true
}

const getStatusType = (status: string) => {
  const types: Record<string, string> = {
    pending: 'info',
    running: 'warning',
    completed: 'success',
    failed: 'danger',
  }
  return types[status] || 'info'
}

const getStatusText = (status: string) => {
  const texts: Record<string, string> = {
    pending: '待运行',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
  }
  return texts[status] || status
}

const formatDate = (dateStr: string | null) => {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleString('zh-CN')
}

watch(() => props.strategyId, () => {
  loadBacktests()
})

onMounted(() => {
  loadBacktests()
  loadStrategies()
})
</script>

<style scoped>
.backtest-list {
  padding: 0;
}

.toolbar {
  margin-bottom: 20px;
}

.pagination {
  margin-top: 20px;
  justify-content: flex-end;
}
</style>
```

**Step 3: 创建回测报告组件**

```vue
<!-- frontend/src/views/StrategyBacktest/BacktestReport.vue -->
<template>
  <div class="backtest-report" v-loading="loading">
    <template v-if="report">
      <!-- 关键指标 -->
      <el-row :gutter="20" class="metrics">
        <el-col :span="6">
          <div class="metric-card">
            <div class="metric-value">{{ formatPercent(report.summary.total_return) }}</div>
            <div class="metric-label">总收益率</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-card">
            <div class="metric-value">{{ formatPercent(report.summary.annual_return) }}</div>
            <div class="metric-label">年化收益率</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-card">
            <div class="metric-value danger">{{ formatPercent(report.summary.max_drawdown) }}</div>
            <div class="metric-label">最大回撤</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-card">
            <div class="metric-value">{{ report.summary.sharpe_ratio?.toFixed(2) }}</div>
            <div class="metric-label">夏普比率</div>
          </div>
        </el-col>
      </el-row>

      <!-- 交易统计 -->
      <el-row :gutter="20" class="metrics">
        <el-col :span="6">
          <div class="metric-card small">
            <div class="metric-value">{{ report.summary.total_trades }}</div>
            <div class="metric-label">总交易次数</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-card small success">
            <div class="metric-value">{{ report.summary.win_trades }}</div>
            <div class="metric-label">盈利次数</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-card small danger">
            <div class="metric-value">{{ report.summary.loss_trades }}</div>
            <div class="metric-label">亏损次数</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-card small">
            <div class="metric-value">{{ formatPercent(report.summary.win_rate) }}</div>
            <div class="metric-label">胜率</div>
          </div>
        </el-col>
      </el-row>

      <!-- 回测信息 -->
      <el-descriptions title="回测信息" :column="2" border class="info-section">
        <el-descriptions-item label="策略名称">{{ report.backtest.strategy_name }}</el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag :type="getStatusType(report.backtest.status)">
            {{ getStatusText(report.backtest.status) }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="开始日期">{{ report.backtest.start_date }}</el-descriptions-item>
        <el-descriptions-item label="结束日期">{{ report.backtest.end_date }}</el-descriptions-item>
        <el-descriptions-item label="初始资金">
          {{ formatMoney(report.backtest.initial_capital) }}
        </el-descriptions-item>
        <el-descriptions-item label="最终资金">
          {{ formatMoney(report.summary.final_capital) }}
        </el-descriptions-item>
      </el-descriptions>
    </template>

    <el-empty v-else-if="!loading" description="暂无回测数据" />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { backtestApi, type BacktestReport as BacktestReportType } from '@/api/backtest'

const props = defineProps<{
  backtestId: number
}>()

const loading = ref(false)
const report = ref<BacktestReportType | null>(null)

const loadReport = async () => {
  loading.value = true
  try {
    report.value = await backtestApi.get(props.backtestId)
  } catch (error) {
    console.error('加载回测报告失败:', error)
    ElMessage.error('加载回测报告失败')
  } finally {
    loading.value = false
  }
}

const formatPercent = (value: number | undefined) => {
  if (value === undefined || value === null) return '-'
  return (value * 100).toFixed(2) + '%'
}

const formatMoney = (value: string | number | undefined | null) => {
  if (!value) return '-'
  const num = typeof value === 'string' ? parseFloat(value) : value
  return num.toLocaleString('zh-CN', { style: 'currency', currency: 'CNY' })
}

const getStatusType = (status: string) => {
  const types: Record<string, string> = {
    pending: 'info',
    running: 'warning',
    completed: 'success',
    failed: 'danger',
  }
  return types[status] || 'info'
}

const getStatusText = (status: string) => {
  const texts: Record<string, string> = {
    pending: '待运行',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
  }
  return texts[status] || status
}

watch(() => props.backtestId, () => {
  loadReport()
})

onMounted(() => {
  loadReport()
})
</script>

<style scoped>
.backtest-report {
  padding: 10px 0;
}

.metrics {
  margin-bottom: 20px;
}

.metric-card {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 8px;
  padding: 20px;
  text-align: center;
  color: #fff;
}

.metric-card.small {
  padding: 15px;
  background: #f5f7fa;
  color: #303133;
}

.metric-card.success {
  background: #e1f3d8;
  color: #67c23a;
}

.metric-card.danger {
  background: #fde2e2;
  color: #f56c6c;
}

.metric-value {
  font-size: 28px;
  font-weight: bold;
  margin-bottom: 5px;
}

.metric-card.small .metric-value {
  font-size: 20px;
}

.metric-label {
  font-size: 14px;
  opacity: 0.9;
}

.metric-card.small .metric-label {
  opacity: 1;
  font-size: 12px;
}

.info-section {
  margin-top: 20px;
}
</style>
```

**Step 4: 验证前端编译**

```bash
cd E:/Projects/GaoshouPlatform/frontend
npm run build
```

**Step 5: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): implement strategy backtest UI

- Strategy list with CRUD operations
- Backtest list and creation dialog
- Backtest report with key metrics display

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: 添加示例策略

**Files:**
- Create: `backend/app/scripts/init_demo_strategy.py`

**Step 1: 创建初始化脚本**

```python
# backend/app/scripts/init_demo_strategy.py
"""
初始化示例策略

运行: python -m app.scripts.init_demo_strategy
"""
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.db.models import Strategy


async def main():
    engine = create_async_engine(settings.database_url, echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        # 检查是否已存在
        from sqlalchemy import select
        result = await session.execute(select(Strategy).where(Strategy.name == "双均线策略"))
        existing = result.scalar_one_or_none()

        if existing:
            print("示例策略已存在")
            return

        # 创建示例策略
        demo_code = '''
"""
双均线策略示例

策略逻辑:
- 当短期均线上穿长期均线时买入
- 当短期均线下穿长期均线时卖出
"""

def init(context):
    # 设置策略参数
    context.short_period = context.params.get('short_period', 5)
    context.long_period = context.params.get('long_period', 20)


def handle_bar(context, bar):
    # 获取历史数据
    prices = context.history('close', context.long_period + 1)

    if len(prices) < context.long_period:
        return

    # 计算均线
    short_ma = prices[-context.short_period:].mean()
    long_ma = prices[-context.long_period:].mean()
    prev_short_ma = prices[-context.short_period-1:-1].mean()
    prev_long_ma = prices[-context.long_period-1:-1].mean()

    # 获取当前持仓
    position = context.get_position(bar.symbol)

    # 金叉买入
    if prev_short_ma <= prev_long_ma and short_ma > long_ma:
        if position.quantity == 0:
            order = context.buy(bar.symbol, 100)
            print(f"买入信号: {bar.symbol} @ {bar.close}")

    # 死叉卖出
    if prev_short_ma >= prev_long_ma and short_ma < long_ma:
        if position.quantity > 0:
            order = context.sell(bar.symbol, position.quantity)
            print(f"卖出信号: {bar.symbol} @ {bar.close}")
'''

        strategy = Strategy(
            name="双均线策略",
            code=demo_code,
            parameters={
                "short_period": 5,
                "long_period": 20,
            },
            description="经典双均线策略，金叉买入，死叉卖出",
        )

        session.add(strategy)
        await session.commit()
        print(f"示例策略创建成功: {strategy.id}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Step 2: 运行初始化脚本**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\python -m app.scripts.init_demo_strategy
```

**Step 3: 验证策略已创建**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\python -c "
import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import settings
from app.db.models import Strategy

async def check():
    engine = create_async_engine(settings.database_url)
    async_session = async_sessionmaker(engine)
    async with async_session() as session:
        result = await session.execute(select(Strategy))
        strategies = result.scalars().all()
        for s in strategies:
            print(f'ID: {s.id}, Name: {s.name}')

asyncio.run(check())
"
```

**Step 4: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add demo strategy initialization script

- Add double moving average strategy example
- Script to initialize demo data

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: 集成测试

**Step 1: 启动后端服务**

```bash
cd E:/Projects/GaoshouPlatform/backend
.\.venv\Scripts\uvicorn app.main:app --reload --port 8000
```

**Step 2: 启动前端服务（新终端）**

```bash
cd E:/Projects/GaoshouPlatform/frontend
npm run dev
```

**Step 3: 功能验证清单**

访问 http://localhost:5173/backtest

1. [ ] 策略列表页面正常显示
2. [ ] 可以创建新策略
3. [ ] 可以编辑策略
4. [ ] 可以删除策略
5. [ ] 可以创建回测任务
6. [ ] 可以运行回测
7. [ ] 可以查看回测报告

**Step 4: 最终提交**

```bash
git add .
git commit -m "$(cat <<'EOF'
feat: complete Phase 3 backtest module

Backend:
- VeighNa engine wrapper
- BacktestService for strategy/backtest management
- REST API for strategies and backtests

Frontend:
- Strategy list with CRUD
- Backtest creation and execution
- Backtest report display

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## 完成标准

- [ ] 后端回测 API 可正常访问 (`/api/backtest/strategies`, `/api/backtest/backtests`)
- [ ] 前端策略列表页面可正常显示
- [ ] 可以创建、编辑、删除策略
- [ ] 可以创建回测任务
- [ ] 可以运行回测并查看结果
- [ ] 示例策略已初始化

---

*计划版本: 1.0*
*创建日期: 2026-04-12*
