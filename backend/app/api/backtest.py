# backend/app/api/backtest.py
"""回测相关 API 接口"""
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services.backtest_service import BacktestService

router = APIRouter()


# ============== Pydantic Models ==============


class StrategyCreate(BaseModel):
    """创建策略请求"""

    name: str = Field(min_length=1, max_length=100, description="策略名称")
    code: str = Field(min_length=1, description="策略代码")
    parameters: dict[str, Any] | None = Field(default=None, description="策略参数")
    description: str | None = Field(default=None, description="策略描述")


class StrategyUpdate(BaseModel):
    """更新策略请求"""

    name: str | None = Field(default=None, min_length=1, max_length=100, description="策略名称")
    code: str | None = Field(default=None, description="策略代码")
    parameters: dict[str, Any] | None = Field(default=None, description="策略参数")
    description: str | None = Field(default=None, description="策略描述")


class StrategyItem(BaseModel):
    """策略项"""

    id: int = Field(description="策略ID")
    name: str = Field(description="策略名称")
    code: str = Field(description="策略代码")
    parameters: dict[str, Any] | None = Field(default=None, description="策略参数")
    description: str | None = Field(default=None, description="策略描述")
    created_at: str | None = Field(default=None, description="创建时间")
    updated_at: str | None = Field(default=None, description="更新时间")


class PaginatedStrategies(BaseModel):
    """分页策略列表"""

    items: list[StrategyItem] = Field(description="策略列表")
    total: int = Field(description="总数")
    page: int = Field(description="当前页码")
    page_size: int = Field(description="每页数量")
    total_pages: int = Field(description="总页数")


class StrategyResponse(BaseModel):
    """策略响应"""

    code: int = Field(default=0, description="响应码")
    message: str = Field(default="success", description="响应消息")
    data: StrategyItem = Field(description="策略详情")


class StrategyListResponse(BaseModel):
    """策略列表响应"""

    code: int = Field(default=0, description="响应码")
    message: str = Field(default="success", description="响应消息")
    data: PaginatedStrategies = Field(description="分页策略数据")


class BacktestCreate(BaseModel):
    """创建回测请求"""

    strategy_id: int = Field(description="策略ID")
    start_date: date = Field(description="回测起始日期")
    end_date: date = Field(description="回测结束日期")
    initial_capital: Decimal = Field(default=Decimal("1000000"), ge=10000, description="初始资金")
    parameters: dict[str, Any] | None = Field(default=None, description="回测参数")


class BacktestItem(BaseModel):
    """回测项"""

    id: int = Field(description="回测ID")
    strategy_id: int = Field(description="策略ID")
    status: str = Field(description="状态: pending/running/completed/failed")
    start_date: date = Field(description="回测起始日期")
    end_date: date = Field(description="回测结束日期")
    initial_capital: Decimal | None = Field(default=None, description="初始资金")
    result: dict[str, Any] | None = Field(default=None, description="回测结果")
    created_at: str | None = Field(default=None, description="创建时间")


class PaginatedBacktests(BaseModel):
    """分页回测列表"""

    items: list[BacktestItem] = Field(description="回测列表")
    total: int = Field(description="总数")
    page: int = Field(description="当前页码")
    page_size: int = Field(description="每页数量")
    total_pages: int = Field(description="总页数")


class BacktestResponse(BaseModel):
    """回测响应"""

    code: int = Field(default=0, description="响应码")
    message: str = Field(default="success", description="响应消息")
    data: BacktestItem = Field(description="回测详情")


class BacktestListResponse(BaseModel):
    """回测列表响应"""

    code: int = Field(default=0, description="响应码")
    message: str = Field(default="success", description="响应消息")
    data: PaginatedBacktests = Field(description="分页回测数据")


# ============== Strategy Endpoints ==============


@router.get("/strategies", response_model=StrategyListResponse, summary="获取策略列表")
async def get_strategies(
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取策略列表(分页)
    """
    service = BacktestService(session)
    strategies, total = await service.get_strategies(page=page, page_size=page_size)

    items = [
        {
            "id": strategy.id,
            "name": strategy.name,
            "code": strategy.code,
            "parameters": strategy.parameters,
            "description": strategy.description,
            "created_at": strategy.created_at.isoformat() if strategy.created_at else None,
            "updated_at": strategy.updated_at.isoformat() if strategy.updated_at else None,
        }
        for strategy in strategies
    ]

    total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

    return {
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        },
    }


@router.post("/strategies", summary="创建策略")
async def create_strategy(
    request: StrategyCreate = Body(description="策略信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    创建新策略
    """
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
            "updated_at": strategy.updated_at.isoformat() if strategy.updated_at else None,
        },
    }


@router.get("/strategies/{strategy_id}", response_model=StrategyResponse, summary="获取策略详情")
async def get_strategy(
    strategy_id: int = Path(description="策略ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取单个策略的详细信息
    """
    service = BacktestService(session)
    strategy = await service.get_strategy(strategy_id)

    if strategy is None:
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
    request: StrategyUpdate = Body(description="策略更新信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    更新策略信息
    """
    service = BacktestService(session)
    strategy = await service.update_strategy(
        strategy_id=strategy_id,
        name=request.name,
        code=request.code,
        parameters=request.parameters,
        description=request.description,
    )

    if strategy is None:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    # Get values before commit to avoid lazy loading issues
    strategy_id_val = strategy.id
    strategy_name = strategy.name
    strategy_code = strategy.code
    strategy_params = strategy.parameters
    strategy_desc = strategy.description
    strategy_created = strategy.created_at.isoformat() if strategy.created_at else None

    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": strategy_id_val,
            "name": strategy_name,
            "code": strategy_code,
            "parameters": strategy_params,
            "description": strategy_desc,
            "created_at": strategy_created,
            "updated_at": None,  # Updated time will be set by DB, client can refetch
        },
    }


@router.delete("/strategies/{strategy_id}", summary="删除策略")
async def delete_strategy(
    strategy_id: int = Path(description="策略ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    删除策略
    """
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


@router.get("/backtests", response_model=BacktestListResponse, summary="获取回测列表")
async def get_backtests(
    strategy_id: int | None = Query(default=None, description="策略ID筛选"),
    status: str | None = Query(default=None, description="状态筛选: pending/running/completed/failed"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取回测列表(分页)

    支持按策略ID和状态筛选
    """
    if status is not None and status not in ("pending", "running", "completed", "failed"):
        raise HTTPException(
            status_code=400,
            detail="status 必须是 pending、running、completed 或 failed",
        )

    service = BacktestService(session)
    backtests, total = await service.get_backtests(
        strategy_id=strategy_id,
        status=status,
        page=page,
        page_size=page_size,
    )

    items = [
        {
            "id": backtest.id,
            "strategy_id": backtest.strategy_id,
            "status": backtest.status,
            "start_date": backtest.start_date,
            "end_date": backtest.end_date,
            "initial_capital": backtest.initial_capital,
            "result": backtest.result,
            "created_at": backtest.created_at.isoformat() if backtest.created_at else None,
        }
        for backtest in backtests
    ]

    total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

    return {
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        },
    }


@router.post("/backtests", summary="创建回测")
async def create_backtest(
    request: BacktestCreate = Body(description="回测信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    创建新的回测任务
    """
    # 验证日期
    if request.end_date < request.start_date:
        raise HTTPException(status_code=400, detail="结束日期不能早于开始日期")

    service = BacktestService(session)

    # 检查策略是否存在
    strategy = await service.get_strategy(request.strategy_id)
    if strategy is None:
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
            "start_date": backtest.start_date,
            "end_date": backtest.end_date,
            "initial_capital": backtest.initial_capital,
            "result": backtest.result,
            "created_at": backtest.created_at.isoformat() if backtest.created_at else None,
        },
    }


@router.post("/backtests/{backtest_id}/run", summary="运行回测")
async def run_backtest(
    backtest_id: int = Path(description="回测ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    运行指定的回测任务
    """
    service = BacktestService(session)
    result = await service.run_backtest(backtest_id)
    await session.commit()

    if not result.get("success"):
        error_msg = result.get("error", "未知错误")
        return {
            "code": 1,
            "message": f"回测运行失败: {error_msg}",
            "data": None,
        }

    return {
        "code": 0,
        "message": "success",
        "data": result.get("result"),
    }


@router.get("/backtests/{backtest_id}", summary="获取回测报告")
async def get_backtest_report(
    backtest_id: int = Path(description="回测ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取回测详细报告
    """
    service = BacktestService(session)
    result = await service.get_backtest_report(backtest_id)

    if not result.get("success"):
        raise HTTPException(status_code=404, detail=f"回测 {backtest_id} 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": result.get("report"),
    }


class BatchDeleteRequest(BaseModel):
    ids: list[int] = Field(min_length=1, description="要删除的回测 ID 列表")


@router.delete("/backtests/{backtest_id}", summary="删除回测")
async def delete_backtest(
    backtest_id: int = Path(description="回测ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    service = BacktestService(session)
    success = await service.delete_backtest(backtest_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"回测 {backtest_id} 不存在")
    await session.commit()
    return {"code": 0, "message": "success", "data": {"deleted": True}}


@router.delete("/backtests/batch", summary="批量删除回测")
async def batch_delete_backtests(
    request: BatchDeleteRequest = Body(description="回测 ID 列表"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    service = BacktestService(session)
    count = await service.delete_backtests_batch(request.ids)
    await session.commit()
    return {"code": 0, "message": f"已删除 {count} 条回测记录", "data": {"deleted_count": count}}
