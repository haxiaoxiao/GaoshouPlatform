# backend/app/api/data.py
"""数据相关 API 接口"""
from datetime import date
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services import DataService, SyncService

router = APIRouter()


# ============== Pydantic Models ==============


class PaginationParams(BaseModel):
    """分页参数"""

    page: int = Field(default=1, ge=1, description="页码")
    page_size: int = Field(default=20, ge=1, le=100, description="每页数量")


class StockResponse(BaseModel):
    """股票信息响应"""

    symbol: str = Field(description="股票代码")
    name: str | None = Field(default=None, description="股票名称")
    exchange: str | None = Field(default=None, description="交易所")
    industry: str | None = Field(default=None, description="所属行业")
    list_date: date | None = Field(default=None, description="上市日期")


class KlineResponse(BaseModel):
    """K线数据响应"""

    symbol: str = Field(description="股票代码")
    datetime: str = Field(description="时间")
    open: float | None = Field(default=None, description="开盘价")
    high: float | None = Field(default=None, description="最高价")
    low: float | None = Field(default=None, description="最低价")
    close: float | None = Field(default=None, description="收盘价")
    volume: int | None = Field(default=None, description="成交量")
    amount: float | None = Field(default=None, description="成交额")


class PaginatedResponse(BaseModel):
    """分页响应"""

    items: list[Any] = Field(description="数据列表")
    total: int = Field(description="总数量")
    page: int = Field(description="当前页码")
    page_size: int = Field(description="每页数量")
    total_pages: int = Field(description="总页数")


class WatchlistGroupCreate(BaseModel):
    """创建自选股分组请求"""

    name: str = Field(min_length=1, max_length=50, description="分组名称")
    description: str | None = Field(default=None, description="分组描述")


class WatchlistGroupResponse(BaseModel):
    """自选股分组响应"""

    id: int = Field(description="分组ID")
    name: str = Field(description="分组名称")
    description: str | None = Field(default=None, description="分组描述")
    stock_count: int = Field(description="股票数量")
    created_at: str | None = Field(default=None, description="创建时间")
    updated_at: str | None = Field(default=None, description="更新时间")


class WatchlistStockAdd(BaseModel):
    """添加股票到自选股请求"""

    symbol: str = Field(description="股票代码")


class WatchlistStockResponse(BaseModel):
    """自选股股票响应"""

    id: int = Field(description="记录ID")
    group_id: int = Field(description="分组ID")
    symbol: str = Field(description="股票代码")
    stock_name: str | None = Field(default=None, description="股票名称")
    added_at: str = Field(description="添加时间")


class SyncRequest(BaseModel):
    """同步请求"""

    sync_type: str = Field(description="同步类型: stock_info/kline_daily/kline_minute")
    symbols: list[str] | None = Field(default=None, description="股票代码列表")
    start_date: date | None = Field(default=None, description="开始日期")
    end_date: date | None = Field(default=None, description="结束日期")
    failure_strategy: str = Field(default="skip", description="失败策略: skip/retry/stop")


class SyncProgressResponse(BaseModel):
    """同步进度响应"""

    sync_type: str = Field(description="同步类型")
    status: str = Field(description="状态: idle/running/completed/failed")
    total: int = Field(default=0, description="总数量")
    current: int = Field(default=0, description="当前数量")
    success_count: int = Field(default=0, description="成功数量")
    failed_count: int = Field(default=0, description="失败数量")
    progress_percent: float = Field(default=0.0, description="进度百分比")
    start_time: str | None = Field(default=None, description="开始时间")
    end_time: str | None = Field(default=None, description="结束时间")
    error_message: str | None = Field(default=None, description="错误信息")
    details: dict[str, Any] = Field(default_factory=dict, description="详细信息")


class SyncLogResponse(BaseModel):
    """同步日志响应"""

    id: int = Field(description="日志ID")
    task_id: int | None = Field(default=None, description="关联任务ID")
    sync_type: str = Field(description="同步类型")
    status: str = Field(description="状态")
    total_count: int | None = Field(default=None, description="总数量")
    success_count: int | None = Field(default=None, description="成功数量")
    failed_count: int | None = Field(default=None, description="失败数量")
    start_time: str = Field(description="开始时间")
    end_time: str | None = Field(default=None, description="结束时间")
    error_message: str | None = Field(default=None, description="错误信息")
    details: dict[str, Any] | None = Field(default=None, description="详细信息")
    created_at: str = Field(description="创建时间")


# ============== Stock Endpoints ==============


@router.get("/stocks", summary="获取股票列表")
async def get_stocks(
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量"),
    search: str | None = Query(default=None, description="搜索关键词(代码或名称)"),
    industry: str | None = Query(default=None, description="行业筛选"),
    group_id: int | None = Query(default=None, description="自选股分组ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取股票列表(分页)

    支持按代码/名称搜索、行业筛选、自选股分组筛选
    """
    service = DataService(session)
    result = await service.get_stocks(
        page=page,
        page_size=page_size,
        search=search,
        industry=industry,
        group_id=group_id,
    )

    # 转换为响应格式
    items = [
        {
            "symbol": item.symbol,
            "name": item.name,
            "exchange": item.exchange,
            "industry": item.industry,
            "list_date": item.list_date.isoformat() if item.list_date else None,
        }
        for item in result.items
    ]

    return {
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": result.total,
            "page": result.page,
            "page_size": result.page_size,
            "total_pages": result.total_pages,
        },
    }


@router.post("/stocks", summary="获取股票列表(POST)")
async def post_stocks(
    pagination: PaginationParams = Body(default=PaginationParams()),
    search: str | None = Body(default=None, description="搜索关键词"),
    industry: str | None = Body(default=None, description="行业筛选"),
    group_id: int | None = Body(default=None, description="自选股分组ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取股票列表(POST方式)

    支持按代码/名称搜索、行业筛选、自选股分组筛选
    """
    service = DataService(session)
    result = await service.get_stocks(
        page=pagination.page,
        page_size=pagination.page_size,
        search=search,
        industry=industry,
        group_id=group_id,
    )

    items = [
        {
            "symbol": item.symbol,
            "name": item.name,
            "exchange": item.exchange,
            "industry": item.industry,
            "list_date": item.list_date.isoformat() if item.list_date else None,
        }
        for item in result.items
    ]

    return {
        "code": 0,
        "message": "success",
        "data": {
            "items": items,
            "total": result.total,
            "page": result.page,
            "page_size": result.page_size,
            "total_pages": result.total_pages,
        },
    }


@router.get("/stocks/{symbol}", summary="获取股票详情")
async def get_stock_detail(
    symbol: str = Path(description="股票代码"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取单个股票的详细信息
    """
    from sqlalchemy import select

    from app.db.models import Stock

    query = select(Stock).where(Stock.symbol == symbol)
    result = await session.execute(query)
    stock = result.scalar_one_or_none()

    if stock is None:
        raise HTTPException(status_code=404, detail=f"股票 {symbol} 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": {
            "symbol": stock.symbol,
            "name": stock.name,
            "exchange": stock.exchange,
            "industry": stock.industry,
            "list_date": stock.list_date.isoformat() if stock.list_date else None,
            "created_at": stock.created_at.isoformat() if stock.created_at else None,
            "updated_at": stock.updated_at.isoformat() if stock.updated_at else None,
        },
    }


# ============== Kline Endpoints ==============


@router.get("/klines", summary="获取K线数据")
async def get_klines(
    symbol: str = Query(description="股票代码"),
    period: str = Query(default="daily", description="周期类型: daily/minute"),
    start_date: date | None = Query(default=None, description="开始日期"),
    end_date: date | None = Query(default=None, description="结束日期"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=100, ge=1, le=500, description="每页数量"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取K线数据

    支持日K线和分钟K线，按日期倒序返回
    """
    if period not in ("daily", "minute"):
        raise HTTPException(status_code=400, detail="period 必须是 daily 或 minute")

    service = DataService(session)
    result = await service.get_klines(
        symbol=symbol,
        period=period,
        start_date=start_date,
        end_date=end_date,
        page=page,
        page_size=page_size,
    )

    # 转换为响应格式
    items = [
        {
            "symbol": item.symbol,
            "datetime": (
                item.datetime.isoformat()
                if isinstance(item.datetime, date)
                else item.datetime.isoformat()
            ),
            "open": float(item.open) if item.open else None,
            "high": float(item.high) if item.high else None,
            "low": float(item.low) if item.low else None,
            "close": float(item.close) if item.close else None,
            "volume": item.volume,
            "amount": float(item.amount) if item.amount else None,
        }
        for item in result.items
    ]

    return {
        "code": 0,
        "message": "success",
        "data": {
            "symbol": symbol,
            "period": period,
            "items": items,
            "total": result.total,
            "page": result.page,
            "page_size": result.page_size,
            "total_pages": result.total_pages,
        },
    }


# ============== Watchlist Endpoints ==============


@router.get("/watchlist/groups", summary="获取自选股分组列表")
async def get_watchlist_groups(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取所有自选股分组
    """
    service = DataService(session)
    groups = await service.get_watchlist_groups()

    items = [
        {
            "id": group.id,
            "name": group.name,
            "description": group.description,
            "stock_count": group.stock_count,
            "created_at": group.created_at.isoformat() if group.created_at else None,
            "updated_at": group.updated_at.isoformat() if group.updated_at else None,
        }
        for group in groups
    ]

    return {
        "code": 0,
        "message": "success",
        "data": items,
    }


@router.post("/watchlist/groups", summary="创建自选股分组")
async def create_watchlist_group(
    request: WatchlistGroupCreate = Body(description="分组信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    创建新的自选股分组
    """
    service = DataService(session)
    group = await service.create_watchlist_group(
        name=request.name, description=request.description
    )
    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": group.id,
            "name": group.name,
            "description": group.description,
            "stock_count": 0,
            "created_at": group.created_at.isoformat() if group.created_at else None,
            "updated_at": group.updated_at.isoformat() if group.updated_at else None,
        },
    }


@router.delete("/watchlist/groups/{group_id}", summary="删除自选股分组")
async def delete_watchlist_group(
    group_id: int = Path(description="分组ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    删除自选股分组(同时删除分组内的股票)
    """
    service = DataService(session)
    success = await service.delete_watchlist_group(group_id)
    await session.commit()

    if not success:
        raise HTTPException(status_code=404, detail=f"分组 {group_id} 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": {"deleted": True},
    }


@router.get("/watchlist/groups/{group_id}/stocks", summary="获取分组内的股票")
async def get_watchlist_stocks(
    group_id: int = Path(description="分组ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取自选股分组内的股票列表
    """
    service = DataService(session)
    stocks = await service.get_watchlist_stocks(group_id)

    items = [
        {
            "id": stock.id,
            "group_id": stock.group_id,
            "symbol": stock.symbol,
            "stock_name": stock.stock_name,
            "added_at": stock.added_at.isoformat(),
        }
        for stock in stocks
    ]

    return {
        "code": 0,
        "message": "success",
        "data": items,
    }


@router.post("/watchlist/groups/{group_id}/stocks", summary="添加股票到分组")
async def add_to_watchlist(
    group_id: int = Path(description="分组ID"),
    request: WatchlistStockAdd = Body(description="股票信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    添加股票到自选股分组
    """
    service = DataService(session)

    # 检查分组是否存在
    groups = await service.get_watchlist_groups()
    if not any(g.id == group_id for g in groups):
        raise HTTPException(status_code=404, detail=f"分组 {group_id} 不存在")

    try:
        watchlist_stock = await service.add_to_watchlist(
            group_id=group_id, symbol=request.symbol
        )
        await session.commit()
    except Exception as e:
        if "UNIQUE constraint" in str(e) or "Duplicate" in str(e):
            raise HTTPException(
                status_code=400, detail=f"股票 {request.symbol} 已在分组中"
            )
        raise

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": watchlist_stock.id,
            "group_id": group_id,
            "symbol": request.symbol,
            "added_at": watchlist_stock.added_at.isoformat(),
        },
    }


@router.delete(
    "/watchlist/groups/{group_id}/stocks/{symbol}", summary="从分组移除股票"
)
async def remove_from_watchlist(
    group_id: int = Path(description="分组ID"),
    symbol: str = Path(description="股票代码"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    从自选股分组移除股票
    """
    service = DataService(session)
    success = await service.remove_from_watchlist(group_id=group_id, symbol=symbol)
    await session.commit()

    if not success:
        raise HTTPException(
            status_code=404, detail=f"股票 {symbol} 不在分组 {group_id} 中"
        )

    return {
        "code": 0,
        "message": "success",
        "data": {"removed": True},
    }


# ============== Sync Endpoints ==============


@router.post("/sync", summary="触发数据同步")
async def trigger_sync(
    request: SyncRequest = Body(description="同步参数"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    触发数据同步任务

    同步类型:
    - stock_info: 股票基础信息
    - kline_daily: 日K线数据
    - kline_minute: 分钟K线数据
    """
    if request.sync_type not in ("stock_info", "kline_daily", "kline_minute"):
        raise HTTPException(
            status_code=400,
            detail="sync_type 必须是 stock_info、kline_daily 或 kline_minute",
        )

    if request.failure_strategy not in ("skip", "retry", "stop"):
        raise HTTPException(
            status_code=400, detail="failure_strategy 必须是 skip、retry 或 stop"
        )

    service = SyncService(session)

    # 检查是否有正在进行的同步任务
    current_status = service.get_sync_status()
    if current_status and current_status.status == "running":
        raise HTTPException(status_code=409, detail="已有同步任务正在进行中")

    try:
        if request.sync_type == "stock_info":
            progress = await service.sync_stock_info(
                failure_strategy=request.failure_strategy
            )
        elif request.sync_type == "kline_daily":
            progress = await service.sync_kline_daily(
                symbols=request.symbols,
                start_date=request.start_date,
                end_date=request.end_date,
                failure_strategy=request.failure_strategy,
            )
        else:  # kline_minute
            progress = await service.sync_kline_minute(
                symbols=request.symbols,
                start_date=request.start_date,
                end_date=request.end_date,
                failure_strategy=request.failure_strategy,
            )

        return {
            "code": 0,
            "message": "success",
            "data": progress.to_dict(),
        }
    except Exception as e:
        return {
            "code": 1,
            "message": str(e),
            "data": None,
        }


@router.get("/sync/status", summary="获取同步状态")
async def get_sync_status(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取当前同步任务状态
    """
    service = SyncService(session)
    status = service.get_sync_status()

    if status is None:
        return {
            "code": 0,
            "message": "success",
            "data": {
                "sync_type": None,
                "status": "idle",
                "total": 0,
                "current": 0,
                "success_count": 0,
                "failed_count": 0,
                "progress_percent": 0.0,
                "start_time": None,
                "end_time": None,
                "error_message": None,
                "details": {},
            },
        }

    return {
        "code": 0,
        "message": "success",
        "data": status.to_dict(),
    }


@router.get("/sync/logs", summary="获取同步日志")
async def get_sync_logs(
    sync_type: str | None = Query(default=None, description="同步类型筛选"),
    task_id: int | None = Query(default=None, description="任务ID筛选"),
    limit: int = Query(default=50, ge=1, le=200, description="返回数量限制"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取同步日志列表
    """
    service = SyncService(session)
    logs = await service.get_sync_logs(
        sync_type=sync_type, task_id=task_id, limit=limit
    )

    items = [
        {
            "id": log.id,
            "task_id": log.task_id,
            "sync_type": log.sync_type,
            "status": log.status,
            "total_count": log.total_count,
            "success_count": log.success_count,
            "failed_count": log.failed_count,
            "start_time": log.start_time.isoformat(),
            "end_time": log.end_time.isoformat() if log.end_time else None,
            "error_message": log.error_message,
            "details": log.details,
            "created_at": log.created_at.isoformat(),
        }
        for log in logs
    ]

    return {
        "code": 0,
        "message": "success",
        "data": items,
    }
