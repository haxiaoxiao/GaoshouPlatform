# backend/app/api/data.py
"""数据相关 API 接口"""
import uuid
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Path, Query
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services import DataService, SyncService
from app.services.cache_invalidation import invalidate_after_sync
from app.services.index_catalog import list_index_catalog
from app.services.runtime_tasks import register_task, update_task
from app.services.sync_proxy import proxy_sync_request, sync_service_health

router = APIRouter()


def _attach_sync_availability(
    data: dict[str, Any],
    *,
    service_available: bool,
    unavailable_reason: str | None = None,
) -> dict[str, Any]:
    status = str(data.get("status") or "idle")
    is_busy = status in {"queued", "running"}
    if not service_available:
        can_trigger = False
        reason = unavailable_reason or "数据同步服务未启动"
    elif is_busy:
        can_trigger = False
        reason = "已有同步任务正在运行"
    else:
        can_trigger = True
        reason = None
    return {
        **data,
        "sync_service_available": service_available,
        "can_trigger": can_trigger,
        "reason": reason,
    }


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

    sync_type: str = Field(description="同步类型: stock_info/stock_full/kline_daily/kline_minute/kline_weekly/realtime_mv")
    symbols: list[str] | None = Field(default=None, description="股票代码列表")
    start_date: date | None = Field(default=None, description="开始日期")
    end_date: date | None = Field(default=None, description="结束日期")
    sync_mode: str = Field(default="range", description="sync mode: incremental/range/full")
    failure_strategy: str = Field(default="skip", description="失败策略: skip/retry/stop")
    full_sync: bool = Field(default=False, description="全量同步标记")
    factor_sync_plan: dict[str, Any] | None = Field(default=None, description="因子依赖同步计划")
    relay_datasets: list[str] | None = Field(default=None, description="Tushare Relay dataset names")
    relay_options: dict[str, Any] | None = Field(default=None, description="Tushare Relay sync options")


    index_symbols: list[str] | None = Field(default=None, description="指数代码列表")


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
    page_size: int = Query(default=20, ge=1, le=500, description="每页数量"),
    search: str | None = Query(default=None, description="搜索关键词(代码或名称)"),
    industry: str | None = Query(default=None, description="行业筛选"),
    exchange: str | None = Query(default=None, description="交易所筛选"),
    is_st: int | None = Query(default=None, description="ST状态: 0-正常, 1-ST"),
    group_id: int | None = Query(default=None, description="自选股分组ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取股票列表(分页)

    支持按代码/名称搜索、行业筛选、交易所筛选、ST状态筛选、自选股分组筛选
    """
    service = DataService(session)
    result = await service.get_stocks(
        page=page,
        page_size=page_size,
        search=search,
        industry=industry,
        exchange=exchange,
        is_st=is_st,
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
            "is_st": item.is_st if hasattr(item, 'is_st') else 0,
            "total_mv": item.total_mv if hasattr(item, 'total_mv') else None,
            "circ_mv": item.circ_mv if hasattr(item, 'circ_mv') else None,
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
    """Return frontend-facing stock detail fields by symbol."""
    from sqlalchemy import select

    from app.db.models import FinancialData, Stock

    stock_result = await session.execute(select(Stock).where(Stock.symbol == symbol))
    stock = stock_result.scalar_one_or_none()

    if stock is None:
        raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")

    financial_result = await session.execute(
        select(FinancialData)
        .where(FinancialData.symbol == symbol)
        .order_by(FinancialData.report_date.desc())
        .limit(1)
    )
    latest_financial = financial_result.scalar_one_or_none()

    total_assets = (
        latest_financial.total_assets
        if latest_financial and latest_financial.total_assets is not None
        else stock.total_assets
    )
    total_liability = (
        latest_financial.total_liability
        if latest_financial and latest_financial.total_liability is not None
        else stock.total_liability
    )
    revenue = (
        latest_financial.revenue
        if latest_financial and latest_financial.revenue is not None
        else stock.revenue
    )
    net_profit = (
        latest_financial.net_profit
        if latest_financial and latest_financial.net_profit is not None
        else stock.net_profit
    )
    debt_ratio = (
        float(total_liability) / float(total_assets)
        if total_assets and total_liability is not None
        else None
    )
    net_margin = (
        float(net_profit) / float(revenue)
        if revenue and net_profit is not None
        else None
    )

    def as_ratio(value: Any) -> float | None:
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric / 100 if abs(numeric) > 1.5 else numeric

    def optional_attr(model: Any, name: str) -> Any:
        return getattr(model, name, None) if model is not None else None

    return {
        "code": 0,
        "message": "success",
        "data": {
            "symbol": stock.symbol,
            "name": stock.name,
            "exchange": stock.exchange,
            "market": stock.exchange,
            "industry": stock.industry,
            "industry2": stock.industry2,
            "industry3": stock.industry3,
            "sector": stock.sector,
            "concept": stock.concept,
            "list_date": stock.list_date.isoformat() if stock.list_date else None,
            "is_st": bool(stock.is_st),
            "is_suspend": bool(stock.is_suspend),
            "is_delist": bool(stock.is_delist),
            "is_active": not bool(stock.is_suspend or stock.is_delist),
            "market_cap": stock.total_mv / 10000 if stock.total_mv is not None else None,
            "float_market_cap": stock.circ_mv / 10000 if stock.circ_mv is not None else None,
            "pe_ratio": (
                latest_financial.pe_ttm
                if latest_financial and latest_financial.pe_ttm is not None
                else stock.pe_ttm
            ),
            "pb_ratio": (
                latest_financial.pb
                if latest_financial and latest_financial.pb is not None
                else stock.pb
            ),
            "roe": as_ratio(
                latest_financial.roe
                if latest_financial and latest_financial.roe is not None
                else stock.roe
            ),
            "eps": (
                latest_financial.eps
                if latest_financial and latest_financial.eps is not None
                else stock.eps
            ),
            "bvps": (
                latest_financial.bvps
                if latest_financial and latest_financial.bvps is not None
                else stock.bvps
            ),
            "revenue_growth": as_ratio(
                latest_financial.revenue_yoy
                if latest_financial and latest_financial.revenue_yoy is not None
                else optional_attr(stock, "revenue_yoy")
            ),
            "profit_growth": as_ratio(
                latest_financial.profit_yoy
                if latest_financial and latest_financial.profit_yoy is not None
                else optional_attr(stock, "profit_yoy")
            ),
            "debt_ratio": debt_ratio,
            "current_ratio": None,
            "gross_margin": as_ratio(
                latest_financial.gross_margin
                if latest_financial and latest_financial.gross_margin is not None
                else optional_attr(stock, "gross_margin")
            ),
            "net_margin": net_margin,
            "dividend_yield": None,
            "latest_report_date": (
                latest_financial.report_date.isoformat()
                if latest_financial and latest_financial.report_date
                else None
            ),
            "latest_ann_date": (
                latest_financial.ann_date.isoformat()
                if latest_financial and latest_financial.ann_date
                else None
            ),
            "created_at": stock.created_at.isoformat() if stock.created_at else None,
            "updated_at": stock.updated_at.isoformat() if stock.updated_at else None,
        },
    }


@router.get("/industries", summary="获取行业列表")
async def get_industries(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取所有行业及其股票数量
    """
    service = DataService(session)
    industries = await service.get_industries()

    return {
        "industries": [
            {"name": item.name, "count": item.stock_count}
            for item in industries
        ],
    }


@router.get("/index-catalog", summary="获取指数目录")
async def get_index_catalog(
    benchmark_only: bool = Query(default=False, description="Only return common benchmark indices"),
    pool_only: bool = Query(default=False, description="Only return indices with constituent pools"),
) -> dict[str, Any]:
    items = list_index_catalog(
        benchmark_only=True if benchmark_only else None,
        pool_only=True if pool_only else None,
    )
    if benchmark_only:
        items = [item for item in items if item.get("common_benchmark")]
    return {
        "code": 0,
        "message": "success",
        "data": items,
    }


# ============== Kline Endpoints ==============


@router.get("/klines", summary="获取K线数据")
async def get_klines(
    symbol: str = Query(description="股票代码"),
    period: str = Query(default="daily", description="周期类型: daily/minute"),
    start_date: date | None = Query(default=None, description="开始日期"),
    end_date: date | None = Query(default=None, description="结束日期"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=100, ge=1, le=1000, description="每页数量"),
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

    # 转换为响应格式 - 使用 trade_date 作为字段名
    items = [
        {
            "symbol": item.symbol,
            "trade_date": (
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
        "items": items,
        "total": result.total,
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


async def _run_sync_task(
    task_id: str,
    sync_type: str,
    symbols: list[str] | None,
    start_date: date | None,
    end_date: date | None,
    failure_strategy: str,
    full_sync: bool,
) -> None:
    """在后台运行同步任务（使用独立的数据库会话）"""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.config import settings
    from app.services.sync_service import SyncService

    engine = create_async_engine(settings.database_url)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    # 快速检查 QMT 连接
    from app.engines.qmt_gateway import qmt_gateway as gw
    if not await gw.check_connection():
        update_task(task_id, status="failed", progress=1.0, result_ref="/data",
                    error="QMT (miniQMT) 未连接，请先启动华泰 miniQMT 客户端后再同步")
        await engine.dispose()
        return

    async with async_session() as session:
        service = SyncService(session)
        try:
            update_task(task_id, status="running", progress=0)
            logger.info(f"symbols={symbols}, start_date={start_date}, end_date={end_date}")
            progress = None
            if sync_type == "datasync":
                progress = await service.sync_datasync(
                    symbols=symbols,
                    end_date=end_date,
                    failure_strategy=failure_strategy,
                    full_sync=full_sync,
                )
            elif sync_type == "stock_info":
                progress = await service.sync_stock_info(
                    failure_strategy=failure_strategy,
                    full_sync=full_sync,
                )
            elif sync_type == "stock_full":
                progress = await service.sync_stock_full(
                    failure_strategy=failure_strategy,
                    run_id=task_id,
                )
            elif sync_type == "financial_data":
                progress = await service.sync_financial_data(
                    failure_strategy=failure_strategy,
                )
            elif sync_type == "realtime_mv":
                progress = await service.sync_realtime_mv(
                    symbols=symbols,
                    failure_strategy=failure_strategy,
                )
            elif sync_type == "kline_daily":
                progress = await service.sync_kline_daily(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    failure_strategy=failure_strategy,
                    full_sync=full_sync,
                    run_id=task_id,
                )
            elif sync_type == "kline_weekly":
                progress = await service.sync_kline_weekly(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    failure_strategy=failure_strategy,
                    full_sync=full_sync,
                )
            elif sync_type == "dividends":
                progress = await service.sync_dividends(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    failure_strategy=failure_strategy,
                )
            elif sync_type == "kline_minute":
                progress = await service.sync_kline_minute(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    failure_strategy=failure_strategy,
                    full_sync=full_sync,
                )
            if getattr(progress, "status", None) == "failed":
                update_task(
                    task_id,
                    status="failed",
                    progress=1.0,
                    result_ref="/data",
                    error=getattr(progress, "error_message", None) or "Data sync failed",
                )
            else:
                update_task(task_id, status="done", progress=1.0, result_ref="/data")
        except Exception as e:
            logger.opt(exception=True).error(f"Sync task {sync_type} failed: {e}")
            update_task(task_id, status="failed", progress=1.0, result_ref="/data", error=str(e))
        finally:
            try:
                invalidated = invalidate_after_sync(sync_type)
                logger.info("Cache invalidated after {} sync: {}", sync_type, invalidated)
            except Exception as exc:
                logger.warning("Cache invalidation after {} sync failed: {}", sync_type, exc)
            await engine.dispose()


@router.post("/sync", summary="触发数据同步")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    request: SyncRequest = Body(description="同步参数"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    return await proxy_sync_request(
        "POST",
        "/api/data/sync",
        json_body=request.model_dump(mode="json"),
    )
    """
    触发数据同步任务

    同步类型:
    - stock_info: 股票基础信息(快速同步)
    - stock_full: 股票完整信息(含市值、财务等扩展字段)
    - financial_data: 下载并同步财务数据(需QMT在线，耗时长)
    - kline_daily: 日K线数据
    - kline_minute: 分钟K线数据
    - kline_weekly: 周K线数据
    - realtime_mv: 实时市值更新
    - dividends: 分红送股数据(需QMT在线)
    """
    valid_types = ("datasync", "stock_info", "stock_full", "financial_data", "kline_daily", "kline_minute", "kline_weekly", "realtime_mv", "dividends", "tushare_relay")
    if request.sync_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"sync_type 必须是: {', '.join(valid_types)}",
        )

    if request.failure_strategy not in ("skip", "retry", "stop"):
        raise HTTPException(
            status_code=400, detail="failure_strategy 必须是 skip、retry 或 stop"
        )

    service = SyncService(session)

    # 检查 QMT 是否在线（所有同步类型都需要）
    from app.engines.qmt_gateway import qmt_gateway as _gw
    if not await _gw.check_connection():
        raise HTTPException(status_code=503, detail="QMT (miniQMT) 未连接，请先启动华泰 miniQMT 客户端后再同步")

    # 检查是否有正在进行的同步任务
    current_status = service.get_sync_status()
    if current_status and current_status.status == "running":
        raise HTTPException(status_code=409, detail="已有同步任务正在进行中")

    task_id = f"sync-{str(uuid.uuid4())[:8]}"
    register_task(
        task_id=task_id,
        kind="data_sync",
        title=f"数据同步 {request.sync_type}",
        status="queued",
        progress=0,
        result_ref="/data",
        meta={
            "sync_type": request.sync_type,
            "start_date": request.start_date.isoformat() if request.start_date else None,
            "end_date": request.end_date.isoformat() if request.end_date else None,
            "symbol_count": len(request.symbols or []),
        },
    )

    # 启动后台同步任务
    background_tasks.add_task(
        _run_sync_task,
        task_id,
        request.sync_type,
        request.symbols,
        request.start_date,
        request.end_date,
        request.failure_strategy,
        request.full_sync,
    )

    # 构造并返回初始运行状态
    from app.services.sync_service import SyncProgress

    initial_progress = SyncProgress(
        sync_type=request.sync_type,
        status="running",
        start_time=datetime.now(),
    )

    return {
        "code": 0,
        "message": "success",
        "data": {**initial_progress.to_dict(), "task_id": task_id},
    }


@router.get("/sync/catalog", summary="获取同步任务目录")
async def get_sync_catalog(refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return await proxy_sync_request("GET", "/api/data/sync/catalog", params={"refresh": refresh})
    except HTTPException as exc:
        logger.warning("Sync service catalog proxy failed: {}", exc.detail)
        from app.services.tushare_relay_sync import build_sync_catalog

        return {"code": 0, "message": "success", "data": build_sync_catalog(refresh=refresh)}


@router.get("/sync/status", summary="获取同步状态")
async def get_sync_status(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    health = await sync_service_health()
    service_available = bool(health.get("healthy"))
    unavailable_reason = None if service_available else str(health.get("error") or "数据同步服务未启动")
    service = SyncService(session)
    persisted = await service.get_persisted_sync_status()
    if persisted:
        return {
            "code": 0,
            "message": "success",
            "data": _attach_sync_availability(
                persisted,
                service_available=service_available,
                unavailable_reason=unavailable_reason,
            ),
        }

    try:
        response = await proxy_sync_request("GET", "/api/data/sync/status")
        if isinstance(response.get("data"), dict):
            response["data"] = _attach_sync_availability(
                response["data"],
                service_available=True,
            )
        return response
    except HTTPException as exc:
        logger.warning("Sync service status proxy failed: {}", exc.detail)
        return {
            "code": 0,
            "message": "success",
            "data": _attach_sync_availability(
                {
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
                    "details": {
                        "sync_service_unavailable": True,
                        "proxy_error": str(exc.detail),
                    },
                },
                service_available=False,
                unavailable_reason=str(exc.detail),
            ),
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
    try:
        return await proxy_sync_request(
            "GET",
            "/api/data/sync/logs",
            params={"sync_type": sync_type, "task_id": task_id, "limit": limit},
        )
    except HTTPException as exc:
        logger.warning("Sync service logs proxy failed: {}", exc.detail)

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


@router.post("/sync/cancel", summary="取消正在运行的同步")
async def cancel_sync(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        return await proxy_sync_request("POST", "/api/data/sync/cancel", json_body={})
    except HTTPException as exc:
        logger.warning("Sync service cancel proxy failed: {}", exc.detail)
        from app.services.sync_run_store import upsert_sync_run

        service = SyncService(session)
        persisted = await service.get_persisted_sync_status()
        if persisted and persisted.get("status") in {"queued", "running"}:
            await upsert_sync_run(
                session,
                run_id=str(persisted.get("run_id") or persisted.get("task_id")),
                sync_type=persisted.get("sync_type"),
                status="cancelled",
                total=persisted.get("total", 0),
                current=persisted.get("current", 0),
                success_count=persisted.get("success_count", 0),
                failed_count=persisted.get("failed_count", 0),
                progress_percent=persisted.get("progress_percent", 0.0),
                end_time=datetime.now(),
                error_message=f"Cancelled while sync service was unavailable: {exc.detail}",
                details=persisted.get("details") or {},
            )
            return {"code": 0, "message": "success", "data": {"cancelled": True}}
        return {"code": 0, "message": "success", "data": {"cancelled": False}}
    """取消当前正在运行的同步任务"""
    service = SyncService(session)
    cancelled = await service.cancel_sync()
    return {
        "code": 0,
        "message": "同步已取消" if cancelled else "当前没有正在运行的同步任务",
        "data": {"cancelled": cancelled},
    }
