# backend/app/services/data_service.py
"""数据查询服务"""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.models import (
    KlineDaily,
    KlineMinute,
    Stock,
    WatchlistGroup,
    WatchlistStock,
)


@dataclass
class PaginatedResult:
    """分页结果"""

    items: list[Any]
    total: int
    page: int
    page_size: int
    total_pages: int

    @property
    def has_next(self) -> bool:
        return self.page < self.total_pages

    @property
    def has_prev(self) -> bool:
        return self.page > 1


@dataclass
class StockInfo:
    """股票信息"""

    symbol: str
    name: str | None
    exchange: str | None
    industry: str | None
    list_date: date | None


@dataclass
class KlineData:
    """K线数据"""

    symbol: str
    datetime: datetime | date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None
    amount: Decimal | None


@dataclass
class IndustryInfo:
    """行业信息"""

    name: str
    stock_count: int


@dataclass
class WatchlistGroupInfo:
    """自选股分组信息"""

    id: int
    name: str
    description: str | None
    stock_count: int
    created_at: datetime | None
    updated_at: datetime | None


@dataclass
class WatchlistStockInfo:
    """自选股股票信息"""

    id: int
    group_id: int
    symbol: str
    stock_name: str | None
    added_at: datetime


class DataService:
    """数据查询服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_stocks(
        self,
        page: int = 1,
        page_size: int = 20,
        search: str | None = None,
        industry: str | None = None,
        group_id: int | None = None,
    ) -> PaginatedResult:
        """
        获取股票列表(分页)

        Args:
            page: 页码(从1开始)
            page_size: 每页数量
            search: 搜索关键词(代码或名称)
            industry: 行业筛选
            group_id: 自选股分组筛选

        Returns:
            PaginatedResult: 分页结果
        """
        # 基础查询
        query = select(Stock)

        # 搜索条件
        if search:
            search_pattern = f"%{search}%"
            query = query.where(
                (Stock.symbol.like(search_pattern))
                | (Stock.name.like(search_pattern))
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

        # 统计总数
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # 分页
        offset = (page - 1) * page_size
        query = query.offset(offset).limit(page_size)
        query = query.order_by(Stock.symbol)

        # 执行查询
        result = await self.session.execute(query)
        stocks = result.scalars().all()

        # 转换为数据类
        items = [
            StockInfo(
                symbol=stock.symbol,
                name=stock.name,
                exchange=stock.exchange,
                industry=stock.industry,
                list_date=stock.list_date,
            )
            for stock in stocks
        ]

        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

        return PaginatedResult(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    async def get_klines(
        self,
        symbol: str,
        period: str = "daily",
        start_date: date | None = None,
        end_date: date | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> PaginatedResult:
        """
        获取K线数据

        Args:
            symbol: 股票代码
            period: 周期类型("daily" 或 "minute")
            start_date: 开始日期
            end_date: 结束日期
            page: 页码(从1开始)
            page_size: 每页数量

        Returns:
            PaginatedResult: 分页结果
        """
        # 选择模型
        if period == "minute":
            model = KlineMinute
            datetime_field = KlineMinute.datetime
        else:
            model = KlineDaily
            datetime_field = KlineDaily.trade_date

        # 基础查询
        query = select(model).where(model.symbol == symbol)

        # 日期筛选
        if start_date:
            query = query.where(datetime_field >= start_date)
        if end_date:
            query = query.where(datetime_field <= end_date)

        # 统计总数
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # 分页和排序(按日期倒序)
        offset = (page - 1) * page_size
        query = query.offset(offset).limit(page_size)
        query = query.order_by(datetime_field.desc())

        # 执行查询
        result = await self.session.execute(query)
        klines = result.scalars().all()

        # 转换为数据类
        items = [
            KlineData(
                symbol=kline.symbol,
                datetime=kline.trade_date if period == "daily" else kline.datetime,
                open=kline.open,
                high=kline.high,
                low=kline.low,
                close=kline.close,
                volume=kline.volume,
                amount=kline.amount,
            )
            for kline in klines
        ]

        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

        return PaginatedResult(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    async def get_industries(self) -> list[IndustryInfo]:
        """
        获取行业列表

        Returns:
            list[IndustryInfo]: 行业列表,按股票数量排序
        """
        query = (
            select(Stock.industry, func.count(Stock.symbol).label("stock_count"))
            .where(Stock.industry.isnot(None))
            .group_by(Stock.industry)
            .order_by(func.count(Stock.symbol).desc())
        )

        result = await self.session.execute(query)
        rows = result.all()

        return [
            IndustryInfo(name=row.industry, stock_count=row.stock_count)
            for row in rows
            if row.industry
        ]

    async def get_watchlist_groups(self) -> list[WatchlistGroupInfo]:
        """
        获取自选股分组列表

        Returns:
            list[WatchlistGroupInfo]: 分组列表
        """
        # 使用子查询统计每个分组的股票数量
        stock_count_subq = (
            select(
                WatchlistStock.group_id,
                func.count(WatchlistStock.id).label("stock_count"),
            )
            .group_by(WatchlistStock.group_id)
            .subquery()
        )

        query = (
            select(
                WatchlistGroup.id,
                WatchlistGroup.name,
                WatchlistGroup.description,
                WatchlistGroup.created_at,
                WatchlistGroup.updated_at,
                func.coalesce(stock_count_subq.c.stock_count, 0).label("stock_count"),
            )
            .outerjoin(
                stock_count_subq, WatchlistGroup.id == stock_count_subq.c.group_id
            )
            .order_by(WatchlistGroup.id)
        )

        result = await self.session.execute(query)
        rows = result.all()

        return [
            WatchlistGroupInfo(
                id=row.id,
                name=row.name,
                description=row.description,
                stock_count=row.stock_count,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )
            for row in rows
        ]

    async def get_watchlist_stocks(self, group_id: int) -> list[WatchlistStockInfo]:
        """
        获取自选股分组中的股票

        Args:
            group_id: 分组ID

        Returns:
            list[WatchlistStockInfo]: 股票列表
        """
        query = (
            select(WatchlistStock, Stock.name.label("stock_name"))
            .outerjoin(Stock, WatchlistStock.symbol == Stock.symbol)
            .where(WatchlistStock.group_id == group_id)
            .order_by(WatchlistStock.added_at.desc())
        )

        result = await self.session.execute(query)
        rows = result.all()

        return [
            WatchlistStockInfo(
                id=row.WatchlistStock.id,
                group_id=row.WatchlistStock.group_id,
                symbol=row.WatchlistStock.symbol,
                stock_name=row.stock_name,
                added_at=row.WatchlistStock.added_at,
            )
            for row in rows
        ]

    async def add_to_watchlist(self, group_id: int, symbol: str) -> WatchlistStock:
        """
        添加股票到自选股分组

        Args:
            group_id: 分组ID
            symbol: 股票代码

        Returns:
            WatchlistStock: 新创建的自选股记录
        """
        watchlist_stock = WatchlistStock(group_id=group_id, symbol=symbol)
        self.session.add(watchlist_stock)
        await self.session.flush()
        return watchlist_stock

    async def remove_from_watchlist(self, group_id: int, symbol: str) -> bool:
        """
        从自选股分组移除股票

        Args:
            group_id: 分组ID
            symbol: 股票代码

        Returns:
            bool: 是否成功移除
        """
        query = select(WatchlistStock).where(
            WatchlistStock.group_id == group_id, WatchlistStock.symbol == symbol
        )
        result = await self.session.execute(query)
        watchlist_stock = result.scalar_one_or_none()

        if watchlist_stock:
            await self.session.delete(watchlist_stock)
            return True
        return False

    async def create_watchlist_group(
        self, name: str, description: str | None = None
    ) -> WatchlistGroup:
        """
        创建自选股分组

        Args:
            name: 分组名称
            description: 分组描述

        Returns:
            WatchlistGroup: 新创建的分组
        """
        group = WatchlistGroup(name=name, description=description)
        self.session.add(group)
        await self.session.flush()
        return group

    async def delete_watchlist_group(self, group_id: int) -> bool:
        """
        删除自选股分组

        Args:
            group_id: 分组ID

        Returns:
            bool: 是否成功删除
        """
        query = select(WatchlistGroup).where(WatchlistGroup.id == group_id)
        result = await self.session.execute(query)
        group = result.scalar_one_or_none()

        if group:
            await self.session.delete(group)
            return True
        return False
