# backend/app/services/data_service.py
"""数据查询服务"""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.clickhouse import get_ch_client
from app.db.models import (
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
        # 选择表名
        table_name = "klines_minute" if period == "minute" else "klines_daily"
        datetime_field = "datetime" if period == "minute" else "trade_date"

        # 获取 ClickHouse 客户端
        client = get_ch_client()

        # 构建 WHERE 条件
        where_conditions = ["symbol = %(symbol)s"]
        params: dict[str, Any] = {"symbol": symbol}

        if start_date:
            where_conditions.append(f"{datetime_field} >= %(start_date)s")
            params["start_date"] = start_date
        if end_date:
            where_conditions.append(f"{datetime_field} <= %(end_date)s")
            params["end_date"] = end_date

        where_clause = " AND ".join(where_conditions)

        # 查询总数
        count_query = f"SELECT count() FROM {table_name} WHERE {where_clause}"
        count_result = client.execute(count_query, params)
        total = count_result[0][0] if count_result else 0

        # 分页查询数据 (按日期倒序)
        offset = (page - 1) * page_size
        data_query = f"""
            SELECT symbol, {datetime_field}, open, high, low, close, volume, amount
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY {datetime_field} DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        params["limit"] = page_size
        params["offset"] = offset

        rows = client.execute(data_query, params)

        # 转换为 KlineData 数据类
        items = [
            KlineData(
                symbol=row[0],
                datetime=row[1],
                open=row[2],
                high=row[3],
                low=row[4],
                close=row[5],
                volume=row[6],
                amount=row[7],
            )
            for row in rows
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
