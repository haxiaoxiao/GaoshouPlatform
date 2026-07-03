# backend/app/services/data_service.py
"""数据查询服务"""
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.db.duckdb import get_duckdb
from app.db.models import (
    Stock,
    StockConceptMembership,
    WatchlistGroup,
    WatchlistStock,
)


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_list(values: list[str]) -> str:
    return "(" + ", ".join(_sql_literal(value) for value in values) + ")"


def _parquet_glob(dataset: str) -> str | None:
    root = Path(settings.parquet_data_dir) / dataset
    if not root.exists():
        return None
    if any(root.glob("year=*/month=??/*.parquet")):
        return str(root / "year=*" / "month=??" / "*.parquet").replace("\\", "/")
    if any(".tmp-" not in str(file) for file in root.rglob("*.parquet")):
        return str(root / "**" / "*.parquet").replace("\\", "/")
    return None


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        if value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_or_none(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        text = str(value)
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        return datetime.fromisoformat(text[:10]).date()
    except (TypeError, ValueError):
        return None


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
    is_st: int = 0
    total_mv: float | None = None
    circ_mv: float | None = None


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
    industry: str | None = None
    industry2: str | None = None
    industry3: str | None = None
    sector: str | None = None
    concept: str | None = None
    ths_concepts: list[str] | None = None
    total_mv: float | None = None
    circ_mv: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    roe: float | None = None
    latest_close: float | None = None
    latest_amount: float | None = None
    change_pct: float | None = None
    latest_trade_date: date | None = None
    buy_elg_amount: float | None = None
    sell_elg_amount: float | None = None
    buy_lg_amount: float | None = None
    sell_lg_amount: float | None = None
    net_amount_xl: float | None = None
    net_amount_l: float | None = None
    net_mf_amount: float | None = None
    net_pct_main: float | None = None
    moneyflow_trade_date: date | None = None


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
        exchange: str | None = None,
        is_st: int | None = None,
        group_id: int | None = None,
    ) -> PaginatedResult:
        """
        获取股票列表(分页)

        Args:
            page: 页码(从1开始)
            page_size: 每页数量
            search: 搜索关键词(代码或名称)
            industry: 行业筛选
            exchange: 交易所筛选
            is_st: ST状态筛选
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

        # 交易所筛选
        if exchange:
            query = query.where(Stock.exchange == exchange)

        # ST状态筛选
        if is_st is not None:
            query = query.where(Stock.is_st == is_st)

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
                is_st=getattr(stock, 'is_st', 0) or 0,
                total_mv=getattr(stock, 'total_mv', None),
                circ_mv=getattr(stock, 'circ_mv', None),
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
        store = get_market_data_store()
        sd = start_date or date(2000, 1, 1)
        ed = end_date or date.today()

        from decimal import Decimal as D

        if period == "minute":
            dt_start = datetime.combine(sd, datetime.min.time())
            dt_end = datetime.combine(ed, datetime.min.time()) + timedelta(days=1)
            df = store.load_minute([symbol], dt_start, dt_end)
        else:
            df = store.load_daily([symbol], sd, ed)

        if df.empty:
            total = 0
            rows_list = []
        else:
            total = len(df)
            df = df.sort_index(ascending=False)
            offset = (page - 1) * page_size
            df_page = df.iloc[offset : offset + page_size]
            rows_list = df_page.reset_index().to_dict("records")

        items = []
        for r in rows_list:
            dt_field = r.get("datetime", r.get("trade_date"))
            items.append(
                KlineData(
                    symbol=r.get("symbol", symbol),
                    datetime=dt_field,
                    open=D(str(round(float(r["open"]), 4))),
                    high=D(str(round(float(r["high"]), 4))),
                    low=D(str(round(float(r["low"]), 4))),
                    close=D(str(round(float(r["close"]), 4))),
                    volume=int(r["volume"]),
                    amount=D(str(round(float(r["amount"]), 2))),
                )
            )

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
            select(
                WatchlistStock,
                Stock.name.label("stock_name"),
                Stock.industry,
                Stock.industry2,
                Stock.industry3,
                Stock.sector,
                Stock.concept,
                Stock.total_mv,
                Stock.circ_mv,
                Stock.pe_ttm,
                Stock.pb,
                Stock.roe,
            )
            .outerjoin(Stock, WatchlistStock.symbol == Stock.symbol)
            .where(WatchlistStock.group_id == group_id)
            .order_by(WatchlistStock.added_at.desc())
        )

        result = await self.session.execute(query)
        rows = result.all()
        symbols = [row.WatchlistStock.symbol for row in rows]
        ths_concepts_by_symbol: dict[str, list[str]] = {}
        latest_daily_by_symbol: dict[str, dict[str, Any]] = {}
        moneyflow_by_symbol: dict[str, dict[str, Any]] = {}
        if symbols:
            latest_snapshot_query = select(
                func.max(StockConceptMembership.snapshot_date)
            ).where(StockConceptMembership.symbol.in_(symbols))
            latest_snapshot_result = await self.session.execute(latest_snapshot_query)
            latest_snapshot = latest_snapshot_result.scalar_one_or_none()
            if latest_snapshot:
                concepts_query = (
                    select(
                        StockConceptMembership.symbol,
                        StockConceptMembership.concept_name,
                    )
                    .where(
                        StockConceptMembership.symbol.in_(symbols),
                        StockConceptMembership.snapshot_date == latest_snapshot,
                    )
                    .order_by(
                        StockConceptMembership.symbol,
                        StockConceptMembership.concept_name,
                    )
                )
                concepts_result = await self.session.execute(concepts_query)
                for symbol, concept_name in concepts_result.all():
                    if concept_name:
                        bucket = ths_concepts_by_symbol.setdefault(symbol, [])
                        if concept_name not in bucket and len(bucket) < 12:
                            bucket.append(concept_name)

            latest_daily_by_symbol, moneyflow_by_symbol = await asyncio.gather(
                asyncio.to_thread(self._load_latest_daily_snapshots, symbols),
                asyncio.to_thread(self._load_latest_moneyflow_snapshots, symbols),
            )

        items: list[WatchlistStockInfo] = []
        for row in rows:
            symbol = row.WatchlistStock.symbol
            daily = latest_daily_by_symbol.get(symbol, {})
            moneyflow = moneyflow_by_symbol.get(symbol, {})
            items.append(
                WatchlistStockInfo(
                    id=row.WatchlistStock.id,
                    group_id=row.WatchlistStock.group_id,
                    symbol=symbol,
                    stock_name=row.stock_name,
                    added_at=row.WatchlistStock.added_at,
                    industry=row.industry,
                    industry2=row.industry2,
                    industry3=row.industry3,
                    sector=row.sector,
                    concept=row.concept,
                    ths_concepts=ths_concepts_by_symbol.get(symbol, []),
                    total_mv=row.total_mv,
                    circ_mv=row.circ_mv,
                    pe_ttm=row.pe_ttm,
                    pb=row.pb,
                    roe=row.roe,
                    latest_close=daily.get("latest_close"),
                    latest_amount=daily.get("latest_amount"),
                    change_pct=daily.get("change_pct"),
                    latest_trade_date=daily.get("latest_trade_date"),
                    buy_elg_amount=moneyflow.get("buy_elg_amount"),
                    sell_elg_amount=moneyflow.get("sell_elg_amount"),
                    buy_lg_amount=moneyflow.get("buy_lg_amount"),
                    sell_lg_amount=moneyflow.get("sell_lg_amount"),
                    net_amount_xl=moneyflow.get("net_amount_xl"),
                    net_amount_l=moneyflow.get("net_amount_l"),
                    net_mf_amount=moneyflow.get("net_mf_amount"),
                    net_pct_main=moneyflow.get("net_pct_main"),
                    moneyflow_trade_date=moneyflow.get("moneyflow_trade_date"),
                )
            )
        return items

    def _load_latest_daily_snapshots(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}

        end_date = date.today()
        start_date = end_date - timedelta(days=180)
        store = get_market_data_store()
        df = store.load_daily(
            symbols,
            start_date,
            end_date,
            columns=["symbol", "trade_date", "close", "amount"],
        )
        if df.empty:
            return {}

        frame = df.reset_index()
        if "trade_date" not in frame.columns:
            return {}
        frame["trade_date"] = frame["trade_date"].apply(_date_or_none)
        frame = frame.dropna(subset=["symbol", "trade_date"]).sort_values(["symbol", "trade_date"])

        snapshots: dict[str, dict[str, Any]] = {}
        for symbol, group in frame.groupby("symbol", sort=False):
            group = group.dropna(subset=["close"])
            if group.empty:
                continue
            latest = group.iloc[-1]
            latest_close = _float_or_none(latest.get("close"))
            latest_amount = _float_or_none(latest.get("amount"))
            previous_close = None
            if len(group) >= 2:
                previous_close = _float_or_none(group.iloc[-2].get("close"))

            change_pct = None
            if latest_close is not None and previous_close and previous_close > 0:
                change_pct = (latest_close / previous_close - 1.0) * 100.0

            snapshots[str(symbol)] = {
                "latest_close": latest_close,
                "latest_amount": latest_amount,
                "change_pct": change_pct,
                "latest_trade_date": latest.get("trade_date"),
            }
        return snapshots

    def _load_latest_moneyflow_snapshots(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}

        snapshots = self._load_latest_moneyflow_dataset(
            dataset="jq_money_flow_daily",
            date_col="trade_date_1",
            symbols=symbols,
        )
        missing = [symbol for symbol in symbols if symbol not in snapshots]
        if missing:
            snapshots.update(
                self._load_latest_moneyflow_dataset(
                    dataset="moneyflow",
                    date_col="trade_date",
                    symbols=missing,
                )
            )
        return snapshots

    def _load_latest_moneyflow_dataset(
        self,
        *,
        dataset: str,
        date_col: str,
        symbols: list[str],
    ) -> dict[str, dict[str, Any]]:
        pattern = _parquet_glob(dataset)
        if not pattern:
            return {}

        db = get_duckdb()
        try:
            columns = set(
                db.execute(
                    f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true) LIMIT 0"
                ).df().columns
            )
        except Exception as exc:
            logger.warning("Failed to inspect watchlist moneyflow dataset {}: {}", dataset, exc)
            return {}

        symbol_col = "symbol" if "symbol" in columns else "ts_code" if "ts_code" in columns else None
        if not symbol_col or date_col not in columns:
            return {}

        value_cols = [
            "net_amount_main",
            "net_pct_main",
            "net_amount_xl",
            "net_amount_l",
            "net_mf_amount",
            "buy_elg_amount",
            "sell_elg_amount",
            "buy_lg_amount",
            "sell_lg_amount",
        ]
        present_value_cols = [col for col in value_cols if col in columns]
        if not present_value_cols:
            return {}

        non_null_expr = (
            "("
            + " OR ".join(f"TRY_CAST({col} AS DOUBLE) IS NOT NULL" for col in present_value_cols)
            + ")"
        )
        try:
            latest_row = db.execute(
                f"""
                SELECT max({date_col})
                FROM read_parquet('{pattern}', hive_partitioning=true)
                WHERE {date_col} IS NOT NULL
                  AND {non_null_expr}
                """
            ).fetchone()
        except Exception as exc:
            logger.warning("Failed to read latest watchlist moneyflow date from {}: {}", dataset, exc)
            return {}
        latest_date = latest_row[0] if latest_row else None
        if latest_date is None:
            return {}

        select_cols = [
            f"{symbol_col} AS symbol",
            f"{date_col} AS moneyflow_trade_date",
        ]
        for col in value_cols:
            select_cols.append(col if col in columns else f"NULL AS {col}")

        try:
            rows = db.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM read_parquet('{pattern}', hive_partitioning=true)
                WHERE {date_col} = {_sql_literal(latest_date)}
                  AND {symbol_col} IN {_sql_list(symbols)}
                  AND {non_null_expr}
                """
            ).fetchall()
        except Exception as exc:
            logger.warning("Failed to load latest watchlist moneyflow rows from {}: {}", dataset, exc)
            return {}

        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            (
                symbol,
                moneyflow_trade_date,
                net_amount_main,
                net_pct_main,
                net_amount_xl,
                net_amount_l,
                net_mf_amount,
                buy_elg_amount,
                sell_elg_amount,
                buy_lg_amount,
                sell_lg_amount,
            ) = row
            buy_elg = _float_or_none(buy_elg_amount)
            sell_elg = _float_or_none(sell_elg_amount)
            buy_lg = _float_or_none(buy_lg_amount)
            sell_lg = _float_or_none(sell_lg_amount)
            amount_xl = _float_or_none(net_amount_xl)
            amount_l = _float_or_none(net_amount_l)
            if amount_xl is None and buy_elg is not None and sell_elg is not None:
                amount_xl = buy_elg - sell_elg
            if amount_l is None and buy_lg is not None and sell_lg is not None:
                amount_l = buy_lg - sell_lg

            main_amount = _float_or_none(net_amount_main)
            if main_amount is None and (amount_xl is not None or amount_l is not None):
                main_amount = (amount_xl or 0.0) + (amount_l or 0.0)

            result[str(symbol)] = {
                "buy_elg_amount": buy_elg,
                "sell_elg_amount": sell_elg,
                "buy_lg_amount": buy_lg,
                "sell_lg_amount": sell_lg,
                "net_amount_xl": amount_xl,
                "net_amount_l": amount_l,
                "net_mf_amount": main_amount if main_amount is not None else _float_or_none(net_mf_amount),
                "net_pct_main": _float_or_none(net_pct_main),
                "moneyflow_trade_date": _date_or_none(moneyflow_trade_date),
            }
        return result

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
