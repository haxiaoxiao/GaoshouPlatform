# backend/app/services/sync_service.py
"""数据同步服务"""
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import KlineDaily, Stock, SyncLog, SyncTask
from app.engines.qmt_gateway import qmt_gateway


@dataclass
class SyncProgress:
    """同步进度信息"""

    sync_type: str
    status: str  # idle, running, completed, failed
    total: int = 0
    current: int = 0
    success_count: int = 0
    failed_count: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None
    error_message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def progress_percent(self) -> float:
        """计算进度百分比"""
        if self.total == 0:
            return 0.0
        return round(self.current / self.total * 100, 2)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return {
            "sync_type": self.sync_type,
            "status": self.status,
            "total": self.total,
            "current": self.current,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "progress_percent": self.progress_percent,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message,
            "details": self.details,
        }


# 全局同步状态
_current_sync: SyncProgress | None = None


class SyncService:
    """数据同步服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    def get_sync_status(self) -> SyncProgress | None:
        """
        获取当前同步状态

        Returns:
            SyncProgress | None: 当前同步进度，无同步任务时返回 None
        """
        return _current_sync

    async def create_sync_log(
        self,
        sync_type: str,
        status: str,
        total_count: int | None = None,
        success_count: int | None = None,
        failed_count: int | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        error_message: str | None = None,
        details: dict[str, Any] | None = None,
        task_id: int | None = None,
    ) -> SyncLog:
        """
        创建同步日志记录

        Args:
            sync_type: 同步类型
            status: 状态
            total_count: 总数量
            success_count: 成功数量
            failed_count: 失败数量
            start_time: 开始时间
            end_time: 结束时间
            error_message: 错误信息
            details: 详细结果
            task_id: 关联任务ID

        Returns:
            SyncLog: 日志记录
        """
        log = SyncLog(
            task_id=task_id,
            sync_type=sync_type,
            status=status,
            total_count=total_count,
            success_count=success_count,
            failed_count=failed_count,
            start_time=start_time or datetime.now(),
            end_time=end_time,
            error_message=error_message,
            details=details,
        )
        self.session.add(log)
        await self.session.flush()
        return log

    async def sync_stock_info(
        self,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """
        同步股票基础信息

        Args:
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)

        Returns:
            SyncProgress: 同步进度
        """
        global _current_sync

        # 初始化进度
        progress = SyncProgress(
            sync_type="stock_info",
            status="running",
            start_time=datetime.now(),
        )
        _current_sync = progress

        try:
            # 从 QMT 获取股票列表
            stocks = await qmt_gateway.get_stock_list()
            progress.total = len(stocks)
            progress.details = {"total_stocks": len(stocks)}

            failed_stocks: list[dict[str, str]] = []

            # 批量处理股票信息
            for i, stock in enumerate(stocks):
                try:
                    # 使用 upsert 插入或更新
                    stmt = insert(Stock).values(
                        symbol=stock.symbol,
                        name=stock.name,
                        exchange=stock.exchange,
                        industry=stock.industry,
                        list_date=stock.list_date,
                        updated_at=datetime.now(),
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["symbol"],
                        set_={
                            "name": stmt.excluded.name,
                            "exchange": stmt.excluded.exchange,
                            "industry": stmt.excluded.industry,
                            "list_date": stmt.excluded.list_date,
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                    await self.session.execute(stmt)

                    progress.current = i + 1
                    progress.success_count += 1

                    # 每 100 条提交一次
                    if (i + 1) % 100 == 0:
                        await self.session.commit()

                except Exception as e:
                    progress.failed_count += 1
                    failed_stocks.append({
                        "symbol": stock.symbol,
                        "error": str(e),
                    })

                    if failure_strategy == "stop":
                        raise
                    elif failure_strategy == "retry":
                        # 简单重试一次
                        try:
                            stmt = insert(Stock).values(
                                symbol=stock.symbol,
                                name=stock.name,
                                exchange=stock.exchange,
                                industry=stock.industry,
                                list_date=stock.list_date,
                                updated_at=datetime.now(),
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["symbol"],
                                set_={
                                    "name": stmt.excluded.name,
                                    "exchange": stmt.excluded.exchange,
                                    "industry": stmt.excluded.industry,
                                    "list_date": stmt.excluded.list_date,
                                    "updated_at": stmt.excluded.updated_at,
                                },
                            )
                            await self.session.execute(stmt)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            # 移除失败记录
                            failed_stocks.pop()
                        except Exception:
                            pass

            # 最终提交
            await self.session.commit()

            # 更新进度
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["failed_stocks"] = failed_stocks[:100]  # 只保留前100条失败记录

            # 记录日志
            await self.create_sync_log(
                sync_type="stock_info",
                status="completed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()

        except Exception as e:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(e)

            # 记录失败日志
            await self.create_sync_log(
                sync_type="stock_info",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(e),
                task_id=task_id,
            )
            await self.session.commit()
            raise

        finally:
            _current_sync = None

        return progress

    async def sync_kline_daily(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """
        同步日K线数据

        Args:
            symbols: 股票代码列表，为空则同步所有股票
            start_date: 起始日期，默认为最近30天
            end_date: 结束日期，默认为今天
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)

        Returns:
            SyncProgress: 同步进度
        """
        global _current_sync

        # 设置默认日期范围
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        # 初始化进度
        progress = SyncProgress(
            sync_type="kline_daily",
            status="running",
            start_time=datetime.now(),
            details={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        _current_sync = progress

        try:
            # 如果没有指定股票列表，获取所有股票
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)
            failed_symbols: list[dict[str, str]] = []
            total_klines = 0

            # 逐个股票同步K线
            for i, symbol in enumerate(symbols):
                try:
                    # 从 QMT 获取K线数据
                    klines = await qmt_gateway.get_kline_daily(
                        symbol, start_date, end_date
                    )

                    if not klines:
                        progress.current = i + 1
                        continue

                    # 批量插入K线数据
                    for kline in klines:
                        try:
                            stmt = insert(KlineDaily).values(
                                symbol=kline.symbol,
                                trade_date=kline.datetime,
                                open=Decimal(str(kline.open)),
                                high=Decimal(str(kline.high)),
                                low=Decimal(str(kline.low)),
                                close=Decimal(str(kline.close)),
                                volume=kline.volume,
                                amount=Decimal(str(kline.amount)),
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["symbol", "trade_date"],
                                set_={
                                    "open": stmt.excluded.open,
                                    "high": stmt.excluded.high,
                                    "low": stmt.excluded.low,
                                    "close": stmt.excluded.close,
                                    "volume": stmt.excluded.volume,
                                    "amount": stmt.excluded.amount,
                                },
                            )
                            await self.session.execute(stmt)
                            total_klines += 1
                        except Exception:
                            continue

                    progress.current = i + 1
                    progress.success_count += 1

                    # 每 50 只股票提交一次
                    if (i + 1) % 50 == 0:
                        await self.session.commit()

                except Exception as e:
                    progress.failed_count += 1
                    failed_symbols.append({
                        "symbol": symbol,
                        "error": str(e),
                    })

                    if failure_strategy == "stop":
                        raise
                    elif failure_strategy == "retry":
                        # 简单重试一次
                        try:
                            klines = await qmt_gateway.get_kline_daily(
                                symbol, start_date, end_date
                            )
                            if klines:
                                for kline in klines:
                                    stmt = insert(KlineDaily).values(
                                        symbol=kline.symbol,
                                        trade_date=kline.datetime,
                                        open=Decimal(str(kline.open)),
                                        high=Decimal(str(kline.high)),
                                        low=Decimal(str(kline.low)),
                                        close=Decimal(str(kline.close)),
                                        volume=kline.volume,
                                        amount=Decimal(str(kline.amount)),
                                    )
                                    stmt = stmt.on_conflict_do_update(
                                        index_elements=["symbol", "trade_date"],
                                        set_={
                                            "open": stmt.excluded.open,
                                            "high": stmt.excluded.high,
                                            "low": stmt.excluded.low,
                                            "close": stmt.excluded.close,
                                            "volume": stmt.excluded.volume,
                                            "amount": stmt.excluded.amount,
                                        },
                                    )
                                    await self.session.execute(stmt)
                                    total_klines += 1
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_symbols.pop()
                        except Exception:
                            pass

            # 最终提交
            await self.session.commit()

            # 更新进度
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["total_klines"] = total_klines
            progress.details["failed_symbols"] = failed_symbols[:100]

            # 记录日志
            await self.create_sync_log(
                sync_type="kline_daily",
                status="completed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()

        except Exception as e:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(e)

            # 记录失败日志
            await self.create_sync_log(
                sync_type="kline_daily",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(e),
                task_id=task_id,
            )
            await self.session.commit()
            raise

        finally:
            _current_sync = None

        return progress

    async def sync_kline_minute(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """
        同步分钟K线数据

        Args:
            symbols: 股票代码列表，为空则同步所有股票
            start_date: 起始日期，默认为今天
            end_date: 结束日期，默认为今天
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)

        Returns:
            SyncProgress: 同步进度
        """
        global _current_sync

        # 设置默认日期范围
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date

        # 初始化进度
        progress = SyncProgress(
            sync_type="kline_minute",
            status="running",
            start_time=datetime.now(),
            details={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        _current_sync = progress

        try:
            # 如果没有指定股票列表，获取所有股票
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)
            failed_symbols: list[dict[str, str]] = []
            total_klines = 0

            # 逐个股票同步K线
            for i, symbol in enumerate(symbols):
                try:
                    # 从 QMT 获取分钟K线数据
                    klines = await qmt_gateway.get_kline_minute(
                        symbol, start_date, end_date
                    )

                    if not klines:
                        progress.current = i + 1
                        continue

                    # 批量插入K线数据
                    from app.db.models import KlineMinute

                    for kline in klines:
                        try:
                            stmt = insert(KlineMinute).values(
                                symbol=kline.symbol,
                                datetime=kline.datetime,
                                open=Decimal(str(kline.open)),
                                high=Decimal(str(kline.high)),
                                low=Decimal(str(kline.low)),
                                close=Decimal(str(kline.close)),
                                volume=kline.volume,
                                amount=Decimal(str(kline.amount)),
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["symbol", "datetime"],
                                set_={
                                    "open": stmt.excluded.open,
                                    "high": stmt.excluded.high,
                                    "low": stmt.excluded.low,
                                    "close": stmt.excluded.close,
                                    "volume": stmt.excluded.volume,
                                    "amount": stmt.excluded.amount,
                                },
                            )
                            await self.session.execute(stmt)
                            total_klines += 1
                        except Exception:
                            continue

                    progress.current = i + 1
                    progress.success_count += 1

                    # 每 20 只股票提交一次
                    if (i + 1) % 20 == 0:
                        await self.session.commit()

                except Exception as e:
                    progress.failed_count += 1
                    failed_symbols.append({
                        "symbol": symbol,
                        "error": str(e),
                    })

                    if failure_strategy == "stop":
                        raise
                    elif failure_strategy == "retry":
                        # 简单重试一次
                        try:
                            klines = await qmt_gateway.get_kline_minute(
                                symbol, start_date, end_date
                            )
                            if klines:
                                for kline in klines:
                                    stmt = insert(KlineMinute).values(
                                        symbol=kline.symbol,
                                        datetime=kline.datetime,
                                        open=Decimal(str(kline.open)),
                                        high=Decimal(str(kline.high)),
                                        low=Decimal(str(kline.low)),
                                        close=Decimal(str(kline.close)),
                                        volume=kline.volume,
                                        amount=Decimal(str(kline.amount)),
                                    )
                                    stmt = stmt.on_conflict_do_update(
                                        index_elements=["symbol", "datetime"],
                                        set_={
                                            "open": stmt.excluded.open,
                                            "high": stmt.excluded.high,
                                            "low": stmt.excluded.low,
                                            "close": stmt.excluded.close,
                                            "volume": stmt.excluded.volume,
                                            "amount": stmt.excluded.amount,
                                        },
                                    )
                                    await self.session.execute(stmt)
                                    total_klines += 1
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_symbols.pop()
                        except Exception:
                            pass

            # 最终提交
            await self.session.commit()

            # 更新进度
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["total_klines"] = total_klines
            progress.details["failed_symbols"] = failed_symbols[:100]

            # 记录日志
            await self.create_sync_log(
                sync_type="kline_minute",
                status="completed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()

        except Exception as e:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(e)

            # 记录失败日志
            await self.create_sync_log(
                sync_type="kline_minute",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(e),
                task_id=task_id,
            )
            await self.session.commit()
            raise

        finally:
            _current_sync = None

        return progress

    async def get_sync_logs(
        self,
        sync_type: str | None = None,
        task_id: int | None = None,
        limit: int = 50,
    ) -> list[SyncLog]:
        """
        获取同步日志列表

        Args:
            sync_type: 同步类型过滤
            task_id: 任务ID过滤
            limit: 返回数量限制

        Returns:
            list[SyncLog]: 日志列表
        """
        query = select(SyncLog)

        if sync_type:
            query = query.where(SyncLog.sync_type == sync_type)
        if task_id:
            query = query.where(SyncLog.task_id == task_id)

        query = query.order_by(SyncLog.start_time.desc()).limit(limit)

        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_sync_log(self, log_id: int) -> SyncLog | None:
        """
        获取单个同步日志

        Args:
            log_id: 日志ID

        Returns:
            SyncLog | None: 日志记录
        """
        query = select(SyncLog).where(SyncLog.id == log_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def clear_sync_logs(self, before_days: int = 30) -> int:
        """
        清理旧的同步日志

        Args:
            before_days: 清理多少天前的日志

        Returns:
            int: 删除的记录数
        """
        cutoff_date = datetime.now() - timedelta(days=before_days)
        stmt = delete(SyncLog).where(SyncLog.created_at < cutoff_date)
        result = await self.session.execute(stmt)
        await self.session.commit()
        return result.rowcount

    async def cancel_sync(self) -> bool:
        """
        取消当前同步任务

        Returns:
            bool: 是否成功取消
        """
        global _current_sync

        if _current_sync is None:
            return False

        if _current_sync.status == "running":
            _current_sync.status = "cancelled"
            _current_sync.end_time = datetime.now()
            _current_sync.error_message = "用户取消"

            # 记录取消日志
            await self.create_sync_log(
                sync_type=_current_sync.sync_type,
                status="cancelled",
                total_count=_current_sync.total,
                success_count=_current_sync.success_count,
                failed_count=_current_sync.failed_count,
                start_time=_current_sync.start_time,
                end_time=_current_sync.end_time,
                error_message="用户取消",
                details=_current_sync.details,
            )
            await self.session.commit()

            _current_sync = None
            return True

        return False
