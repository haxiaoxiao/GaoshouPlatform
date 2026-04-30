# backend/app/services/sync_service.py
"""数据同步服务"""
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.clickhouse import get_ch_client
from app.db.models import Stock, SyncLog
from app.db.models.financial import FinancialData
from app.engines.qmt_gateway import qmt_gateway
from app.indicators.scheduler import indicator_scheduler


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
        full_sync: bool = False,
    ) -> SyncProgress:
        """
        同步股票基础信息

        Args:
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)
            full_sync: 是否全量同步(包括市值等需要实时数据的字段)

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
            progress.details = {"total_stocks": len(stocks), "full_sync": full_sync}

            failed_stocks: list[dict[str, str]] = []

            # 批量处理股票信息
            for i, stock in enumerate(stocks):
                try:
                    insert_data = {
                        "symbol": stock.symbol,
                        "name": stock.name,
                        "exchange": stock.exchange,
                        "industry": stock.industry,
                        "industry2": stock.industry2,
                        "industry3": stock.industry3,
                        "sector": stock.sector,
                        "concept": stock.concept,
                        "list_date": stock.list_date,
                        "delist_date": stock.delist_date,
                        "is_st": stock.is_st,
                        "is_delist": stock.is_delist,
                        "is_suspend": stock.is_suspend,
                        "product_class": stock.product_class,
                        "security_type": stock.security_type,
                        "total_shares": stock.total_shares,
                        "float_shares": stock.float_shares,
                        "total_mv": stock.total_mv,
                        "circ_mv": stock.circ_mv,
                        "updated_at": datetime.now(),
                    }

                    # 全量同步时包含更多字段
                    if full_sync:
                        insert_data.update({
                            "total_shares": stock.total_shares,
                            "float_shares": stock.float_shares,
                            "a_float_shares": stock.a_float_shares,
                            "limit_sell_shares": stock.limit_sell_shares,
                            "total_mv": stock.total_mv,
                            "circ_mv": stock.circ_mv,
                            "company_name": stock.company_name,
                            "province": stock.province,
                            "city": stock.city,
                            "office_addr": stock.office_addr,
                            "business_scope": stock.business_scope,
                            "main_business": stock.main_business,
                            "website": stock.website,
                            "employees": stock.employees,
                            "eps": stock.eps,
                            "bvps": stock.bvps,
                            "roe": stock.roe,
                            "pe_ttm": stock.pe_ttm,
                            "pb": stock.pb,
                            "total_assets": stock.total_assets,
                            "total_liability": stock.total_liability,
                            "total_equity": stock.total_equity,
                            "net_profit": stock.net_profit,
                            "revenue": stock.revenue,
                        })

                    # 使用 upsert 插入或更新
                    stmt = insert(Stock).values(**insert_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["symbol"],
                        set_={k: stmt.excluded[k] for k in insert_data.keys() if k != "symbol"},
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
                                sector=stock.sector,
                                list_date=stock.list_date,
                                is_st=stock.is_st,
                                updated_at=datetime.now(),
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["symbol"],
                                set_={
                                    "name": stmt.excluded.name,
                                    "exchange": stmt.excluded.exchange,
                                    "industry": stmt.excluded.industry,
                                    "sector": stmt.excluded.sector,
                                    "list_date": stmt.excluded.list_date,
                                    "is_st": stmt.excluded.is_st,
                                    "updated_at": stmt.excluded.updated_at,
                                },
                            )
                            await self.session.execute(stmt)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_stocks.pop()
                        except Exception:
                            pass

            # 最终提交
            await self.session.commit()

            # 触发指标计算
            synced_symbols = [s.symbol for s in stocks]
            indicator_scheduler.run_after_sync("stock_info", symbols=synced_symbols, trade_date=date.today())

            # 更新进度
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["failed_stocks"] = failed_stocks[:100]

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

    async def sync_stock_full(
        self,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """全量同步: 基础信息(已有) + 批量财务数据 + 批量市值"""
        global _current_sync

        progress = SyncProgress(
            sync_type="stock_full",
            status="running",
            start_time=datetime.now(),
        )
        _current_sync = progress

        try:
            # 阶段1: 用已有 stock_list 做基础 upsert (复用 sync_stock_info)
            stocks = await qmt_gateway.get_stock_list()
            progress.total = len(stocks)
            progress.details = {"total_stocks": len(stocks), "phase": "basic_info"}
            failed_stocks: list[dict[str, str]] = []

            for i, stock in enumerate(stocks):
                try:
                    insert_data = {
                        "symbol": stock.symbol,
                        "name": stock.name,
                        "exchange": stock.exchange,
                        "industry": stock.industry,
                        "industry2": stock.industry2,
                        "industry3": stock.industry3,
                        "sector": stock.sector,
                        "concept": stock.concept,
                        "list_date": stock.list_date,
                        "delist_date": stock.delist_date,
                        "is_st": stock.is_st,
                        "is_delist": stock.is_delist,
                        "is_suspend": stock.is_suspend,
                        "product_class": stock.product_class,
                        "security_type": stock.security_type,
                        "updated_at": datetime.now(),
                    }
                    stmt = insert(Stock).values(**insert_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["symbol"],
                        set_={k: stmt.excluded[k] for k in insert_data.keys() if k != "symbol"},
                    )
                    await self.session.execute(stmt)
                    progress.current = i + 1
                    progress.success_count += 1
                    if (i + 1) % 100 == 0:
                        await self.session.commit()
                except Exception as e:
                    progress.failed_count += 1
                    failed_stocks.append({"symbol": stock.symbol, "error": str(e)})

            await self.session.commit()

            # 阶段2: 批量获取市值
            progress.details["phase"] = "market_value"
            progress.current = 0
            progress.success_count = 0
            progress.failed_count = 0

            symbols = [s.symbol for s in stocks]
            quote_dict: dict[str, dict] = {}

            for batch_start in range(0, len(symbols), 200):
                batch = symbols[batch_start:batch_start + 200]
                try:
                    quotes = await qmt_gateway.get_realtime_quotes(batch)
                    for q in quotes:
                        quote_dict[q["symbol"]] = q
                except Exception:
                    pass

                for symbol in batch:
                    try:
                        q = quote_dict.get(symbol, {})
                        if q:
                            stmt = insert(Stock).values(
                                symbol=symbol,
                                total_mv=q.get("total_value"),
                                circ_mv=q.get("float_value"),
                                updated_at=datetime.now(),
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["symbol"],
                                set_={
                                    "total_mv": stmt.excluded.total_mv,
                                    "circ_mv": stmt.excluded.circ_mv,
                                    "updated_at": stmt.excluded.updated_at,
                                },
                            )
                            await self.session.execute(stmt)
                            progress.success_count += 1
                        progress.current += 1
                        if progress.current % 100 == 0:
                            await self.session.commit()
                    except Exception:
                        progress.failed_count += 1

            await self.session.commit()

            # 阶段3: 读取本地已缓存的财务数据(不触发download，用户需在QMT客户端手动下载)
            progress.details["phase"] = "financial_query"
            progress.current = 0
            progress.success_count = 0
            progress.failed_count = 0
            fin_success = 0

            for symbol in symbols:
                try:
                    quarters = await qmt_gateway.get_financial_quarters(symbol, report_count=8)
                    if not quarters:
                        progress.current += 1
                        continue

                    latest = quarters[0]
                    stock_update = {}
                    if latest.total_mv is not None:
                        stock_update["total_mv"] = latest.total_mv
                    if latest.circ_mv is not None:
                        stock_update["circ_mv"] = latest.circ_mv

                    mv = quote_dict.get(symbol, {}).get("total_value") or latest.total_mv
                    if mv and latest.net_profit and latest.net_profit != 0:
                        latest.pe_ttm = round(mv / latest.net_profit, 4)
                        stock_update["pe_ttm"] = latest.pe_ttm
                    if mv and latest.total_equity and latest.total_equity != 0:
                        latest.pb = round(mv / latest.total_equity, 4)
                        stock_update["pb"] = latest.pb

                    for fq in quarters:
                        fin_data = {
                            "symbol": fq.symbol,
                            "report_date": fq.report_date,
                            "report_type": fq.report_type,
                            "eps": fq.eps,
                            "bvps": fq.bvps,
                            "roe": fq.roe,
                            "revenue": fq.revenue,
                            "net_profit": fq.net_profit,
                            "revenue_yoy": fq.revenue_yoy,
                            "profit_yoy": fq.profit_yoy,
                            "gross_margin": fq.gross_margin,
                            "total_assets": fq.total_assets,
                            "total_liability": fq.total_liability,
                            "total_equity": fq.total_equity,
                            "total_shares": fq.total_shares,
                            "float_shares": fq.float_shares,
                            "a_float_shares": fq.a_float_shares,
                            "limit_sell_shares": fq.limit_sell_shares,
                            "total_mv": fq.total_mv,
                            "circ_mv": fq.circ_mv,
                            "pe_ttm": fq.pe_ttm,
                            "pb": fq.pb,
                            "raw_data": fq.raw_data,
                            "updated_at": datetime.now(),
                        }
                        fin_stmt = insert(FinancialData).values(**fin_data)
                        fin_stmt = fin_stmt.on_conflict_do_update(
                            index_elements=["symbol", "report_date"],
                            set_={k: fin_stmt.excluded[k] for k in fin_data.keys() if k not in ("symbol", "report_date")},
                        )
                        await self.session.execute(fin_stmt)

                    if stock_update:
                        stock_update["updated_at"] = datetime.now()
                        stmt = insert(Stock).values(symbol=symbol, **stock_update)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["symbol"],
                            set_={k: stmt.excluded[k] for k in stock_update.keys()},
                        )
                        await self.session.execute(stmt)

                    fin_success += 1
                    progress.success_count += 1
                except Exception as e:
                    progress.failed_count += 1
                    failed_stocks.append({"symbol": symbol, "error": str(e)})

                progress.current += 1
                if progress.current % 50 == 0:
                    await self.session.commit()

            await self.session.commit()

            try:
                cleaned = qmt_gateway.clean_local_cache(symbols=symbols, data_type="all")
                progress.details["cache_cleaned"] = cleaned
            except Exception:
                pass

            indicator_scheduler.run_after_sync("stock_full", symbols=symbols, trade_date=date.today())

            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["failed_stocks"] = failed_stocks[:100]
            progress.details["fin_success"] = fin_success

            await self.create_sync_log(
                sync_type="stock_full",
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
            await self.create_sync_log(
                sync_type="stock_full",
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

    async def sync_financial_data(
        self,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        global _current_sync

        progress = SyncProgress(
            sync_type="financial_data",
            status="running",
            start_time=datetime.now(),
        )
        _current_sync = progress

        try:
            from app.engines.qmt_gateway import qmt_gateway as gw
            xt = gw._get_xt()
            loop = asyncio.get_running_loop()

            stocks = await qmt_gateway.get_stock_list()
            if not stocks:
                raise RuntimeError("QMT返回股票列表为空，请确认QMT客户端是否在线")
            symbols = [s.symbol for s in stocks]
            progress.total = len(symbols)
            progress.details = {"total_stocks": len(symbols), "phase": "download"}
            failed_stocks: list[dict[str, str]] = []

            tables = ["PershareIndex", "Balance", "Income", "Capital"]
            download_results: dict[str, dict] = {}

            batch_size = 200
            total_batches = (len(symbols) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                batch = symbols[batch_idx * batch_size: (batch_idx + 1) * batch_size]
                progress.details["download_batch"] = f"{batch_idx + 1}/{total_batches}"

                try:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.info(f"Downloading financial data batch {batch_idx+1}/{total_batches} ({len(batch)} stocks)")
                    await asyncio.wait_for(
                        loop.run_in_executor(
                            None,
                            lambda b=batch: xt.download_financial_data2(
                                b, tables
                            ),
                        ),
                        timeout=300,
                    )
                except asyncio.TimeoutError:
                    progress.details["download_timeout"] = progress.details.get("download_timeout", 0) + 1
                except Exception as e:
                    progress.details["download_error"] = str(e)[:200]

                try:
                    fin_data = await loop.run_in_executor(
                        None,
                        lambda b=batch: xt.get_financial_data(
                            b, tables, start_time="20200101"
                        ),
                    )
                    download_results.update(fin_data)
                except Exception as e:
                    progress.details["get_data_error"] = str(e)[:200]

                progress.current = min((batch_idx + 1) * batch_size, len(symbols))
                if (batch_idx + 1) % 3 == 0:
                    await asyncio.sleep(0)

            progress.details["phase"] = "parse"
            progress.current = 0
            progress.success_count = 0
            progress.failed_count = 0
            fin_success = 0

            quote_dict: dict[str, dict] = {}
            try:
                for batch_start in range(0, len(symbols), 200):
                    batch = symbols[batch_start:batch_start + 200]
                    quotes = await qmt_gateway.get_realtime_quotes(batch)
                    for q in quotes:
                        quote_dict[q["symbol"]] = q
            except Exception:
                pass

            for symbol in symbols:
                try:
                    if symbol not in download_results:
                        progress.current += 1
                        continue

                    tables_raw = download_results[symbol]
                    quarters = qmt_gateway._parse_financial_dataframes(
                        symbol, tables_raw, report_count=8,
                    )
                    if not quarters:
                        progress.current += 1
                        continue

                    latest = quarters[0]
                    stock_update = {}
                    mv = quote_dict.get(symbol, {}).get("total_value") or latest.total_mv
                    if mv and latest.net_profit and latest.net_profit != 0:
                        stock_update["pe_ttm"] = round(mv / latest.net_profit, 4)
                    if mv and latest.total_equity and latest.total_equity != 0:
                        stock_update["pb"] = round(mv / latest.total_equity, 4)
                    stock_update["roe"] = latest.roe
                    stock_update["eps"] = latest.eps
                    stock_update["bvps"] = latest.bvps
                    stock_update["revenue"] = latest.revenue
                    stock_update["net_profit"] = latest.net_profit
                    stock_update["total_assets"] = latest.total_assets
                    stock_update["total_liability"] = latest.total_liability
                    stock_update["total_equity"] = latest.total_equity
                    if latest.total_shares is not None:
                        stock_update["total_shares"] = latest.total_shares
                    if latest.float_shares is not None:
                        stock_update["float_shares"] = latest.float_shares
                    stock_update = {k: v for k, v in stock_update.items() if v is not None}

                    for fq in quarters:
                        fin_data = {
                            "symbol": fq.symbol, "report_date": fq.report_date,
                            "report_type": fq.report_type,
                            "eps": fq.eps, "bvps": fq.bvps, "roe": fq.roe,
                            "revenue": fq.revenue, "net_profit": fq.net_profit,
                            "revenue_yoy": fq.revenue_yoy, "profit_yoy": fq.profit_yoy,
                            "gross_margin": fq.gross_margin,
                            "total_assets": fq.total_assets, "total_liability": fq.total_liability,
                            "total_equity": fq.total_equity,
                            "total_shares": fq.total_shares, "float_shares": fq.float_shares,
                            "a_float_shares": fq.a_float_shares,
                            "limit_sell_shares": fq.limit_sell_shares,
                            "total_mv": fq.total_mv, "circ_mv": fq.circ_mv,
                            "pe_ttm": fq.pe_ttm, "pb": fq.pb,
                            "raw_data": fq.raw_data, "updated_at": datetime.now(),
                        }
                        fin_stmt = insert(FinancialData).values(**fin_data)
                        fin_stmt = fin_stmt.on_conflict_do_update(
                            index_elements=["symbol", "report_date"],
                            set_={k: fin_stmt.excluded[k] for k in fin_data.keys() if k not in ("symbol", "report_date")},
                        )
                        await self.session.execute(fin_stmt)

                    if stock_update:
                        stock_update["updated_at"] = datetime.now()
                        stmt = insert(Stock).values(symbol=symbol, **stock_update)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["symbol"],
                            set_={k: stmt.excluded[k] for k in stock_update.keys()},
                        )
                        await self.session.execute(stmt)

                    fin_success += 1
                    progress.success_count += 1
                except Exception as e:
                    progress.failed_count += 1
                    failed_stocks.append({"symbol": symbol, "error": str(e)})
                    if failure_strategy == "stop":
                        raise

                progress.current += 1
                if progress.current % 50 == 0:
                    await self.session.commit()

            await self.session.commit()

            try:
                cleaned = qmt_gateway.clean_local_cache(symbols=symbols, data_type="financial")
                progress.details["cache_cleaned"] = cleaned
            except Exception:
                pass

            indicator_scheduler.run_after_sync("stock_full", symbols=symbols, trade_date=date.today())

            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["fin_success"] = fin_success
            progress.details["failed_stocks"] = failed_stocks[:100]

            await self.create_sync_log(
                sync_type="financial_data",
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
            progress.details["error"] = str(e)[:500]
            progress.details["error_type"] = type(e).__name__
            import logging
            logging.getLogger(__name__).exception(f"sync_financial_data failed: {e}")
            await self.create_sync_log(
                sync_type="financial_data",
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
        full_sync: bool = False,
    ) -> SyncProgress:
        """
        同步日K线数据

        Args:
            symbols: 股票代码列表，为空则同步所有股票
            start_date: 起始日期，默认为最近30天
            end_date: 结束日期，默认为今天
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)
            full_sync: 是否全量同步(True=先删除已有数据，False=增量追加)

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
                "full_sync": full_sync,
            },
        )
        _current_sync = progress

        # 获取 ClickHouse 客户端
        ch_client = get_ch_client()

        try:
            # 如果没有指定股票列表，获取所有股票
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)

            # 全量同步时先删除已有数据
            if full_sync and symbols:
                progress.details["message"] = "正在删除已有数据..."
                for symbol in symbols:
                    try:
                        ch_client.execute(
                            "DELETE FROM klines_daily WHERE symbol = %(symbol)s "
                            "AND trade_date >= %(start_date)s AND trade_date <= %(end_date)s",
                            {"symbol": symbol, "start_date": start_date, "end_date": end_date},
                        )
                    except Exception:
                        pass
                progress.details["message"] = "删除完成，开始同步..."

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

                    # 批量插入K线数据到 ClickHouse
                    # ClickHouse 驱动需要 Python date 对象
                    rows = [
                        {
                            "symbol": kline.symbol,
                            "trade_date": kline.datetime if isinstance(kline.datetime, date) else date.fromisoformat(str(kline.datetime)),
                            "open": kline.open,
                            "high": kline.high,
                            "low": kline.low,
                            "close": kline.close,
                            "volume": kline.volume,
                            "amount": kline.amount,
                        }
                        for kline in klines
                    ]

                    if rows:
                        ch_client.execute(
                            "INSERT INTO klines_daily "
                            "(symbol, trade_date, open, high, low, close, volume, amount) "
                            "VALUES",
                            rows,
                        )
                        total_klines += len(rows)

                    progress.current = i + 1
                    progress.success_count += 1

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
                                rows = [
                                    {
                                        "symbol": kline.symbol,
                                        "trade_date": kline.datetime if isinstance(kline.datetime, date) else date.fromisoformat(str(kline.datetime)),
                                        "open": kline.open,
                                        "high": kline.high,
                                        "low": kline.low,
                                        "close": kline.close,
                                        "volume": kline.volume,
                                        "amount": kline.amount,
                                    }
                                    for kline in klines
                                ]
                                if rows:
                                    ch_client.execute(
                                        "INSERT INTO klines_daily "
                                        "(symbol, trade_date, open, high, low, close, volume, amount) "
                                        "VALUES",
                                        rows,
                                    )
                                    total_klines += len(rows)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_symbols.pop()
                        except Exception:
                            pass

            # 更新进度
            try:
                cleaned = qmt_gateway.clean_local_cache(symbols=symbols, data_type="kline")
                progress.details["cache_cleaned"] = cleaned
            except Exception:
                pass

            indicator_scheduler.run_after_sync("kline_daily", symbols=symbols, trade_date=end_date)
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
            progress.details["error"] = str(e)[:500]
            progress.details["error_type"] = type(e).__name__
            import logging
            logging.getLogger(__name__).exception(f"sync_kline_daily failed: {e}")
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
        full_sync: bool = False,
    ) -> SyncProgress:
        """
        同步分钟K线数据

        Args:
            symbols: 股票代码列表，为空则同步所有股票
            start_date: 起始日期，默认为今天
            end_date: 结束日期，默认为今天
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)
            full_sync: 是否全量同步(True=先删除已有数据，False=增量追加)

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
                "full_sync": full_sync,
            },
        )
        _current_sync = progress

        # 获取 ClickHouse 客户端
        ch_client = get_ch_client()

        try:
            # 如果没有指定股票列表，获取所有股票
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)

            # 全量同步时先删除已有数据
            if full_sync and symbols:
                progress.details["message"] = "正在删除已有数据..."
                for symbol in symbols:
                    try:
                        ch_client.execute(
                            "DELETE FROM klines_minute WHERE symbol = %(symbol)s "
                            "AND toDate(datetime) >= %(start_date)s AND toDate(datetime) <= %(end_date)s",
                            {"symbol": symbol, "start_date": start_date, "end_date": end_date},
                        )
                    except Exception:
                        pass
                progress.details["message"] = "删除完成，开始同步..."

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

                    # 批量插入K线数据到 ClickHouse
                    # ClickHouse 驱动需要 Python datetime 对象
                    rows = [
                        {
                            "symbol": kline.symbol,
                            "datetime": kline.datetime if isinstance(kline.datetime, datetime) else datetime.fromisoformat(str(kline.datetime)),
                            "open": kline.open,
                            "high": kline.high,
                            "low": kline.low,
                            "close": kline.close,
                            "volume": kline.volume,
                            "amount": kline.amount,
                        }
                        for kline in klines
                    ]

                    if rows:
                        ch_client.execute(
                            "INSERT INTO klines_minute "
                            "(symbol, datetime, open, high, low, close, volume, amount) "
                            "VALUES",
                            rows,
                        )
                        total_klines += len(rows)

                    progress.current = i + 1
                    progress.success_count += 1

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
                                rows = [
                                    {
                                        "symbol": kline.symbol,
                                        "datetime": kline.datetime if isinstance(kline.datetime, datetime) else datetime.fromisoformat(str(kline.datetime)),
                                        "open": kline.open,
                                        "high": kline.high,
                                        "low": kline.low,
                                        "close": kline.close,
                                        "volume": kline.volume,
                                        "amount": kline.amount,
                                    }
                                    for kline in klines
                                ]
                                if rows:
                                    ch_client.execute(
                                        "INSERT INTO klines_minute "
                                        "(symbol, datetime, open, high, low, close, volume, amount) "
                                        "VALUES",
                                        rows,
                                    )
                                    total_klines += len(rows)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_symbols.pop()
                        except Exception:
                            pass

            # 更新进度
            try:
                cleaned = qmt_gateway.clean_local_cache(symbols=symbols, data_type="kline")
                progress.details["cache_cleaned"] = cleaned
            except Exception:
                pass

            indicator_scheduler.run_after_sync("kline_minute", symbols=symbols, trade_date=end_date)
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

    async def sync_realtime_mv(
        self,
        symbols: list[str] | None = None,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """
        同步实时市值数据

        Args:
            symbols: 股票代码列表，为空则同步所有股票
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)

        Returns:
            SyncProgress: 同步进度
        """
        global _current_sync

        # 初始化进度
        progress = SyncProgress(
            sync_type="realtime_mv",
            status="running",
            start_time=datetime.now(),
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

            # 批量获取实时行情
            quotes = await qmt_gateway.get_realtime_quotes(symbols)

            # 构建行情字典
            quote_dict = {q["symbol"]: q for q in quotes}

            # 更新市值数据
            for i, symbol in enumerate(symbols):
                try:
                    quote = quote_dict.get(symbol)
                    if quote:
                        # 更新市值
                        stmt = insert(Stock).values(
                            symbol=symbol,
                            total_mv=quote.get("total_value"),
                            circ_mv=quote.get("float_value"),
                            updated_at=datetime.now(),
                        )
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["symbol"],
                            set_={
                                "total_mv": stmt.excluded.total_mv,
                                "circ_mv": stmt.excluded.circ_mv,
                                "updated_at": stmt.excluded.updated_at,
                            },
                        )
                        await self.session.execute(stmt)
                        progress.success_count += 1

                    progress.current = i + 1

                    # 每 100 条提交一次
                    if (i + 1) % 100 == 0:
                        await self.session.commit()

                except Exception as e:
                    progress.failed_count += 1
                    failed_symbols.append({
                        "symbol": symbol,
                        "error": str(e),
                    })

                    if failure_strategy == "stop":
                        raise

            # 最终提交
            await self.session.commit()

            # 更新进度
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["failed_symbols"] = failed_symbols[:100]

            # 记录日志
            await self.create_sync_log(
                sync_type="realtime_mv",
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
                sync_type="realtime_mv",
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
