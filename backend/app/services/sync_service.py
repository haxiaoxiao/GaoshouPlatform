# backend/app/services/sync_service.py
"""数据同步服务"""
import asyncio
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta as td
from pathlib import Path
from typing import Any

from loguru import logger

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.retry import async_retry
from app.db.clickhouse import get_ch_client
from app.data_stores import get_market_data_store
from app.core.config import settings as app_settings
from app.db.models import Stock, SyncLog
from app.db.models.financial import FinancialData
from app.engines.qmt_gateway import qmt_gateway
from app.indicators.scheduler import indicator_scheduler
from app.services.index_catalog import IndexCatalogItem, get_index_item, list_index_items
from app.services.sync_run_store import get_current_sync_run, run_to_status, upsert_sync_run


QMT_MINUTE_BATCH_SIZE = int(os.getenv("QMT_MINUTE_BATCH_SIZE", "100"))
QMT_DAILY_BATCH_SIZE = int(os.getenv("QMT_DAILY_BATCH_SIZE", "50"))
QMT_WEEKLY_BATCH_SIZE = int(os.getenv("QMT_WEEKLY_BATCH_SIZE", "100"))
QMT_FINANCIAL_BATCH_SIZE = int(os.getenv("QMT_FINANCIAL_BATCH_SIZE", "200"))
QMT_DIVIDEND_BATCH_SIZE = int(os.getenv("QMT_DIVIDEND_BATCH_SIZE", "100"))
QMT_STOCK_INFO_TIMEOUT_SECONDS = int(os.getenv("QMT_STOCK_INFO_TIMEOUT_SECONDS", "45"))
QMT_STOCK_INFO_COMMIT_BATCH_SIZE = int(os.getenv("QMT_STOCK_INFO_COMMIT_BATCH_SIZE", "50"))
QMT_STOCK_FULL_MARKET_BATCH_SIZE = int(os.getenv("QMT_STOCK_FULL_MARKET_BATCH_SIZE", "50"))
SYNC_STOCK_INFO_COMPUTE_INDICATORS = os.getenv("SYNC_STOCK_INFO_COMPUTE_INDICATORS", "false").lower() in {"1", "true", "yes"}
DATASYNC_INITIAL_DAILY_DAYS = int(os.getenv("DATASYNC_INITIAL_DAILY_DAYS", "30"))
DATASYNC_INITIAL_INDEX_DAILY_DAYS = int(os.getenv("DATASYNC_INITIAL_INDEX_DAILY_DAYS", "30"))
DATASYNC_INITIAL_MINUTE_DAYS = int(os.getenv("DATASYNC_INITIAL_MINUTE_DAYS", "7"))
TUSHARE_INDEX_DAILY_PAUSE_SECONDS = float(os.getenv("TUSHARE_INDEX_DAILY_PAUSE_SECONDS", "0.2"))
TUSHARE_SW_DAILY_PAUSE_SECONDS = float(os.getenv("TUSHARE_SW_DAILY_PAUSE_SECONDS", "61"))


def _should_write_clickhouse() -> bool:
    return app_settings.clickhouse_enabled or app_settings.market_data_backend == "clickhouse"


def _write_ch_daily(ch_client: Any, rows: list[dict[str, Any]]) -> None:
    if not rows or ch_client is None:
        return
    ch_client.execute(
        "INSERT INTO klines_daily "
        "(symbol, trade_date, open, high, low, close, volume, amount) "
        "VALUES",
        rows,
    )


def _write_ch_minute(ch_client: Any, rows: list[dict[str, Any]]) -> None:
    if not rows or ch_client is None:
        return
    ch_client.execute(
        "INSERT INTO klines_minute "
        "(symbol, datetime, open, high, low, close, volume, amount) "
        "VALUES",
        rows,
    )


def _write_store_daily(rows: list[dict[str, Any]]) -> None:
    if not rows or app_settings.market_data_backend != "parquet":
        return
    import pandas as pd

    store = get_market_data_store()
    store.write_daily(pd.DataFrame(rows))


def _write_store_minute(rows: list[dict[str, Any]], *, dataset: str = "klines_minute") -> None:
    if not rows or app_settings.market_data_backend != "parquet":
        return
    import pandas as pd

    store = get_market_data_store()
    store.write_minute(pd.DataFrame(rows), dataset=dataset)


def _latest_parquet_date(dataset: str, date_column: str) -> date | None:
    from app.db.duckdb import get_duckdb

    root = Path(app_settings.parquet_data_dir) / dataset
    if not root.exists() or not any(".tmp-" not in str(file) for file in root.rglob("*.parquet")):
        return None

    pattern = str(root / "year=*" / "month=??" / "*.parquet")
    if not any(root.glob("year=*/month=??/*.parquet")):
        pattern = str(root / "**" / "*.parquet")
    pattern = pattern.replace("\\", "/")

    row = get_duckdb().execute(
        f"SELECT max({date_column}) FROM read_parquet(?, hive_partitioning=true)",
        [pattern],
    ).fetchone()
    value = row[0] if row else None
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _latest_clickhouse_date(table: str, date_column: str) -> date | None:
    ch_client = get_ch_client()
    try:
        row = ch_client.execute(f"SELECT max({date_column}) FROM {table}")
    finally:
        try:
            ch_client.disconnect()
        except Exception:
            pass

    value = row[0][0] if row and row[0] else None
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _latest_market_date(dataset: str) -> date | None:
    date_column = "datetime" if "minute" in dataset else "trade_date"
    if app_settings.market_data_backend == "parquet":
        return _latest_parquet_date(dataset, date_column)
    table = "klines_minute" if "minute" in dataset else "klines_daily"
    return _latest_clickhouse_date(table, date_column)


def _next_sync_start(latest_date: date | None, end_date: date, initial_days: int) -> date:
    if latest_date is None:
        return end_date - td(days=initial_days)
    return latest_date + td(days=1)


def _tushare_token() -> str | None:
    import tushare as ts

    return os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or ts.get_token()


def _is_tushare_rate_limit_error(error: Exception | str) -> bool:
    text = str(error)
    markers = (
        "频率超限",
        "rate limit",
        "too many requests",
        "1次/小时",
    )
    return any(marker in text.lower() if marker.isascii() else marker in text for marker in markers)


def _exchange_from_symbol(symbol: str) -> str:
    upper = symbol.upper()
    if upper.endswith(".SH"):
        return "SH"
    if upper.endswith(".SZ"):
        return "SZ"
    if upper.endswith(".SI"):
        return "SI"
    if upper.endswith(".CSI"):
        return "CSI"
    return ""


def _normalize_index_daily_rows(
    item: IndexCatalogItem,
    dataframe: Any,
) -> list[dict[str, Any]]:
    import pandas as pd

    if dataframe is None or dataframe.empty:
        return []

    frame = dataframe.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d", errors="coerce").dt.date
    frame = frame[frame["trade_date"].notna()]
    if frame.empty:
        return []
    frame.sort_values("trade_date", inplace=True)

    rows: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        volume = getattr(row, "vol", None)
        amount = getattr(row, "amount", None)
        rows.append(
            {
                "symbol": item.symbol,
                "trade_date": getattr(row, "trade_date"),
                "open": float(getattr(row, "open")) if getattr(row, "open", None) is not None else None,
                "high": float(getattr(row, "high")) if getattr(row, "high", None) is not None else None,
                "low": float(getattr(row, "low")) if getattr(row, "low", None) is not None else None,
                "close": float(getattr(row, "close")) if getattr(row, "close", None) is not None else None,
                "volume": float(volume) * 100 if volume is not None else None,
                "amount": float(amount) * 1000 if amount is not None else None,
            }
        )
    return rows


def _fetch_tushare_index_daily_rows(
    item: IndexCatalogItem,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    import tushare as ts

    token = _tushare_token()
    if not token:
        raise RuntimeError("Tushare token is not configured")
    ts.set_token(token)
    pro = ts.pro_api()

    params = {
        "ts_code": item.provider_symbol,
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date": end_date.strftime("%Y%m%d"),
    }
    if item.provider == "tushare.sw_daily":
        dataframe = pro.sw_daily(**params)
    elif item.provider == "tushare.index_daily":
        dataframe = pro.index_daily(**params)
    else:
        raise RuntimeError(f"Unsupported index provider: {item.provider}")

    return _normalize_index_daily_rows(item, dataframe)


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


def _sync_cancelled(progress: SyncProgress) -> bool:
    return _current_sync is not progress or progress.status == "cancelled"


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

    async def get_persisted_sync_status(self) -> dict[str, Any] | None:
        run = await get_current_sync_run(self.session)
        return run_to_status(run) if run is not None else None

    async def persist_sync_progress(
        self,
        progress: SyncProgress,
        *,
        run_id: str | None = None,
        request: dict[str, Any] | None = None,
        sync_task_id: int | None = None,
        commit: bool = True,
    ) -> None:
        target_run_id = run_id or str(progress.details.get("run_id") or "")
        if not target_run_id:
            return
        await upsert_sync_run(
            self.session,
            run_id=target_run_id,
            sync_type=progress.sync_type,
            status=progress.status,
            total=progress.total,
            current=progress.current,
            success_count=progress.success_count,
            failed_count=progress.failed_count,
            progress_percent=progress.progress_percent,
            start_time=progress.start_time,
            end_time=progress.end_time,
            error_message=progress.error_message,
            request=request,
            details=progress.details,
            sync_task_id=sync_task_id,
            commit=commit,
        )

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

    async def _upsert_index_catalog_entries(self, items: list[IndexCatalogItem]) -> None:
        if not items:
            return

        rows: list[dict[str, Any]] = []
        for item in items:
            list_date = None
            if item.available_from:
                try:
                    list_date = date.fromisoformat(item.available_from)
                except ValueError:
                    list_date = None
            rows.append(
                {
                    "symbol": item.symbol,
                    "name": item.display_name,
                    "exchange": _exchange_from_symbol(item.symbol),
                    "sector": "指数",
                    "industry": "指数",
                    "is_st": 0,
                    "is_delist": 0,
                    "is_suspend": 0,
                    "list_date": list_date,
                    "security_type": "index",
                    "product_class": item.market_family,
                    "updated_at": datetime.now(),
                }
            )

        stmt = insert(Stock).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol"],
            set_={
                "name": stmt.excluded.name,
                "exchange": stmt.excluded.exchange,
                "sector": stmt.excluded.sector,
                "industry": stmt.excluded.industry,
                "list_date": stmt.excluded.list_date,
                "security_type": stmt.excluded.security_type,
                "product_class": stmt.excluded.product_class,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await self.session.execute(stmt)

    async def build_datasync_plan(self, end_date: date | None = None) -> dict[str, Any]:
        target_end = end_date or date.today()

        latest_daily = _latest_market_date("klines_daily")
        latest_minute = _latest_market_date("klines_minute")
        daily_start = _next_sync_start(latest_daily, target_end, DATASYNC_INITIAL_DAILY_DAYS)
        index_daily_start = target_end - td(days=DATASYNC_INITIAL_INDEX_DAILY_DAYS)
        minute_start = _next_sync_start(latest_minute, target_end, DATASYNC_INITIAL_MINUTE_DAYS)

        plan = {
            "end_date": target_end.isoformat(),
            "market_data_backend": app_settings.market_data_backend,
            "latest": {
                "kline_daily": latest_daily.isoformat() if latest_daily else None,
                "kline_minute": latest_minute.isoformat() if latest_minute else None,
            },
            "ranges": {
                "kline_daily": {
                    "start_date": daily_start.isoformat(),
                    "end_date": target_end.isoformat(),
                    "will_sync": daily_start <= target_end,
                },
                "index_daily": {
                    "start_date": index_daily_start.isoformat(),
                    "end_date": target_end.isoformat(),
                    "will_sync": index_daily_start <= target_end,
                },
                "kline_minute": {
                    "start_date": minute_start.isoformat(),
                    "end_date": target_end.isoformat(),
                    "will_sync": minute_start <= target_end,
                },
            },
            "steps": ["stock_info", "stock_full", "financial_data", "kline_daily", "index_daily", "kline_minute", "realtime_mv"],
        }
        if _should_write_clickhouse():
            plan["steps"].append("dividends")
        else:
            plan["skipped"] = {
                "dividends": "requires ClickHouse stock_indicators",
            }
        return plan

    async def sync_datasync(
        self,
        symbols: list[str] | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        run_id: str | None = None,
        failure_strategy: str = "skip",
        full_sync: bool = False,
    ) -> SyncProgress:
        global _current_sync

        plan = await self.build_datasync_plan(end_date=end_date)
        progress = SyncProgress(
            sync_type="datasync",
            status="running",
            total=len(plan["steps"]),
            start_time=datetime.now(),
            details={"run_id": run_id, "plan": plan, "step_results": []},
        )
        _current_sync = progress
        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

        try:
            for step in plan["steps"]:
                if _sync_cancelled(progress):
                    progress.status = "cancelled"
                    progress.end_time = datetime.now()
                    progress.error_message = "User cancelled"
                    return progress

                progress.details["current_step"] = step
                await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

                if step == "stock_info":
                    step_progress = await self.sync_stock_info(
                        task_id=task_id,
                        failure_strategy=failure_strategy,
                        full_sync=full_sync,
                    )
                elif step == "stock_full":
                    step_progress = await self.sync_stock_full(
                        task_id=task_id,
                        run_id=run_id,
                        failure_strategy=failure_strategy,
                    )
                elif step == "financial_data":
                    step_progress = await self.sync_financial_data(
                        task_id=task_id,
                        failure_strategy=failure_strategy,
                    )
                elif step == "kline_daily":
                    daily_range = plan["ranges"]["kline_daily"]
                    if not daily_range["will_sync"]:
                        step_progress = SyncProgress(sync_type=step, status="completed")
                        step_progress.details = {"skipped": "already up to date", **daily_range}
                    else:
                        step_progress = await self.sync_kline_daily(
                            symbols=symbols,
                            start_date=date.fromisoformat(daily_range["start_date"]),
                            end_date=date.fromisoformat(daily_range["end_date"]),
                            task_id=task_id,
                            run_id=run_id,
                            failure_strategy=failure_strategy,
                            full_sync=full_sync,
                        )
                elif step == "index_daily":
                    index_daily_range = plan["ranges"]["index_daily"]
                    if not index_daily_range["will_sync"]:
                        step_progress = SyncProgress(sync_type=step, status="completed")
                        step_progress.details = {"skipped": "already up to date", **index_daily_range}
                    else:
                        step_progress = await self.sync_index_daily(
                            start_date=date.fromisoformat(index_daily_range["start_date"]),
                            end_date=date.fromisoformat(index_daily_range["end_date"]),
                            task_id=task_id,
                            run_id=run_id,
                            failure_strategy=failure_strategy,
                            full_sync=full_sync,
                        )
                elif step == "kline_minute":
                    minute_range = plan["ranges"]["kline_minute"]
                    if not minute_range["will_sync"]:
                        step_progress = SyncProgress(sync_type=step, status="completed")
                        step_progress.details = {"skipped": "already up to date", **minute_range}
                    else:
                        step_progress = await self.sync_kline_minute(
                            symbols=symbols,
                            start_date=date.fromisoformat(minute_range["start_date"]),
                            end_date=date.fromisoformat(minute_range["end_date"]),
                            task_id=task_id,
                            run_id=run_id,
                            failure_strategy=failure_strategy,
                            full_sync=full_sync,
                        )
                elif step == "realtime_mv":
                    step_progress = await self.sync_realtime_mv(
                        symbols=symbols,
                        task_id=task_id,
                        failure_strategy=failure_strategy,
                    )
                elif step == "dividends":
                    step_progress = await self.sync_dividends(
                        symbols=symbols,
                        end_date=date.fromisoformat(plan["end_date"]),
                        task_id=task_id,
                        failure_strategy=failure_strategy,
                    )
                else:
                    continue

                progress.details["step_results"].append({
                    "sync_type": step_progress.sync_type,
                    "status": step_progress.status,
                    "total": step_progress.total,
                    "success_count": step_progress.success_count,
                    "failed_count": step_progress.failed_count,
                    "error_message": step_progress.error_message,
                    "details": step_progress.details,
                })
                progress.current += 1
                progress.success_count += step_progress.success_count
                progress.failed_count += step_progress.failed_count

                if step_progress.status == "failed" and failure_strategy == "stop":
                    raise RuntimeError(step_progress.error_message or f"{step} failed")

                _current_sync = progress
                await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

            progress.status = "completed"
            progress.end_time = datetime.now()
            await self.create_sync_log(
                sync_type="datasync",
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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
        except Exception as exc:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(exc)
            await self.create_sync_log(
                sync_type="datasync",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(exc),
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
            raise
        finally:
            _current_sync = None

        return progress

    async def sync_factor_dependency(
        self,
        plan: dict[str, Any] | None,
        task_id: int | None = None,
        run_id: str | None = None,
        failure_strategy: str = "stop",
    ) -> SyncProgress:
        """Sync data dependencies required by factor precompute."""
        global _current_sync

        from app.services.factor_dependency_sync import execute_factor_dependency_sync

        if not plan or not isinstance(plan, dict):
            raise ValueError("factor_sync_plan is required")
        steps = [step for step in plan.get("steps") or [] if isinstance(step, dict)]
        progress = SyncProgress(
            sync_type="factor_dependency",
            status="running",
            total=len(steps),
            start_time=datetime.now(),
            details={"run_id": run_id, "plan": plan, "step_results": []},
        )
        _current_sync = progress
        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

        try:
            results = await execute_factor_dependency_sync(
                self,
                plan,
                run_id=run_id,
                task_id=task_id,
                failure_strategy=failure_strategy,
                progress=progress,
            )
            progress.details["step_results"] = results
            progress.status = "completed"
            progress.current = progress.total
            progress.end_time = datetime.now()
            await self.create_sync_log(
                sync_type="factor_dependency",
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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
        except Exception as exc:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(exc)
            await self.create_sync_log(
                sync_type="factor_dependency",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(exc),
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
            raise
        finally:
            _current_sync = None

        return progress

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
            stocks = await asyncio.wait_for(
                qmt_gateway.get_stock_list(),
                timeout=QMT_STOCK_INFO_TIMEOUT_SECONDS,
            )
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
                    if (i + 1) % QMT_STOCK_INFO_COMMIT_BATCH_SIZE == 0:
                        await self.session.commit()
                        await self.persist_sync_progress(progress, commit=True)
                        await asyncio.sleep(0)

                except Exception as e:
                    progress.failed_count += 1
                    failed_stocks.append({
                        "symbol": stock.symbol,
                        "error": str(e),
                    })

                    if failure_strategy == "stop":
                        raise
                    elif failure_strategy == "retry":
                        async def _retry_stock_insert():
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

                        try:
                            await async_retry(_retry_stock_insert, max_retries=3, base_delay=1.0)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_stocks.pop()
                        except Exception:
                            pass

            # 最终提交
            await self.session.commit()

            # 触发指标计算
            if SYNC_STOCK_INFO_COMPUTE_INDICATORS:
                synced_symbols = [s.symbol for s in stocks]
                indicator_scheduler.run_after_sync("stock_info", symbols=synced_symbols, trade_date=date.today())
            else:
                progress.details["indicator_compute"] = "skipped"

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
        run_id: str | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """全量同步: 基础信息(已有) + 批量财务数据 + 批量市值"""
        global _current_sync

        progress = SyncProgress(
            sync_type="stock_full",
            status="running",
            start_time=datetime.now(),
            details={"run_id": run_id},
        )
        _current_sync = progress
        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

        try:
            # 阶段1: 用已有 stock_list 做基础 upsert (复用 sync_stock_info)
            stocks = await qmt_gateway.get_stock_list()
            progress.total = len(stocks)
            progress.details.update({"total_stocks": len(stocks), "phase": "basic_info"})
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
                        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
                except Exception as e:
                    progress.failed_count += 1
                    failed_stocks.append({"symbol": stock.symbol, "error": str(e)})

                    if failure_strategy == "stop":
                        raise
                    elif failure_strategy == "retry":
                        async def _retry_stock_full_insert():
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

                        try:
                            await async_retry(_retry_stock_full_insert, max_retries=3, base_delay=1.0)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_stocks.pop()
                        except Exception:
                            pass

            await self.session.commit()

            # 阶段2: 批量获取市值
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
            progress.details["phase"] = "market_value"
            progress.current = 0
            progress.success_count = 0
            progress.failed_count = 0

            symbols = [s.symbol for s in stocks]
            quote_dict: dict[str, dict] = {}

            market_batch_size = max(1, QMT_STOCK_FULL_MARKET_BATCH_SIZE)
            progress.details["market_batch_size"] = market_batch_size
            for batch_start in range(0, len(symbols), market_batch_size):
                batch = symbols[batch_start:batch_start + market_batch_size]
                try:
                    quotes = await qmt_gateway.get_realtime_quotes(batch)
                    for q in quotes:
                        quote_dict[q["symbol"]] = q
                except Exception as exc:
                    progress.details["last_market_value_error"] = str(exc)[:200]

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
                            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
                    except Exception:
                        progress.failed_count += 1

            await self.session.commit()
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

            # 阶段3: 读取本地已缓存的财务数据(不触发download，用户需在QMT客户端手动下载)
            progress.details["phase"] = "financial_query"
            progress.current = 0
            progress.success_count = 0
            progress.failed_count = 0
            fin_success = 0

            progress.details["financial_query"] = "disabled_per_symbol_qmt_call"
            progress.details["financial_note"] = "Use financial_data sync for QMT financial download/query"
            progress.current = progress.total
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["failed_stocks"] = failed_stocks[:100]
            progress.details["fin_success"] = fin_success
            await self.session.commit()
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
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
            return progress

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
                    await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
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
                    elif failure_strategy == "retry":
                        async def _retry_financial_insert():
                            k_tables_raw = download_results[symbol]
                            k_quarters = qmt_gateway._parse_financial_dataframes(
                                symbol, k_tables_raw, report_count=8,
                            )
                            if not k_quarters:
                                return
                            k_latest = k_quarters[0]
                            k_stock_update = {}
                            k_mv = quote_dict.get(symbol, {}).get("total_value") or k_latest.total_mv
                            if k_mv and k_latest.net_profit and k_latest.net_profit != 0:
                                k_stock_update["pe_ttm"] = round(k_mv / k_latest.net_profit, 4)
                            if k_mv and k_latest.total_equity and k_latest.total_equity != 0:
                                k_stock_update["pb"] = round(k_mv / k_latest.total_equity, 4)
                            k_stock_update["roe"] = k_latest.roe
                            k_stock_update["eps"] = k_latest.eps
                            k_stock_update["bvps"] = k_latest.bvps
                            k_stock_update["revenue"] = k_latest.revenue
                            k_stock_update["net_profit"] = k_latest.net_profit
                            k_stock_update["total_assets"] = k_latest.total_assets
                            k_stock_update["total_liability"] = k_latest.total_liability
                            k_stock_update["total_equity"] = k_latest.total_equity
                            if k_latest.total_shares is not None:
                                k_stock_update["total_shares"] = k_latest.total_shares
                            if k_latest.float_shares is not None:
                                k_stock_update["float_shares"] = k_latest.float_shares
                            k_stock_update = {k: v for k, v in k_stock_update.items() if v is not None}
                            for fq in k_quarters:
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
                            if k_stock_update:
                                k_stock_update["updated_at"] = datetime.now()
                                stmt = insert(Stock).values(symbol=symbol, **k_stock_update)
                                stmt = stmt.on_conflict_do_update(
                                    index_elements=["symbol"],
                                    set_={k: stmt.excluded[k] for k in k_stock_update.keys()},
                                )
                                await self.session.execute(stmt)
                            nonlocal fin_success
                            fin_success += 1

                        try:
                            await async_retry(_retry_financial_insert, max_retries=3, base_delay=1.0)
                            progress.success_count += 1
                            progress.failed_count -= 1
                            failed_stocks.pop()
                        except Exception:
                            pass

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
            logger.opt(exception=True).error(f"sync_financial_data failed: {e}")
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

    async def sync_index_daily(
        self,
        index_symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        run_id: str | None = None,
        failure_strategy: str = "skip",
        full_sync: bool = False,
    ) -> SyncProgress:
        global _current_sync

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - td(days=365)

        requested_symbols = index_symbols or [item.symbol for item in list_index_items(benchmark_only=True)]
        invalid_symbols: list[str] = []
        items: list[IndexCatalogItem] = []
        seen: set[str] = set()
        for raw_symbol in requested_symbols:
            item = get_index_item(raw_symbol)
            if item is None:
                invalid_symbols.append(str(raw_symbol))
                continue
            if not item.requires_daily_market_data or item.symbol in seen:
                continue
            seen.add(item.symbol)
            items.append(item)

        progress = SyncProgress(
            sync_type="index_daily",
            status="running",
            total=len(items),
            start_time=datetime.now(),
            details={
                "run_id": run_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "full_sync": full_sync,
                "requested_symbols": requested_symbols,
                "invalid_symbols": invalid_symbols,
            },
        )
        _current_sync = progress
        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

        if not items:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = "No valid index symbols resolved for index_daily sync"
            await self.create_sync_log(
                sync_type="index_daily",
                status="failed",
                total_count=0,
                success_count=0,
                failed_count=len(invalid_symbols),
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=progress.error_message,
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()
            return progress

        await self._upsert_index_catalog_entries(items)
        await self.session.commit()

        write_ch = _should_write_clickhouse()
        ch_client = get_ch_client() if write_ch else None
        empty_symbols: list[str] = []
        failed_symbols: list[dict[str, str]] = []
        skipped_symbols: list[str] = []
        blocked_providers: dict[str, str] = {}
        total_rows = 0
        synced_symbols: list[str] = []

        try:
            for index, item in enumerate(items, start=1):
                if _sync_cancelled(progress):
                    progress.status = "cancelled"
                    progress.end_time = datetime.now()
                    progress.error_message = "User cancelled"
                    return progress

                progress.details["current_symbol"] = item.symbol
                progress.details["current_index"] = item.symbol
                progress.details["current_display_name"] = item.display_name

                if item.provider in blocked_providers:
                    skipped_symbols.append(item.symbol)
                    progress.current = index
                    progress.details["blocked_provider"] = item.provider
                    progress.details["blocked_reason"] = blocked_providers[item.provider]
                    progress.details["skipped_symbols"] = skipped_symbols[:200]
                    await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
                    continue

                await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

                async def _fetch_one() -> list[dict[str, Any]]:
                    return await asyncio.to_thread(_fetch_tushare_index_daily_rows, item, start_date, end_date)

                provider_blocked_now = False
                try:
                    rows = await _fetch_one()
                except Exception as exc:
                    rate_limited = _is_tushare_rate_limit_error(exc)
                    if failure_strategy == "retry":
                        try:
                            rows = await async_retry(_fetch_one, max_retries=3, base_delay=1.0)
                        except Exception as retry_exc:
                            progress.failed_count += 1
                            failed_symbols.append({"symbol": item.symbol, "error": str(retry_exc)})
                            if _is_tushare_rate_limit_error(retry_exc) and item.provider == "tushare.sw_daily":
                                blocked_providers[item.provider] = str(retry_exc)
                                progress.details["blocked_provider"] = item.provider
                                progress.details["blocked_reason"] = str(retry_exc)
                                provider_blocked_now = True
                            if failure_strategy == "stop":
                                raise
                            rows = []
                    else:
                        progress.failed_count += 1
                        failed_symbols.append({"symbol": item.symbol, "error": str(exc)})
                        if rate_limited and item.provider == "tushare.sw_daily":
                            blocked_providers[item.provider] = str(exc)
                            progress.details["blocked_provider"] = item.provider
                            progress.details["blocked_reason"] = str(exc)
                            provider_blocked_now = True
                        if failure_strategy == "stop":
                            raise
                        rows = []

                if rows:
                    if full_sync and ch_client is not None:
                        ch_client.execute(
                            "DELETE FROM klines_daily WHERE symbol = %(symbol)s "
                            "AND trade_date >= %(start_date)s AND trade_date <= %(end_date)s",
                            {"symbol": item.symbol, "start_date": start_date, "end_date": end_date},
                        )
                    _write_ch_daily(ch_client, rows)
                    _write_store_daily(rows)
                    total_rows += len(rows)
                    progress.success_count += 1
                    synced_symbols.append(item.symbol)
                elif not any(entry["symbol"] == item.symbol for entry in failed_symbols):
                    empty_symbols.append(item.symbol)

                progress.current = index
                await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

                if provider_blocked_now:
                    continue

                pause_seconds = (
                    TUSHARE_SW_DAILY_PAUSE_SECONDS
                    if item.provider == "tushare.sw_daily"
                    else TUSHARE_INDEX_DAILY_PAUSE_SECONDS
                )
                if pause_seconds > 0 and index < len(items):
                    await asyncio.sleep(pause_seconds)

            progress.details["empty_symbols"] = empty_symbols
            progress.details["failed_symbols"] = failed_symbols[:100]
            progress.details["skipped_symbols"] = skipped_symbols[:200]
            progress.details["total_rows"] = total_rows
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

            if synced_symbols:
                indicator_scheduler.run_after_sync("kline_daily", symbols=synced_symbols, trade_date=end_date)

            progress.status = "completed"
            progress.end_time = datetime.now()
            await self.create_sync_log(
                sync_type="index_daily",
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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
        except Exception as exc:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(exc)
            progress.details["failed_symbols"] = failed_symbols[:100]
            progress.details["skipped_symbols"] = skipped_symbols[:200]
            await self.create_sync_log(
                sync_type="index_daily",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(exc),
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()
            raise
        finally:
            if ch_client is not None:
                try:
                    ch_client.disconnect()
                except Exception:
                    pass
            _current_sync = None

        return progress

    async def sync_kline_daily(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        run_id: str | None = None,
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
            start_date = end_date - td(days=30)

        # 初始化进度
        progress = SyncProgress(
            sync_type="kline_daily",
            status="running",
            start_time=datetime.now(),
            details={
                "run_id": run_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "full_sync": full_sync,
            },
        )
        _current_sync = progress
        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

        # ClickHouse is optional. In parquet mode without clickhouse_enabled,
        # sync writes directly to Parquet and does not require Docker/CH.
        write_ch = _should_write_clickhouse()
        ch_client = get_ch_client() if write_ch else None

        try:
            # 如果没有指定股票列表，获取所有股票
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

            # 全量同步时先删除已有数据
            if full_sync and symbols:
                progress.details["message"] = "正在删除已有数据..."
                for symbol in symbols:
                    try:
                        if ch_client is not None:
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

            batch_size = max(1, QMT_DAILY_BATCH_SIZE)
            progress.details["batch_size"] = batch_size
            for offset in range(0, len(symbols), batch_size):
                if _sync_cancelled(progress):
                    progress.status = "cancelled"
                    progress.end_time = datetime.now()
                    progress.error_message = "User cancelled"
                    return progress

                batch_symbols = symbols[offset: offset + batch_size]
                progress.details["current_batch"] = {
                    "from": batch_symbols[0],
                    "to": batch_symbols[-1],
                    "size": len(batch_symbols),
                }

                try:
                    batch_data = await qmt_gateway.get_kline_daily_batch(
                        batch_symbols, start_date, end_date
                    )
                    batch_rows: list[dict[str, Any]] = []
                    batch_success = 0
                    for symbol in batch_symbols:
                        klines = batch_data.get(symbol, [])
                        if not klines:
                            continue
                        batch_rows.extend(
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
                        )
                        batch_success += 1

                    if batch_rows:
                        _write_ch_daily(ch_client, batch_rows)
                        try:
                            _write_store_daily(batch_rows)
                        except Exception as e:
                            logger.warning(f"Parquet daily write failed for batch {offset // batch_size + 1}: {e}")
                        total_klines += len(batch_rows)

                    progress.success_count += batch_success
                    progress.current = min(offset + len(batch_symbols), progress.total)
                    await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
                    await asyncio.sleep(0)

                except Exception as e:
                    progress.failed_count += len(batch_symbols)
                    failed_symbols.extend({"symbol": symbol, "error": str(e)} for symbol in batch_symbols)

                    if failure_strategy == "stop":
                        raise
                    elif failure_strategy == "retry":
                        async def _retry_kline_insert():
                            retry_data = await qmt_gateway.get_kline_daily_batch(
                                batch_symbols, start_date, end_date
                            )
                            retry_rows: list[dict[str, Any]] = []
                            retry_success = 0
                            for symbol in batch_symbols:
                                klines = retry_data.get(symbol, [])
                                if not klines:
                                    continue
                                retry_rows.extend(
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
                                )
                                retry_success += 1
                            if retry_rows:
                                retry_ch = get_ch_client() if write_ch else None
                                try:
                                    _write_ch_daily(retry_ch, retry_rows)
                                finally:
                                    if retry_ch is not None:
                                        try:
                                            retry_ch.disconnect()
                                        except Exception:
                                            pass
                                _write_store_daily(retry_rows)
                                nonlocal total_klines
                                total_klines += len(retry_rows)
                            return retry_success

                        try:
                            retry_success = await async_retry(_retry_kline_insert, max_retries=3, base_delay=1.0)
                            progress.success_count += retry_success
                            progress.failed_count = max(0, progress.failed_count - retry_success)
                            if retry_success:
                                del failed_symbols[-retry_success:]
                        except Exception:
                            pass
                    progress.current = min(offset + len(batch_symbols), progress.total)
                    await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
            logger.opt(exception=True).error(f"sync_kline_daily failed: {e}")
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
            if ch_client is not None:
                try:
                    ch_client.disconnect()
                except Exception:
                    pass
            _current_sync = None

        return progress

    async def sync_kline_minute(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        run_id: str | None = None,
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
                "run_id": run_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "full_sync": full_sync,
            },
        )
        _current_sync = progress
        await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

        # ClickHouse is optional. In parquet mode without clickhouse_enabled,
        # sync writes directly to Parquet and does not require Docker/CH.
        write_ch = _should_write_clickhouse()
        ch_client = get_ch_client() if write_ch else None

        try:
            # 如果没有指定股票列表，获取所有股票
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

            # 全量同步时先删除已有数据
            if full_sync and symbols:
                progress.details["message"] = "正在删除已有数据..."
                for symbol in symbols:
                    try:
                        if ch_client is not None:
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

            def _minute_rows(klines: list[Any]) -> list[dict[str, Any]]:
                return [
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

            async def _sync_minute_symbol(symbol: str) -> bool:
                nonlocal total_klines
                klines = await qmt_gateway.get_kline_minute(symbol, start_date, end_date)
                if not klines:
                    return False
                rows = _minute_rows(klines)
                if rows:
                    _write_ch_minute(ch_client, rows)
                    _write_store_minute(rows)
                    total_klines += len(rows)
                return True

            batch_size = max(1, QMT_MINUTE_BATCH_SIZE)
            progress.details["batch_size"] = batch_size

            for offset in range(0, len(symbols), batch_size):
                if _sync_cancelled(progress):
                    progress.status = "cancelled"
                    progress.end_time = datetime.now()
                    progress.error_message = "User cancelled"
                    return progress

                batch_symbols = symbols[offset: offset + batch_size]
                progress.details["current_batch"] = {
                    "from": batch_symbols[0],
                    "to": batch_symbols[-1],
                    "size": len(batch_symbols),
                }

                try:
                    batch_klines = await qmt_gateway.get_kline_minute_batch(
                        batch_symbols,
                        start_date,
                        end_date,
                    )
                    rows: list[dict[str, Any]] = []
                    successful_symbols = 0
                    for symbol in batch_symbols:
                        klines = batch_klines.get(symbol, [])
                        if not klines:
                            continue
                        successful_symbols += 1
                        rows.extend(_minute_rows(klines))

                    if rows:
                        _write_ch_minute(ch_client, rows)
                        try:
                            _write_store_minute(rows)
                        except Exception as e:
                            logger.warning(
                                f"Parquet write failed for minute batch "
                                f"{batch_symbols[0]}..{batch_symbols[-1]}: {e}"
                            )
                        total_klines += len(rows)

                    progress.current = min(offset + len(batch_symbols), progress.total)
                    progress.success_count += successful_symbols
                    await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

                except Exception as e:
                    logger.warning(
                        f"QMT minute batch sync failed for "
                        f"{batch_symbols[0]}..{batch_symbols[-1]}, fallback to single-symbol: {e}"
                    )
                    if failure_strategy == "stop":
                        raise

                    for symbol in batch_symbols:
                        if _sync_cancelled(progress):
                            progress.status = "cancelled"
                            progress.end_time = datetime.now()
                            progress.error_message = "User cancelled"
                            return progress

                        try:
                            ok = await _sync_minute_symbol(symbol)
                            if ok:
                                progress.success_count += 1
                        except Exception as symbol_error:
                            progress.failed_count += 1
                            failed_symbols.append({
                                "symbol": symbol,
                                "error": str(symbol_error),
                            })

                            if failure_strategy == "stop":
                                raise
                            elif failure_strategy == "retry":
                                try:
                                    ok = await async_retry(
                                        lambda: _sync_minute_symbol(symbol),
                                        max_retries=3,
                                        base_delay=1.0,
                                    )
                                    if ok:
                                        progress.success_count += 1
                                    progress.failed_count -= 1
                                    failed_symbols.pop()
                                except Exception:
                                    pass

                        progress.current += 1
                        if progress.current % 10 == 0 or progress.current >= progress.total:
                            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

            # 更新进度
            if app_settings.qmt_minute_clean_cache_after_sync:
                progress.details["post_sync_step"] = "clean_local_cache"
                try:
                    cleaned = qmt_gateway.clean_local_cache(symbols=symbols, data_type="kline")
                    progress.details["cache_cleaned"] = cleaned
                except Exception as exc:
                    progress.details["cache_clean_error"] = str(exc)
            else:
                progress.details["cache_cleaned"] = "skipped"

            if app_settings.qmt_minute_compute_indicators_after_sync:
                progress.details["post_sync_step"] = "compute_indicators"
                indicator_scheduler.run_after_sync("kline_minute", symbols=symbols, trade_date=end_date)
            else:
                progress.details["indicator_compute"] = "skipped"
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["total_klines"] = total_klines
            progress.details["failed_symbols"] = failed_symbols[:100]
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

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
            await self.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)

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
            if ch_client is not None:
                try:
                    ch_client.disconnect()
                except Exception:
                    pass
            _current_sync = None

        return progress

    async def sync_kline_weekly(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        failure_strategy: str = "skip",
        full_sync: bool = False,
    ) -> SyncProgress:
        """
        同步周K线数据 — 从 ClickHouse 日线直接聚合，不走 QMT

        Args:
            symbols: 股票代码列表，为空则同步所有股票
            start_date: 起始日期，默认为7年前
            end_date: 结束日期，默认为今天
            task_id: 关联任务ID
            failure_strategy: 失败策略 (skip/retry/stop)
            full_sync: 是否全量同步

        Returns:
            SyncProgress: 同步进度
        """
        import pandas as pd

        global _current_sync

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - td(days=365 * 7)

        progress = SyncProgress(
            sync_type="kline_weekly",
            status="running",
            start_time=datetime.now(),
            details={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "full_sync": full_sync,
            },
        )
        _current_sync = progress

        ch_client = get_ch_client()
        BATCH_SIZE = 50

        try:
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)

            if full_sync:
                progress.details["message"] = "正在删除已有数据..."
                ch_client.execute(
                    "DELETE FROM klines_weekly WHERE trade_date >= %(start)s AND trade_date <= %(end)s",
                    {"start": start_date, "end": end_date},
                )
                progress.details["message"] = "删除完成，开始同步..."

            total_klines = 0

            for batch_start in range(0, len(symbols), BATCH_SIZE):
                batch = symbols[batch_start:batch_start + BATCH_SIZE]

                # Batch query daily data from ClickHouse
                daily_rows = ch_client.execute(
                    "SELECT symbol, trade_date, open, high, low, close, volume, amount "
                    "FROM klines_daily "
                    "WHERE symbol IN %(syms)s "
                    "  AND trade_date >= %(start)s AND trade_date <= %(end)s "
                    "ORDER BY symbol, trade_date",
                    {"syms": tuple(batch), "start": start_date, "end": end_date},
                )
                if not daily_rows:
                    progress.current = min(batch_start + BATCH_SIZE, len(symbols))
                    continue

                # Build DataFrame
                df = pd.DataFrame(
                    daily_rows,
                    columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
                )
                for col in ["open", "high", "low", "close", "amount"]:
                    df[col] = df[col].astype(float)
                df["trade_date"] = pd.to_datetime(df["trade_date"])

                # Aggregate to weekly per symbol
                weekly_rows = []
                for sym, grp in df.groupby("symbol"):
                    grp = grp.set_index("trade_date").sort_index()
                    weekly = grp.resample("W").agg({
                        "open": "first", "high": "max", "low": "min",
                        "close": "last", "volume": "sum", "amount": "sum",
                    }).dropna()
                    for idx, row in weekly.iterrows():
                        weekly_rows.append({
                            "symbol": sym,
                            "trade_date": idx.date(),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": int(row["volume"]),
                            "amount": float(row["amount"]),
                        })

                if weekly_rows:
                    # Split by month to stay under 100-partition limit
                    weekly_rows.sort(key=lambda r: r["trade_date"])
                    chunk_start = 0
                    while chunk_start < len(weekly_rows):
                        chunk_end = chunk_start
                        chunk_month = weekly_rows[chunk_start]["trade_date"].strftime("%Y%m")
                        while chunk_end < len(weekly_rows) and weekly_rows[chunk_end]["trade_date"].strftime("%Y%m") == chunk_month:
                            chunk_end += 1
                        ch_client.execute(
                            "INSERT INTO klines_weekly "
                            "(symbol, trade_date, open, high, low, close, volume, amount) VALUES",
                            weekly_rows[chunk_start:chunk_end],
                        )
                        chunk_start = chunk_end
                    total_klines += len(weekly_rows)

                progress.current = min(batch_start + BATCH_SIZE, len(symbols))
                progress.success_count += len(set(r["symbol"] for r in weekly_rows))

            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["total_klines"] = total_klines

            await self.create_sync_log(
                sync_type="kline_weekly",
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
            logger.opt(exception=True).error(f"sync_kline_weekly failed: {e}")
            await self.create_sync_log(
                sync_type="kline_weekly",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(e),
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()
            raise

        finally:
            _current_sync = None

        return progress

    async def sync_tushare_relay(
        self,
        relay_datasets: list[str] | None = None,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        relay_options: dict[str, Any] | None = None,
        failure_strategy: str = "skip",
        task_id: int | None = None,
        run_id: str | None = None,
    ) -> SyncProgress:
        """Sync structured Indevs Tushare Relay datasets into local Parquet."""
        from app.services.tushare_relay_sync import run_tushare_relay_sync

        global _current_sync
        progress = SyncProgress(
            sync_type="tushare_relay",
            status="running",
            start_time=datetime.now(),
            details={"run_id": run_id} if run_id else {},
        )
        _current_sync = progress

        try:
            await run_tushare_relay_sync(
                self.session,
                progress,
                relay_datasets=relay_datasets,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                relay_options=relay_options,
                failure_strategy=failure_strategy,
            )
            await self.create_sync_log(
                sync_type="tushare_relay",
                status=progress.status,
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=progress.error_message,
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
            logger.opt(exception=True).error(f"sync_tushare_relay failed: {e}")
            await self.create_sync_log(
                sync_type="tushare_relay",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(e),
                details=progress.details,
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
        cutoff_date = datetime.now() - td(days=before_days)
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

    async def sync_dividends(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        failure_strategy: str = "skip",
    ) -> SyncProgress:
        """
        同步分红送股数据 + 计算股息率

        1. 从 QMT 批量下载分红数据
        2. 写入 dividend_cash 到 ClickHouse
        3. 批量计算 dividend_yield（历史时间点）
        """
        global _current_sync

        start_str = start_date.strftime("%Y%m%d") if start_date else ""
        end_str = end_date.strftime("%Y%m%d") if end_date else ""

        progress = SyncProgress(
            sync_type="dividends",
            status="running",
            start_time=datetime.now(),
            details={"start_date": start_str or "全部", "end_date": end_str or "全部"},
        )
        _current_sync = progress

        ch_client = get_ch_client()

        try:
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)

            # ========== 阶段1: 批量下载 & 读取 QMT 分红数据 ==========
            progress.details["phase"] = "download"
            all_rows = []
            symbols_with_cash = set()

            for i, symbol in enumerate(symbols):
                progress.current = i + 1
                try:
                    dividends = await qmt_gateway.get_dividends(symbol, start_str, end_str)
                    if not dividends:
                        continue

                    for div in dividends:
                        ex_date = div.get("ex_date")
                        cash = div.get("cash_dividend")
                        if ex_date and cash and cash > 0:
                            dt = datetime.strptime(ex_date, "%Y%m%d").date()
                            all_rows.append({
                                "symbol": symbol,
                                "indicator_name": "dividend_cash",
                                "trade_date": dt,
                                "value": float(cash),
                            })
                            symbols_with_cash.add(symbol)

                    progress.success_count += 1
                except Exception as e:
                    progress.failed_count += 1
                    if failure_strategy == "stop":
                        raise
                    continue

            # ========== 阶段2: 批量写入 dividend_cash ==========
            if all_rows:
                progress.details["phase"] = "insert_cash"
                ch_client.execute(
                    "DELETE FROM stock_indicators WHERE indicator_name = 'dividend_cash'"
                )
                # Sort and batch by month to avoid partition limit
                all_rows.sort(key=lambda r: r["trade_date"])
                batch_start = 0
                while batch_start < len(all_rows):
                    batch_end = batch_start
                    month = all_rows[batch_start]["trade_date"].strftime("%Y%m")
                    while batch_end < len(all_rows) and all_rows[batch_end]["trade_date"].strftime("%Y%m") == month:
                        batch_end += 1
                    ch_client.execute(
                        "INSERT INTO stock_indicators (symbol, indicator_name, trade_date, value) VALUES",
                        all_rows[batch_start:batch_end],
                    )
                    batch_start = batch_end

            # ========== 阶段3: 批量计算 dividend_yield ==========
            progress.details["phase"] = "yield"
            progress.details["total_dividends"] = len(all_rows)
            progress.details["symbols_with_cash"] = len(symbols_with_cash)

            yield_rows = []

            for symbol in symbols_with_cash:
                progress.current += 1
                try:
                    # Query dividend_cash for this symbol, sorted ASC for sliding window
                    cash_rows = ch_client.execute(
                        "SELECT trade_date, value FROM stock_indicators "
                        "WHERE symbol=%(s)s AND indicator_name='dividend_cash' "
                        "ORDER BY trade_date ASC",
                        {"s": symbol},
                    )
                    if not cash_rows:
                        continue

                    # Get prices only at ex-dates (not all klines!)
                    ex_dates = [r[0] for r in cash_rows]
                    min_date, max_date = ex_dates[0], ex_dates[-1]
                    price_rows = ch_client.execute(
                        "SELECT trade_date, close FROM klines_daily "
                        "WHERE symbol=%(s)s AND trade_date>=%(min)s AND trade_date<=%(max)s "
                        "ORDER BY trade_date ASC",
                        {"s": symbol, "min": min_date, "max": max_date},
                    )
                    if not price_rows:
                        continue

                    # Sliding window: O(n) trailing 12-month sum
                    dates = [r[0] for r in cash_rows]
                    values = [float(r[1] or 0) for r in cash_rows]
                    price_dates = [r[0] for r in price_rows]
                    price_values = [float(r[1]) for r in price_rows]

                    p_idx = 0
                    trailing_sum = 0.0
                    left = 0

                    for right, (ex_date, cash) in enumerate(zip(dates, values)):
                        trailing_sum += cash
                        # Shrink window: remove entries older than 365 days
                        while left <= right and (ex_date - dates[left]).days > 365:
                            trailing_sum -= values[left]
                            left += 1
                        if trailing_sum <= 0:
                            continue
                        # Find closest price on or before ex_date
                        while p_idx < len(price_dates) and price_dates[p_idx] <= ex_date:
                            p_idx += 1
                        if p_idx == 0:
                            continue
                        price = price_values[p_idx - 1]
                        if price <= 0:
                            continue
                        div_yield = (trailing_sum / price) * 100
                        yield_rows.append({
                            "symbol": symbol,
                            "indicator_name": "dividend_yield",
                            "trade_date": dates[right],
                            "value": round(div_yield, 4),
                        })
                except Exception as e:
                    if failure_strategy == "stop":
                        raise
                    continue

            if yield_rows:
                ch_client.execute(
                    "DELETE FROM stock_indicators WHERE indicator_name = 'dividend_yield'"
                )
                # Sort and batch by month to avoid partition limit
                yield_rows.sort(key=lambda r: r["trade_date"])
                y_start = 0
                while y_start < len(yield_rows):
                    y_end = y_start
                    y_month = yield_rows[y_start]["trade_date"].strftime("%Y%m")
                    while y_end < len(yield_rows) and yield_rows[y_end]["trade_date"].strftime("%Y%m") == y_month:
                        y_end += 1
                    ch_client.execute(
                        "INSERT INTO stock_indicators (symbol, indicator_name, trade_date, value) VALUES",
                        yield_rows[y_start:y_end],
                    )
                    y_start = y_end

            progress.details["yield_rows"] = len(yield_rows)

            # ========== 阶段4: 清理缓存 ==========
            try:
                cleaned = qmt_gateway.clean_local_cache(data_type="financial")
                progress.details["cache_cleaned"] = cleaned
            except Exception:
                pass

            progress.status = "completed"
            progress.end_time = datetime.now()

            await self.create_sync_log(
                sync_type="dividends",
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
            logger.opt(exception=True).error(f"sync_dividends failed: {e}")
            await self.create_sync_log(
                sync_type="dividends",
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
