from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.schemas import (
    AIActionCard,
    AIToolDefinitionPublic,
    AIToolExecutionResponse,
    AIToolRiskLevel,
)
from app.db.sqlite import async_session_factory
from app.services.runtime_tasks import list_tasks
from app.services.security_symbols import normalize_security_symbol


class EmptyInput(BaseModel):
    pass


class RuntimeTasksInput(BaseModel):
    include_finished: bool = True


class ToolCatalogInput(BaseModel):
    category: str | None = None


class StockSnapshotInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")


class StockBatchInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=200)


class StockScreenInput(BaseModel):
    industry: str | None = None
    exchange: str | None = None
    is_st: int | None = None
    min_mv: float | None = None
    max_mv: float | None = None
    min_pe: float | None = None
    max_pe: float | None = None
    min_roe: float | None = None
    limit: int = Field(default=100, ge=1, le=500)


class KlineInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")
    start_date: date | None = None
    end_date: date | None = None
    limit: int = Field(default=120, ge=1, le=5000)
    timer_times: list[str] | None = None


class MarketSnapshotInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")
    daily_limit: int = Field(default=5, ge=1, le=120)
    include_realtime: bool = True


class FinancialInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")
    report_count: int = Field(default=8, ge=1, le=20)


class FinancialBatchInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=100)
    report_count: int = Field(default=1, ge=1, le=8)


class QuoteInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")


class QuoteBatchInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=200)


class SymbolsInput(BaseModel):
    industry: str | None = None
    limit: int = Field(default=500, ge=1, le=10000)


class WatchlistStocksInput(BaseModel):
    group_id: int = Field(ge=1)


class SyncCatalogInput(BaseModel):
    refresh: bool = False


class SyncLogsInput(BaseModel):
    sync_type: str | None = None
    task_id: int | None = None
    limit: int = Field(default=50, ge=1, le=200)


class IndicatorValueInput(BaseModel):
    symbol: str
    name: str
    trade_date: date | None = None


class IndicatorBatchInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=500)
    names: list[str] | None = None
    trade_date: date | None = None


class IndicatorTimeseriesInput(BaseModel):
    symbol: str
    names: list[str] = Field(min_length=1, max_length=20)
    start_date: date
    end_date: date
    limit: int = Field(default=5000, ge=1, le=200000)


class IndexCatalogInput(BaseModel):
    benchmark_only: bool = False
    pool_only: bool = False


class IndexPoolInput(BaseModel):
    index_symbol: str = Field(description="Index symbol or alias, e.g. 399101.SZ")
    start_date: date | None = None
    end_date: date | None = None


class SentimentOverviewInput(BaseModel):
    sources: list[str] | None = None


class SentimentSymbolInput(BaseModel):
    symbol: str
    start_date: date | None = None
    end_date: date | None = None
    sources: list[str] | None = None
    limit: int = Field(default=50, ge=1, le=200)


class LiveQueryInput(BaseModel):
    profile_key: str | None = None
    mode: str | None = None
    limit: int = Field(default=100, ge=1, le=500)


class PaginationInput(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class IdInput(BaseModel):
    id: int = Field(ge=1)


class BacktestRecordsInput(PaginationInput):
    strategy_id: int | None = None
    status: str | None = None


class BacktestTaskInput(BaseModel):
    task_id: str = Field(min_length=1)


class BacktestTimerCoverageInput(BaseModel):
    index_symbol: str | None = None
    symbols: list[str] | None = None
    start_date: date | None = None
    end_date: date | None = None
    times: list[str] = Field(default_factory=lambda: ["10:00", "10:30", "14:30", "14:50"])


class ExplorerPreviewInput(BaseModel):
    table_name: str = Field(min_length=1)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)
    order_by: str | None = None
    order_dir: str = "DESC"
    where: str | None = None
    include_total: bool = False


class ExplorerQueryInput(BaseModel):
    sql: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=200, ge=1, le=1000)


class ParquetDatasetInput(BaseModel):
    dataset: str = Field(min_length=1)
    limit: int = Field(default=20, ge=1, le=200)
    offset: int = Field(default=0, ge=0)
    include_total: bool = False


class ParquetCoverageInput(BaseModel):
    dataset: str = Field(min_length=1)
    symbols: list[str] | None = None
    start: date = date(2000, 1, 1)
    end: date = date(2099, 12, 31)


class FactorListInput(BaseModel):
    category: str | None = None
    source: str | None = None


class FactorValueCoverageInput(BaseModel):
    factor_name: str = Field(min_length=1)
    start_date: date
    end_date: date
    index_symbol: str | None = None
    symbols: list[str] | None = None
    as_of_time: str | None = None
    full_range: bool = False


class FactorValuePreviewInput(BaseModel):
    factor_name: str = Field(min_length=1)
    trade_date: date
    index_symbol: str | None = None
    symbols: list[str] | None = None
    as_of_time: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


class FactorResearchLatestInput(BaseModel):
    factor_name: str = Field(min_length=1)
    stock_pool_value: str | None = None
    params_hash: str | None = None


class FactorResearchRunInput(BaseModel):
    run_id: str = Field(min_length=1)


class DataSyncSubmitInput(BaseModel):
    sync_type: str
    symbols: list[str] | None = None
    index_symbols: list[str] | None = None
    start_date: date | None = None
    end_date: date | None = None
    sync_mode: str = "range"
    failure_strategy: str = "skip"
    full_sync: bool = False
    factor_sync_plan: dict[str, Any] | None = None
    relay_datasets: list[str] | None = None
    relay_options: dict[str, Any] | None = None


class ReportStrategyInput(BaseModel):
    report_text: str = Field(min_length=1)
    report_filename: str = "report.txt"


class StrategyConvertInput(BaseModel):
    source_code: str = Field(min_length=1)


ToolHandler = Callable[[BaseModel, "AIToolExecutionContext"], Awaitable["AIToolRunResult"]]


@dataclass(frozen=True)
class AIToolExecutionContext:
    session: AsyncSession | None = None
    confirmed: bool = False


@dataclass(frozen=True)
class AIToolRunResult:
    summary: str
    result: Any
    task_id: str | None = None
    result_ref: str | None = None


@dataclass(frozen=True)
class AITool:
    name: str
    title: str
    description: str
    category: str
    input_model: type[BaseModel]
    handler: ToolHandler
    risk_level: AIToolRiskLevel = "read"
    requires_confirmation: bool = False

    def public(self) -> AIToolDefinitionPublic:
        return AIToolDefinitionPublic(
            name=self.name,
            title=self.title,
            description=self.description,
            category=self.category,
            risk_level=self.risk_level,
            requires_confirmation=self.requires_confirmation,
            input_schema=self.input_model.model_json_schema(),
            output_schema=AIToolExecutionResponse.model_json_schema(),
        )

    def action(self, arguments: dict[str, Any] | None = None) -> AIActionCard:
        return AIActionCard(
            tool_name=self.name,
            title=self.title,
            description=self.description,
            arguments=arguments or {},
            risk_level=self.risk_level,
            requires_confirmation=self.requires_confirmation,
        )


class AIToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, AITool] = {}

    def register(self, tool: AITool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"AI tool already registered: {tool.name}")
        self._tools[tool.name] = tool

    def get(self, name: str) -> AITool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"Unknown AI tool: {name}") from exc

    def list(self) -> list[AITool]:
        return [self._tools[key] for key in sorted(self._tools)]

    def public_definitions(self) -> list[AIToolDefinitionPublic]:
        return [tool.public() for tool in self.list()]

    async def execute(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        confirmed: bool = False,
        session: AsyncSession | None = None,
    ) -> AIToolExecutionResponse:
        tool = self.get(name)
        if tool.requires_confirmation and not confirmed:
            return AIToolExecutionResponse(
                tool_name=tool.name,
                status="needs_confirmation",
                summary=f"{tool.title} 需要确认后执行。",
                result={"arguments": arguments or {}},
            )
        payload = tool.input_model.model_validate(arguments or {})
        ctx = AIToolExecutionContext(session=session, confirmed=confirmed)
        try:
            result = await tool.handler(payload, ctx)
            return AIToolExecutionResponse(
                tool_name=tool.name,
                status="ok",
                summary=result.summary,
                result=result.result,
                task_id=result.task_id,
                result_ref=result.result_ref,
            )
        except HTTPException as exc:
            return AIToolExecutionResponse(
                tool_name=tool.name,
                status="error",
                summary=str(exc.detail),
                error=str(exc.detail),
            )
        except Exception as exc:
            return AIToolExecutionResponse(
                tool_name=tool.name,
                status="error",
                summary=str(exc),
                error=str(exc),
            )


async def _with_session(
    ctx: AIToolExecutionContext,
    callback: Callable[[AsyncSession], Awaitable[AIToolRunResult]],
) -> AIToolRunResult:
    if ctx.session is not None:
        return await callback(ctx.session)
    async with async_session_factory() as session:
        result = await callback(session)
        await session.commit()
        return result


def _dataclass_to_dict(value: Any) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for field_name in getattr(value, "__dataclass_fields__", {}):
        field_value = getattr(value, field_name)
        if hasattr(field_value, "isoformat"):
            field_value = field_value.isoformat()
        data[field_name] = field_value
    return data


def _dataclass_list_to_dicts(values: list[Any]) -> list[dict[str, Any]]:
    return [_dataclass_to_dict(value) for value in values]


def _source_csv(sources: list[str] | None) -> str | None:
    if not sources:
        return None
    return ",".join(source.strip() for source in sources if source.strip())


def _csv(values: list[str] | None) -> str | None:
    if not values:
        return None
    return ",".join(value.strip() for value in values if value.strip())


def _api_data(result: Any) -> Any:
    if isinstance(result, dict) and "data" in result:
        return result.get("data")
    return result


async def _system_status_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import get_system_status

    result = await get_system_status()
    backend = result.get("market_data_backend") or "-"
    return AIToolRunResult(summary=f"系统运行中，行情后端 {backend}。", result=result)


async def _system_data_summary_handler(
    _payload: BaseModel,
    ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.api.system import get_data_summary

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_data_summary(session)
        return AIToolRunResult(
            summary=f"数据总览状态：{result.get('overall_status', '-')}",
            result=result,
        )

    return await _with_session(ctx, read)


async def _runtime_tasks_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    req = RuntimeTasksInput.model_validate(payload)
    result = list_tasks(include_finished=req.include_finished)
    return AIToolRunResult(summary=f"找到 {len(result)} 个运行任务。", result=result)


async def _tool_catalog_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    req = ToolCatalogInput.model_validate(payload)
    tools = [tool.public().model_dump() for tool in get_ai_tool_registry().list()]
    if req.category:
        tools = [tool for tool in tools if tool.get("category") == req.category]
    categories: dict[str, int] = {}
    for tool in tools:
        category = str(tool.get("category") or "other")
        categories[category] = categories.get(category, 0) + 1
    return AIToolRunResult(
        summary=f"已注册 {len(tools)} 个 AI 工具。",
        result={"categories": categories, "tools": tools},
    )


async def _stock_snapshot_handler(
    payload: BaseModel,
    ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = StockSnapshotInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        snapshot = await DataSkill(session).get_stock(symbol)
        if snapshot is None:
            return AIToolRunResult(summary=f"未找到股票 {symbol}。", result=None)
        data = _dataclass_to_dict(snapshot)
        return AIToolRunResult(
            summary=f"{symbol} {data.get('name') or ''} 快照已读取。",
            result=data,
            result_ref=f"/stock/{symbol}",
        )

    return await _with_session(ctx, read)


async def _stock_batch_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = StockBatchInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip().upper() for symbol in req.symbols]

    async def read(session: AsyncSession) -> AIToolRunResult:
        snapshots = await DataSkill(session).get_stocks(symbols)
        result = {symbol: _dataclass_to_dict(snapshot) for symbol, snapshot in snapshots.items()}
        return AIToolRunResult(summary=f"已读取 {len(result)} / {len(symbols)} 只股票快照。", result=result)

    return await _with_session(ctx, read)


async def _stock_screen_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = StockScreenInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await DataSkill(session).screen_stocks(**req.model_dump())
        stocks = _dataclass_list_to_dicts(result.stocks)
        return AIToolRunResult(
            summary=f"筛选命中 {result.total} 只股票，返回 {len(stocks)} 只。",
            result={"total": result.total, "stocks": stocks},
            result_ref="/factors/screen",
        )

    return await _with_session(ctx, read)


async def _kline_daily_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = KlineInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        bars = await DataSkill(session).get_kline_daily(
            symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
        )
        data = _dataclass_list_to_dicts(bars)
        return AIToolRunResult(
            summary=f"{symbol} 日 K 已读取 {len(data)} 条。",
            result=data,
            result_ref=f"/stock/{symbol}",
        )

    return await _with_session(ctx, read)


async def _kline_minute_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = KlineInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        bars = await DataSkill(session).get_kline_minute(
            symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
            timer_times=req.timer_times,
        )
        data = _dataclass_list_to_dicts(bars)
        return AIToolRunResult(
            summary=f"{symbol} 分钟 K 已读取 {len(data)} 条。",
            result=data,
            result_ref=f"/stock/{symbol}",
        )

    return await _with_session(ctx, read)


async def _market_snapshot_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = MarketSnapshotInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        skill = DataSkill(session)
        stock = await skill.get_stock(symbol)
        daily_bars = await skill.get_kline_daily(symbol, limit=req.daily_limit)
        realtime: dict[str, Any] | None = None
        realtime_error: str | None = None
        if req.include_realtime:
            try:
                realtime = await asyncio.wait_for(skill.get_realtime_quote(symbol), timeout=5.0)
            except asyncio.TimeoutError:
                realtime_error = "实时行情查询超过 5 秒，已返回本地日线行情"
            except Exception as exc:
                realtime_error = str(exc)
        data = {
            "symbol": symbol,
            "stock": _dataclass_to_dict(stock) if stock is not None else None,
            "daily_bars": _dataclass_list_to_dicts(daily_bars),
            "realtime": realtime,
            "realtime_error": realtime_error,
        }
        latest = data["daily_bars"][0] if data["daily_bars"] else {}
        price = latest.get("close") if isinstance(latest, dict) else None
        name = data["stock"].get("name") if isinstance(data["stock"], dict) else ""
        suffix = f"，最新日收盘 {price}" if price is not None else ""
        if realtime_error:
            suffix += "；实时行情不可用，已返回本地日线"
        return AIToolRunResult(
            summary=f"{symbol} {name or ''} 行情快照已读取{suffix}。",
            result=data,
            result_ref=f"/stock/{symbol}",
        )

    return await _with_session(ctx, read)


async def _financial_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = FinancialInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        reports = await DataSkill(session).get_financial(symbol, report_count=req.report_count)
        data = _dataclass_list_to_dicts(reports)
        return AIToolRunResult(summary=f"{symbol} 财务报告已读取 {len(data)} 期。", result=data)

    return await _with_session(ctx, read)


async def _financial_batch_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = FinancialBatchInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip().upper() for symbol in req.symbols]

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await DataSkill(session).get_financial_batch(symbols, report_count=req.report_count)
        data = {symbol: _dataclass_list_to_dicts(reports) for symbol, reports in result.items()}
        return AIToolRunResult(summary=f"已读取 {len(data)} 只股票财务报告。", result=data)

    return await _with_session(ctx, read)


async def _quote_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = QuoteInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        quote = await DataSkill(session).get_realtime_quote(symbol)
        if not quote:
            return AIToolRunResult(summary=f"未获取到 {symbol} 实时行情。", result=None)
        return AIToolRunResult(summary=f"{symbol} 实时行情已读取。", result=quote, result_ref=f"/stock/{symbol}")

    return await _with_session(ctx, read)


async def _quote_batch_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = QuoteBatchInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip().upper() for symbol in req.symbols]

    async def read(session: AsyncSession) -> AIToolRunResult:
        quotes = await DataSkill(session).get_realtime_quotes(symbols)
        return AIToolRunResult(summary=f"已读取 {len(quotes)} / {len(symbols)} 条实时行情。", result=quotes)

    return await _with_session(ctx, read)


async def _industries_handler(_payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    async def read(session: AsyncSession) -> AIToolRunResult:
        industries = await DataSkill(session).get_industries()
        data = _dataclass_list_to_dicts(industries)
        return AIToolRunResult(summary=f"已读取 {len(data)} 个行业。", result=data, result_ref="/data/stock-list")

    return await _with_session(ctx, read)


async def _symbols_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = SymbolsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        skill = DataSkill(session)
        symbols = await skill.get_symbols_by_industry(req.industry) if req.industry else await skill.get_all_symbols()
        return AIToolRunResult(
            summary=f"已读取股票代码 {len(symbols)} 个。",
            result={"total": len(symbols), "symbols": symbols[: req.limit]},
            result_ref="/data/stock-list",
        )

    return await _with_session(ctx, read)


async def _watchlist_groups_handler(_payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_watchlist_groups

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_watchlist_groups(session=session)
        data = _api_data(result) or []
        return AIToolRunResult(summary=f"已读取 {len(data)} 个自选股分组。", result=data, result_ref="/watchlist")

    return await _with_session(ctx, read)


async def _watchlist_stocks_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_watchlist_stocks

    req = WatchlistStocksInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_watchlist_stocks(group_id=req.group_id, session=session)
        data = _api_data(result) or []
        return AIToolRunResult(
            summary=f"自选股分组 {req.group_id} 已读取 {len(data)} 只股票。",
            result=data,
            result_ref="/watchlist",
        )

    return await _with_session(ctx, read)


async def _sync_catalog_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_sync_catalog

    req = SyncCatalogInput.model_validate(payload)
    result = await get_sync_catalog(refresh=req.refresh)
    data = _api_data(result)
    count = len(data or []) if isinstance(data, list) else 0
    return AIToolRunResult(summary=f"同步任务目录已读取 {count} 项。", result=data, result_ref="/data/sync")


async def _sync_status_handler(_payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_sync_status

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_sync_status(session=session)
        data = _api_data(result)
        status = data.get("status") if isinstance(data, dict) else "-"
        return AIToolRunResult(summary=f"同步状态：{status}。", result=data, result_ref="/data/sync")

    return await _with_session(ctx, read)


async def _sync_logs_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_sync_logs

    req = SyncLogsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_sync_logs(sync_type=req.sync_type, task_id=req.task_id, limit=req.limit, session=session)
        data = _api_data(result) or []
        return AIToolRunResult(summary=f"同步日志已读取 {len(data)} 条。", result=data, result_ref="/data/sync")

    return await _with_session(ctx, read)


async def _indicator_value_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = IndicatorValueInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        value = await DataSkill(session).get_indicator(symbol, req.name, req.trade_date)
        return AIToolRunResult(
            summary=f"{symbol} 指标 {req.name} = {value}。",
            result={
                "symbol": symbol,
                "indicator": req.name,
                "trade_date": req.trade_date.isoformat() if req.trade_date else None,
                "value": value,
            },
        )

    return await _with_session(ctx, read)


async def _indicator_batch_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = IndicatorBatchInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip().upper() for symbol in req.symbols]

    async def read(session: AsyncSession) -> AIToolRunResult:
        rows = await DataSkill(session).get_indicators_batch(symbols=symbols, names=req.names, trade_date=req.trade_date)
        return AIToolRunResult(summary=f"已读取截面指标 {len(rows)} 行。", result=rows)

    return await _with_session(ctx, read)


async def _indicator_timeseries_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.data_skill import DataSkill

    req = IndicatorTimeseriesInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        rows = await DataSkill(session).get_indicator_timeseries(
            symbol=symbol,
            names=req.names,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
        )
        return AIToolRunResult(summary=f"{symbol} 指标时序已读取 {len(rows)} 行。", result=rows)

    return await _with_session(ctx, read)


async def _index_catalog_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.index_catalog import catalog_item_to_dict, list_index_items

    req = IndexCatalogInput.model_validate(payload)
    items = list_index_items(
        benchmark_only=True if req.benchmark_only else None,
        pool_only=True if req.pool_only else None,
    )
    data = [catalog_item_to_dict(item) for item in items]
    return AIToolRunResult(summary=f"已读取 {len(data)} 个指数目录项。", result=data, result_ref="/backtest")


async def _index_pool_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from datetime import timedelta

    from app.services.index_catalog import get_index_item
    from app.services.index_components import index_pool_summary

    req = IndexPoolInput.model_validate(payload)
    end = req.end_date or date.today()
    start = req.start_date or end - timedelta(days=365)
    item = get_index_item(req.index_symbol)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Unknown index symbol: {req.index_symbol}")
    if not item.pool_enabled:
        return AIToolRunResult(
            summary=f"{item.display_name} 不能作为历史股票池。",
            result={"index_symbol": item.symbol, "pool_enabled": False, "symbols": []},
        )
    data = await index_pool_summary(item.symbol, start, end)
    return AIToolRunResult(
        summary=f"{item.display_name} 指数池包含 {data.get('symbol_count', 0)} 只股票。",
        result=data,
        result_ref="/backtest",
    )


async def _sentiment_overview_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.sentiment import get_sentiment_overview

    req = SentimentOverviewInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_sentiment_overview(sources=_source_csv(req.sources), session=session)
        data = result.get("data") if isinstance(result, dict) else result
        return AIToolRunResult(summary="舆情总览已读取。", result=data, result_ref="/sentiment")

    return await _with_session(ctx, read)


async def _sentiment_summary_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.sentiment import get_sentiment_summary

    req = SentimentSymbolInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_sentiment_summary(
            symbol=symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            sources=_source_csv(req.sources),
            session=session,
        )
        data = result.get("data") if isinstance(result, dict) else result
        return AIToolRunResult(summary=f"{symbol} 舆情摘要已读取。", result=data, result_ref=f"/stock/{symbol}")

    return await _with_session(ctx, read)


async def _sentiment_posts_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.sentiment import get_sentiment_posts

    req = SentimentSymbolInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_sentiment_posts(
            symbol=symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            sources=_source_csv(req.sources),
            limit=req.limit,
            session=session,
        )
        data = result.get("data") if isinstance(result, dict) else result
        return AIToolRunResult(summary=f"{symbol} 舆情帖子已读取 {len(data or [])} 条。", result=data)

    return await _with_session(ctx, read)


async def _data_sync_submit_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.sync_proxy import proxy_sync_request

    req = DataSyncSubmitInput.model_validate(payload)
    body = req.model_dump(mode="json", exclude_none=True)
    result = await proxy_sync_request("POST", "/api/data/sync", json_body=body)
    data = result.get("data") if isinstance(result, dict) else None
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"已提交数据同步：{req.sync_type}",
        result=result,
        task_id=task_id,
        result_ref="/data/sync",
    )


async def _backtest_submit_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import RunBacktestRequest, run_backtest

    req = RunBacktestRequest.model_validate(payload.model_dump())
    result = await run_backtest(req)
    data = result.get("data") if isinstance(result, dict) else None
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"已提交回测任务：{req.strategy_name or req.strategy_id or req.engine}",
        result=result,
        task_id=task_id,
        result_ref=f"/backtest?task_id={task_id}" if task_id else "/backtest",
    )


async def _report_strategy_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.report_to_strategy import generate_strategy

    req = ReportStrategyInput.model_validate(payload)
    result = generate_strategy(req.report_text)
    return AIToolRunResult(
        summary=str(result.get("summary") or "研报策略已生成。"),
        result={**result, "report_filename": req.report_filename},
    )


async def _strategy_convert_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.llm_strategy import convert_to_akquant

    req = StrategyConvertInput.model_validate(payload)
    code = convert_to_akquant(req.source_code)
    return AIToolRunResult(summary="策略代码已转换为 AKQuant 格式。", result={"code": code})


async def _indicator_catalog_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import list_indicators

    result = await list_indicators()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="指标目录已读取。", result=data, result_ref="/factors/overview")


async def _factor_templates_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import list_templates

    data = await list_templates()
    return AIToolRunResult(summary=f"因子模板已读取 {len(data)} 个。", result=data, result_ref="/factors/list")


async def _compute_operators_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.api import list_operators

    result = await list_operators()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="计算算子目录已读取。", result=data, result_ref="/factors")


async def _compute_capabilities_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.api import capabilities

    result = await capabilities()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="计算引擎能力已读取。", result=data, result_ref="/factors")


async def _factor_list_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import list_factors

    req = FactorListInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await list_factors(category=req.category, source=req.source, session=session)
        data = _api_data(result) or []
        return AIToolRunResult(summary=f"因子列表已读取 {len(data)} 个。", result=data, result_ref="/factor")

    return await _with_session(ctx, read)


async def _factor_detail_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import get_factor

    req = IdInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_factor(factor_id=req.id, session=session)
        return AIToolRunResult(summary=f"因子 {req.id} 详情已读取。", result=_api_data(result), result_ref="/factor")

    return await _with_session(ctx, read)


async def _factor_value_definitions_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import definitions

    result = await definitions()
    data = _api_data(result) or []
    return AIToolRunResult(summary=f"因子值定义已读取 {len(data)} 个。", result=data, result_ref="/factor")


async def _factor_value_groups_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import groups

    result = await groups()
    data = _api_data(result) or []
    return AIToolRunResult(summary=f"因子值分组已读取 {len(data)} 个。", result=data, result_ref="/factor")


async def _factor_value_coverage_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import coverage

    req = FactorValueCoverageInput.model_validate(payload)
    result = await coverage(
        factor_name=req.factor_name,
        start_date=req.start_date,
        end_date=req.end_date,
        index_symbol=req.index_symbol,
        symbols=_csv(req.symbols),
        as_of_time=req.as_of_time,
        full_range=req.full_range,
    )
    return AIToolRunResult(
        summary=f"{req.factor_name} 因子值覆盖率已读取。",
        result=_api_data(result),
        result_ref="/factor",
    )


async def _factor_value_preview_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import preview

    req = FactorValuePreviewInput.model_validate(payload)
    result = await preview(
        factor_name=req.factor_name,
        trade_date=req.trade_date,
        index_symbol=req.index_symbol,
        symbols=_csv(req.symbols),
        as_of_time=req.as_of_time,
        limit=req.limit,
    )
    data = _api_data(result)
    count = len(data.get("items") or data.get("rows") or []) if isinstance(data, dict) else 0
    return AIToolRunResult(
        summary=f"{req.factor_name} 因子值预览已读取 {count} 行。",
        result=data,
        result_ref="/factor",
    )


async def _factor_research_latest_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_research import get_latest_factor_research_run

    req = FactorResearchLatestInput.model_validate(payload)
    result = await get_latest_factor_research_run(
        factor_name=req.factor_name,
        stock_pool_value=req.stock_pool_value,
        params_hash=req.params_hash,
    )
    found = _api_data(result) is not None
    return AIToolRunResult(
        summary=f"{req.factor_name} 最新研究运行{'已找到' if found else '不存在'}。",
        result=_api_data(result),
        result_ref="/factor",
    )


async def _factor_research_run_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_research import get_factor_research_run

    req = FactorResearchRunInput.model_validate(payload)
    result = await get_factor_research_run(run_id=req.run_id)
    return AIToolRunResult(summary=f"因子研究运行 {req.run_id} 已读取。", result=_api_data(result), result_ref="/factor")


async def _backtest_capabilities_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_capabilities

    result = await get_capabilities()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="回测引擎能力已读取。", result=data, result_ref="/backtest")


async def _backtest_index_pools_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import list_index_pools

    result = await list_index_pools()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary=f"回测指数池已读取 {len(data or [])} 个。", result=data, result_ref="/backtest")


async def _strategy_list_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import get_strategies

    req = PaginationInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_strategies(page=req.page, page_size=req.page_size, session=session)
        data = _api_data(result) or {}
        total = data.get("total", 0) if isinstance(data, dict) else 0
        return AIToolRunResult(summary=f"策略列表已读取，总数 {total}。", result=data, result_ref="/backtest")

    return await _with_session(ctx, read)


async def _strategy_detail_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import get_strategy

    req = IdInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_strategy(strategy_id=req.id, session=session)
        return AIToolRunResult(summary=f"策略 {req.id} 详情已读取。", result=_api_data(result), result_ref="/backtest")

    return await _with_session(ctx, read)


async def _backtest_records_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import get_backtests

    req = BacktestRecordsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_backtests(
            strategy_id=req.strategy_id,
            status=req.status,
            page=req.page,
            page_size=req.page_size,
            session=session,
        )
        data = _api_data(result) or {}
        total = data.get("total", 0) if isinstance(data, dict) else 0
        return AIToolRunResult(summary=f"回测记录已读取，总数 {total}。", result=data, result_ref="/backtest")

    return await _with_session(ctx, read)


async def _backtest_record_detail_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import get_backtest_report

    req = IdInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_backtest_report(backtest_id=req.id, session=session)
        return AIToolRunResult(summary=f"回测记录 {req.id} 报告已读取。", result=_api_data(result), result_ref="/backtest")

    return await _with_session(ctx, read)


async def _backtest_task_status_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_status

    req = BacktestTaskInput.model_validate(payload)
    result = await get_status(req.task_id)
    data = _api_data(result)
    status = data.get("status") if isinstance(data, dict) else result.get("message") if isinstance(result, dict) else "-"
    return AIToolRunResult(summary=f"回测任务 {req.task_id} 状态：{status}。", result=data, task_id=req.task_id, result_ref="/backtest")


async def _backtest_task_result_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_result

    req = BacktestTaskInput.model_validate(payload)
    result = await get_result(req.task_id)
    return AIToolRunResult(summary=f"回测任务 {req.task_id} 结果已读取。", result=_api_data(result), task_id=req.task_id, result_ref="/backtest")


async def _backtest_timer_coverage_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_timer_coverage

    req = BacktestTimerCoverageInput.model_validate(payload)
    result = await get_timer_coverage(
        index_symbol=req.index_symbol,
        symbols=_csv(req.symbols) or "",
        start_date=req.start_date.isoformat() if req.start_date else None,
        end_date=req.end_date.isoformat() if req.end_date else None,
        times=_csv(req.times) or "10:00,10:30,14:30,14:50",
    )
    return AIToolRunResult(summary="minute_timer 覆盖率已读取。", result=_api_data(result), result_ref="/backtest")


async def _explorer_tables_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import list_tables

    result = list_tables()
    data = _api_data(result) or []
    return AIToolRunResult(summary=f"数据浏览器表目录已读取 {len(data)} 张表。", result=data, result_ref="/explorer")


async def _explorer_table_preview_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import preview_table

    req = ExplorerPreviewInput.model_validate(payload)
    result = preview_table(
        table_name=req.table_name,
        page=req.page,
        page_size=req.page_size,
        order_by=req.order_by,
        order_dir=req.order_dir,
        where=req.where,
        include_total=req.include_total,
    )
    data = _api_data(result)
    rows = data.get("rows") if isinstance(data, dict) else []
    return AIToolRunResult(
        summary=f"{req.table_name} 预览已读取 {len(rows or [])} 行。",
        result=data,
        result_ref="/explorer",
    )


async def _explorer_query_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import execute_query

    req = ExplorerQueryInput.model_validate(payload)
    result = execute_query(sql=req.sql, limit=req.limit)
    data = _api_data(result)
    rows = data.get("rows") if isinstance(data, dict) else []
    return AIToolRunResult(summary=f"只读 SQL 已返回 {len(rows or [])} 行。", result=data, result_ref="/explorer")


async def _parquet_datasets_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.parquet_explorer import list_datasets

    result = await list_datasets()
    data = _api_data(result) or []
    return AIToolRunResult(summary=f"Parquet 数据集已读取 {len(data)} 个。", result=data, result_ref="/explorer")


async def _parquet_dataset_preview_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.parquet_explorer import dataset_preview

    req = ParquetDatasetInput.model_validate(payload)
    result = await dataset_preview(
        dataset=req.dataset,
        limit=req.limit,
        offset=req.offset,
        include_total=req.include_total,
    )
    data = _api_data(result)
    rows = data.get("rows") if isinstance(data, dict) else []
    return AIToolRunResult(summary=f"{req.dataset} 数据集预览已读取 {len(rows or [])} 行。", result=data, result_ref="/explorer")


async def _parquet_dataset_coverage_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.parquet_explorer import dataset_coverage

    req = ParquetCoverageInput.model_validate(payload)
    result = await dataset_coverage(
        dataset=req.dataset,
        symbols=_csv(req.symbols) or "",
        start=req.start.isoformat(),
        end=req.end.isoformat(),
    )
    return AIToolRunResult(summary=f"{req.dataset} 数据集覆盖率已读取。", result=_api_data(result), result_ref="/explorer")


async def _live_status_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import live_status

    result = await live_status()
    data = result.get("data") if isinstance(result, dict) else result
    armed = bool(isinstance(data, dict) and data.get("order_submit_enabled"))
    return AIToolRunResult(
        summary="实盘交易状态已读取。" if not armed else "实盘下单开关已开启，请保持人工确认。",
        result=result,
        result_ref="/trade",
    )


async def _live_account_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import live_account

    req = LiveQueryInput.model_validate(payload)
    result = await live_account(profile_key=req.profile_key, mode=req.mode or "paper")
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="实盘/模拟账户状态已读取。", result=data, result_ref="/trade")


async def _live_pending_orders_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import pending_orders

    req = LiveQueryInput.model_validate(payload)
    mode = req.mode or "live"
    result = await pending_orders(profile_key=req.profile_key, mode=mode, limit=req.limit, sync=True)
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary=f"{mode} 待处理订单已读取。", result=data, result_ref="/trade")


async def _live_order_audit_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import order_audit

    req = LiveQueryInput.model_validate(payload)
    result = await order_audit(profile_key=req.profile_key, mode=req.mode, limit=req.limit)
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="订单审计记录已读取。", result=data, result_ref="/trade")


async def _live_trades_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import trade_records

    req = LiveQueryInput.model_validate(payload)
    result = await trade_records(profile_key=req.profile_key, mode=req.mode, limit=req.limit)
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="成交记录已读取。", result=data, result_ref="/trade")


_registry: AIToolRegistry | None = None


def get_ai_tool_registry() -> AIToolRegistry:
    global _registry
    if _registry is None:
        registry = AIToolRegistry()
        registry.register(
            AITool(
                name="system.status",
                title="系统状态",
                description="读取后端运行状态、行情后端和同步服务健康信息。",
                category="system",
                input_model=EmptyInput,
                handler=_system_status_handler,
            )
        )
        registry.register(
            AITool(
                name="system.data_summary",
                title="数据总览",
                description="读取行情、财务、因子、舆情等本地数据新鲜度。",
                category="system",
                input_model=EmptyInput,
                handler=_system_data_summary_handler,
            )
        )
        registry.register(
            AITool(
                name="runtime.tasks",
                title="运行任务",
                description="列出回测、同步、因子预计算等运行任务。",
                category="system",
                input_model=RuntimeTasksInput,
                handler=_runtime_tasks_handler,
            )
        )
        registry.register(
            AITool(
                name="system.tool_catalog",
                title="工具目录",
                description="列出当前 AI tool registry 已注册的全部平台工具和分类。",
                category="system",
                input_model=ToolCatalogInput,
                handler=_tool_catalog_handler,
            )
        )
        registry.register(
            AITool(
                name="data.stock_snapshot",
                title="股票快照",
                description="通过 DataSkill 读取单只 A 股基础快照。",
                category="data",
                input_model=StockSnapshotInput,
                handler=_stock_snapshot_handler,
            )
        )
        registry.register(
            AITool(
                name="data.stock_batch",
                title="批量股票快照",
                description="通过 DataSkill 批量读取 A 股基础快照。",
                category="data",
                input_model=StockBatchInput,
                handler=_stock_batch_handler,
            )
        )
        registry.register(
            AITool(
                name="data.stock_screen",
                title="条件选股",
                description="按行业、交易所、ST、市值、PE、ROE 等条件筛选股票。",
                category="data",
                input_model=StockScreenInput,
                handler=_stock_screen_handler,
            )
        )
        registry.register(
            AITool(
                name="data.kline_daily",
                title="日 K 行情",
                description="通过 DataSkill 读取单只股票日 K 线，优先本地 Parquet/DuckDB。",
                category="data",
                input_model=KlineInput,
                handler=_kline_daily_handler,
            )
        )
        registry.register(
            AITool(
                name="data.kline_minute",
                title="分钟 K 行情",
                description="通过 DataSkill 读取单只股票分钟 K 线，可按 timer_times 过滤固定时点。",
                category="data",
                input_model=KlineInput,
                handler=_kline_minute_handler,
            )
        )
        registry.register(
            AITool(
                name="data.market_snapshot",
                title="综合行情快照",
                description="读取股票快照、最近日 K，并尽量附带实时行情；实时不可用时仍返回本地行情。",
                category="data",
                input_model=MarketSnapshotInput,
                handler=_market_snapshot_handler,
            )
        )
        registry.register(
            AITool(
                name="data.realtime_quote",
                title="实时行情",
                description="通过 QMT 网关读取单只股票实时行情。",
                category="data",
                input_model=QuoteInput,
                handler=_quote_handler,
            )
        )
        registry.register(
            AITool(
                name="data.realtime_quotes",
                title="批量实时行情",
                description="通过 QMT 网关批量读取股票实时行情。",
                category="data",
                input_model=QuoteBatchInput,
                handler=_quote_batch_handler,
            )
        )
        registry.register(
            AITool(
                name="data.financial",
                title="财务报告",
                description="读取单只股票最近多期财务报告。",
                category="data",
                input_model=FinancialInput,
                handler=_financial_handler,
            )
        )
        registry.register(
            AITool(
                name="data.financial_batch",
                title="批量财务报告",
                description="批量读取多只股票最近财务报告。",
                category="data",
                input_model=FinancialBatchInput,
                handler=_financial_batch_handler,
            )
        )
        registry.register(
            AITool(
                name="data.industries",
                title="行业列表",
                description="读取本地股票行业列表及行业股票数量。",
                category="data",
                input_model=EmptyInput,
                handler=_industries_handler,
            )
        )
        registry.register(
            AITool(
                name="data.symbols",
                title="股票代码列表",
                description="读取全部股票代码，或按行业筛选股票代码。",
                category="data",
                input_model=SymbolsInput,
                handler=_symbols_handler,
            )
        )
        registry.register(
            AITool(
                name="data.watchlist_groups",
                title="自选股分组",
                description="读取自选股分组列表和每组股票数量。",
                category="data",
                input_model=EmptyInput,
                handler=_watchlist_groups_handler,
            )
        )
        registry.register(
            AITool(
                name="data.watchlist_stocks",
                title="自选股明细",
                description="读取指定自选股分组内的股票及最新本地指标。",
                category="data",
                input_model=WatchlistStocksInput,
                handler=_watchlist_stocks_handler,
            )
        )
        registry.register(
            AITool(
                name="data.sync_catalog",
                title="同步目录",
                description="读取数据同步服务支持的同步任务目录。",
                category="data",
                input_model=SyncCatalogInput,
                handler=_sync_catalog_handler,
            )
        )
        registry.register(
            AITool(
                name="data.sync_status",
                title="同步状态",
                description="读取当前或最近一次数据同步状态。",
                category="data",
                input_model=EmptyInput,
                handler=_sync_status_handler,
            )
        )
        registry.register(
            AITool(
                name="data.sync_logs",
                title="同步日志",
                description="读取数据同步日志，可按同步类型或任务 ID 过滤。",
                category="data",
                input_model=SyncLogsInput,
                handler=_sync_logs_handler,
            )
        )
        registry.register(
            AITool(
                name="data.indicator_value",
                title="指标值",
                description="查询单只股票某个指标在指定日期或最新日期的值。",
                category="data",
                input_model=IndicatorValueInput,
                handler=_indicator_value_handler,
            )
        )
        registry.register(
            AITool(
                name="data.indicator_batch",
                title="批量截面指标",
                description="批量查询股票截面指标值。",
                category="data",
                input_model=IndicatorBatchInput,
                handler=_indicator_batch_handler,
            )
        )
        registry.register(
            AITool(
                name="data.indicator_timeseries",
                title="指标时序",
                description="查询单只股票多个指标的历史时序。",
                category="data",
                input_model=IndicatorTimeseriesInput,
                handler=_indicator_timeseries_handler,
            )
        )
        registry.register(
            AITool(
                name="data.index_catalog",
                title="指数目录",
                description="读取平台支持的指数、基准和可用历史股票池目录。",
                category="data",
                input_model=IndexCatalogInput,
                handler=_index_catalog_handler,
            )
        )
        registry.register(
            AITool(
                name="data.index_pool",
                title="指数历史成分池",
                description="按日期范围读取指数历史成分股票池摘要和代码列表。",
                category="data",
                input_model=IndexPoolInput,
                handler=_index_pool_handler,
            )
        )
        registry.register(
            AITool(
                name="data.sync_submit",
                title="提交数据同步",
                description="提交数据同步任务到隔离同步服务。",
                category="data",
                input_model=DataSyncSubmitInput,
                handler=_data_sync_submit_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="explorer.tables",
                title="数据表目录",
                description="读取数据浏览器可用表和 Parquet 快速摘要。",
                category="explorer",
                input_model=EmptyInput,
                handler=_explorer_tables_handler,
            )
        )
        registry.register(
            AITool(
                name="explorer.table_preview",
                title="数据表预览",
                description="预览数据浏览器中指定表的行数据。",
                category="explorer",
                input_model=ExplorerPreviewInput,
                handler=_explorer_table_preview_handler,
            )
        )
        registry.register(
            AITool(
                name="explorer.sql_query",
                title="只读 SQL 查询",
                description="在数据浏览器执行只读 SELECT 查询。",
                category="explorer",
                input_model=ExplorerQueryInput,
                handler=_explorer_query_handler,
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="parquet.datasets",
                title="Parquet 数据集",
                description="读取本地 Parquet 数据湖的数据集目录和分区摘要。",
                category="explorer",
                input_model=EmptyInput,
                handler=_parquet_datasets_handler,
            )
        )
        registry.register(
            AITool(
                name="parquet.dataset_preview",
                title="Parquet 预览",
                description="预览指定 Parquet 数据集行数据。",
                category="explorer",
                input_model=ParquetDatasetInput,
                handler=_parquet_dataset_preview_handler,
            )
        )
        registry.register(
            AITool(
                name="parquet.dataset_coverage",
                title="Parquet 覆盖率",
                description="查询指定 Parquet 数据集的代码和日期覆盖范围。",
                category="explorer",
                input_model=ParquetCoverageInput,
                handler=_parquet_dataset_coverage_handler,
            )
        )
        registry.register(
            AITool(
                name="indicator.catalog",
                title="指标目录",
                description="读取 Indicator 体系的指标分类、元数据和预计算状态。",
                category="factor",
                input_model=EmptyInput,
                handler=_indicator_catalog_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.templates",
                title="因子模板",
                description="读取表达式因子模板和内置因子示例。",
                category="factor",
                input_model=EmptyInput,
                handler=_factor_templates_handler,
            )
        )
        registry.register(
            AITool(
                name="compute.operators",
                title="计算算子",
                description="读取 Compute Engine 支持的 L0-L3 算子目录。",
                category="factor",
                input_model=EmptyInput,
                handler=_compute_operators_handler,
            )
        )
        registry.register(
            AITool(
                name="compute.capabilities",
                title="计算能力",
                description="读取 Compute/TA/AKQuant 因子计算能力。",
                category="factor",
                input_model=EmptyInput,
                handler=_compute_capabilities_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.list",
                title="因子列表",
                description="读取因子研究库中的因子列表，可按分类或来源过滤。",
                category="factor",
                input_model=FactorListInput,
                handler=_factor_list_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.detail",
                title="因子详情",
                description="读取指定因子的代码、参数和描述。",
                category="factor",
                input_model=IdInput,
                handler=_factor_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.definitions",
                title="因子值定义",
                description="读取 Factor Value Store 支持的因子值定义。",
                category="factor",
                input_model=EmptyInput,
                handler=_factor_value_definitions_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.groups",
                title="因子值分组",
                description="读取 Factor Value Store 的内置因子分组。",
                category="factor",
                input_model=EmptyInput,
                handler=_factor_value_groups_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.coverage",
                title="因子值覆盖率",
                description="查询指定因子值在股票池和日期区间内的缓存覆盖率。",
                category="factor",
                input_model=FactorValueCoverageInput,
                handler=_factor_value_coverage_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.preview",
                title="因子值预览",
                description="预览指定交易日的因子值截面。",
                category="factor",
                input_model=FactorValuePreviewInput,
                handler=_factor_value_preview_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_research.latest_run",
                title="最新因子研究",
                description="读取指定因子最近一次研究运行摘要。",
                category="factor",
                input_model=FactorResearchLatestInput,
                handler=_factor_research_latest_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_research.run_detail",
                title="因子研究详情",
                description="读取指定因子研究运行的完整结果。",
                category="factor",
                input_model=FactorResearchRunInput,
                handler=_factor_research_run_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.capabilities",
                title="回测能力",
                description="读取 builtin/AKQuant 回测引擎能力和可用特性。",
                category="backtest",
                input_model=EmptyInput,
                handler=_backtest_capabilities_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.index_pools",
                title="回测指数池",
                description="读取回测可用指数池列表。",
                category="backtest",
                input_model=EmptyInput,
                handler=_backtest_index_pools_handler,
            )
        )
        registry.register(
            AITool(
                name="strategy.list",
                title="策略列表",
                description="读取策略库列表和分页信息。",
                category="strategy",
                input_model=PaginationInput,
                handler=_strategy_list_handler,
            )
        )
        registry.register(
            AITool(
                name="strategy.detail",
                title="策略详情",
                description="读取指定策略的代码、参数和描述。",
                category="strategy",
                input_model=IdInput,
                handler=_strategy_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.records",
                title="回测记录",
                description="读取历史回测记录，可按策略或状态过滤。",
                category="backtest",
                input_model=BacktestRecordsInput,
                handler=_backtest_records_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.record_detail",
                title="回测报告",
                description="读取指定历史回测记录的详细报告。",
                category="backtest",
                input_model=IdInput,
                handler=_backtest_record_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.task_status",
                title="回测任务状态",
                description="读取异步回测或优化任务状态。",
                category="backtest",
                input_model=BacktestTaskInput,
                handler=_backtest_task_status_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.task_result",
                title="回测任务结果",
                description="读取已完成异步回测或优化任务结果。",
                category="backtest",
                input_model=BacktestTaskInput,
                handler=_backtest_task_result_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.timer_coverage",
                title="Timer 分钟覆盖",
                description="查询 minute_timer 回测所需固定时点分钟线覆盖范围。",
                category="backtest",
                input_model=BacktestTimerCoverageInput,
                handler=_backtest_timer_coverage_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.submit",
                title="提交回测",
                description="按现有 BacktestConfig 契约提交异步回测任务。",
                category="backtest",
                input_model=_backtest_input_model(),
                handler=_backtest_submit_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="sentiment.overview",
                title="舆情总览",
                description="读取统一舆情模块概览和数据源状态。",
                category="sentiment",
                input_model=SentimentOverviewInput,
                handler=_sentiment_overview_handler,
            )
        )
        registry.register(
            AITool(
                name="sentiment.summary",
                title="个股舆情摘要",
                description="读取单只股票缓存舆情摘要。",
                category="sentiment",
                input_model=SentimentSymbolInput,
                handler=_sentiment_summary_handler,
            )
        )
        registry.register(
            AITool(
                name="sentiment.posts",
                title="个股舆情帖子",
                description="读取单只股票缓存舆情帖子列表。",
                category="sentiment",
                input_model=SentimentSymbolInput,
                handler=_sentiment_posts_handler,
            )
        )
        registry.register(
            AITool(
                name="report.strategy_generate",
                title="研报生成策略",
                description="从研报文本提取逻辑并生成平台策略候选。",
                category="strategy",
                input_model=ReportStrategyInput,
                handler=_report_strategy_handler,
                risk_level="write",
            )
        )
        registry.register(
            AITool(
                name="strategy.convert_to_akquant",
                title="转换 AKQuant 策略",
                description="把旧框架策略代码转换为 AKQuant Strategy 代码。",
                category="strategy",
                input_model=StrategyConvertInput,
                handler=_strategy_convert_handler,
                risk_level="write",
            )
        )
        registry.register(
            AITool(
                name="live_trading.status",
                title="实盘状态",
                description="只读取实盘桥接、下单护栏和 runner 状态，不提交订单。",
                category="trading",
                input_model=EmptyInput,
                handler=_live_status_handler,
            )
        )
        registry.register(
            AITool(
                name="live_trading.account",
                title="实盘账户",
                description="读取实盘/模拟账户状态和权益信息，不提交订单。",
                category="trading",
                input_model=LiveQueryInput,
                handler=_live_account_handler,
            )
        )
        registry.register(
            AITool(
                name="live_trading.pending_orders",
                title="待处理订单",
                description="读取实盘/模拟待处理订单，不取消、不重报。",
                category="trading",
                input_model=LiveQueryInput,
                handler=_live_pending_orders_handler,
            )
        )
        registry.register(
            AITool(
                name="live_trading.order_audit",
                title="订单审计",
                description="读取订单审计记录。",
                category="trading",
                input_model=LiveQueryInput,
                handler=_live_order_audit_handler,
            )
        )
        registry.register(
            AITool(
                name="live_trading.trades",
                title="成交记录",
                description="读取实盘/模拟成交记录。",
                category="trading",
                input_model=LiveQueryInput,
                handler=_live_trades_handler,
            )
        )
        _registry = registry
    return _registry


def _backtest_input_model() -> type[BaseModel]:
    from app.backtest.api import RunBacktestRequest

    return RunBacktestRequest
