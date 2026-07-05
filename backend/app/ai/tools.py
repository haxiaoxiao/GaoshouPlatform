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
    AIWorkflowRunRequest,
)
from app.db.sqlite import async_session_factory
from app.services.runtime_tasks import list_tasks
from app.services.security_symbols import normalize_security_symbol


class EmptyInput(BaseModel):
    pass


class RuntimeTasksInput(BaseModel):
    include_finished: bool = True


class RuntimeTaskInput(BaseModel):
    task_id: str = Field(min_length=1)


class ToolCatalogInput(BaseModel):
    category: str | None = None


class AIDiagnosticsInput(BaseModel):
    limit: int = Field(default=50, ge=1, le=200)


class AIArtifactsInput(BaseModel):
    kind: str | None = None
    limit: int = Field(default=50, ge=1, le=200)


class AIArtifactInput(BaseModel):
    artifact_id: str = Field(min_length=1)


class StockSnapshotInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")


class StockDetailInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")


class StockReviewInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")
    as_of_date: date | None = None
    lookback_days: int = Field(default=60, ge=1, le=365)


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


class StockListInput(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=500)
    search: str | None = None
    industry: str | None = None
    exchange: str | None = None
    is_st: int | None = None
    group_id: int | None = Field(default=None, ge=1)


class KlineInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")
    start_date: date | None = None
    end_date: date | None = None
    limit: int = Field(default=120, ge=1, le=5000)
    timer_times: list[str] | None = None


class KlinesQueryInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH")
    period: str = Field(default="daily", pattern=r"^(daily|minute)$")
    start_date: date | None = None
    end_date: date | None = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=1000)


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


class AkshareDailyInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH or sh600519")
    start_date: str = Field(pattern=r"^\d{8}$", description="YYYYMMDD")
    end_date: str = Field(pattern=r"^\d{8}$", description="YYYYMMDD")
    adjust: str = "qfq"


class AkshareDailyBatchInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=50)
    start_date: str = Field(pattern=r"^\d{8}$", description="YYYYMMDD")
    end_date: str = Field(pattern=r"^\d{8}$", description="YYYYMMDD")
    adjust: str = "qfq"


class AkshareListInput(BaseModel):
    limit: int = Field(default=500, ge=1, le=10000)


class AkshareSpotInput(BaseModel):
    limit: int = Field(default=200, ge=1, le=10000)


class AkshareInfoInput(BaseModel):
    symbol: str = Field(description="A-share symbol, e.g. 600519.SH or sh600519")


class AkshareHistInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=50)
    start_date: str = Field(pattern=r"^\d{8}$", description="YYYYMMDD")
    end_date: str = Field(pattern=r"^\d{8}$", description="YYYYMMDD")
    adjust: str = "qfq"


class SymbolsInput(BaseModel):
    industry: str | None = None
    limit: int = Field(default=500, ge=1, le=10000)


class WatchlistStocksInput(BaseModel):
    group_id: int = Field(ge=1)


class WatchlistStockMutationInput(BaseModel):
    group_id: int = Field(ge=1)
    symbol: str = Field(min_length=1)


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


class IndicatorTimeseriesBatchInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=500)
    names: list[str] = Field(min_length=1, max_length=50)
    start_date: date
    end_date: date
    limit: int = Field(default=200000, ge=1, le=500000)


class IndicatorDescriptionInput(BaseModel):
    name: str = Field(min_length=1)


class IndicatorQueryInput(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=500)
    indicator_names: list[str] = Field(min_length=1, max_length=50)
    trade_date: date | None = None


class IndicatorComputeInput(BaseModel):
    indicator_names: list[str] | None = None
    symbols: list[str] | None = None
    full_compute: bool = False


class IndicatorScreenInput(BaseModel):
    filters: list[dict[str, Any]] = Field(min_length=1)
    trade_date: date | None = None
    sort_by: str | None = None
    sort_order: str = "desc"
    limit: int = Field(default=50, ge=1, le=500)


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


class IdsInput(BaseModel):
    ids: list[int] = Field(min_length=1, max_length=500)


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


class ExplorerTableInput(BaseModel):
    table_name: str = Field(min_length=1)


class ExplorerFilterInput(BaseModel):
    column: str
    op: str = "="
    value: Any = None
    value_to: Any = None
    values: list[Any] | None = None


class ExplorerSearchInput(BaseModel):
    table_name: str = Field(min_length=1)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)
    order_by: str | None = None
    order_dir: str = "ASC"
    columns: list[str] | None = None
    filters: list[ExplorerFilterInput] = Field(default_factory=list)
    quick_search: dict[str, Any] = Field(default_factory=dict)
    include_total: bool = False


class ExplorerDistinctInput(BaseModel):
    table_name: str = Field(min_length=1)
    column: str = Field(min_length=1)
    q: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)


class ExplorerQueryInput(BaseModel):
    sql: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=200, ge=1, le=1000)


class ParquetDatasetInput(BaseModel):
    dataset: str = Field(min_length=1)
    limit: int = Field(default=20, ge=1, le=200)
    offset: int = Field(default=0, ge=0)
    include_total: bool = False


class ParquetSchemaInput(BaseModel):
    dataset: str = Field(min_length=1)


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


class ComputePrecomputeInput(BaseModel):
    expressions: list[str] = Field(min_length=1)
    symbols: list[str] = Field(min_length=1, max_length=500)
    start_date: date
    end_date: date
    engine: str = "builtin"


class ComputeBatchInput(BaseModel):
    configs: list[dict[str, Any]] = Field(min_length=1, max_length=50)


class ComputeValidateInput(BaseModel):
    expression: str = Field(min_length=1)


class ComputeEvaluateInput(BaseModel):
    expression: str = Field(min_length=1)
    symbols: list[str] = Field(min_length=1, max_length=200)
    start_date: date
    end_date: date
    use_cache: bool = True
    engine: str = "builtin"


class ComputeScreenInput(BaseModel):
    condition: str = Field(min_length=1)
    universe: str = "all"
    trade_date: date
    limit: int = Field(default=50, ge=1, le=500)


class FactorParamHashInput(BaseModel):
    factor_names: list[str] = Field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    symbols: list[str] | None = None
    index_symbol: str | None = None
    limit_per_factor: int = Field(default=12, ge=1, le=50)


class FactorQueryInput(BaseModel):
    factor_name: str = Field(min_length=1)
    trade_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    as_of_time: str | None = None
    params: dict[str, Any] | None = None


class PaperFeatureSnapshotInput(BaseModel):
    factor_names: list[str] = Field(default_factory=list)
    trade_dates: list[date] = Field(default_factory=list)
    symbols: list[str] | None = None
    min_feature_coverage: float = Field(default=0.0, ge=0.0, le=1.0)
    limit: int = Field(default=200, ge=1, le=1000)


class EvaluationConfigInput(BaseModel):
    config: dict[str, Any]
    eval_config: dict[str, Any] | None = None


class FactorBoardInput(BaseModel):
    query: dict[str, Any] = Field(default_factory=dict)


class SavedFactorPreviewInput(BaseModel):
    factor_id: int = Field(ge=1)
    symbols: list[str] | None = None
    stock_pool: str | None = None
    start_date: date
    end_date: date
    limit: int = Field(default=200, ge=1, le=5000)


class SavedPythonFactorRunInput(SavedFactorPreviewInput):
    params: dict[str, Any] = Field(default_factory=dict)


class SavedFactorPrecomputeInput(BaseModel):
    factor_id: int = Field(ge=1)
    symbols: list[str] | None = None
    stock_pool: str | None = None
    start_date: date
    end_date: date
    as_of_time: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class SavedFactorCoverageInput(BaseModel):
    factor_id: int = Field(ge=1)
    start_date: date
    end_date: date


class SavedFactorAnalyzeInput(BaseModel):
    factor_id: int = Field(ge=1)
    symbols: list[str] | None = None
    stock_pool: str | None = None
    benchmark: str | None = None
    start_date: date
    end_date: date
    n_groups: int = Field(default=5, ge=2, le=20)
    group_count: int | None = Field(default=None, ge=2, le=20)
    direction: str | None = None
    rebalance_period: str | None = None
    ic_method: str | None = None
    outlier_handling: str | None = None
    standardize: bool | None = None
    industry_neutralization: bool | None = None
    include_st: bool | None = None
    include_new: bool | None = None
    filter_limit_up: bool | None = None
    filter_limit_down: bool | None = None
    fee_rate: float | None = None
    slippage: float | None = None
    use_cache: bool | None = None
    write_cache: bool | None = None


class LegacyFactorUpdateInput(BaseModel):
    id: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=50)
    category: str | None = None
    source: str | None = None
    code: str | None = None
    parameters: dict[str, Any] | None = None
    description: str | None = None


class FactorAnalysisListInput(BaseModel):
    factor_id: int | None = None
    limit: int = Field(default=20, ge=1, le=100)


class SavedFactorUpdateInput(BaseModel):
    factor_id: int = Field(ge=1)
    name: str | None = None
    expression: str | None = None
    source_type: str | None = None
    engine: str | None = None
    stock_pool: str | None = None
    direction: str | None = None
    default_stock_pool: str | None = None
    default_benchmark: str | None = None
    cache_enabled: bool | None = None
    default_eval_config: dict[str, Any] | None = None
    category: str | None = None
    description: str | None = None
    params: dict[str, Any] | None = None


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


class SentimentThreadsInput(BaseModel):
    start_date: date | None = None
    end_date: date | None = None
    sources: list[str] | None = None
    symbol: str | None = None
    limit: int = Field(default=50, ge=1, le=200)


class ReportStrategyInput(BaseModel):
    report_text: str = Field(min_length=1)
    report_filename: str = "report.txt"


class StrategyConvertInput(BaseModel):
    source_code: str = Field(min_length=1)


class ReportChatSessionInput(BaseModel):
    report_text: str = Field(min_length=1)
    report_filename: str = "report.txt"


class ReportChatMessageInput(BaseModel):
    session_id: str = Field(min_length=1)
    message: str = Field(min_length=1)


class WorkflowRunInput(AIWorkflowRunRequest):
    workflow_name: str = Field(description="Workflow name, e.g. CommandGraph")


class StockNamesInput(BaseModel):
    symbols: list[str] = Field(default_factory=list, max_length=1000)


class PoolSymbolsInput(BaseModel):
    pool_name: str = Field(default="all", min_length=1)


class BacktestFactorInput(BaseModel):
    config: dict[str, Any]
    bt_config: dict[str, Any] | None = None


class BacktestReportInput(BaseModel):
    task_id: str = Field(min_length=1)


class LiveProfilesInput(BaseModel):
    include_disabled: bool = True


class LiveWeeklyTradesInput(BaseModel):
    profile_key: str | None = None
    mode: str | None = None
    week_start: str | None = None


class StrategyUpdateToolInput(BaseModel):
    id: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=100)
    code: str | None = None
    parameters: dict[str, Any] | None = None
    description: str | None = None


class StrategySignalsInput(BaseModel):
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    use_watchlist: bool = False


class LiveProfileUpdateToolInput(BaseModel):
    profile_key: str = Field(min_length=1)
    strategy_id: int | None = None
    display_name: str | None = None
    description: str | None = None
    enabled: bool | None = None
    is_default: bool | None = None
    adapter_type: str | None = None
    params_override: dict[str, Any] | None = None
    universe_config: dict[str, Any] | None = None
    execution_policy: dict[str, Any] | None = None


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


def _api_data_or_raise(result: Any) -> Any:
    if isinstance(result, dict) and result.get("code") not in (None, 0):
        detail = result.get("message") or result.get("error") or "platform API returned an error"
        raise HTTPException(status_code=400, detail=str(detail))
    return _api_data(result)


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


async def _runtime_task_detail_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.runtime_tasks import get_task

    req = RuntimeTaskInput.model_validate(payload)
    task = get_task(req.task_id)
    if task is None:
        return AIToolRunResult(summary=f"运行任务 {req.task_id} 不存在。", result=None)
    return AIToolRunResult(summary=f"运行任务 {req.task_id} 已读取。", result=task)


async def _system_health_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import health_check

    result = await health_check()
    return AIToolRunResult(summary=f"健康检查：{result.get('status', '-')}。", result=result)


async def _system_cache_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import get_cache_status

    result = await get_cache_status()
    redis = result.get("redis") if isinstance(result, dict) else {}
    available = bool(isinstance(redis, dict) and redis.get("available"))
    return AIToolRunResult(
        summary=f"缓存状态已读取，Redis {'可用' if available else '不可用'}。",
        result=result,
        result_ref="/system",
    )


async def _system_dev_data_mode_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import get_dev_data_mode

    result = await get_dev_data_mode()
    enabled = bool(isinstance(result, dict) and result.get("enabled"))
    using_prod = bool(isinstance(result, dict) and result.get("use_prod_data"))
    return AIToolRunResult(
        summary=f"Dev data mode 已读取：{'启用' if enabled else '不可用'}，生产数据目录 {'开启' if using_prod else '关闭'}。",
        result=result,
        result_ref="/system",
    )


async def _system_update_dev_data_mode_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import DevDataModeUpdate, update_dev_data_mode

    req = DevDataModeUpdate.model_validate(payload.model_dump())
    result = await update_dev_data_mode(req)
    return AIToolRunResult(summary="Dev data mode 已更新。", result=result, result_ref="/system")


async def _system_live_trading_guardrails_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import get_live_trading_guardrails

    result = await get_live_trading_guardrails()
    order_enabled = bool(isinstance(result, dict) and result.get("enable_order_submit"))
    auto_enabled = bool(isinstance(result, dict) and result.get("auto_execute_enabled"))
    return AIToolRunResult(
        summary=f"实盘防护已读取：下单 {'开启' if order_enabled else '关闭'}，自动执行 {'开启' if auto_enabled else '关闭'}。",
        result=result,
        result_ref="/system",
    )


async def _system_update_live_trading_guardrails_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.system import LiveTradingGuardrailsUpdate, update_live_trading_guardrails

    req = LiveTradingGuardrailsUpdate.model_validate(payload.model_dump())
    result = await update_live_trading_guardrails(req)
    return AIToolRunResult(summary="实盘防护开关已更新。", result=result, result_ref="/system")


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


async def _mcp_manifest_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.ai.manifest import build_ai_tool_manifest

    manifest = build_ai_tool_manifest()
    return AIToolRunResult(
        summary=f"MCP/HTTP 工具 manifest 已读取，包含 {manifest['counts']['tools']} 个工具。",
        result=manifest,
        result_ref="/system",
    )


async def _ai_diagnostics_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.ai.artifacts import list_artifacts
    from app.ai.diagnostics import build_ai_diagnostics
    from app.ai.gateway import get_llm_gateway
    from app.ai.manifest import build_ai_tool_manifest
    from app.core.config import settings

    req = AIDiagnosticsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        artifacts = await list_artifacts(session, limit=req.limit)
        diagnostics = build_ai_diagnostics(
            enabled=bool(settings.ai_enabled),
            gateway=get_llm_gateway().status(),
            manifest=build_ai_tool_manifest(),
            artifacts=artifacts,
            sample_limit=req.limit,
        )
        return AIToolRunResult(
            summary=(
                "AI Native 诊断已读取："
                f"{diagnostics['manifest']['tool_count']} 个工具，"
                f"健康状态 {diagnostics['health']['status']}。"
            ),
            result=diagnostics,
            result_ref="/system",
        )

    return await _with_session(ctx, read)


async def _ai_artifacts_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.ai.artifacts import list_artifacts

    req = AIArtifactsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        artifacts = await list_artifacts(session, kind=req.kind, limit=req.limit)
        kind_label = req.kind or "全部"
        return AIToolRunResult(
            summary=f"AI artifact 已读取 {len(artifacts)} 条（{kind_label}）。",
            result={"items": artifacts, "kind": req.kind, "limit": req.limit},
            result_ref="/system",
        )

    return await _with_session(ctx, read)


async def _ai_artifact_detail_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.ai.artifacts import get_artifact

    req = AIArtifactInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        artifact = await get_artifact(session, req.artifact_id)
        if artifact is None:
            raise ValueError(f"AI artifact {req.artifact_id} not found")
        return AIToolRunResult(
            summary=f"AI artifact {req.artifact_id} 已读取。",
            result=artifact,
            result_ref="/system",
        )

    return await _with_session(ctx, read)


async def _workflow_catalog_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.ai.workflows import list_ai_workflows

    workflows = [workflow.model_dump() for workflow in list_ai_workflows()]
    return AIToolRunResult(
        summary=f"AI workflow registry 已读取，包含 {len(workflows)} 张图。",
        result={"workflows": workflows},
        result_ref="/system",
    )


async def _run_workflow_by_name(
    workflow_name: str,
    request: AIWorkflowRunRequest,
    ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.ai.workflows import run_ai_workflow

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await run_ai_workflow(workflow_name, request, session=session)
        return AIToolRunResult(
            summary=result.summary,
            result=result.model_dump(),
            result_ref="/system",
        )

    return await _with_session(ctx, run)


async def _workflow_run_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    req = WorkflowRunInput.model_validate(payload)
    request = AIWorkflowRunRequest(**req.model_dump(exclude={"workflow_name"}))
    return await _run_workflow_by_name(req.workflow_name, request, ctx)


async def _command_graph_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    req = AIWorkflowRunRequest.model_validate(payload)
    return await _run_workflow_by_name("CommandGraph", req, ctx)


async def _report_strategy_graph_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    req = AIWorkflowRunRequest.model_validate(payload)
    return await _run_workflow_by_name("ReportStrategyGraph", req, ctx)


async def _quant_research_graph_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    req = AIWorkflowRunRequest.model_validate(payload)
    return await _run_workflow_by_name("QuantResearchGraph", req, ctx)


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


async def _stock_detail_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_stock_detail

    req = StockDetailInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_stock_detail(symbol=symbol, session=session)
        data = _api_data_or_raise(result)
        name = data.get("name") if isinstance(data, dict) else ""
        return AIToolRunResult(
            summary=f"{symbol} {name or ''} 前端详情已读取。",
            result=data,
            result_ref=f"/stock/{symbol}",
        )

    return await _with_session(ctx, read)


async def _stock_review_context_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_skill import get_stock_review_context

    req = StockReviewInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_stock_review_context(
            symbol=symbol,
            as_of_date=req.as_of_date,
            lookback_days=req.lookback_days,
            session=session,
        )
        data = _api_data_or_raise(result)
        return AIToolRunResult(
            summary=f"{symbol} review context 已读取，回看 {req.lookback_days} 天。",
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


async def _stock_list_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_stocks

    req = StockListInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_stocks(
            page=req.page,
            page_size=req.page_size,
            search=req.search,
            industry=req.industry,
            exchange=req.exchange,
            is_st=req.is_st,
            group_id=req.group_id,
            session=session,
        )
        data = _api_data_or_raise(result)
        total = data.get("total", 0) if isinstance(data, dict) else 0
        items = data.get("items") if isinstance(data, dict) else []
        return AIToolRunResult(
            summary=f"股票列表已读取第 {req.page} 页，返回 {len(items or [])} / {total} 只。",
            result=data,
            result_ref="/data/stock-list",
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


async def _klines_query_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import get_klines

    req = KlinesQueryInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_klines(
            symbol=symbol,
            period=req.period,
            start_date=req.start_date,
            end_date=req.end_date,
            page=req.page,
            page_size=req.page_size,
            session=session,
        )
        data = _api_data_or_raise(result)
        items = data.get("items") if isinstance(data, dict) else []
        total = data.get("total", len(items or [])) if isinstance(data, dict) else len(items or [])
        period_label = "分钟 K" if req.period == "minute" else "日 K"
        return AIToolRunResult(
            summary=f"{symbol} {period_label} 查询已读取第 {req.page} 页，返回 {len(items or [])} / {total} 条。",
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


async def _akshare_daily_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.akshare import get_stock_daily

    req = AkshareDailyInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip()
    result = await get_stock_daily(
        symbol=symbol,
        start_date=req.start_date,
        end_date=req.end_date,
        adjust=req.adjust,
    )
    data = _api_data_or_raise(result)
    count = data.get("count", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(
        summary=f"AKShare 日线已读取 {symbol}，{count} 条。",
        result=data,
        result_ref=f"/stock/{symbol}",
    )


async def _akshare_daily_batch_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.akshare import BatchDailyRequest, get_stock_daily_batch

    req = AkshareDailyBatchInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip() for symbol in req.symbols]
    result = await get_stock_daily_batch(
        BatchDailyRequest(
            symbols=symbols,
            start_date=req.start_date,
            end_date=req.end_date,
            adjust=req.adjust,
        )
    )
    data = _api_data_or_raise(result)
    count = data.get("count", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"AKShare 批量日线已读取 {len(symbols)} 只，{count} 条。", result=data)


async def _akshare_spot_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.akshare import get_stock_spot

    req = AkshareSpotInput.model_validate(payload)
    result = await get_stock_spot()
    data = _api_data_or_raise(result)
    if isinstance(data, dict):
        records = data.get("records") or []
        data = {**data, "records": records[: req.limit], "returned": min(len(records), req.limit)}
    count = data.get("count", 0) if isinstance(data, dict) else 0
    returned = data.get("returned", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"AKShare 实时全市场快照已读取 {count} 条，返回 {returned} 条。", result=data)


async def _akshare_stock_list_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.akshare import get_stock_list

    req = AkshareListInput.model_validate(payload)
    result = await get_stock_list()
    data = _api_data_or_raise(result)
    if isinstance(data, dict):
        records = data.get("records") or []
        data = {**data, "records": records[: req.limit], "returned": min(len(records), req.limit)}
    count = data.get("count", 0) if isinstance(data, dict) else 0
    returned = data.get("returned", 0) if isinstance(data, dict) else 0
    source = data.get("source", "akshare") if isinstance(data, dict) else "akshare"
    return AIToolRunResult(summary=f"AKShare 股票列表已读取 {count} 条，返回 {returned} 条，来源 {source}。", result=data)


async def _akshare_stock_info_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.akshare import get_stock_info

    req = AkshareInfoInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip()
    result = await get_stock_info(symbol=symbol)
    data = _api_data_or_raise(result)
    info_count = len(data.get("info") or {}) if isinstance(data, dict) else 0
    return AIToolRunResult(
        summary=f"AKShare 个股信息已读取 {symbol}，{info_count} 个字段。",
        result=data,
        result_ref=f"/stock/{symbol}",
    )


async def _akshare_hist_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.akshare import get_stock_hist_for_backtest

    req = AkshareHistInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip() for symbol in req.symbols]
    result = await get_stock_hist_for_backtest(
        symbols=_csv(symbols) or "",
        start_date=req.start_date,
        end_date=req.end_date,
        adjust=req.adjust,
    )
    data = _api_data_or_raise(result)
    returned = len(data.get("symbols") or []) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"AKShare 回测历史数据已读取 {returned} / {len(symbols)} 只。", result=data)


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


async def _watchlist_group_create_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import WatchlistGroupCreate, create_watchlist_group

    req = WatchlistGroupCreate.model_validate(payload.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await create_watchlist_group(request=req, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(
            summary=f"自选股分组已创建：{data.get('name') if isinstance(data, dict) else '-'}。",
            result=data,
            result_ref="/watchlist",
        )

    return await _with_session(ctx, run)


async def _watchlist_group_delete_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import delete_watchlist_group

    req = WatchlistStocksInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await delete_watchlist_group(group_id=req.group_id, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"自选股分组 {req.group_id} 已删除。", result=data, result_ref="/watchlist")

    return await _with_session(ctx, run)


async def _watchlist_stock_add_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import WatchlistStockAdd, add_to_watchlist

    req = WatchlistStockMutationInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()
    request = WatchlistStockAdd(symbol=symbol)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await add_to_watchlist(group_id=req.group_id, request=request, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"{symbol} 已加入自选股分组 {req.group_id}。", result=data, result_ref="/watchlist")

    return await _with_session(ctx, run)


async def _watchlist_stock_remove_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import remove_from_watchlist

    req = WatchlistStockMutationInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await remove_from_watchlist(group_id=req.group_id, symbol=symbol, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"{symbol} 已从自选股分组 {req.group_id} 移除。", result=data, result_ref="/watchlist")

    return await _with_session(ctx, run)


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


async def _indicator_timeseries_batch_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_skill import get_indicators_timeseries_batch

    req = IndicatorTimeseriesBatchInput.model_validate(payload)
    symbols = [normalize_security_symbol(symbol) or symbol.strip().upper() for symbol in req.symbols]

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_indicators_timeseries_batch(
            symbols=symbols,
            names=req.names,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
            session=session,
        )
        data = _api_data_or_raise(result) or []
        return AIToolRunResult(
            summary=f"批量指标时序已读取 {len(data)} 行，覆盖 {len(symbols)} 只股票。",
            result=data,
            result_ref="/factors/overview",
        )

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


async def _sentiment_threads_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.sentiment import get_sentiment_threads

    req = SentimentThreadsInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) if req.symbol else None

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_sentiment_threads(
            start_date=req.start_date,
            end_date=req.end_date,
            sources=_source_csv(req.sources),
            symbol=symbol,
            limit=req.limit,
            session=session,
        )
        data = _api_data_or_raise(result) or []
        return AIToolRunResult(summary=f"舆情主题线程已读取 {len(data)} 条。", result=data, result_ref="/sentiment")

    return await _with_session(ctx, read)


async def _sentiment_ingest_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.sentiment import IngestRunRequest, run_sentiment_ingest

    req = IngestRunRequest.model_validate(payload.model_dump())
    result = await run_sentiment_ingest(req)
    data = _api_data_or_raise(result)
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"舆情抓取任务已提交：{task_id or '-'}。",
        result=data,
        task_id=task_id,
        result_ref=f"/api/system/tasks/{task_id}" if task_id else "/sentiment",
    )


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


async def _data_sync_cancel_handler(_payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import cancel_sync

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await cancel_sync(session=session)
        data = _api_data_or_raise(result)
        cancelled = bool(isinstance(data, dict) and data.get("cancelled"))
        return AIToolRunResult(
            summary="同步取消请求已发送。" if cancelled else "当前没有可取消的同步任务。",
            result=data,
            result_ref="/data/sync",
        )

    return await _with_session(ctx, run)


async def _data_sync_cancel_all_handler(_payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data import cancel_all_sync

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await cancel_all_sync(session=session)
        data = _api_data_or_raise(result)
        cancelled = bool(isinstance(data, dict) and data.get("cancelled"))
        return AIToolRunResult(
            summary="全部同步取消请求已发送。" if cancelled else "当前没有可取消的同步任务。",
            result=data,
            result_ref="/data/sync",
        )

    return await _with_session(ctx, run)


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


async def _backtest_factor_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import run_factor_backtest
    from app.models.factor import BtConfig, FactorConfig

    req = BacktestFactorInput.model_validate(payload)
    config = FactorConfig.model_validate(req.config)
    bt_config = BtConfig.model_validate(req.bt_config) if req.bt_config else None
    result = await run_factor_backtest(config=config, bt_config=bt_config)
    data = _api_data_or_raise(result)
    metrics = data.get("metrics") if isinstance(data, dict) else None
    metric_summary = ""
    if isinstance(metrics, dict):
        total_return = metrics.get("total_return")
        sharpe = metrics.get("sharpe")
        metric_summary = f" 总收益={total_return}, Sharpe={sharpe}。"
    return AIToolRunResult(
        summary=f"因子分层回测已完成：{config.expression}。{metric_summary}",
        result=data,
        result_ref="/factor-backtest",
    )


async def _backtest_task_report_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from fastapi.responses import HTMLResponse

    from app.backtest.api import get_report

    req = BacktestReportInput.model_validate(payload)
    result = await get_report(task_id=req.task_id)
    if isinstance(result, HTMLResponse):
        body = bytes(result.body or b"")
        preview = body[:500].decode("utf-8", errors="ignore")
        return AIToolRunResult(
            summary=f"回测报告 {req.task_id} 已生成 HTML，长度 {len(body)} bytes。",
            result={
                "task_id": req.task_id,
                "content_type": result.media_type or "text/html",
                "html_length": len(body),
                "html_preview": preview,
            },
            result_ref=f"/backtest/report/{req.task_id}",
        )
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"回测报告 {req.task_id} 已读取。",
        result=data,
        result_ref=f"/backtest/report/{req.task_id}",
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


async def _report_chat_session_create_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.llm_strategy import create_chat_session

    req = ReportChatSessionInput.model_validate(payload)
    result = await asyncio.to_thread(create_chat_session, req.report_text, req.report_filename)
    session_id = result.get("session_id") if isinstance(result, dict) else None
    return AIToolRunResult(
        summary=f"研报对话会话已创建：{session_id or '-'}。",
        result=result,
        result_ref="/strategy",
    )


async def _report_chat_session_send_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.services.llm_strategy import send_chat_message

    req = ReportChatMessageInput.model_validate(payload)
    result = await asyncio.to_thread(send_chat_message, req.session_id, req.message)
    has_code = bool(isinstance(result, dict) and result.get("code"))
    return AIToolRunResult(
        summary=f"研报对话会话 {req.session_id} 已回复{'，包含策略代码' if has_code else ''}。",
        result=result,
        result_ref="/strategy",
    )


async def _indicator_catalog_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import list_indicators

    result = await list_indicators()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="指标目录已读取。", result=data, result_ref="/factors/overview")


async def _indicator_categories_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import get_categories

    result = await get_categories()
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"指标分类已读取 {len(data)} 个。", result=data, result_ref="/factors/overview")


async def _indicator_description_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import get_indicator_description

    req = IndicatorDescriptionInput.model_validate(payload)
    result = await get_indicator_description(name=req.name)
    data = _api_data_or_raise(result)
    title = data.get("display_name") if isinstance(data, dict) else req.name
    return AIToolRunResult(summary=f"指标 {req.name}（{title}）详情已读取。", result=data, result_ref="/factors/overview")


async def _indicator_query_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import query_indicators

    req = IndicatorQueryInput.model_validate(payload)
    result = await query_indicators(
        symbols=_csv(req.symbols) or "",
        indicator_names=_csv(req.indicator_names) or "",
        trade_date=req.trade_date.isoformat() if req.trade_date else None,
    )
    data = _api_data_or_raise(result)
    items = data.get("items") if isinstance(data, dict) else []
    return AIToolRunResult(summary=f"指标值查询已返回 {len(items or [])} 只股票。", result=data, result_ref="/factors/overview")


async def _indicator_compute_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import compute_indicators

    req = IndicatorComputeInput.model_validate(payload)
    result = await compute_indicators(
        indicator_names=req.indicator_names,
        symbols=req.symbols,
        full_compute=req.full_compute,
    )
    data = _api_data_or_raise(result)
    results = data.get("results") if isinstance(data, dict) else []
    return AIToolRunResult(summary=f"指标计算已完成，返回 {len(results or [])} 项结果。", result=data, result_ref="/factors/overview")


async def _indicator_screen_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import screen_stocks

    req = IndicatorScreenInput.model_validate(payload)
    result = await screen_stocks(
        filters=req.filters,
        trade_date=req.trade_date.isoformat() if req.trade_date else None,
        sort_by=req.sort_by,
        sort_order=req.sort_order,
        limit=req.limit,
    )
    data = _api_data_or_raise(result)
    total = data.get("total", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"指标筛选命中 {total} 只股票。", result=data, result_ref="/factors/screen")


async def _indicator_financial_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.indicator import get_financial_data

    req = FinancialInput.model_validate(payload)
    symbol = normalize_security_symbol(req.symbol) or req.symbol.strip().upper()

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_financial_data(symbol=symbol, report_count=req.report_count, session=session)
        data = _api_data_or_raise(result)
        items = data.get("items") if isinstance(data, dict) else []
        return AIToolRunResult(summary=f"{symbol} 指标财务数据已读取 {len(items or [])} 期。", result=data, result_ref=f"/stock/{symbol}")

    return await _with_session(ctx, read)


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


async def _compute_validate_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.expression import validate_expression

    req = ComputeValidateInput.model_validate(payload)
    valid, error = validate_expression(req.expression)
    return AIToolRunResult(
        summary=f"表达式校验{'通过' if valid else '失败'}。",
        result={"expression": req.expression, "valid": valid, "error": error},
        result_ref="/factors",
    )


async def _compute_evaluate_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.api import EvaluateRequest, evaluate

    req = ComputeEvaluateInput.model_validate(payload)
    result = await evaluate(EvaluateRequest.model_validate(req.model_dump()))
    data = _api_data_or_raise(result)
    if isinstance(data, dict):
        row_count = sum(len(rows) for rows in data.values() if isinstance(rows, list))
    else:
        row_count = 0
    return AIToolRunResult(
        summary=f"表达式计算已返回 {row_count} 行。",
        result={"data": data, "meta": result.get("meta") if isinstance(result, dict) else None},
        result_ref="/factors",
    )


async def _compute_screen_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.api import ScreenRequest, screen

    req = ComputeScreenInput.model_validate(payload)
    result = await screen(ScreenRequest.model_validate(req.model_dump()))
    data = _api_data_or_raise(result)
    count = data.get("count", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"表达式选股命中 {count} 只。", result=data, result_ref="/factors/screen")


async def _compute_precompute_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.api import PrecomputeExpressionsRequest, precompute_expressions

    req = ComputePrecomputeInput.model_validate(payload)
    result = await precompute_expressions(PrecomputeExpressionsRequest.model_validate(req.model_dump()))
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"表达式预计算已处理 {len(req.expressions)} 个表达式。",
        result=data,
        result_ref="/factors",
    )


async def _compute_batch_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.compute.api import batch_evaluate
    from app.models.factor import FactorConfig

    req = ComputeBatchInput.model_validate(payload)
    configs = [FactorConfig.model_validate(item) for item in req.configs]
    result = await batch_evaluate(configs)
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"批量因子计算已返回 {len(data or []) if isinstance(data, list) else 0} 项。",
        result=data,
        result_ref="/factors",
    )


async def _evaluation_ic_analysis_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.evaluation import ICAnalysisRequest, ic_analysis

    req = ICAnalysisRequest.model_validate(payload.model_dump())
    result = await ic_analysis(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="因子 IC 分析已完成。", result=data, result_ref="/factors/analysis")


async def _evaluation_quantile_backtest_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.evaluation import QuantileBacktestRequest, quantile_backtest

    req = QuantileBacktestRequest.model_validate(payload.model_dump())
    result = await quantile_backtest(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="因子分层回测已完成。", result=data, result_ref="/factor-backtest")


async def _evaluation_full_report_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.evaluation import FullReportRequest, full_report

    req = FullReportRequest.model_validate(payload.model_dump())
    result = await full_report(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="因子完整评估报告已生成。", result=data, result_ref="/factors/analysis")


async def _evaluation_report_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.evaluation import factor_report
    from app.models.factor import EvalConfig, FactorConfig

    req = EvaluationConfigInput.model_validate(payload)
    config = FactorConfig.model_validate(req.config)
    eval_config = EvalConfig.model_validate(req.eval_config) if req.eval_config else None
    result = await factor_report(config=config, eval_config=eval_config)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="六模块因子评估报告已生成。", result=data, result_ref="/factors/analysis")


async def _evaluation_board_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.evaluation import factor_board
    from app.models.factor import BoardQuery

    req = FactorBoardInput.model_validate(payload)
    result = await factor_board(BoardQuery.model_validate(req.query))
    data = _api_data_or_raise(result)
    total = data.get("total", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"因子看板已读取，总数 {total}。", result=data, result_ref="/factors/board")


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


async def _factor_create_legacy_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import FactorCreateRequest, create_factor

    req = FactorCreateRequest.model_validate(payload.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await create_factor(request=req, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(
            summary=f"Legacy 因子已创建：{data.get('name') if isinstance(data, dict) else '-'}。",
            result=data,
            result_ref="/factor",
        )

    return await _with_session(ctx, run)


async def _factor_update_legacy_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import FactorUpdateRequest, update_factor

    req = LegacyFactorUpdateInput.model_validate(payload)
    request = FactorUpdateRequest.model_validate(req.model_dump(exclude={"id"}, exclude_unset=True))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await update_factor(factor_id=req.id, request=request, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"Legacy 因子 {req.id} 已更新。", result=data, result_ref="/factor")

    return await _with_session(ctx, run)


async def _factor_delete_legacy_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import delete_factor

    req = IdInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await delete_factor(factor_id=req.id, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"Legacy 因子 {req.id} 已删除。", result=data, result_ref="/factor")

    return await _with_session(ctx, run)


async def _factor_analysis_list_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import list_analyses

    req = FactorAnalysisListInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await list_analyses(factor_id=req.factor_id, limit=req.limit, session=session)
        data = _api_data_or_raise(result) or []
        return AIToolRunResult(summary=f"因子分析记录已读取 {len(data)} 条。", result=data, result_ref="/factor")

    return await _with_session(ctx, read)


async def _factor_analysis_detail_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor import get_analysis

    req = IdInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_analysis(analysis_id=req.id, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"因子分析记录 {req.id} 已读取。", result=data, result_ref="/factor")

    return await _with_session(ctx, read)


async def _factor_templates_v2_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import list_templates

    result = await list_templates()
    data = [item.model_dump() if hasattr(item, "model_dump") else item for item in result]
    return AIToolRunResult(summary=f"V2 因子模板已读取 {len(data)} 个。", result=data, result_ref="/factors/list")


async def _factor_validate_python_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import PythonFactorValidateRequest, validate_python_code

    req = PythonFactorValidateRequest.model_validate(payload.model_dump())
    result = await validate_python_code(req)
    valid = bool(isinstance(result, dict) and result.get("valid"))
    return AIToolRunResult(
        summary=f"Python 因子校验{'通过' if valid else '失败'}。",
        result=result,
        result_ref="/factors/list",
    )


async def _factor_validate_v2_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import validate_expression
    from app.models.factor import ValidateRequest

    req = ValidateRequest.model_validate(payload.model_dump())
    result = await validate_expression(req)
    data = result.model_dump() if hasattr(result, "model_dump") else result
    valid = bool(isinstance(data, dict) and data.get("valid"))
    return AIToolRunResult(
        summary=f"V2 因子表达式校验{'通过' if valid else '失败'}。",
        result=data,
        result_ref="/factors/list",
    )


async def _saved_factor_create_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import create_factor
    from app.models.factor import FactorCreate

    req = FactorCreate.model_validate(payload.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await create_factor(data=req, session=session)
        data = result.model_dump() if hasattr(result, "model_dump") else result
        return AIToolRunResult(
            summary=f"V2 因子已创建：{data.get('name') if isinstance(data, dict) else '-'}。",
            result=data,
            result_ref="/factors/list",
        )

    return await _with_session(ctx, run)


async def _saved_factor_update_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import update_factor
    from app.models.factor import FactorUpdate

    req = SavedFactorUpdateInput.model_validate(payload)
    request = FactorUpdate.model_validate(req.model_dump(exclude={"factor_id"}, exclude_unset=True))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await update_factor(factor_id=req.factor_id, data=request, session=session)
        data = result.model_dump() if hasattr(result, "model_dump") else result
        return AIToolRunResult(summary=f"V2 因子 {req.factor_id} 已更新。", result=data, result_ref="/factors/list")

    return await _with_session(ctx, run)


async def _saved_factor_delete_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import delete_factor

    req = IdInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await delete_factor(factor_id=req.id, session=session)
        data = result.model_dump() if hasattr(result, "model_dump") else result
        return AIToolRunResult(summary=f"V2 因子 {req.id} 已删除。", result=data, result_ref="/factors/list")

    return await _with_session(ctx, run)


async def _saved_factor_preview_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import FactorPreviewRequest, preview_factor

    req = SavedFactorPreviewInput.model_validate(payload)
    request = FactorPreviewRequest.model_validate(req.model_dump(exclude={"factor_id"}))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await preview_factor(factor_id=req.factor_id, request=request, session=session)
        data = _api_data_or_raise(result)
        total = data.get("total", 0) if isinstance(data, dict) else 0
        return AIToolRunResult(summary=f"因子 {req.factor_id} 预览已返回 {total} 行。", result=data, result_ref="/factors/list")

    return await _with_session(ctx, run)


async def _saved_python_factor_run_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import FactorPreviewRequest, run_python_factor

    req = SavedPythonFactorRunInput.model_validate(payload)
    request = FactorPreviewRequest.model_validate(req.model_dump(exclude={"factor_id"}))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await run_python_factor(factor_id=req.factor_id, request=request, session=session)
        data = _api_data_or_raise(result)
        rows = data.get("rows") if isinstance(data, dict) else []
        errors = data.get("errors") if isinstance(data, dict) else []
        error_suffix = f"，错误 {len(errors)} 个" if errors else ""
        return AIToolRunResult(
            summary=f"Python 因子 {req.factor_id} 已运行，返回 {len(rows or [])} 行{error_suffix}。",
            result=data,
            result_ref="/factors/list",
        )

    return await _with_session(ctx, run)


async def _saved_factor_precompute_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import FactorPrecomputeRequest, precompute_factor

    req = SavedFactorPrecomputeInput.model_validate(payload)
    request = FactorPrecomputeRequest.model_validate(req.model_dump(exclude={"factor_id"}))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await precompute_factor(factor_id=req.factor_id, request=request, session=session)
        data = _api_data_or_raise(result)
        rows = data.get("rows_written", 0) if isinstance(data, dict) else 0
        return AIToolRunResult(summary=f"因子 {req.factor_id} 已预计算写入 {rows} 行。", result=data, result_ref="/factor")

    return await _with_session(ctx, run)


async def _saved_factor_coverage_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import factor_coverage

    req = SavedFactorCoverageInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await factor_coverage(
            factor_id=req.factor_id,
            start_date=req.start_date,
            end_date=req.end_date,
            session=session,
        )
        return AIToolRunResult(summary=f"因子 {req.factor_id} 覆盖率已读取。", result=result, result_ref="/factor")

    return await _with_session(ctx, run)


async def _saved_factor_analyze_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import FactorAnalyzeRequest, analyze_factor

    req = SavedFactorAnalyzeInput.model_validate(payload)
    request = FactorAnalyzeRequest.model_validate(req.model_dump(exclude={"factor_id"}))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await analyze_factor(factor_id=req.factor_id, request=request, session=session)
        data = _api_data_or_raise(result)
        summary = data.get("summary", {}) if isinstance(data, dict) else {}
        ic_mean = summary.get("ic_mean") if isinstance(summary, dict) else None
        return AIToolRunResult(
            summary=f"因子 {req.factor_id} 分析已完成，IC mean={ic_mean if ic_mean is not None else '-'}。",
            result=data,
            result_ref="/factors/analysis",
        )

    return await _with_session(ctx, run)


async def _saved_factor_evaluate_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factors import FactorAnalyzeRequest, evaluate_factor

    req = SavedFactorAnalyzeInput.model_validate(payload)
    request = FactorAnalyzeRequest.model_validate(req.model_dump(exclude={"factor_id"}))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await evaluate_factor(factor_id=req.factor_id, request=request, session=session)
        data = _api_data_or_raise(result)
        summary = data.get("summary", {}) if isinstance(data, dict) else {}
        ic_mean = summary.get("ic_mean") if isinstance(summary, dict) else None
        return AIToolRunResult(
            summary=f"因子 {req.factor_id} 评估已完成，IC mean={ic_mean if ic_mean is not None else '-'}。",
            result=data,
            result_ref="/factors/analysis",
        )

    return await _with_session(ctx, run)


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


async def _factor_value_param_hashes_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import FactorParamHashRequest, param_hashes

    req = FactorParamHashInput.model_validate(payload)
    result = await param_hashes(FactorParamHashRequest.model_validate(req.model_dump()))
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"因子参数哈希已读取 {len(data)} 组。", result=data, result_ref="/factor")


async def _factor_value_query_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import FactorQueryRequest, query

    req = FactorQueryInput.model_validate(payload)
    result = await query(FactorQueryRequest.model_validate(req.model_dump()))
    data = _api_data_or_raise(result)
    count = len(data.get("items") or []) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"{req.factor_name} 因子值查询返回 {count} 行。", result=data, result_ref="/factor")


async def _factor_value_paper_manifest_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import paper_manifest

    result = await paper_manifest()
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"论文因子实现清单已读取 {len(data)} 项。", result=data, result_ref="/factor")


async def _factor_value_paper_experiments_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import paper_experiments

    result = await paper_experiments()
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"论文实验规格已读取 {len(data)} 项。", result=data, result_ref="/factor")


async def _factor_value_paper_snapshot_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import (
        PaperExperimentSnapshotRequest,
        paper_experiment_feature_snapshot,
    )

    req = PaperFeatureSnapshotInput.model_validate(payload)
    result = await paper_experiment_feature_snapshot(PaperExperimentSnapshotRequest.model_validate(req.model_dump()))
    data = _api_data_or_raise(result)
    count = len(data.get("items") or []) if isinstance(data, dict) else 0
    return AIToolRunResult(summary=f"论文因子特征快照已读取 {count} 行。", result=data, result_ref="/factor")


async def _factor_value_precompute_prepare_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import FactorPrecomputePrepareRequest, prepare_precompute

    req = FactorPrecomputePrepareRequest.model_validate(payload.model_dump())
    result = await prepare_precompute(req)
    data = _api_data_or_raise(result)
    missing = len(data.get("missing_dependencies") or []) if isinstance(data, dict) else 0
    return AIToolRunResult(
        summary=f"因子预计算准备已完成，缺口 {missing} 项。",
        result=data,
        result_ref="/factor",
    )


async def _factor_value_precompute_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import FactorPrecomputeRequest, precompute

    req = FactorPrecomputeRequest.model_validate(payload.model_dump())
    result = await precompute(req)
    data = _api_data_or_raise(result)
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"因子预计算{'任务已提交' if task_id else '已完成'}：{', '.join(req.factor_names)}。",
        result=data,
        task_id=task_id,
        result_ref="/factor",
    )


async def _factor_value_group_precompute_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_values import FactorGroupPrecomputeRequest, precompute_group

    req = FactorGroupPrecomputeRequest.model_validate(payload.model_dump())
    result = await precompute_group(req)
    data = _api_data_or_raise(result)
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"因子集合预计算{'任务已提交' if task_id else '已完成'}：{req.group_name}。",
        result=data,
        task_id=task_id,
        result_ref="/factor",
    )


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


async def _factor_research_prepare_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_research import FactorResearchRunRequest, prepare_factor_research_run

    req = FactorResearchRunRequest.model_validate(payload.model_dump())
    result = await prepare_factor_research_run(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.factor_name} 因子研究准备已完成。", result=data, result_ref="/factor")


async def _factor_research_submit_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_research import FactorResearchRunRequest, run_factor_research

    req = FactorResearchRunRequest.model_validate(payload.model_dump())
    result = await run_factor_research(req)
    data = _api_data_or_raise(result)
    run_id = data.get("run_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"{req.factor_name} 因子研究已运行：{run_id or '-'}。",
        result=data,
        task_id=run_id,
        result_ref="/factor",
    )


async def _factor_research_batch_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_research import FactorResearchBatchRequest, batch_factor_research

    req = FactorResearchBatchRequest.model_validate(payload.model_dump())
    result = await batch_factor_research(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"批量因子研究已运行 {len(req.factor_names)} 个因子。",
        result=data,
        result_ref="/factor",
    )


async def _factor_research_combinations_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.factor_research import (
        FactorResearchCombinationRequest,
        list_factor_research_combinations,
    )

    req = FactorResearchCombinationRequest.model_validate(payload.model_dump())
    result = await list_factor_research_combinations(req)
    data = _api_data_or_raise(result)
    if isinstance(data, dict):
        count = len(data.get("items") or [])
    elif isinstance(data, list):
        count = len(data)
    else:
        count = 0
    return AIToolRunResult(summary=f"因子组合候选已读取 {count} 项。", result=data, result_ref="/factor")


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


async def _backtest_engines_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import list_engines

    result = await list_engines()
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"回测引擎已读取 {len(data or [])} 个。", result=data, result_ref="/backtest")


async def _backtest_dual_stock_grid_preset_handler(
    _payload: BaseModel,
    _ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.backtest.api import get_dual_stock_grid_preset

    result = await get_dual_stock_grid_preset()
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary="双标的底仓网格预设已读取。",
        result=data,
        result_ref="/backtest",
    )


async def _backtest_create_dual_stock_grid_strategy_handler(
    payload: BaseModel,
    _ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.backtest.api import BuiltinStrategyCreateRequest, create_dual_stock_grid_strategy

    req = BuiltinStrategyCreateRequest.model_validate(payload.model_dump())
    result = await create_dual_stock_grid_strategy(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"内置策略已写入：{data.get('name') if isinstance(data, dict) else '双标的底仓网格'}。",
        result=data,
        result_ref="/backtest",
    )


async def _backtest_create_multi_factor_strategy_handler(
    payload: BaseModel,
    _ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.backtest.api import BuiltinStrategyCreateRequest, create_multi_factor_strategy

    req = BuiltinStrategyCreateRequest.model_validate(payload.model_dump())
    result = await create_multi_factor_strategy(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"内置策略已写入：{data.get('name') if isinstance(data, dict) else '通用多因子模型'}。",
        result=data,
        result_ref="/backtest",
    )


async def _backtest_create_tech_small_cap_strategy_handler(
    payload: BaseModel,
    _ctx: AIToolExecutionContext,
) -> AIToolRunResult:
    from app.backtest.api import BuiltinStrategyCreateRequest, create_tech_small_cap_strategy

    req = BuiltinStrategyCreateRequest.model_validate(payload.model_dump())
    result = await create_tech_small_cap_strategy(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(
        summary=f"内置策略已写入：{data.get('name') if isinstance(data, dict) else '科技主线小市值'}。",
        result=data,
        result_ref="/backtest",
    )


async def _backtest_index_pools_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import list_index_pools

    result = await list_index_pools()
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary=f"回测指数池已读取 {len(data or [])} 个。", result=data, result_ref="/backtest")


async def _backtest_index_pool_detail_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_index_pool

    req = IndexPoolInput.model_validate(payload)
    result = await get_index_pool(
        index_symbol=req.index_symbol,
        start_date=req.start_date.isoformat() if req.start_date else None,
        end_date=req.end_date.isoformat() if req.end_date else None,
    )
    data = _api_data_or_raise(result)
    symbol_count = data.get("symbol_count", 0) if isinstance(data, dict) else 0
    return AIToolRunResult(
        summary=f"{req.index_symbol} 回测指数池详情已读取，股票数 {symbol_count}。",
        result=data,
        result_ref="/backtest",
    )


async def _backtest_pool_symbols_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_pool_symbols

    req = PoolSymbolsInput.model_validate(payload)
    result = await get_pool_symbols(req.pool_name)
    data = _api_data_or_raise(result)
    symbols = data.get("symbols") if isinstance(data, dict) else []
    return AIToolRunResult(
        summary=f"股票池 {req.pool_name} 已读取 {len(symbols or [])} 只。",
        result=data,
        result_ref="/backtest",
    )


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


async def _strategy_create_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import StrategyCreate, create_strategy

    req = StrategyCreate.model_validate(payload.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await create_strategy(request=req, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(
            summary=f"策略已创建：{data.get('name') if isinstance(data, dict) else '-'}。",
            result=data,
            result_ref="/backtest",
        )

    return await _with_session(ctx, run)


async def _strategy_update_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import StrategyUpdate, update_strategy

    req = StrategyUpdateToolInput.model_validate(payload)
    request = StrategyUpdate.model_validate(req.model_dump(exclude={"id"}))

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await update_strategy(strategy_id=req.id, request=request, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"策略 {req.id} 已更新。", result=data, result_ref="/backtest")

    return await _with_session(ctx, run)


async def _strategy_delete_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import delete_strategy

    req = IdInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await delete_strategy(strategy_id=req.id, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"策略 {req.id} 已删除。", result=data, result_ref="/backtest")

    return await _with_session(ctx, run)


async def _strategy_trend_signals_daily_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.strategy import get_daily_signals

    req = StrategySignalsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_daily_signals(
            start_date=req.start_date.isoformat(),
            end_date=req.end_date.isoformat(),
            symbols=_csv(req.symbols),
            use_watchlist=req.use_watchlist,
            session=session,
        )
        data = [item.model_dump() if hasattr(item, "model_dump") else item for item in result]
        trigger_count = sum(int(item.get("triggered_count") or 0) for item in data if isinstance(item, dict))
        return AIToolRunResult(
            summary=f"趋势资金日度信号已读取 {len(data)} 个交易日，触发 {trigger_count} 次。",
            result=data,
            result_ref="/strategy",
        )

    return await _with_session(ctx, read)


async def _strategy_trend_signals_summary_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.strategy import get_signals_summary

    req = StrategySignalsInput.model_validate(payload)

    async def read(session: AsyncSession) -> AIToolRunResult:
        result = await get_signals_summary(
            start_date=req.start_date.isoformat(),
            end_date=req.end_date.isoformat(),
            symbols=_csv(req.symbols),
            use_watchlist=req.use_watchlist,
            session=session,
        )
        days = result.get("trading_days_with_signals", 0) if isinstance(result, dict) else 0
        total = result.get("total_composite_triggers", 0) if isinstance(result, dict) else 0
        return AIToolRunResult(
            summary=f"趋势资金信号汇总已读取：{days} 个交易日有信号，合计 {total} 次。",
            result=result,
            result_ref="/strategy",
        )

    return await _with_session(ctx, read)


async def _strategy_trend_backtest_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.strategy import run_backtest as run_strategy_backtest

    req = StrategySignalsInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await run_strategy_backtest(
            start_date=req.start_date.isoformat(),
            end_date=req.end_date.isoformat(),
            symbols=_csv(req.symbols),
            use_watchlist=req.use_watchlist,
            session=session,
        )
        total_trades = result.get("total_trades", 0) if isinstance(result, dict) else 0
        return AIToolRunResult(
            summary=f"趋势资金组合回测已完成，交易 {total_trades} 笔。",
            result=result,
            result_ref="/strategy",
        )

    return await _with_session(ctx, run)


async def _strategy_deep_value_backtest_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.strategy import DeepValueBacktestRequest, run_deep_value_backtest

    req = DeepValueBacktestRequest.model_validate(payload.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await run_deep_value_backtest(req=req, session=session)
        data = _api_data_or_raise(result)
        backtest_id = data.get("backtest_id") if isinstance(data, dict) else None
        return AIToolRunResult(
            summary=f"深度价值策略回测已完成，记录 ID {backtest_id or '-'}。",
            result=data,
            result_ref=f"/backtest/report/{backtest_id}" if backtest_id else "/strategy",
        )

    return await _with_session(ctx, run)


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


async def _backtest_create_record_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import BacktestCreate, create_backtest

    req = BacktestCreate.model_validate(payload.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await create_backtest(request=req, session=session)
        data = _api_data_or_raise(result)
        backtest_id = data.get("id") if isinstance(data, dict) else None
        return AIToolRunResult(
            summary=f"传统回测记录已创建：{backtest_id or '-'}。",
            result=data,
            result_ref=f"/backtest/report/{backtest_id}" if backtest_id else "/backtest",
        )

    return await _with_session(ctx, run)


async def _backtest_run_record_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import run_backtest as run_record_backtest

    req = IdInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await run_record_backtest(backtest_id=req.id, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(
            summary=f"传统回测记录 {req.id} 已运行。",
            result=data,
            result_ref=f"/backtest/report/{req.id}",
        )

    return await _with_session(ctx, run)


async def _backtest_delete_record_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import delete_backtest

    req = IdInput.model_validate(payload)

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await delete_backtest(backtest_id=req.id, session=session)
        data = _api_data_or_raise(result)
        return AIToolRunResult(summary=f"传统回测记录 {req.id} 已删除。", result=data, result_ref="/backtest")

    return await _with_session(ctx, run)


async def _backtest_delete_records_batch_handler(payload: BaseModel, ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.backtest import BatchDeleteRequest, batch_delete_backtests

    req = IdsInput.model_validate(payload)
    request = BatchDeleteRequest.model_validate(req.model_dump())

    async def run(session: AsyncSession) -> AIToolRunResult:
        result = await batch_delete_backtests(request=request, session=session)
        data = _api_data_or_raise(result)
        deleted_count = data.get("deleted_count", 0) if isinstance(data, dict) else 0
        return AIToolRunResult(summary=f"传统回测记录已批量删除 {deleted_count} 条。", result=data, result_ref="/backtest")

    return await _with_session(ctx, run)


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


async def _backtest_task_cancel_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import cancel_task

    req = BacktestTaskInput.model_validate(payload)
    result = await cancel_task(req.task_id)
    data = _api_data_or_raise(result)
    status = data.get("status") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"回测任务 {req.task_id} 取消请求已处理：{status or '-'}。",
        result=data,
        task_id=req.task_id,
        result_ref="/backtest",
    )


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


async def _backtest_data_coverage_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import RunBacktestRequest, get_backtest_data_coverage

    req = RunBacktestRequest.model_validate(payload.model_dump())
    result = await get_backtest_data_coverage(req)
    data = _api_data_or_raise(result)
    ok = bool(isinstance(data, dict) and data.get("ok"))
    return AIToolRunResult(
        summary=f"回测数据覆盖检查{'通过' if ok else '已返回告警/缺口'}。",
        result=data,
        result_ref="/backtest",
    )


async def _backtest_stock_names_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import get_stock_names

    req = StockNamesInput.model_validate(payload)
    result = await get_stock_names(symbols=_csv(req.symbols) or "")
    data = _api_data_or_raise(result) or {}
    return AIToolRunResult(summary=f"股票名称映射已读取 {len(data)} 项。", result=data, result_ref="/backtest")


async def _backtest_optimize_grid_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import OptimizeRequest, optimize_grid

    req = OptimizeRequest.model_validate(payload.model_dump())
    result = await optimize_grid(req)
    data = _api_data_or_raise(result)
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"Grid Search 优化任务已提交：{task_id or '-'}。",
        result=data,
        task_id=task_id,
        result_ref=f"/backtest?task_id={task_id}" if task_id else "/backtest",
    )


async def _backtest_optimize_walk_forward_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import WalkForwardRequest, optimize_walk_forward

    req = WalkForwardRequest.model_validate(payload.model_dump())
    result = await optimize_walk_forward(req)
    data = _api_data_or_raise(result)
    task_id = data.get("task_id") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"Walk-forward 优化任务已提交：{task_id or '-'}。",
        result=data,
        task_id=task_id,
        result_ref=f"/backtest?task_id={task_id}" if task_id else "/backtest",
    )


async def _backtest_strategy_params_schema_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import StrategyParamsSchemaRequest, get_strategy_params_schema

    req = StrategyParamsSchemaRequest.model_validate(payload.model_dump())
    result = await get_strategy_params_schema(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="策略参数 schema 已读取。", result=data, result_ref="/backtest")


async def _backtest_strategy_params_validate_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.backtest.api import StrategyParamsValidateRequest, validate_strategy_params_payload

    req = StrategyParamsValidateRequest.model_validate(payload.model_dump())
    result = await validate_strategy_params_payload(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="策略参数 payload 校验通过。", result=data, result_ref="/backtest")


async def _explorer_tables_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import list_tables

    result = list_tables()
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"数据浏览器表目录已读取 {len(data)} 张表。", result=data, result_ref="/explorer")


async def _explorer_table_schema_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import get_table_schema

    req = ExplorerTableInput.model_validate(payload)
    result = get_table_schema(req.table_name)
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"{req.table_name} schema 已读取 {len(data)} 列。", result=data, result_ref="/explorer")


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
    data = _api_data_or_raise(result)
    rows = data.get("rows") if isinstance(data, dict) else []
    return AIToolRunResult(
        summary=f"{req.table_name} 预览已读取 {len(rows or [])} 行。",
        result=data,
        result_ref="/explorer",
    )


async def _explorer_table_search_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import ExplorerSearchRequest, search_table

    req = ExplorerSearchInput.model_validate(payload)
    request = ExplorerSearchRequest.model_validate(req.model_dump(exclude={"table_name"}))
    result = search_table(req.table_name, request=request)
    data = _api_data_or_raise(result)
    rows = data.get("rows") if isinstance(data, dict) else []
    return AIToolRunResult(summary=f"{req.table_name} 搜索已返回 {len(rows or [])} 行。", result=data, result_ref="/explorer")


async def _explorer_distinct_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import get_distinct_values

    req = ExplorerDistinctInput.model_validate(payload)
    result = get_distinct_values(table_name=req.table_name, column=req.column, q=req.q, limit=req.limit)
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(
        summary=f"{req.table_name}.{req.column} distinct 已读取 {len(data)} 项。",
        result=data,
        result_ref="/explorer",
    )


async def _explorer_query_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.data_explorer import execute_query

    req = ExplorerQueryInput.model_validate(payload)
    result = execute_query(sql=req.sql, limit=req.limit)
    data = _api_data_or_raise(result)
    rows = data.get("rows") if isinstance(data, dict) else []
    return AIToolRunResult(summary=f"只读 SQL 已返回 {len(rows or [])} 行。", result=data, result_ref="/explorer")


async def _parquet_datasets_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.parquet_explorer import list_datasets

    result = await list_datasets()
    data = _api_data_or_raise(result) or []
    return AIToolRunResult(summary=f"Parquet 数据集已读取 {len(data)} 个。", result=data, result_ref="/explorer")


async def _parquet_dataset_schema_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.parquet_explorer import dataset_schema

    req = ParquetSchemaInput.model_validate(payload)
    result = await dataset_schema(dataset=req.dataset)
    data = _api_data_or_raise(result)
    columns = data.get("columns") if isinstance(data, dict) else []
    return AIToolRunResult(
        summary=f"{req.dataset} 数据集 schema 已读取 {len(columns or [])} 列。",
        result=data,
        result_ref="/explorer",
    )


async def _parquet_dataset_preview_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.parquet_explorer import dataset_preview

    req = ParquetDatasetInput.model_validate(payload)
    result = await dataset_preview(
        dataset=req.dataset,
        limit=req.limit,
        offset=req.offset,
        include_total=req.include_total,
    )
    data = _api_data_or_raise(result)
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
    return AIToolRunResult(
        summary=f"{req.dataset} 数据集覆盖率已读取。",
        result=_api_data_or_raise(result),
        result_ref="/explorer",
    )


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


async def _live_strategy_profiles_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import list_strategy_profiles

    req = LiveProfilesInput.model_validate(payload)
    result = await list_strategy_profiles(include_disabled=req.include_disabled)
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary=f"实盘策略配置已读取 {len(data or [])} 项。", result=data, result_ref="/trade")


async def _live_weekly_trades_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import weekly_trade_analysis

    req = LiveWeeklyTradesInput.model_validate(payload)
    result = await weekly_trade_analysis(profile_key=req.profile_key, mode=req.mode, week_start=req.week_start)
    data = result.get("data") if isinstance(result, dict) else result
    return AIToolRunResult(summary="周度成交分析已读取。", result=data, result_ref="/trade")


async def _live_account_initialize_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveStrategyAccountInitRequest, initialize_strategy_account

    req = LiveStrategyAccountInitRequest.model_validate(payload.model_dump())
    result = await initialize_strategy_account(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.mode} 策略账户已初始化。", result=data, result_ref="/trade")


async def _live_profile_create_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveProfileCreateRequest, create_strategy_profile

    req = LiveProfileCreateRequest.model_validate(payload.model_dump())
    result = await create_strategy_profile(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"实盘策略 profile 已创建：{req.profile_key}。", result=data, result_ref="/trade")


async def _live_profile_update_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveProfileUpdateRequest, update_strategy_profile

    req = LiveProfileUpdateToolInput.model_validate(payload)
    request = LiveProfileUpdateRequest.model_validate(req.model_dump(exclude={"profile_key"}, exclude_unset=True))
    result = await update_strategy_profile(profile_key=req.profile_key, req=request)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"实盘策略 profile 已更新：{req.profile_key}。", result=data, result_ref="/trade")


async def _live_preflight_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LivePreflightRequest, live_preflight

    req = LivePreflightRequest.model_validate(payload.model_dump())
    try:
        result = await asyncio.wait_for(live_preflight(req), timeout=25.0)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="实盘策略预检超过 25 秒，已中止等待。") from exc
    data = _api_data_or_raise(result)
    can_generate = data.get("can_generate") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"{req.mode} 实盘预检已完成：{'可生成信号' if can_generate else '存在阻塞/告警'}。",
        result=data,
        result_ref="/trade",
    )


async def _live_signals_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveSignalRequest, generate_live_signals

    req = LiveSignalRequest.model_validate(payload.model_dump())
    try:
        result = await asyncio.wait_for(generate_live_signals(req), timeout=60.0)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="实盘策略信号生成超过 60 秒，已中止等待。") from exc
    data = _api_data_or_raise(result)
    orders = data.get("orders") if isinstance(data, dict) else []
    return AIToolRunResult(
        summary=f"{req.mode} 策略信号已生成，候选订单 {len(orders or [])} 条；未提交真实委托。",
        result=data,
        result_ref="/trade",
    )


async def _live_runner_start_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveRunnerStartRequest, start_runner

    req = LiveRunnerStartRequest.model_validate(payload.model_dump())
    result = await start_runner(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.mode} runner 启动请求已处理。", result=data, result_ref="/trade")


async def _live_runner_stop_handler(_payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import stop_runner

    result = await stop_runner()
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="runner 停止请求已处理。", result=data, result_ref="/trade")


async def _live_runner_takeover_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveRunnerTakeoverRequest, takeover_runner

    req = LiveRunnerTakeoverRequest.model_validate(payload.model_dump())
    result = await takeover_runner(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary="runner 人工接管请求已处理。", result=data, result_ref="/trade")


async def _live_orders_sync_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveOrderSyncRequest, sync_orders

    req = LiveOrderSyncRequest.model_validate(payload.model_dump())
    result = await sync_orders(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.mode} 委托状态同步已处理。", result=data, result_ref="/trade")


async def _live_orders_submit_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveSubmitOrdersRequest, submit_orders

    req = LiveSubmitOrdersRequest.model_validate(payload.model_dump())
    result = await submit_orders(req)
    data = _api_data_or_raise(result)
    submitted = data.get("submitted_count") if isinstance(data, dict) else None
    return AIToolRunResult(
        summary=f"{req.mode} 委托提交入口已处理，提交数 {submitted if submitted is not None else '-'}。",
        result=data,
        result_ref="/trade",
    )


async def _live_orders_cancel_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveOrderCancelRequest, cancel_orders

    req = LiveOrderCancelRequest.model_validate(payload.model_dump())
    result = await cancel_orders(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.mode} 批量撤单入口已处理。", result=data, result_ref="/trade")


async def _live_orders_cancel_resubmit_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveOrderCancelResubmitRequest, cancel_and_resubmit_orders

    req = LiveOrderCancelResubmitRequest.model_validate(payload.model_dump())
    result = await cancel_and_resubmit_orders(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.mode} 撤单重报入口已处理。", result=data, result_ref="/trade")


async def _live_orders_close_local_handler(payload: BaseModel, _ctx: AIToolExecutionContext) -> AIToolRunResult:
    from app.api.live_trading import LiveOrderLocalCloseRequest, close_local_orders

    req = LiveOrderLocalCloseRequest.model_validate(payload.model_dump())
    result = await close_local_orders(req)
    data = _api_data_or_raise(result)
    return AIToolRunResult(summary=f"{req.mode} 本地关闭委托入口已处理。", result=data, result_ref="/trade")


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
                name="runtime.task_detail",
                title="运行任务详情",
                description="读取指定 runtime task 的状态、进度、结果和错误。",
                category="system",
                input_model=RuntimeTaskInput,
                handler=_runtime_task_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="system.health",
                title="健康检查",
                description="读取后端最轻量健康检查。",
                category="system",
                input_model=EmptyInput,
                handler=_system_health_handler,
            )
        )
        registry.register(
            AITool(
                name="system.cache",
                title="缓存状态",
                description="读取 Redis、回测缓存和因子计算缓存状态。",
                category="system",
                input_model=EmptyInput,
                handler=_system_cache_handler,
            )
        )
        registry.register(
            AITool(
                name="system.dev_data_mode",
                title="Dev 数据模式",
                description="读取开发环境是否启用生产真实数据目录。",
                category="system",
                input_model=EmptyInput,
                handler=_system_dev_data_mode_handler,
            )
        )
        registry.register(
            AITool(
                name="system.update_dev_data_mode",
                title="更新 Dev 数据模式",
                description="切换开发环境生产真实数据目录开关，需要确认。",
                category="system",
                input_model=_dev_data_mode_update_input_model(),
                handler=_system_update_dev_data_mode_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="system.live_trading_guardrails",
                title="实盘防护开关",
                description="读取实盘下单和自动执行防护开关。",
                category="system",
                input_model=EmptyInput,
                handler=_system_live_trading_guardrails_handler,
            )
        )
        registry.register(
            AITool(
                name="system.update_live_trading_guardrails",
                title="更新实盘防护开关",
                description="更新实盘下单和自动执行防护开关，开启时需要强确认文本。",
                category="system",
                input_model=_live_trading_guardrails_update_input_model(),
                handler=_system_update_live_trading_guardrails_handler,
                risk_level="danger",
                requires_confirmation=True,
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
                name="system.mcp_manifest",
                title="MCP 工具契约",
                description="读取 Copilot、HTTP 和 stdio MCP 共用的工具 manifest、传输方式和风险策略。",
                category="system",
                input_model=EmptyInput,
                handler=_mcp_manifest_handler,
            )
        )
        registry.register(
            AITool(
                name="system.ai_diagnostics",
                title="AI Native 诊断",
                description="汇总模型网关、工具 manifest、最近路由来源、答案模式、artifact 和工具失败摘要。",
                category="system",
                input_model=AIDiagnosticsInput,
                handler=_ai_diagnostics_handler,
            )
        )
        registry.register(
            AITool(
                name="system.ai_artifacts",
                title="AI Artifact 列表",
                description="读取 AI Native 对话、工具和工作流 artifact 记录，用于审计最近对话与工具执行证据。",
                category="system",
                input_model=AIArtifactsInput,
                handler=_ai_artifacts_handler,
            )
        )
        registry.register(
            AITool(
                name="system.ai_artifact_detail",
                title="AI Artifact 详情",
                description="读取单条 AI Native artifact 的输入摘要、工具调用、关键输出和错误。",
                category="system",
                input_model=AIArtifactInput,
                handler=_ai_artifact_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="workflow.catalog",
                title="AI 工作流目录",
                description="列出 CommandGraph、ReportStrategyGraph、QuantResearchGraph 的节点定义和输入契约。",
                category="workflow",
                input_model=EmptyInput,
                handler=_workflow_catalog_handler,
            )
        )
        registry.register(
            AITool(
                name="workflow.run",
                title="运行 AI 工作流",
                description="按名称运行 AI workflow graph，并返回节点 trace、工具 observation 和待确认项。",
                category="workflow",
                input_model=WorkflowRunInput,
                handler=_workflow_run_handler,
            )
        )
        registry.register(
            AITool(
                name="workflow.command_graph",
                title="CommandGraph",
                description="运行命令图：上下文、LLM/本地路由、工具执行、答案摘要。",
                category="workflow",
                input_model=AIWorkflowRunRequest,
                handler=_command_graph_handler,
            )
        )
        registry.register(
            AITool(
                name="workflow.report_strategy_graph",
                title="ReportStrategyGraph",
                description="运行研报策略图：研报校验、策略生成、可选 AKQuant 转换、证据保存。",
                category="workflow",
                input_model=AIWorkflowRunRequest,
                handler=_report_strategy_graph_handler,
            )
        )
        registry.register(
            AITool(
                name="workflow.quant_research_graph",
                title="QuantResearchGraph",
                description="运行量化研究图：行情、快照、因子、舆情和研究简报节点。",
                category="workflow",
                input_model=AIWorkflowRunRequest,
                handler=_quant_research_graph_handler,
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
                name="data.stock_detail",
                title="股票详情",
                description="读取前端股票详情页使用的股票、财务、估值和状态字段。",
                category="data",
                input_model=StockDetailInput,
                handler=_stock_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="data.review_context",
                title="个股复盘上下文",
                description="读取单只 A 股的投研复盘上下文，包括基础信息、最近状态和本地数据摘要。",
                category="data",
                input_model=StockReviewInput,
                handler=_stock_review_context_handler,
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
                name="data.stock_list",
                title="股票分页列表",
                description="读取股票列表分页，支持代码/名称搜索、行业、交易所、ST 和自选分组过滤。",
                category="data",
                input_model=StockListInput,
                handler=_stock_list_handler,
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
                name="data.klines_query",
                title="K 线分页查询",
                description="调用平台通用 K 线接口，按 daily/minute 分页读取 K 线数据。",
                category="data",
                input_model=KlinesQueryInput,
                handler=_klines_query_handler,
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
                name="akshare.stock_daily",
                title="AKShare 日线",
                description="显式调用 AKShare 外部数据源读取单只股票日线；不作为默认行情源替代本地/QMT。",
                category="data",
                input_model=AkshareDailyInput,
                handler=_akshare_daily_handler,
            )
        )
        registry.register(
            AITool(
                name="akshare.stock_daily_batch",
                title="AKShare 批量日线",
                description="显式调用 AKShare 外部数据源批量读取日线；不作为默认行情源替代本地/QMT。",
                category="data",
                input_model=AkshareDailyBatchInput,
                handler=_akshare_daily_batch_handler,
            )
        )
        registry.register(
            AITool(
                name="akshare.stock_spot",
                title="AKShare 实时快照",
                description="显式调用 AKShare 外部数据源读取全市场实时快照，可限制返回条数。",
                category="data",
                input_model=AkshareSpotInput,
                handler=_akshare_spot_handler,
            )
        )
        registry.register(
            AITool(
                name="akshare.stock_list",
                title="AKShare 股票列表",
                description="显式调用 AKShare 外部数据源读取 A 股代码名称列表，失败时沿用平台 SQLite 兜底。",
                category="data",
                input_model=AkshareListInput,
                handler=_akshare_stock_list_handler,
            )
        )
        registry.register(
            AITool(
                name="akshare.stock_info",
                title="AKShare 个股信息",
                description="显式调用 AKShare 外部数据源读取个股基础信息和财务指标。",
                category="data",
                input_model=AkshareInfoInput,
                handler=_akshare_stock_info_handler,
            )
        )
        registry.register(
            AITool(
                name="akshare.stock_hist",
                title="AKShare 回测历史数据",
                description="显式调用 AKShare 外部数据源读取 akquant 兼容历史数据；不作为默认回测数据源。",
                category="data",
                input_model=AkshareHistInput,
                handler=_akshare_hist_handler,
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
                name="data.watchlist_group_create",
                title="创建自选股分组",
                description="创建自选股分组，需要确认。",
                category="data",
                input_model=_watchlist_group_create_input_model(),
                handler=_watchlist_group_create_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="data.watchlist_group_delete",
                title="删除自选股分组",
                description="删除自选股分组及其股票，需要确认。",
                category="data",
                input_model=WatchlistStocksInput,
                handler=_watchlist_group_delete_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="data.watchlist_stock_add",
                title="加入自选股",
                description="将股票加入指定自选股分组，需要确认。",
                category="data",
                input_model=WatchlistStockMutationInput,
                handler=_watchlist_stock_add_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="data.watchlist_stock_remove",
                title="移除自选股",
                description="从指定自选股分组移除股票，需要确认。",
                category="data",
                input_model=WatchlistStockMutationInput,
                handler=_watchlist_stock_remove_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="data.indicator_timeseries_batch",
                title="批量指标时序",
                description="批量查询多只股票多个指标的历史时序数据。",
                category="data",
                input_model=IndicatorTimeseriesBatchInput,
                handler=_indicator_timeseries_batch_handler,
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
                name="data.sync_cancel",
                title="取消当前同步",
                description="取消当前正在运行的数据同步任务，需要确认。",
                category="data",
                input_model=EmptyInput,
                handler=_data_sync_cancel_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="data.sync_cancel_all",
                title="取消全部同步",
                description="取消所有排队或运行中的数据同步任务，需要确认。",
                category="data",
                input_model=EmptyInput,
                handler=_data_sync_cancel_all_handler,
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
                name="explorer.table_schema",
                title="数据表 Schema",
                description="读取数据浏览器指定表的列名和类型。",
                category="explorer",
                input_model=ExplorerTableInput,
                handler=_explorer_table_schema_handler,
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
                name="explorer.table_search",
                title="数据表搜索",
                description="按列、过滤条件和 quick_search 搜索数据浏览器表。",
                category="explorer",
                input_model=ExplorerSearchInput,
                handler=_explorer_table_search_handler,
            )
        )
        registry.register(
            AITool(
                name="explorer.distinct_values",
                title="数据表去重值",
                description="读取指定表某列的 distinct 值，用于筛选器和字段探索。",
                category="explorer",
                input_model=ExplorerDistinctInput,
                handler=_explorer_distinct_handler,
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
                name="parquet.dataset_schema",
                title="Parquet Schema",
                description="读取指定 Parquet 数据集的列名和 DuckDB 推断类型。",
                category="explorer",
                input_model=ParquetSchemaInput,
                handler=_parquet_dataset_schema_handler,
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
                name="indicator.categories",
                title="指标分类",
                description="读取 Indicator 体系分类列表。",
                category="factor",
                input_model=EmptyInput,
                handler=_indicator_categories_handler,
            )
        )
        registry.register(
            AITool(
                name="indicator.description",
                title="指标详情",
                description="读取单个 Indicator 指标的描述、依赖、标签和预计算状态。",
                category="factor",
                input_model=IndicatorDescriptionInput,
                handler=_indicator_description_handler,
            )
        )
        registry.register(
            AITool(
                name="indicator.query",
                title="指标值查询",
                description="按股票、指标名和日期查询指标存储中的截面值。",
                category="factor",
                input_model=IndicatorQueryInput,
                handler=_indicator_query_handler,
            )
        )
        registry.register(
            AITool(
                name="indicator.compute",
                title="指标计算",
                description="触发 Indicator 指标计算任务，需要确认。",
                category="factor",
                input_model=IndicatorComputeInput,
                handler=_indicator_compute_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="indicator.screen",
                title="指标选股",
                description="基于 Indicator 存储的指标条件执行选股筛选。",
                category="factor",
                input_model=IndicatorScreenInput,
                handler=_indicator_screen_handler,
            )
        )
        registry.register(
            AITool(
                name="indicator.financial",
                title="指标财务数据",
                description="读取指标模块使用的季度财务数据。",
                category="factor",
                input_model=FinancialInput,
                handler=_indicator_financial_handler,
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
                name="compute.validate",
                title="表达式校验",
                description="校验 Compute Engine 因子表达式语法。",
                category="factor",
                input_model=ComputeValidateInput,
                handler=_compute_validate_handler,
            )
        )
        registry.register(
            AITool(
                name="compute.evaluate",
                title="表达式计算",
                description="按 symbols 和日期区间计算因子表达式。",
                category="factor",
                input_model=ComputeEvaluateInput,
                handler=_compute_evaluate_handler,
            )
        )
        registry.register(
            AITool(
                name="compute.screen",
                title="表达式选股",
                description="用布尔表达式在指定交易日筛选股票。",
                category="factor",
                input_model=ComputeScreenInput,
                handler=_compute_screen_handler,
            )
        )
        registry.register(
            AITool(
                name="compute.precompute",
                title="表达式预计算",
                description="将表达式因子预计算并写入持久因子缓存，需要确认。",
                category="factor",
                input_model=ComputePrecomputeInput,
                handler=_compute_precompute_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="compute.batch",
                title="批量因子计算",
                description="按 FactorConfig 列表批量计算多个因子，返回每个配置的结果或错误。",
                category="factor",
                input_model=ComputeBatchInput,
                handler=_compute_batch_handler,
            )
        )
        registry.register(
            AITool(
                name="evaluation.ic_analysis",
                title="因子 IC 分析",
                description="对表达式因子执行 IC 序列分析。",
                category="factor",
                input_model=_evaluation_ic_analysis_input_model(),
                handler=_evaluation_ic_analysis_handler,
            )
        )
        registry.register(
            AITool(
                name="evaluation.quantile_backtest",
                title="因子分层回测",
                description="对表达式因子执行分层组合回测。",
                category="factor",
                input_model=_evaluation_quantile_backtest_input_model(),
                handler=_evaluation_quantile_backtest_handler,
            )
        )
        registry.register(
            AITool(
                name="evaluation.full_report",
                title="因子完整评估",
                description="生成表达式因子的 IC 和分层回测完整报告。",
                category="factor",
                input_model=_evaluation_full_report_input_model(),
                handler=_evaluation_full_report_handler,
            )
        )
        registry.register(
            AITool(
                name="evaluation.report",
                title="六模块因子报告",
                description="按统一 FactorConfig/EvalConfig 生成六模块因子评估报告。",
                category="factor",
                input_model=EvaluationConfigInput,
                handler=_evaluation_report_handler,
            )
        )
        registry.register(
            AITool(
                name="evaluation.board",
                title="因子看板查询",
                description="查询因子看板排行、覆盖率和最新研究指标。",
                category="factor",
                input_model=FactorBoardInput,
                handler=_evaluation_board_handler,
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
                name="factor.create_legacy",
                title="创建 Legacy 因子",
                description="在 legacy 因子库中创建因子记录，需要确认。",
                category="factor",
                input_model=_legacy_factor_create_input_model(),
                handler=_factor_create_legacy_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.update_legacy",
                title="更新 Legacy 因子",
                description="更新 legacy 因子库中的因子记录，需要确认。",
                category="factor",
                input_model=LegacyFactorUpdateInput,
                handler=_factor_update_legacy_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.delete_legacy",
                title="删除 Legacy 因子",
                description="删除 legacy 因子记录，需要确认。",
                category="factor",
                input_model=IdInput,
                handler=_factor_delete_legacy_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.analysis_list",
                title="因子分析记录",
                description="读取 legacy 因子分析记录列表。",
                category="factor",
                input_model=FactorAnalysisListInput,
                handler=_factor_analysis_list_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.analysis_detail",
                title="因子分析详情",
                description="读取 legacy 因子分析记录详情。",
                category="factor",
                input_model=IdInput,
                handler=_factor_analysis_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.templates_v2",
                title="V2 因子模板",
                description="读取 V2 因子创建模板和预置表达式。",
                category="factor",
                input_model=EmptyInput,
                handler=_factor_templates_v2_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.validate_python",
                title="Python 因子校验",
                description="校验 Python 因子代码语法和 compute 函数入口。",
                category="factor",
                input_model=_python_factor_validate_input_model(),
                handler=_factor_validate_python_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.validate_v2",
                title="V2 因子表达式校验",
                description="按 V2 因子模型校验表达式、股票池和预览参数。",
                category="factor",
                input_model=_factor_validate_v2_input_model(),
                handler=_factor_validate_v2_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.create_v2",
                title="创建 V2 因子",
                description="在 V2 因子库中创建表达式或 Python 因子，需要确认。",
                category="factor",
                input_model=_factor_create_v2_input_model(),
                handler=_saved_factor_create_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.update_v2",
                title="更新 V2 因子",
                description="更新 V2 因子表达式、参数和元数据，需要确认。",
                category="factor",
                input_model=SavedFactorUpdateInput,
                handler=_saved_factor_update_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.delete_v2",
                title="删除 V2 因子",
                description="删除 V2 因子记录，需要确认。",
                category="factor",
                input_model=IdInput,
                handler=_saved_factor_delete_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.preview_saved",
                title="保存因子预览",
                description="预览已保存因子在日期区间和股票池上的因子值，不写入缓存。",
                category="factor",
                input_model=SavedFactorPreviewInput,
                handler=_saved_factor_preview_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.run_python_saved",
                title="运行保存的 Python 因子",
                description="运行已保存的 Python 因子并返回日期区间内的因子值，不写入缓存。",
                category="factor",
                input_model=SavedPythonFactorRunInput,
                handler=_saved_python_factor_run_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.precompute_saved",
                title="保存因子预计算",
                description="将已保存因子计算并写入共享因子值缓存，需要确认。",
                category="factor",
                input_model=SavedFactorPrecomputeInput,
                handler=_saved_factor_precompute_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.coverage_saved",
                title="保存因子覆盖率",
                description="查询已保存因子的因子值缓存覆盖率。",
                category="factor",
                input_model=SavedFactorCoverageInput,
                handler=_saved_factor_coverage_handler,
            )
        )
        registry.register(
            AITool(
                name="factor.analyze_saved",
                title="保存因子分析",
                description="对已保存因子执行 IC、分层和六模块分析并保存结果，需要确认。",
                category="factor",
                input_model=SavedFactorAnalyzeInput,
                handler=_saved_factor_analyze_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor.evaluate_saved",
                title="保存因子评估",
                description="对已保存因子执行 evaluate alias 并保存结果，需要确认。",
                category="factor",
                input_model=SavedFactorAnalyzeInput,
                handler=_saved_factor_evaluate_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="factor_value.param_hashes",
                title="因子参数哈希",
                description="读取指定因子在日期/股票范围内已缓存的参数哈希。",
                category="factor",
                input_model=FactorParamHashInput,
                handler=_factor_value_param_hashes_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.query",
                title="因子值查询",
                description="读取指定因子在某个交易日的截面值。",
                category="factor",
                input_model=FactorQueryInput,
                handler=_factor_value_query_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.paper_manifest",
                title="论文因子实现清单",
                description="读取论文因子实现 manifest，定位已实现和待验证因子。",
                category="factor",
                input_model=EmptyInput,
                handler=_factor_value_paper_manifest_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.paper_experiments",
                title="论文因子实验规格",
                description="读取内置中文论文因子实验规格。",
                category="factor",
                input_model=EmptyInput,
                handler=_factor_value_paper_experiments_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.paper_feature_snapshot",
                title="论文因子特征快照",
                description="读取论文因子在指定日期/股票上的特征覆盖快照。",
                category="factor",
                input_model=PaperFeatureSnapshotInput,
                handler=_factor_value_paper_snapshot_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.precompute_prepare",
                title="因子预计算准备",
                description="检查因子预计算的依赖、股票池和数据缺口。",
                category="factor",
                input_model=_factor_precompute_prepare_input_model(),
                handler=_factor_value_precompute_prepare_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_value.precompute",
                title="因子预计算",
                description="提交或执行 Factor Value Store 因子预计算任务，需要确认。",
                category="factor",
                input_model=_factor_precompute_input_model(),
                handler=_factor_value_precompute_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor_value.group_precompute",
                title="因子集合预计算",
                description="提交或执行 Factor Value Store 因子集合预计算任务，需要确认。",
                category="factor",
                input_model=_factor_group_precompute_input_model(),
                handler=_factor_value_group_precompute_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="factor_research.prepare",
                title="因子研究准备",
                description="检查指定因子研究运行的股票池、因子值覆盖和参数缺口。",
                category="factor",
                input_model=_factor_research_run_input_model(),
                handler=_factor_research_prepare_handler,
            )
        )
        registry.register(
            AITool(
                name="factor_research.submit",
                title="提交因子研究",
                description="运行并保存单因子研究结果，需要确认。",
                category="factor",
                input_model=_factor_research_run_input_model(),
                handler=_factor_research_submit_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor_research.batch",
                title="批量因子研究",
                description="批量运行并保存多个因子的研究结果，需要确认。",
                category="factor",
                input_model=_factor_research_batch_input_model(),
                handler=_factor_research_batch_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="factor_research.combinations",
                title="因子组合候选",
                description="按候选因子和筛选规则读取可组合研究方案。",
                category="factor",
                input_model=_factor_research_combination_input_model(),
                handler=_factor_research_combinations_handler,
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
                name="backtest.engines",
                title="回测引擎",
                description="读取当前注册的回测引擎列表。",
                category="backtest",
                input_model=EmptyInput,
                handler=_backtest_engines_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.preset_dual_stock_grid",
                title="双标的网格预设",
                description="读取内置双标的底仓网格策略代码、参数和优化配置。",
                category="backtest",
                input_model=EmptyInput,
                handler=_backtest_dual_stock_grid_preset_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.create_preset_dual_stock_grid_strategy",
                title="写入双标的网格策略",
                description="创建或更新内置双标的底仓网格策略到策略库，需要确认。",
                category="backtest",
                input_model=_builtin_strategy_create_input_model(),
                handler=_backtest_create_dual_stock_grid_strategy_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.create_preset_multi_factor_strategy",
                title="写入通用多因子策略",
                description="创建或更新内置通用多因子策略到策略库，需要确认。",
                category="backtest",
                input_model=_builtin_strategy_create_input_model(),
                handler=_backtest_create_multi_factor_strategy_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.create_preset_tech_small_cap_strategy",
                title="写入科技小市值策略",
                description="创建或更新内置科技主线小市值策略到策略库，需要确认。",
                category="backtest",
                input_model=_builtin_strategy_create_input_model(),
                handler=_backtest_create_tech_small_cap_strategy_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="backtest.index_pool_detail",
                title="回测指数池详情",
                description="按日期区间读取回测指数池成分覆盖、快照数量和股票列表。",
                category="backtest",
                input_model=IndexPoolInput,
                handler=_backtest_index_pool_detail_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.pool_symbols",
                title="回测股票池",
                description="读取预定义回测股票池代码列表。",
                category="backtest",
                input_model=PoolSymbolsInput,
                handler=_backtest_pool_symbols_handler,
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
                name="strategy.create",
                title="创建策略",
                description="在传统策略库中创建策略记录，需要确认。",
                category="strategy",
                input_model=_legacy_strategy_create_input_model(),
                handler=_strategy_create_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="strategy.update",
                title="更新策略",
                description="更新传统策略库中的策略记录，需要确认。",
                category="strategy",
                input_model=StrategyUpdateToolInput,
                handler=_strategy_update_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="strategy.delete",
                title="删除策略",
                description="删除传统策略库中的策略记录，需要确认。",
                category="strategy",
                input_model=IdInput,
                handler=_strategy_delete_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="strategy.trend_signals_daily",
                title="趋势资金日度信号",
                description="运行趋势资金策略日度信号计算，读取触发明细。",
                category="strategy",
                input_model=StrategySignalsInput,
                handler=_strategy_trend_signals_daily_handler,
            )
        )
        registry.register(
            AITool(
                name="strategy.trend_signals_summary",
                title="趋势资金信号汇总",
                description="运行趋势资金策略信号汇总，读取每日复合触发股票。",
                category="strategy",
                input_model=StrategySignalsInput,
                handler=_strategy_trend_signals_summary_handler,
            )
        )
        registry.register(
            AITool(
                name="strategy.trend_backtest",
                title="趋势资金组合回测",
                description="运行趋势资金组合策略回测，需要确认。",
                category="strategy",
                input_model=StrategySignalsInput,
                handler=_strategy_trend_backtest_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="strategy.deep_value_backtest",
                title="深度价值回测",
                description="运行深度价值策略独立回测并保存记录，需要确认。",
                category="strategy",
                input_model=_deep_value_backtest_input_model(),
                handler=_strategy_deep_value_backtest_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="backtest.create_record",
                title="创建传统回测记录",
                description="创建传统 BacktestService 回测记录，需要确认。",
                category="backtest",
                input_model=_legacy_backtest_create_input_model(),
                handler=_backtest_create_record_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.run_record",
                title="运行传统回测记录",
                description="运行指定传统回测记录，需要确认。",
                category="backtest",
                input_model=IdInput,
                handler=_backtest_run_record_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.delete_record",
                title="删除传统回测记录",
                description="删除指定传统回测记录，需要确认。",
                category="backtest",
                input_model=IdInput,
                handler=_backtest_delete_record_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.delete_records_batch",
                title="批量删除传统回测",
                description="批量删除传统回测记录，需要确认。",
                category="backtest",
                input_model=IdsInput,
                handler=_backtest_delete_records_batch_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="backtest.task_report",
                title="回测任务 HTML 报告",
                description="读取异步回测任务的 QuantStats HTML 报告摘要和报告链接。",
                category="backtest",
                input_model=BacktestReportInput,
                handler=_backtest_task_report_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.task_cancel",
                title="取消回测任务",
                description="取消运行中的回测或优化任务，需要确认。",
                category="backtest",
                input_model=BacktestTaskInput,
                handler=_backtest_task_cancel_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="backtest.data_coverage",
                title="回测数据覆盖检查",
                description="按回测请求检查行情、分钟 timer、因子和基准数据覆盖。",
                category="backtest",
                input_model=_backtest_input_model(),
                handler=_backtest_data_coverage_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.stock_names",
                title="回测股票名称映射",
                description="将回测结果中的股票代码映射为名称。",
                category="backtest",
                input_model=StockNamesInput,
                handler=_backtest_stock_names_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.factor",
                title="因子分层回测",
                description="按 FactorConfig 和 BtConfig 运行因子分层回测，返回净值、基准和指标。",
                category="backtest",
                input_model=BacktestFactorInput,
                handler=_backtest_factor_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.optimize_grid",
                title="Grid Search 优化",
                description="提交 AKQuant Grid Search 参数优化任务，需要确认。",
                category="backtest",
                input_model=_backtest_optimize_input_model(),
                handler=_backtest_optimize_grid_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.optimize_walk_forward",
                title="Walk-forward 优化",
                description="提交 AKQuant Walk-forward 滚动验证任务，需要确认。",
                category="backtest",
                input_model=_backtest_walk_forward_input_model(),
                handler=_backtest_optimize_walk_forward_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="backtest.strategy_params_schema",
                title="策略参数 Schema",
                description="读取 AKQuant 策略参数 schema。",
                category="backtest",
                input_model=_strategy_params_schema_input_model(),
                handler=_backtest_strategy_params_schema_handler,
            )
        )
        registry.register(
            AITool(
                name="backtest.strategy_params_validate",
                title="策略参数校验",
                description="校验 AKQuant 策略参数 payload。",
                category="backtest",
                input_model=_strategy_params_validate_input_model(),
                handler=_backtest_strategy_params_validate_handler,
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
                name="sentiment.threads",
                title="舆情主题线程",
                description="读取未展开到个股前的缓存主题线程。",
                category="sentiment",
                input_model=SentimentThreadsInput,
                handler=_sentiment_threads_handler,
            )
        )
        registry.register(
            AITool(
                name="sentiment.ingest_run",
                title="提交舆情抓取",
                description="提交本地舆情抓取任务，需要确认。",
                category="sentiment",
                input_model=_sentiment_ingest_input_model(),
                handler=_sentiment_ingest_handler,
                risk_level="write",
                requires_confirmation=True,
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
                name="report.chat_session_create",
                title="创建研报对话",
                description="基于研报文本创建策略生成对话会话，返回首次回复和 session_id。",
                category="strategy",
                input_model=ReportChatSessionInput,
                handler=_report_chat_session_create_handler,
                risk_level="write",
            )
        )
        registry.register(
            AITool(
                name="report.chat_session_send",
                title="发送研报对话消息",
                description="向已有研报策略对话会话发送消息，返回 LLM 回复和可能的策略代码。",
                category="strategy",
                input_model=ReportChatMessageInput,
                handler=_report_chat_session_send_handler,
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
                name="live_trading.strategy_profiles",
                title="实盘策略配置",
                description="读取实盘/模拟策略 profile 配置，不修改、不启动 runner。",
                category="trading",
                input_model=LiveProfilesInput,
                handler=_live_strategy_profiles_handler,
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
        registry.register(
            AITool(
                name="live_trading.weekly_trades",
                title="周度成交分析",
                description="读取实盘/模拟成交周度分析，不提交订单。",
                category="trading",
                input_model=LiveWeeklyTradesInput,
                handler=_live_weekly_trades_handler,
            )
        )
        registry.register(
            AITool(
                name="live_trading.account_initialize",
                title="初始化策略账户",
                description="初始化实盘/模拟策略账户资金状态，需要确认。",
                category="trading",
                input_model=_live_account_init_input_model(),
                handler=_live_account_initialize_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.profile_create",
                title="创建实盘策略配置",
                description="创建实盘/模拟策略 profile，需要确认。",
                category="trading",
                input_model=_live_profile_create_input_model(),
                handler=_live_profile_create_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.profile_update",
                title="更新实盘策略配置",
                description="更新实盘/模拟策略 profile，需要确认。",
                category="trading",
                input_model=LiveProfileUpdateToolInput,
                handler=_live_profile_update_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.preflight",
                title="实盘策略预检",
                description="运行实盘/模拟策略预检，检查账户、因子覆盖和阻塞原因。",
                category="trading",
                input_model=_live_preflight_input_model(),
                handler=_live_preflight_handler,
            )
        )
        registry.register(
            AITool(
                name="live_trading.signals",
                title="生成实盘策略信号",
                description="生成实盘/模拟策略候选信号和订单草案，不提交委托；会写审计，需要确认。",
                category="trading",
                input_model=_live_signal_input_model(),
                handler=_live_signals_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.runner_start",
                title="启动实盘 Runner",
                description="启动实盘/模拟 runner，需要确认。",
                category="trading",
                input_model=_live_runner_start_input_model(),
                handler=_live_runner_start_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.runner_stop",
                title="停止实盘 Runner",
                description="停止当前 runner，需要确认。",
                category="trading",
                input_model=EmptyInput,
                handler=_live_runner_stop_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.runner_takeover",
                title="人工接管 Runner",
                description="将 runner 切换为人工接管状态，需要确认。",
                category="trading",
                input_model=_live_runner_takeover_input_model(),
                handler=_live_runner_takeover_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.orders_sync",
                title="同步委托状态",
                description="从交易端同步委托状态，会更新本地状态，需要确认。",
                category="trading",
                input_model=_live_order_sync_input_model(),
                handler=_live_orders_sync_handler,
                risk_level="write",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.orders_submit",
                title="提交委托",
                description="提交实盘/模拟委托，除工具确认外仍要求 payload confirm=true。",
                category="trading",
                input_model=_live_order_submit_input_model(),
                handler=_live_orders_submit_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.orders_cancel",
                title="批量撤单",
                description="撤销实盘/模拟待处理委托，除工具确认外仍要求 payload confirm=true。",
                category="trading",
                input_model=_live_order_cancel_input_model(),
                handler=_live_orders_cancel_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.orders_cancel_resubmit",
                title="撤单重报",
                description="撤销并重报待处理委托，除工具确认外仍要求 payload confirm_cancel/confirm_submit=true。",
                category="trading",
                input_model=_live_order_cancel_resubmit_input_model(),
                handler=_live_orders_cancel_resubmit_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        registry.register(
            AITool(
                name="live_trading.orders_close_local",
                title="本地关闭委托",
                description="仅关闭本地待处理委托记录，除工具确认外仍要求 payload confirm=true。",
                category="trading",
                input_model=_live_order_local_close_input_model(),
                handler=_live_orders_close_local_handler,
                risk_level="danger",
                requires_confirmation=True,
            )
        )
        _registry = registry
    return _registry


def _backtest_input_model() -> type[BaseModel]:
    from app.backtest.api import RunBacktestRequest

    return RunBacktestRequest


def _backtest_optimize_input_model() -> type[BaseModel]:
    from app.backtest.api import OptimizeRequest

    return OptimizeRequest


def _backtest_walk_forward_input_model() -> type[BaseModel]:
    from app.backtest.api import WalkForwardRequest

    return WalkForwardRequest


def _legacy_strategy_create_input_model() -> type[BaseModel]:
    from app.api.backtest import StrategyCreate

    return StrategyCreate


def _legacy_backtest_create_input_model() -> type[BaseModel]:
    from app.api.backtest import BacktestCreate

    return BacktestCreate


def _builtin_strategy_create_input_model() -> type[BaseModel]:
    from app.backtest.api import BuiltinStrategyCreateRequest

    return BuiltinStrategyCreateRequest


def _deep_value_backtest_input_model() -> type[BaseModel]:
    from app.api.strategy import DeepValueBacktestRequest

    return DeepValueBacktestRequest


def _watchlist_group_create_input_model() -> type[BaseModel]:
    from app.api.data import WatchlistGroupCreate

    return WatchlistGroupCreate


def _strategy_params_schema_input_model() -> type[BaseModel]:
    from app.backtest.api import StrategyParamsSchemaRequest

    return StrategyParamsSchemaRequest


def _strategy_params_validate_input_model() -> type[BaseModel]:
    from app.backtest.api import StrategyParamsValidateRequest

    return StrategyParamsValidateRequest


def _factor_precompute_prepare_input_model() -> type[BaseModel]:
    from app.api.factor_values import FactorPrecomputePrepareRequest

    return FactorPrecomputePrepareRequest


def _factor_precompute_input_model() -> type[BaseModel]:
    from app.api.factor_values import FactorPrecomputeRequest

    return FactorPrecomputeRequest


def _factor_group_precompute_input_model() -> type[BaseModel]:
    from app.api.factor_values import FactorGroupPrecomputeRequest

    return FactorGroupPrecomputeRequest


def _evaluation_ic_analysis_input_model() -> type[BaseModel]:
    from app.api.evaluation import ICAnalysisRequest

    return ICAnalysisRequest


def _evaluation_quantile_backtest_input_model() -> type[BaseModel]:
    from app.api.evaluation import QuantileBacktestRequest

    return QuantileBacktestRequest


def _evaluation_full_report_input_model() -> type[BaseModel]:
    from app.api.evaluation import FullReportRequest

    return FullReportRequest


def _python_factor_validate_input_model() -> type[BaseModel]:
    from app.api.factors import PythonFactorValidateRequest

    return PythonFactorValidateRequest


def _factor_validate_v2_input_model() -> type[BaseModel]:
    from app.models.factor import ValidateRequest

    return ValidateRequest


def _factor_research_run_input_model() -> type[BaseModel]:
    from app.api.factor_research import FactorResearchRunRequest

    return FactorResearchRunRequest


def _factor_research_batch_input_model() -> type[BaseModel]:
    from app.api.factor_research import FactorResearchBatchRequest

    return FactorResearchBatchRequest


def _factor_research_combination_input_model() -> type[BaseModel]:
    from app.api.factor_research import FactorResearchCombinationRequest

    return FactorResearchCombinationRequest


def _legacy_factor_create_input_model() -> type[BaseModel]:
    from app.api.factor import FactorCreateRequest

    return FactorCreateRequest


def _factor_create_v2_input_model() -> type[BaseModel]:
    from app.models.factor import FactorCreate

    return FactorCreate


def _sentiment_ingest_input_model() -> type[BaseModel]:
    from app.api.sentiment import IngestRunRequest

    return IngestRunRequest


def _dev_data_mode_update_input_model() -> type[BaseModel]:
    from app.api.system import DevDataModeUpdate

    return DevDataModeUpdate


def _live_trading_guardrails_update_input_model() -> type[BaseModel]:
    from app.api.system import LiveTradingGuardrailsUpdate

    return LiveTradingGuardrailsUpdate


def _live_account_init_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveStrategyAccountInitRequest

    return LiveStrategyAccountInitRequest


def _live_profile_create_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveProfileCreateRequest

    return LiveProfileCreateRequest


def _live_preflight_input_model() -> type[BaseModel]:
    from app.api.live_trading import LivePreflightRequest

    return LivePreflightRequest


def _live_signal_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveSignalRequest

    return LiveSignalRequest


def _live_runner_start_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveRunnerStartRequest

    return LiveRunnerStartRequest


def _live_runner_takeover_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveRunnerTakeoverRequest

    return LiveRunnerTakeoverRequest


def _live_order_sync_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveOrderSyncRequest

    return LiveOrderSyncRequest


def _live_order_submit_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveSubmitOrdersRequest

    return LiveSubmitOrdersRequest


def _live_order_cancel_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveOrderCancelRequest

    return LiveOrderCancelRequest


def _live_order_cancel_resubmit_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveOrderCancelResubmitRequest

    return LiveOrderCancelResubmitRequest


def _live_order_local_close_input_model() -> type[BaseModel]:
    from app.api.live_trading import LiveOrderLocalCloseRequest

    return LiveOrderLocalCloseRequest
