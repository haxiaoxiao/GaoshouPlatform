from __future__ import annotations

import ast
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models import Backtest, Factor
from app.services.data_skill import DataSkill
from app.services.sentiment import SentimentService

ToolHandler = Callable[[AsyncSession, dict[str, Any]], Awaitable[Any]]


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    input_schema: dict[str, Any]
    risk: str
    handler: ToolHandler

    def llm_schema(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_schema,
            },
        }

    def public_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
            "risk": self.risk,
        }


async def _system_status(_: AsyncSession, __: dict[str, Any]) -> dict[str, Any]:
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "market_data_backend": settings.market_data_backend,
        "data_dir": settings.gaoshou_data_dir,
    }


async def _stock_snapshot(session: AsyncSession, args: dict[str, Any]) -> dict[str, Any]:
    snapshot = await DataSkill(session).get_stock(str(args["symbol"]))
    return asdict(snapshot) if snapshot else {"found": False, "symbol": args["symbol"]}


async def _factor_list(session: AsyncSession, args: dict[str, Any]) -> list[dict[str, Any]]:
    limit = min(max(int(args.get("limit") or 20), 1), 100)
    rows = list((await session.scalars(select(Factor).order_by(Factor.id.desc()).limit(limit))).all())
    return [{"id": row.id, "name": row.name, "category": row.category, "description": row.description} for row in rows]


async def _backtest_list(session: AsyncSession, args: dict[str, Any]) -> list[dict[str, Any]]:
    limit = min(max(int(args.get("limit") or 10), 1), 50)
    rows = list((await session.scalars(select(Backtest).order_by(Backtest.id.desc()).limit(limit))).all())
    return [{
        "id": row.id,
        "strategy_id": row.strategy_id,
        "status": row.status,
        "start_date": str(row.start_date),
        "end_date": str(row.end_date),
        "engine": row.engine,
        "result": row.result,
    } for row in rows]


async def _sentiment_summary(session: AsyncSession, args: dict[str, Any]) -> dict[str, Any]:
    start = date.fromisoformat(args["start_date"]) if args.get("start_date") else None
    end = date.fromisoformat(args["end_date"]) if args.get("end_date") else None
    return await SentimentService(session).summary(str(args["symbol"]), start, end)


async def _save_strategy(session: AsyncSession, args: dict[str, Any]) -> dict[str, Any]:
    from app.services.backtest_service import BacktestService

    code = str(args["code"])
    ast.parse(code)
    forbidden = [name for name in ("subprocess", "os.system", "eval(", "exec(") if name in code]
    if forbidden:
        raise ValueError("Candidate strategy contains forbidden operations: " + ", ".join(forbidden))
    strategy = await BacktestService(session).create_strategy(
        name=str(args["name"])[:100],
        code=code,
        parameters=dict(args.get("parameters") or {}),
        description=str(args.get("description") or "AI-generated candidate; requires validation"),
    )
    await session.commit()
    return {"strategy_id": strategy.id, "name": strategy.name, "status": "candidate"}


async def _run_backtest(_: AsyncSession, args: dict[str, Any]) -> dict[str, Any]:
    from app.backtest.api import RunBacktestRequest, run_backtest

    return await run_backtest(RunBacktestRequest(**args))


_OBJECT = {"type": "object", "additionalProperties": False}

TOOLS: dict[str, ToolDefinition] = {
    item.name: item for item in (
        ToolDefinition("system_status", "Read platform configuration status.", {**_OBJECT, "properties": {}}, "read", _system_status),
        ToolDefinition("stock_snapshot", "Read an A-share stock snapshot.", {**_OBJECT, "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}, "read", _stock_snapshot),
        ToolDefinition("factor_list", "List available factors.", {**_OBJECT, "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}}}, "read", _factor_list),
        ToolDefinition("backtest_list", "List recent backtest results.", {**_OBJECT, "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50}}}, "read", _backtest_list),
        ToolDefinition("sentiment_summary", "Read cached sentiment for one stock.", {**_OBJECT, "properties": {"symbol": {"type": "string"}, "start_date": {"type": "string"}, "end_date": {"type": "string"}}, "required": ["symbol"]}, "read", _sentiment_summary),
        ToolDefinition("save_strategy_candidate", "Save generated code as a candidate strategy.", {**_OBJECT, "properties": {"name": {"type": "string"}, "code": {"type": "string"}, "parameters": {"type": "object"}, "description": {"type": "string"}}, "required": ["name", "code"]}, "write", _save_strategy),
        ToolDefinition("run_candidate_backtest", "Submit a candidate to the existing asynchronous backtest API.", {"type": "object", "additionalProperties": True, "properties": {"engine": {"type": "string"}, "start_date": {"type": "string"}, "end_date": {"type": "string"}}, "required": ["engine", "start_date", "end_date"]}, "write", _run_backtest),
    )
}


def list_tools(*, read_only: bool = False) -> list[ToolDefinition]:
    return [tool for tool in TOOLS.values() if not read_only or tool.risk == "read"]


def get_tool(name: str) -> ToolDefinition:
    if name not in TOOLS:
        raise ValueError(f"AI tool is not registered: {name}")
    return TOOLS[name]


async def execute_tool(session: AsyncSession, name: str, arguments: dict[str, Any]) -> Any:
    definition = get_tool(name)
    from jsonschema import validate

    validate(instance=arguments, schema=definition.input_schema)
    return await definition.handler(session, arguments)
