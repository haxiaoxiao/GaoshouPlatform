from __future__ import annotations

import ast
import asyncio
import json
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph


class ReportStrategyState(TypedDict, total=False):
    report_text: str
    filename: str
    result: dict[str, Any]
    validation: dict[str, Any]


class QuantResearchState(TypedDict, total=False):
    question: str
    context: dict[str, Any]
    report: str
    backtest_request: dict[str, Any] | None


async def _generate_report_strategy(state: ReportStrategyState) -> dict[str, Any]:
    from app.services.report_to_strategy import generate_strategy

    result = await asyncio.to_thread(generate_strategy, state["report_text"])
    return {"result": result}


async def _validate_report_strategy(state: ReportStrategyState) -> dict[str, Any]:
    code = str((state.get("result") or {}).get("code") or "")
    errors: list[str] = []
    try:
        ast.parse(code)
    except SyntaxError as exc:
        errors.append(f"Python syntax error: {exc.msg} at line {exc.lineno}")
    forbidden = [name for name in ("subprocess", "eval(", "exec(", "os.system") if name in code]
    if forbidden:
        errors.append("Forbidden generated-code operations: " + ", ".join(forbidden))
    return {"validation": {"valid": not errors, "errors": errors}}


async def _quant_research(state: QuantResearchState) -> dict[str, Any]:
    from app.ai.gateway import complete
    from app.ai.tools import execute_tool
    from app.db.sqlite import async_session_factory

    context = dict(state.get("context") or {})
    evidence: dict[str, Any] = {}
    async with async_session_factory() as session:
        evidence["factors"] = await execute_tool(session, "factor_list", {"limit": 20})
        evidence["recent_backtests"] = await execute_tool(session, "backtest_list", {"limit": 10})
        symbol = str(context.get("symbol") or "").strip()
        if symbol:
            evidence["stock"] = await execute_tool(session, "stock_snapshot", {"symbol": symbol})
            evidence["sentiment"] = await execute_tool(session, "sentiment_summary", {"symbol": symbol})

    result = await complete(
        [{"role": "user", "content": state["question"] + "\nEvidence:\n" + json.dumps(evidence, ensure_ascii=False, default=str)}],
        system=(
            "Produce an evidence-aware A-share quantitative research plan. "
            "Use only supplied evidence and identify missing data. Return JSON with report and optional backtest_request; "
            "the backtest_request is only a candidate and requires user confirmation."
        ),
    )
    try:
        parsed = json.loads(result.content)
    except json.JSONDecodeError:
        parsed = {"report": result.content, "backtest_request": None}
    return {"report": str(parsed.get("report") or result.content), "backtest_request": parsed.get("backtest_request")}


def build_report_strategy_graph(checkpointer: Any = None):
    graph = StateGraph(ReportStrategyState)
    graph.add_node("generate", _generate_report_strategy)
    graph.add_node("validate", _validate_report_strategy)
    graph.add_edge(START, "generate")
    graph.add_edge("generate", "validate")
    graph.add_edge("validate", END)
    return graph.compile(checkpointer=checkpointer)


def build_quant_research_graph(checkpointer: Any = None):
    graph = StateGraph(QuantResearchState)
    graph.add_node("research", _quant_research)
    graph.add_edge(START, "research")
    graph.add_edge("research", END)
    return graph.compile(checkpointer=checkpointer)
