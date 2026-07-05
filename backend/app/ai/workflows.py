from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.gateway import get_llm_gateway
from app.ai.router import AIRoutePlan, route_actions_with_llm
from app.ai.schemas import (
    AIActionCard,
    AIChatMessage,
    AIWorkflowDefinitionPublic,
    AIWorkflowNodeTrace,
    AIWorkflowRunRequest,
    AIWorkflowRunResponse,
)
from app.ai.tools import AITool, get_ai_tool_registry


class LangGraphWorkflowState(TypedDict, total=False):
    workflow_name: str
    request: AIWorkflowRunRequest
    session: AsyncSession | None
    response: AIWorkflowRunResponse


@dataclass(frozen=True)
class WorkflowNodeDefinition:
    name: str
    title: str
    detail: str
    tool_name: str | None = None


@dataclass(frozen=True)
class AIWorkflowDefinition:
    name: str
    title: str
    description: str
    category: str
    nodes: list[WorkflowNodeDefinition] = field(default_factory=list)

    def public(self) -> AIWorkflowDefinitionPublic:
        return AIWorkflowDefinitionPublic(
            name=self.name,
            title=self.title,
            description=self.description,
            category=self.category,
            nodes=[
                AIWorkflowNodeTrace(
                    name=node.name,
                    title=node.title,
                    status="defined",
                    detail=node.detail,
                    tool_name=node.tool_name,
                )
                for node in self.nodes
            ],
            input_schema=AIWorkflowRunRequest.model_json_schema(),
        )


WORKFLOWS: dict[str, AIWorkflowDefinition] = {
    "CommandGraph": AIWorkflowDefinition(
        name="CommandGraph",
        title="命令执行图",
        description="将客户自然语言命令路由到 tool registry，并保存节点、工具和答案证据。",
        category="command",
        nodes=[
            WorkflowNodeDefinition("context", "上下文节点", "整理命令、页面和对话上下文。"),
            WorkflowNodeDefinition("router", "路由节点", "优先使用 LLM RouterNode，失败时使用本地兜底。"),
            WorkflowNodeDefinition("execute", "工具执行节点", "执行安全 read 工具，写入工具等待确认。"),
            WorkflowNodeDefinition("answer", "答案节点", "输出工具 observation 摘要和待确认项。"),
        ],
    ),
    "ReportStrategyGraph": AIWorkflowDefinition(
        name="ReportStrategyGraph",
        title="研报策略图",
        description="把研报文本转成平台策略草案，并可继续转成 AKQuant 策略代码。",
        category="research",
        nodes=[
            WorkflowNodeDefinition("validate_report", "研报校验节点", "检查研报文本、文件名和生成参数。"),
            WorkflowNodeDefinition("generate_strategy", "策略生成节点", "调用研报生成策略工具。", "report.strategy_generate"),
            WorkflowNodeDefinition("convert_strategy", "AKQuant 转换节点", "按需转成 AKQuant 策略。", "strategy.convert_to_akquant"),
            WorkflowNodeDefinition("evidence", "证据节点", "保存策略草案、工具输出和下一步建议。"),
        ],
    ),
    "QuantResearchGraph": AIWorkflowDefinition(
        name="QuantResearchGraph",
        title="量化研究图",
        description="围绕单只股票或研究问题组织行情、快照、因子、舆情等只读工具。",
        category="research",
        nodes=[
            WorkflowNodeDefinition("scope", "研究范围节点", "解析 symbol、topic、窗口和附加数据要求。"),
            WorkflowNodeDefinition("market", "行情节点", "读取股票快照和日 K。", "data.kline_daily"),
            WorkflowNodeDefinition("factor", "因子节点", "按需读取指标/因子目录。", "indicator.catalog"),
            WorkflowNodeDefinition("sentiment", "舆情节点", "按需读取个股舆情摘要。", "sentiment.summary"),
            WorkflowNodeDefinition("brief", "研究简报节点", "汇总 observation 和缺口。"),
        ],
    ),
}


def list_ai_workflows() -> list[AIWorkflowDefinitionPublic]:
    return [WORKFLOWS[name].public() for name in sorted(WORKFLOWS)]


def get_ai_workflow(name: str) -> AIWorkflowDefinition:
    try:
        return WORKFLOWS[name]
    except KeyError as exc:
        raise KeyError(f"Unknown AI workflow: {name}") from exc


def _trace(
    name: str,
    title: str,
    status: str,
    detail: str | None = None,
    *,
    tool_name: str | None = None,
    arguments: dict[str, Any] | None = None,
) -> AIWorkflowNodeTrace:
    return AIWorkflowNodeTrace(
        name=name,
        title=title,
        status=status,
        detail=detail,
        tool_name=tool_name,
        arguments=arguments or {},
    )


def _messages_from_request(request: AIWorkflowRunRequest) -> list[AIChatMessage]:
    if request.messages:
        return request.messages
    command = (request.command or "").strip()
    return [AIChatMessage(role="user", content=command)] if command else []


def _latest_text(request: AIWorkflowRunRequest) -> str:
    for message in reversed(_messages_from_request(request)):
        if message.role == "user":
            return message.content.strip()
    return (request.command or "").strip()


def _local_command_actions(text: str) -> list[AIActionCard]:
    lower = text.lower()
    registry = get_ai_tool_registry()
    actions: list[AIActionCard] = []
    if any(word in lower for word in ["诊断", "diagnostics", "trace", "artifact", "路由"]):
        actions.append(registry.get("system.ai_diagnostics").action({"limit": 50}))
    if any(word in lower for word in ["状态", "health", "系统", "后端"]):
        actions.append(registry.get("system.status").action())
    if any(word in lower for word in ["工具", "tool", "mcp", "manifest"]):
        actions.append(registry.get("system.mcp_manifest").action())
    if any(word in lower for word in ["任务", "进度", "runtime"]):
        actions.append(registry.get("runtime.tasks").action({"include_finished": True}))
    if any(word in lower for word in ["策略", "strategy"]):
        actions.append(registry.get("strategy.list").action({"page": 1, "page_size": 20}))
    if any(word in lower for word in ["回测", "backtest"]):
        actions.append(registry.get("backtest.records").action({"page": 1, "page_size": 20}))
    seen: set[str] = set()
    deduped: list[AIActionCard] = []
    for action in actions:
        if action.tool_name in seen:
            continue
        seen.add(action.tool_name)
        deduped.append(action)
    return deduped[:6]


def _action_from_call(call: dict[str, Any]) -> AIActionCard | None:
    tool_name = str(call.get("tool_name") or call.get("name") or "").strip()
    if not tool_name:
        return None
    registry = get_ai_tool_registry()
    try:
        tool = registry.get(tool_name)
    except KeyError:
        return None
    arguments = call.get("arguments") or {}
    if not isinstance(arguments, dict):
        arguments = {}
    try:
        payload = tool.input_model.model_validate(arguments)
    except Exception:
        return None
    return tool.action(payload.model_dump(mode="json", exclude_none=True))


def _actions_from_request_calls(request: AIWorkflowRunRequest) -> list[AIActionCard]:
    calls = request.arguments.get("tool_calls") or []
    if not isinstance(calls, list):
        return []
    actions = [_action_from_call(call) for call in calls if isinstance(call, dict)]
    return [action for action in actions if action is not None][:6]


def _without_workflow_actions(actions: list[AIActionCard]) -> tuple[list[AIActionCard], list[str]]:
    filtered = [action for action in actions if not action.tool_name.startswith("workflow.")]
    removed = [action.tool_name for action in actions if action.tool_name.startswith("workflow.")]
    return filtered, removed


def _can_execute_tool(tool: AITool, request: AIWorkflowRunRequest) -> bool:
    if request.dry_run or not request.auto_execute:
        return False
    if request.confirmed:
        return True
    return tool.risk_level == "read" and not tool.requires_confirmation


async def _route_actions_with_llm_async(**kwargs: Any):
    return await asyncio.to_thread(route_actions_with_llm, **kwargs)


def _llm_router_timeout() -> float:
    gateway = get_llm_gateway()
    return max(1.0, min(float(getattr(gateway, "timeout_seconds", 20.0)), 30.0))


async def _execute_actions(
    actions: list[AIActionCard],
    request: AIWorkflowRunRequest,
    session: AsyncSession | None,
) -> tuple[list[AIWorkflowNodeTrace], list[dict[str, Any]], list[dict[str, Any]]]:
    registry = get_ai_tool_registry()
    nodes: list[AIWorkflowNodeTrace] = []
    tool_results: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for action in actions:
        tool = registry.get(action.tool_name)
        if not _can_execute_tool(tool, request):
            reason = "dry_run" if request.dry_run else "manual execution requested" if not request.auto_execute else "requires confirmation"
            pending_item = {
                "tool_name": action.tool_name,
                "title": action.title,
                "arguments": action.arguments,
                "risk_level": action.risk_level,
                "requires_confirmation": action.requires_confirmation,
                "reason": reason,
            }
            pending.append(pending_item)
            nodes.append(
                _trace(
                    f"tool:{action.tool_name}",
                    action.title,
                    "pending",
                    reason,
                    tool_name=action.tool_name,
                    arguments=action.arguments,
                )
            )
            continue
        result = await registry.execute(
            action.tool_name,
            action.arguments,
            confirmed=request.confirmed,
            session=session,
        )
        payload = result.model_dump()
        payload["arguments"] = action.arguments
        tool_results.append(payload)
        nodes.append(
            _trace(
                f"tool:{action.tool_name}",
                action.title,
                result.status,
                result.summary,
                tool_name=action.tool_name,
                arguments=action.arguments,
            )
        )
    return nodes, tool_results, pending


def _workflow_status(
    *,
    tool_results: list[dict[str, Any]],
    pending_tools: list[dict[str, Any]],
    dry_run: bool,
) -> str:
    if any(item.get("status") == "error" for item in tool_results):
        return "error"
    if pending_tools:
        return "planned" if dry_run else "needs_confirmation"
    return "completed"


def _summary_for_workflow(name: str, tool_results: list[dict[str, Any]], pending_tools: list[dict[str, Any]]) -> str:
    ok_count = sum(1 for item in tool_results if item.get("status") == "ok")
    error_count = sum(1 for item in tool_results if item.get("status") == "error")
    pending_count = len(pending_tools)
    return f"{name} 已处理：{ok_count} 个工具完成，{error_count} 个异常，{pending_count} 个待确认/待执行。"


def _compact_observations(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for item in tool_results:
        result = item.get("result")
        if isinstance(result, list):
            result_summary: Any = {"count": len(result), "sample": result[:5]}
        elif isinstance(result, dict):
            result_summary = {key: result.get(key) for key in list(result)[:20]}
        else:
            result_summary = result
        observations.append(
            {
                "tool_name": item.get("tool_name"),
                "status": item.get("status"),
                "summary": item.get("summary"),
                "arguments": item.get("arguments"),
                "result_summary": result_summary,
                "error": item.get("error"),
            }
        )
    return observations


async def _run_command_graph(
    request: AIWorkflowRunRequest,
    session: AsyncSession | None,
) -> AIWorkflowRunResponse:
    messages = _messages_from_request(request)
    text = _latest_text(request)
    nodes = [_trace("context", "上下文节点", "ok", f"{len(messages)} message(s) prepared")]
    actions = _actions_from_request_calls(request)
    route_source = "explicit"
    route_error: str | None = None
    actions, removed_workflows = _without_workflow_actions(actions)
    if not actions:
        gateway = get_llm_gateway()
        try:
            route_plan = await asyncio.wait_for(
                _route_actions_with_llm_async(
                    gateway=gateway,
                    messages=messages,
                    page_context=request.page_context,
                    context_hints={"latest_user": text, "workflow": "CommandGraph"},
                ),
                timeout=_llm_router_timeout(),
            )
        except TimeoutError:
            route_plan = AIRoutePlan(source="fallback", error="RouterNode timeout")
        actions = route_plan.actions
        actions, removed_workflows = _without_workflow_actions(actions)
        route_source = route_plan.source
        route_error = route_plan.error
        if not actions:
            actions = _local_command_actions(text)
            if actions:
                route_source = f"{route_source}+local"
    router_detail = route_error or f"{route_source}: {len(actions)} tool(s)"
    if removed_workflows:
        router_detail = f"{router_detail}; ignored nested workflow tools: {', '.join(removed_workflows)}"
    nodes.append(_trace("router", "路由节点", "ok" if actions else "empty", router_detail))
    execution_nodes, tool_results, pending = await _execute_actions(actions, request, session)
    nodes.extend(execution_nodes)
    observations = _compact_observations(tool_results)
    nodes.append(_trace("answer", "答案节点", "ok", f"{len(observations)} observation(s), {len(pending)} pending"))
    status = _workflow_status(tool_results=tool_results, pending_tools=pending, dry_run=request.dry_run)
    return AIWorkflowRunResponse(
        workflow_name="CommandGraph",
        status=status,  # type: ignore[arg-type]
        summary=_summary_for_workflow("CommandGraph", tool_results, pending),
        nodes=nodes,
        tool_results=tool_results,
        pending_tools=pending,
        result={
            "route_source": route_source,
            "observations": observations,
            "tool_calls": [action.model_dump() for action in actions],
        },
    )


async def _run_report_strategy_graph(
    request: AIWorkflowRunRequest,
    session: AsyncSession | None,
) -> AIWorkflowRunResponse:
    args = request.arguments
    report_text = str(args.get("report_text") or request.command or "").strip()
    report_filename = str(args.get("report_filename") or "report.txt").strip() or "report.txt"
    convert_to_akquant = bool(args.get("convert_to_akquant", False))
    nodes = [
        _trace(
            "validate_report",
            "研报校验节点",
            "ok" if report_text else "error",
            f"{len(report_text)} chars from {report_filename}",
        )
    ]
    if not report_text:
        return AIWorkflowRunResponse(
            workflow_name="ReportStrategyGraph",
            status="error",
            summary="ReportStrategyGraph 缺少 report_text。",
            nodes=nodes,
            error="report_text is required",
        )
    actions = [
        get_ai_tool_registry().get("report.strategy_generate").action(
            {"report_text": report_text, "report_filename": report_filename}
        )
    ]
    execution_nodes, tool_results, pending = await _execute_actions(actions, request, session)
    nodes.extend(execution_nodes)
    generated_code = ""
    if tool_results and isinstance(tool_results[0].get("result"), dict):
        generated_code = str(tool_results[0]["result"].get("code") or "")
    if convert_to_akquant and generated_code:
        convert_action = get_ai_tool_registry().get("strategy.convert_to_akquant").action({"source_code": generated_code})
        convert_nodes, convert_results, convert_pending = await _execute_actions([convert_action], request, session)
        nodes.extend(convert_nodes)
        tool_results.extend(convert_results)
        pending.extend(convert_pending)
    elif convert_to_akquant:
        nodes.append(_trace("convert_strategy", "AKQuant 转换节点", "skipped", "strategy code not available yet"))
    observations = _compact_observations(tool_results)
    nodes.append(_trace("evidence", "证据节点", "ok", f"{len(observations)} observation(s), {len(pending)} pending"))
    status = _workflow_status(tool_results=tool_results, pending_tools=pending, dry_run=request.dry_run)
    return AIWorkflowRunResponse(
        workflow_name="ReportStrategyGraph",
        status=status,  # type: ignore[arg-type]
        summary=_summary_for_workflow("ReportStrategyGraph", tool_results, pending),
        nodes=nodes,
        tool_results=tool_results,
        pending_tools=pending,
        result={"observations": observations, "convert_to_akquant": convert_to_akquant},
    )


async def _run_quant_research_graph(
    request: AIWorkflowRunRequest,
    session: AsyncSession | None,
) -> AIWorkflowRunResponse:
    args = request.arguments
    symbol = str(args.get("symbol") or "").strip().upper()
    topic = str(args.get("topic") or request.command or "").strip()
    daily_limit = int(args.get("daily_limit") or 60)
    include_factors = bool(args.get("include_factors", True))
    include_sentiment = bool(args.get("include_sentiment", False))
    nodes = [_trace("scope", "研究范围节点", "ok", f"symbol={symbol or '-'}, topic={topic[:80] or '-'}")]
    actions: list[AIActionCard] = []
    registry = get_ai_tool_registry()
    if symbol:
        actions.append(registry.get("data.stock_snapshot").action({"symbol": symbol}))
        actions.append(registry.get("data.kline_daily").action({"symbol": symbol, "limit": max(1, min(daily_limit, 5000))}))
        if include_sentiment:
            actions.append(registry.get("sentiment.summary").action({"symbol": symbol}))
    else:
        actions.append(registry.get("system.data_summary").action())
    if include_factors:
        actions.append(registry.get("indicator.catalog").action())
        actions.append(registry.get("factor_value.definitions").action())
    execution_nodes, tool_results, pending = await _execute_actions(actions, request, session)
    nodes.extend(execution_nodes)
    observations = _compact_observations(tool_results)
    nodes.append(_trace("brief", "研究简报节点", "ok", f"{len(observations)} observation(s), {len(pending)} pending"))
    status = _workflow_status(tool_results=tool_results, pending_tools=pending, dry_run=request.dry_run)
    return AIWorkflowRunResponse(
        workflow_name="QuantResearchGraph",
        status=status,  # type: ignore[arg-type]
        summary=_summary_for_workflow("QuantResearchGraph", tool_results, pending),
        nodes=nodes,
        tool_results=tool_results,
        pending_tools=pending,
        result={
            "topic": topic,
            "symbol": symbol or None,
            "observations": observations,
            "data_gaps": [
                item.get("summary")
                for item in tool_results
                if item.get("status") == "error" and item.get("summary")
            ],
        },
    )


async def _command_graph_node(state: LangGraphWorkflowState) -> dict[str, AIWorkflowRunResponse]:
    return {
        "response": await _run_command_graph(
            state["request"],
            state.get("session"),
        )
    }


async def _report_strategy_graph_node(state: LangGraphWorkflowState) -> dict[str, AIWorkflowRunResponse]:
    return {
        "response": await _run_report_strategy_graph(
            state["request"],
            state.get("session"),
        )
    }


async def _quant_research_graph_node(state: LangGraphWorkflowState) -> dict[str, AIWorkflowRunResponse]:
    return {
        "response": await _run_quant_research_graph(
            state["request"],
            state.get("session"),
        )
    }


def _build_langgraph_workflow(name: str):
    node_by_name = {
        "CommandGraph": ("command_graph", _command_graph_node),
        "ReportStrategyGraph": ("report_strategy_graph", _report_strategy_graph_node),
        "QuantResearchGraph": ("quant_research_graph", _quant_research_graph_node),
    }
    node = node_by_name.get(name)
    if node is None:
        raise KeyError(f"Unknown AI workflow: {name}")
    node_name, node_fn = node
    graph = StateGraph(LangGraphWorkflowState)
    graph.add_node(node_name, node_fn)
    graph.add_edge(START, node_name)
    graph.add_edge(node_name, END)
    return graph.compile()


_COMPILED_LANGGRAPH_WORKFLOWS: dict[str, Any] = {}


def get_compiled_langgraph_workflow(name: str) -> Any:
    get_ai_workflow(name)
    if name not in _COMPILED_LANGGRAPH_WORKFLOWS:
        _COMPILED_LANGGRAPH_WORKFLOWS[name] = _build_langgraph_workflow(name)
    return _COMPILED_LANGGRAPH_WORKFLOWS[name]


async def _run_langgraph_workflow(
    name: str,
    request: AIWorkflowRunRequest,
    session: AsyncSession | None,
) -> AIWorkflowRunResponse:
    compiled = get_compiled_langgraph_workflow(name)
    final_state = await compiled.ainvoke(
        {
            "workflow_name": name,
            "request": request,
            "session": session,
        }
    )
    response = final_state["response"]
    response.result = {
        **response.result,
        "graph_runtime": "langgraph",
        "compiled_graph": type(compiled).__name__,
    }
    return response


async def run_ai_workflow(
    name: str,
    request: AIWorkflowRunRequest,
    *,
    session: AsyncSession | None = None,
) -> AIWorkflowRunResponse:
    return await _run_langgraph_workflow(name, request, session)
