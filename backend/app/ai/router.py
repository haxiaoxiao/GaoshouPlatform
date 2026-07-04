from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from app.ai.gateway import LLMGateway, LLMGatewayError
from app.ai.schemas import AIActionCard, AIChatMessage
from app.ai.tools import AITool, get_ai_tool_registry


@dataclass(frozen=True)
class AIRouterNodeTrace:
    name: str
    status: str
    detail: str | None = None

    def model_dump(self) -> dict[str, Any]:
        return {"name": self.name, "status": self.status, "detail": self.detail}


@dataclass(frozen=True)
class AIRoutePlan:
    actions: list[AIActionCard] = field(default_factory=list)
    source: str = "fallback"
    raw_response: str | None = None
    error: str | None = None
    clarification: str | None = None
    confidence: float | None = None
    tool_reasons: list[dict[str, Any]] = field(default_factory=list)
    nodes: list[AIRouterNodeTrace] = field(default_factory=list)

    def model_dump(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "error": self.error,
            "clarification": self.clarification,
            "confidence": self.confidence,
            "tool_reasons": self.tool_reasons,
            "nodes": [node.model_dump() for node in self.nodes],
        }


def _compact_json(value: Any, *, max_chars: int = 14000) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...<truncated>"


def _compact_schema(tool: AITool) -> dict[str, Any]:
    schema = tool.input_model.model_json_schema()
    properties = schema.get("properties") if isinstance(schema, dict) else {}
    compact_properties: dict[str, Any] = {}
    if isinstance(properties, dict):
        for name, prop in properties.items():
            if not isinstance(prop, dict):
                compact_properties[name] = {}
                continue
            item: dict[str, Any] = {}
            for key in ("type", "format", "description", "default", "minimum", "maximum"):
                if key in prop:
                    item[key] = prop[key]
            if "anyOf" in prop:
                item["anyOf"] = prop["anyOf"]
            if "items" in prop:
                item["items"] = prop["items"]
            compact_properties[name] = item
    return {
        "name": tool.name,
        "title": tool.title,
        "category": tool.category,
        "risk_level": tool.risk_level,
        "requires_confirmation": tool.requires_confirmation,
        "description": tool.description,
        "arguments": compact_properties,
        "required": schema.get("required", []) if isinstance(schema, dict) else [],
    }


def _conversation_payload(messages: list[AIChatMessage]) -> list[dict[str, str]]:
    return [{"role": message.role, "content": message.content} for message in messages[-10:]]


def _extract_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", stripped, re.DOTALL | re.IGNORECASE)
    if fence_match:
        stripped = fence_match.group(1)
    if not stripped.startswith("{"):
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end > start:
            stripped = stripped[start : end + 1]
    data = json.loads(stripped)
    if not isinstance(data, dict):
        raise ValueError("router response is not a JSON object")
    return data


def _router_system_prompt(tool_catalog: list[dict[str, Any]]) -> str:
    return (
        "你是 GaoshouPlatform 的工具路由节点 RouterNode。"
        "你的任务是基于完整客户对话选择平台工具，不回答正文。"
        "必须只输出一个 JSON 对象，不能输出 Markdown。"
        "如果最新用户消息只是补充条件，例如“7个交易日”，必须结合上一轮用户意图。"
        "“交易日”是行情日期单位，不能因为包含“交易”而调用 live_trading；"
        "live_trading 只用于实盘、账户、持仓、下单、订单、成交、撤单、QMT、runner 等明确实盘语义。"
        "需要查询股票价格、收盘价、均价、涨跌、K线、过去N个交易日时，优先使用 data.kline_daily。"
        "需要实时盘口时再使用 data.market_snapshot；不要为普通历史均价查询调用 live_trading。"
        "写入、高风险或 requires_confirmation=true 的工具可以列出，但不能假装已经执行。"
        "输出格式："
        "{\"tool_calls\":[{\"tool_name\":\"data.kline_daily\",\"arguments\":{\"symbol\":\"601318.SH\",\"limit\":7},\"reason\":\"...\"}],"
        "\"clarification\":null,\"confidence\":0.0}"
        "\n可用工具目录：\n"
        + _compact_json(tool_catalog, max_chars=24000)
    )


def _router_user_prompt(
    *,
    messages: list[AIChatMessage],
    page_context: dict[str, Any] | None,
    context_hints: dict[str, Any] | None,
) -> str:
    return _compact_json(
        {
            "conversation": _conversation_payload(messages),
            "page_context": page_context or {},
            "context_hints": context_hints or {},
            "instructions": [
                "选择 0-6 个工具调用。",
                "arguments 必须符合工具 schema；日期使用 YYYY-MM-DD。",
                "如果本地 context_hints.resolved_symbol 存在，股票工具优先使用该 symbol。",
                "如果需要澄清但仍可先查询上下文，可以同时给 tool_calls 和 clarification。",
            ],
        },
        max_chars=12000,
    )


def _actions_from_router_json(data: dict[str, Any], *, max_actions: int = 6) -> tuple[list[AIActionCard], list[dict[str, Any]]]:
    registry = get_ai_tool_registry()
    calls = data.get("tool_calls") or data.get("tools") or []
    if not isinstance(calls, list):
        return [], []

    actions: list[AIActionCard] = []
    reasons: list[dict[str, Any]] = []
    seen: set[str] = set()
    for call in calls[:max_actions]:
        if not isinstance(call, dict):
            continue
        tool_name = str(call.get("tool_name") or call.get("name") or "").strip()
        if not tool_name or tool_name in seen:
            continue
        try:
            tool = registry.get(tool_name)
        except KeyError:
            continue
        raw_arguments = call.get("arguments") or {}
        if not isinstance(raw_arguments, dict):
            raw_arguments = {}
        try:
            payload = tool.input_model.model_validate(raw_arguments)
        except Exception:
            continue
        arguments = payload.model_dump(mode="json", exclude_none=True)
        actions.append(tool.action(arguments))
        reasons.append(
            {
                "tool_name": tool.name,
                "reason": str(call.get("reason") or tool.description)[:500],
                "arguments": arguments,
            }
        )
        seen.add(tool_name)
    return actions, reasons


def route_actions_with_llm(
    *,
    gateway: LLMGateway,
    messages: list[AIChatMessage],
    page_context: dict[str, Any] | None = None,
    context_hints: dict[str, Any] | None = None,
) -> AIRoutePlan:
    nodes = [AIRouterNodeTrace(name="context", status="ok", detail="conversation and local hints prepared")]
    if not gateway.is_ready():
        return AIRoutePlan(
            source="fallback",
            error="llm gateway not ready",
            nodes=[*nodes, AIRouterNodeTrace(name="router", status="skipped", detail="gateway not ready")],
        )

    tool_catalog = [_compact_schema(tool) for tool in get_ai_tool_registry().list()]
    try:
        raw = gateway.chat(
            system=_router_system_prompt(tool_catalog),
            messages=[
                {
                    "role": "user",
                    "content": _router_user_prompt(
                        messages=messages,
                        page_context=page_context,
                        context_hints=context_hints,
                    ),
                }
            ],
            temperature=0.0,
            max_tokens=1600,
        )
        data = _extract_json_object(raw)
        actions, reasons = _actions_from_router_json(data)
        clarification = data.get("clarification")
        confidence = data.get("confidence")
        try:
            confidence_value = float(confidence) if confidence is not None else None
        except (TypeError, ValueError):
            confidence_value = None
        return AIRoutePlan(
            actions=actions,
            source="llm",
            raw_response=raw[:4000],
            clarification=str(clarification) if clarification else None,
            confidence=confidence_value,
            tool_reasons=reasons,
            nodes=[*nodes, AIRouterNodeTrace(name="router", status="ok", detail=f"{len(actions)} tool call(s)")],
        )
    except (LLMGatewayError, ValueError, json.JSONDecodeError) as exc:
        return AIRoutePlan(
            source="fallback",
            error=str(exc),
            nodes=[*nodes, AIRouterNodeTrace(name="router", status="fallback", detail=str(exc)[:500])],
        )
    except Exception as exc:
        return AIRoutePlan(
            source="fallback",
            error=str(exc),
            nodes=[*nodes, AIRouterNodeTrace(name="router", status="fallback", detail=str(exc)[:500])],
        )
