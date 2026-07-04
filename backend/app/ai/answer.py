from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from app.ai.gateway import LLMGateway, LLMGatewayError
from app.ai.schemas import AIActionCard, AIChatMessage


@dataclass(frozen=True)
class AIAnswerResult:
    content: str | None = None
    source: str = "template"
    error: str | None = None
    observations: list[dict[str, Any]] = field(default_factory=list)


def _compact_json(value: Any, *, max_chars: int = 18000) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...<truncated>"


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _summarize_kline_rows(rows: list[Any]) -> dict[str, Any]:
    clean_rows = [row for row in rows if isinstance(row, dict)]
    closes = [_to_float(row.get("close")) for row in clean_rows]
    closes = [value for value in closes if value is not None]
    dates = [str(row.get("datetime") or row.get("trade_date") or "")[:10] for row in clean_rows]
    amounts = [_to_float(row.get("amount")) for row in clean_rows]
    amounts = [value for value in amounts if value is not None]
    newest_close = closes[0] if closes else None
    oldest_close = closes[-1] if closes else None
    change_pct = None
    if newest_close is not None and oldest_close:
        change_pct = (newest_close / oldest_close - 1) * 100
    return {
        "rows": clean_rows[:20],
        "count": len(clean_rows),
        "date_start": dates[-1] if dates else None,
        "date_end": dates[0] if dates else None,
        "newest_close": newest_close,
        "oldest_close": oldest_close,
        "min_close": min(closes) if closes else None,
        "max_close": max(closes) if closes else None,
        "avg_close": sum(closes) / len(closes) if closes else None,
        "change_pct": change_pct,
        "avg_amount": sum(amounts) / len(amounts) if amounts else None,
    }


def _compact_result(item: dict[str, Any]) -> dict[str, Any]:
    result = item.get("result")
    tool_name = str(item.get("tool_name") or "")
    compact: dict[str, Any] = {
        "tool_name": tool_name,
        "status": item.get("status"),
        "summary": item.get("summary"),
        "arguments": item.get("arguments") if isinstance(item.get("arguments"), dict) else {},
        "error": item.get("error"),
    }
    if tool_name in {"data.kline_daily", "data.kline_minute"} and isinstance(result, list):
        compact["result_summary"] = _summarize_kline_rows(result)
    elif tool_name == "data.market_snapshot" and isinstance(result, dict):
        bars = result.get("daily_bars") if isinstance(result.get("daily_bars"), list) else []
        compact["result_summary"] = {
            "symbol": result.get("symbol"),
            "stock": result.get("stock"),
            "daily_bars": _summarize_kline_rows(bars),
            "realtime": result.get("realtime"),
            "realtime_error": result.get("realtime_error"),
        }
    elif isinstance(result, dict):
        compact["result_summary"] = {key: result.get(key) for key in list(result)[:30]}
    elif isinstance(result, list):
        compact["result_summary"] = {"count": len(result), "sample": result[:10]}
    else:
        compact["result_summary"] = result
    return compact


def _answer_system_prompt() -> str:
    return (
        "你是 GaoshouPlatform 的 ReAct-style AnswerNode。"
        "你会看到客户完整对话、已执行工具的 observation、待确认动作。"
        "请基于 observation 直接回答客户当前问题，输出中文。"
        "不要编造 observation 中没有的数据；缺数据要明确说明。"
        "A股投研解读要给出：关键数据、走势判断、可能原因或交易含义、风险/下一步验证。"
        "不要输出隐藏思维链，不要写“Thought/Action/Observation”格式；只给最终答复。"
        "如果工具结果不足以得出投资建议，用谨慎措辞。"
    )


def synthesize_answer_with_llm(
    *,
    gateway: LLMGateway,
    messages: list[AIChatMessage],
    routing_text: str,
    executed_tools: list[dict[str, Any]],
    pending_actions: list[AIActionCard],
    trace: dict[str, Any],
) -> AIAnswerResult:
    observations = [_compact_result(item) for item in executed_tools]
    if not gateway.is_ready() or not observations:
        return AIAnswerResult(source="template", observations=observations)

    payload = {
        "conversation": [{"role": message.role, "content": message.content} for message in messages[-10:]],
        "current_task": routing_text,
        "observations": observations,
        "pending_actions": [
            {
                "tool_name": action.tool_name,
                "arguments": action.arguments,
                "risk_level": action.risk_level,
                "requires_confirmation": action.requires_confirmation,
            }
            for action in pending_actions
        ],
        "audit_trace": trace,
    }
    try:
        content = gateway.chat(
            system=_answer_system_prompt(),
            messages=[{"role": "user", "content": _compact_json(payload)}],
            temperature=0.2,
            max_tokens=1800,
        ).strip()
    except LLMGatewayError as exc:
        return AIAnswerResult(source="template", error=str(exc), observations=observations)
    if not content:
        return AIAnswerResult(source="template", error="empty answer", observations=observations)
    return AIAnswerResult(content=content, source="react_answer", observations=observations)
