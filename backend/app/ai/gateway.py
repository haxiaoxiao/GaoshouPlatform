from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

from app.core.config import settings


@dataclass
class LLMResult:
    content: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    model: str = ""
    usage: dict[str, Any] = field(default_factory=dict)


def gateway_status() -> dict[str, Any]:
    configured = bool(settings.llm_api_key.strip() and settings.llm_default_model.strip())
    return {
        "configured": configured,
        "state": "ready" if configured else "blocked",
        "api_base": settings.llm_api_base,
        "model": settings.llm_default_model or None,
        "reason": None if configured else "LLM_API_KEY and LLM_DEFAULT_MODEL are required",
    }


def _require_config() -> None:
    status = gateway_status()
    if not status["configured"]:
        raise RuntimeError(str(status["reason"]))


def _normalize_response(response: Any) -> LLMResult:
    choice = response.choices[0]
    message = choice.message
    calls: list[dict[str, Any]] = []
    for call in getattr(message, "tool_calls", None) or []:
        function = getattr(call, "function", None)
        arguments = getattr(function, "arguments", "{}")
        calls.append({
            "id": getattr(call, "id", ""),
            "name": getattr(function, "name", ""),
            "arguments": json.dumps(arguments, ensure_ascii=False) if isinstance(arguments, dict) else arguments,
        })
    usage = getattr(response, "usage", None)
    usage_data = usage.model_dump() if hasattr(usage, "model_dump") else {}
    return LLMResult(
        content=str(getattr(message, "content", "") or ""),
        tool_calls=calls,
        model=str(getattr(response, "model", settings.llm_default_model) or ""),
        usage=usage_data,
    )


def complete_sync(
    messages: list[dict[str, Any]],
    *,
    system: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> LLMResult:
    _require_config()
    from litellm import completion

    payload = ([{"role": "system", "content": system}] if system else []) + messages
    kwargs: dict[str, Any] = {
        "model": settings.llm_default_model,
        "api_base": settings.llm_api_base,
        "api_key": settings.llm_api_key,
        "messages": payload,
        "temperature": temperature,
        "max_tokens": min(max_tokens or settings.llm_max_output_tokens, settings.llm_max_output_tokens),
        "timeout": settings.llm_timeout_seconds,
        "num_retries": 2,
    }
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"
    return _normalize_response(completion(**kwargs))


async def complete(*args: Any, **kwargs: Any) -> LLMResult:
    return await asyncio.to_thread(complete_sync, *args, **kwargs)
