from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Literal

from sqlalchemy import case, create_engine, or_, select, update
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models.llm_endpoint import LlmEndpoint
from app.services.llm_config import ReasoningEffort, WireApi
from app.services.llm_endpoints import LlmEndpointService

logger = logging.getLogger(__name__)

_COOLDOWN_FAILURES = 3
_COOLDOWN_SECONDS = 60
_FAILOVER_ERROR_NAMES = {
    "APIConnectionError",
    "APIError",
    "AuthenticationError",
    "BadGatewayError",
    "InternalServerError",
    "RateLimitError",
    "RouterRateLimitError",
    "ServiceUnavailableError",
    "Timeout",
}
_URL_CREDENTIALS = re.compile(r"(?P<scheme>https?://)(?:[^/@\s]+@)", re.IGNORECASE)
_URL_SECRET_QUERY = re.compile(
    r"(?i)([?&](?:api[_-]?key|access[_-]?token|token|key|secret|password)=)[^&#\s]+"
)


@dataclass
class LLMResult:
    content: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    model: str = ""
    usage: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GatewayCandidate:
    endpoint_id: str | None
    name: str
    api_base: str
    api_key: str | None
    model: str
    source: Literal["database", "environment"]
    provider: str | None = None
    model_role: Literal["primary", "review"] = "primary"
    wire_api: WireApi = "chat_completions"
    reasoning_effort: ReasoningEffort | None = None
    disable_response_storage: bool = False
    requires_openai_auth: bool = False
    api_key_encrypted: str | None = field(default=None, repr=False)


def _sync_database_url() -> str:
    return settings.database_url.replace("+aiosqlite", "")


def _load_enabled_endpoints() -> list[LlmEndpoint]:
    engine = create_engine(_sync_database_url())
    try:
        with Session(engine) as session:
            return list(
                session.scalars(
                    select(LlmEndpoint)
                    .where(LlmEndpoint.enabled.is_(True))
                    .order_by(LlmEndpoint.priority, LlmEndpoint.created_at, LlmEndpoint.id)
                )
            )
    finally:
        engine.dispose()


def _load_candidates(
    *, model_role: Literal["primary", "review"] = "primary"
) -> tuple[list[GatewayCandidate], list[LlmEndpoint]]:
    endpoints = _load_enabled_endpoints()
    if not endpoints:
        if settings.llm_api_key.strip() and settings.llm_default_model.strip():
            return ([
                GatewayCandidate(
                    endpoint_id=None,
                    name="environment",
                    api_base=settings.llm_api_base,
                    api_key=settings.llm_api_key,
                    model=settings.llm_default_model,
                    source="environment",
                    model_role=model_role,
                )
            ], endpoints)
        return [], endpoints

    now = datetime.now()
    candidates = [
        GatewayCandidate(
            endpoint_id=endpoint.id,
            name=endpoint.name,
            api_base=endpoint.api_base,
            api_key=None,
            model=(endpoint.review_model if model_role == "review" and endpoint.review_model else endpoint.model),
            source="database",
            provider=endpoint.provider,
            model_role=model_role,
            wire_api=endpoint.wire_api,
            reasoning_effort=endpoint.reasoning_effort,
            disable_response_storage=endpoint.disable_response_storage,
            requires_openai_auth=endpoint.requires_openai_auth,
            api_key_encrypted=endpoint.api_key_encrypted,
        )
        for endpoint in endpoints
        if endpoint.cooldown_until is None or endpoint.cooldown_until <= now
    ]
    return candidates, endpoints


def gateway_status() -> dict[str, Any]:
    endpoints = _load_enabled_endpoints()
    environment_configured = bool(settings.llm_api_key.strip() and settings.llm_default_model.strip())
    configured = bool(endpoints) or environment_configured
    primary = endpoints[0] if endpoints else None
    configuration_mode = "database" if endpoints else "environment"
    return {
        "configured": configured,
        "state": "ready" if configured else "blocked",
        "api_base": primary.api_base if primary else settings.llm_api_base,
        "model": primary.model if primary else settings.llm_default_model or None,
        "reason": None if configured else "LLM_API_KEY and LLM_DEFAULT_MODEL are required",
        "configuration_mode": configuration_mode,
        "enabled_endpoint_count": len(endpoints),
        "primary_endpoint": (
            {
                "id": primary.id,
                "name": primary.name,
                "model": primary.model,
                "api_base": primary.api_base,
            }
            if primary
            else None
        ),
    }


def _require_config() -> None:
    status = gateway_status()
    if not status["configured"]:
        raise RuntimeError(str(status["reason"]))


def sanitize_error(error: object, api_key: str = "") -> str:
    message = LlmEndpointService._sanitize_error(error, secret=api_key)
    message = _URL_CREDENTIALS.sub(r"\g<scheme>[REDACTED]@", message)
    return _URL_SECRET_QUERY.sub(r"\1[REDACTED]", message)


def _write_health(
    endpoint_id: str,
    *,
    error: object | None = None,
    api_key: str = "",
    attempt_started_at: datetime | None = None,
) -> None:
    now = datetime.now()
    engine = create_engine(_sync_database_url())
    try:
        with Session(engine) as session:
            if error is None:
                statement = (
                    update(LlmEndpoint)
                    .where(
                        LlmEndpoint.id == endpoint_id,
                        or_(
                            LlmEndpoint.last_failure_at.is_(None),
                            LlmEndpoint.last_failure_at < (attempt_started_at or now),
                        ),
                    )
                    .values(
                        consecutive_failures=0,
                        cooldown_until=None,
                        last_error=None,
                        last_success_at=now,
                    )
                )
            else:
                next_failures = LlmEndpoint.consecutive_failures + 1
                statement = (
                    update(LlmEndpoint)
                    .where(LlmEndpoint.id == endpoint_id)
                    .values(
                        consecutive_failures=next_failures,
                        cooldown_until=case(
                            (next_failures >= _COOLDOWN_FAILURES, now + timedelta(seconds=_COOLDOWN_SECONDS)),
                            else_=LlmEndpoint.cooldown_until,
                        ),
                        last_failure_at=now,
                        last_error=sanitize_error(error, api_key),
                    )
                )
            session.execute(statement)
            session.commit()
    finally:
        engine.dispose()


def _try_write_health(
    endpoint_id: str,
    *,
    endpoint_name: str,
    source: Literal["database", "environment"],
    error: object | None = None,
    api_key: str = "",
    attempt_started_at: datetime | None = None,
) -> None:
    try:
        _write_health(
            endpoint_id,
            error=error,
            api_key=api_key,
            attempt_started_at=attempt_started_at,
        )
    except Exception as health_error:
        safe_name = re.sub(r"\s+", " ", endpoint_name).strip()[:100]
        logger.warning(
            "LLM endpoint health write failed endpoint_id=%s endpoint_name=%s source=%s error=%s",
            endpoint_id,
            safe_name,
            source,
            sanitize_error(health_error, api_key),
        )


def _is_failover_error(error: BaseException) -> bool:
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    status_code = getattr(error, "status_code", None)
    if status_code in {401, 403, 404, 429} or isinstance(status_code, int) and status_code >= 500:
        return True
    return type(error).__name__ in _FAILOVER_ERROR_NAMES or type(error).__name__ == "NotFoundError"


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


def _value(item: Any, name: str, default: Any = None) -> Any:
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def _usage_data(response: Any) -> dict[str, Any]:
    usage = _value(response, "usage")
    if isinstance(usage, dict):
        return usage
    return usage.model_dump() if hasattr(usage, "model_dump") else {}


def _normalize_responses_response(response: Any) -> LLMResult:
    calls: list[dict[str, Any]] = []
    for item in _value(response, "output", []) or []:
        if _value(item, "type") != "function_call":
            continue
        arguments = _value(item, "arguments", "{}")
        calls.append({
            "id": _value(item, "call_id") or _value(item, "id", ""),
            "name": _value(item, "name", ""),
            "arguments": json.dumps(arguments, ensure_ascii=False) if isinstance(arguments, dict) else arguments,
        })
    return LLMResult(
        content=str(_value(response, "output_text", "") or ""),
        tool_calls=calls,
        model=str(_value(response, "model", settings.llm_default_model) or ""),
        usage=_usage_data(response),
    )


def _responses_input(
    messages: list[dict[str, Any]], system: str | None
) -> tuple[list[dict[str, Any]], str | None]:
    instructions = [system] if system else []
    response_input: list[dict[str, Any]] = []
    for message in messages:
        if message.get("role") == "system":
            content = message.get("content")
            if isinstance(content, str) and content:
                instructions.append(content)
        else:
            response_input.append(dict(message))
    return response_input, "\n\n".join(instructions) or None


def _responses_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for tool in tools:
        function = tool.get("function")
        if tool.get("type") == "function" and isinstance(function, dict):
            normalized.append({"type": "function", **function})
        else:
            normalized.append(dict(tool))
    return normalized


def complete_candidate_sync(
    candidate: GatewayCandidate,
    messages: list[dict[str, Any]],
    *,
    system: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> LLMResult:
    """Call one explicit candidate without selection, failover, or health writes."""
    common_kwargs: dict[str, Any] = {
        "model": candidate.model,
        "api_base": candidate.api_base,
        "api_key": candidate.api_key,
        "temperature": temperature,
        "timeout": settings.llm_timeout_seconds,
        "num_retries": 0,
    }
    output_tokens = min(max_tokens or settings.llm_max_output_tokens, settings.llm_max_output_tokens)
    if candidate.disable_response_storage:
        common_kwargs["store"] = False

    if candidate.wire_api == "responses":
        from litellm import responses

        response_input, instructions = _responses_input(messages, system)
        kwargs = {**common_kwargs, "input": response_input, "max_output_tokens": output_tokens}
        if instructions:
            kwargs["instructions"] = instructions
        if candidate.reasoning_effort:
            kwargs["reasoning"] = {"effort": candidate.reasoning_effort}
        if tools:
            kwargs["tools"] = _responses_tools(tools)
            kwargs["tool_choice"] = "auto"
        return _normalize_responses_response(responses(**kwargs))

    from litellm import completion

    payload = ([{"role": "system", "content": system}] if system else []) + messages
    kwargs = {**common_kwargs, "messages": payload, "max_tokens": output_tokens}
    if candidate.reasoning_effort:
        kwargs["reasoning_effort"] = candidate.reasoning_effort
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"
    return _normalize_response(completion(**kwargs))


def _decrypt_candidate_key(candidate: GatewayCandidate) -> str:
    if candidate.api_key is not None:
        return candidate.api_key
    endpoint = SimpleNamespace(api_key_encrypted=candidate.api_key_encrypted or "")
    return LlmEndpointService(None)._decrypt_endpoint_key(endpoint)


async def complete_candidate(*args: Any, **kwargs: Any) -> LLMResult:
    return await asyncio.to_thread(complete_candidate_sync, *args, **kwargs)


def complete_sync(
    messages: list[dict[str, Any]],
    *,
    system: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.2,
    max_tokens: int | None = None,
    model_role: Literal["primary", "review"] = "primary",
) -> LLMResult:
    candidates, _ = _load_candidates(model_role=model_role)
    if not candidates:
        _require_config()
        raise RuntimeError("All enabled LLM endpoints are in cooldown")
    failures: list[tuple[str, str]] = []
    for candidate in candidates:
        attempt_started_at = datetime.now()
        try:
            api_key = _decrypt_candidate_key(candidate)
        except ValueError as exc:
            if candidate.endpoint_id:
                _try_write_health(
                    candidate.endpoint_id,
                    endpoint_name=candidate.name,
                    source=candidate.source,
                    error=exc,
                )
            failures.append((candidate.name, sanitize_error(exc)))
            continue
        resolved_candidate = GatewayCandidate(
            endpoint_id=candidate.endpoint_id,
            name=candidate.name,
            api_base=candidate.api_base,
            api_key=api_key,
            model=candidate.model,
            source=candidate.source,
            provider=candidate.provider,
            model_role=candidate.model_role,
            wire_api=candidate.wire_api,
            reasoning_effort=candidate.reasoning_effort,
            disable_response_storage=candidate.disable_response_storage,
            requires_openai_auth=candidate.requires_openai_auth,
        )
        terminal_error: str | None = None
        should_failover = False
        try:
            result = complete_candidate_sync(
                resolved_candidate,
                messages,
                system=system,
                tools=tools,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        except Exception as exc:
            sanitized_error = sanitize_error(exc, api_key)
            if _is_failover_error(exc):
                if candidate.endpoint_id:
                    _try_write_health(
                        candidate.endpoint_id,
                        endpoint_name=candidate.name,
                        source=candidate.source,
                        error=exc,
                        api_key=api_key,
                    )
                failures.append((candidate.name, sanitized_error))
                should_failover = True
            else:
                terminal_error = sanitized_error
        if terminal_error is not None:
            raise RuntimeError(terminal_error) from None
        if should_failover:
            continue
        if candidate.endpoint_id:
            _try_write_health(
                candidate.endpoint_id,
                endpoint_name=candidate.name,
                source=candidate.source,
                attempt_started_at=attempt_started_at,
            )
        return result

    details = "; ".join(f"{name}: {error}" for name, error in failures)
    raise RuntimeError(f"All LLM endpoints failed: {details}") from None


async def complete(*args: Any, **kwargs: Any) -> LLMResult:
    return await asyncio.to_thread(complete_sync, *args, **kwargs)
