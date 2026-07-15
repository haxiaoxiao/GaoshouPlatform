from __future__ import annotations

import asyncio
import json
import logging
import re
import socket
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from ipaddress import ip_address
from types import SimpleNamespace
from typing import Any, Literal
from urllib.parse import urlparse, urlunparse

import httpx
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
    review_model: str | None = None
    provider: str | None = None
    wire_api: WireApi = "chat_completions"
    reasoning_effort: ReasoningEffort | None = None
    disable_response_storage: bool = False
    requires_openai_auth: bool = False
    api_key_encrypted: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class PinnedHttpDestination:
    url: str
    host_header: str
    sni_hostname: str


def resolve_public_http_destination(url: str) -> PinnedHttpDestination:
    parsed = urlparse(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("LLM endpoint test requires a public HTTP(S) destination")
    try:
        default_port = 443 if parsed.scheme == "https" else 80
        port = parsed.port or default_port
        addresses = socket.getaddrinfo(parsed.hostname, port, proto=socket.IPPROTO_TCP)
        resolved_addresses = []
        for address in addresses:
            resolved = ip_address(address[4][0])
            if resolved.version == 6 and resolved.ipv4_mapped is not None:
                resolved = resolved.ipv4_mapped
            if not resolved.is_global or resolved.is_multicast:
                raise ValueError
            resolved_addresses.append(resolved)
    except (OSError, TypeError, ValueError):
        raise ValueError("LLM endpoint test requires a public HTTP(S) destination") from None
    if not resolved_addresses:
        raise ValueError("LLM endpoint test requires a public HTTP(S) destination")

    selected = resolved_addresses[0]
    ip_host = f"[{selected}]" if selected.version == 6 else str(selected)
    pinned_netloc = f"{ip_host}:{port}" if parsed.port is not None else ip_host
    original_host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    host_header = f"{original_host}:{port}" if parsed.port is not None else original_host
    return PinnedHttpDestination(
        url=urlunparse((parsed.scheme, pinned_netloc, parsed.path, "", parsed.query, "")),
        host_header=host_header,
        sni_hostname=parsed.hostname,
    )


def _health_model_name(model: str) -> str:
    return model.removeprefix("openai/")


async def probe_pinned_llm_connection(
    candidate: GatewayCandidate,
    destination: PinnedHttpDestination,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> LLMResult:
    suffix = "/responses" if candidate.wire_api == "responses" else "/chat/completions"
    request_url = f"{destination.url.rstrip('/')}{suffix}"
    model = _health_model_name(candidate.model)
    if candidate.wire_api == "responses":
        payload: dict[str, Any] = {
            "model": model,
            "input": "Reply with OK.",
            "max_output_tokens": 1,
        }
    else:
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": "Reply with OK."}],
            "max_tokens": 1,
        }
    headers = {
        "Authorization": f"Bearer {candidate.api_key or ''}",
        "Content-Type": "application/json",
        "Host": destination.host_header,
    }
    async with httpx.AsyncClient(
        transport=transport,
        timeout=settings.llm_timeout_seconds,
        follow_redirects=False,
        trust_env=False,
    ) as client:
        request = client.build_request("POST", request_url, headers=headers, json=payload)
        request.extensions["sni_hostname"] = destination.sni_hostname
        try:
            response = await client.send(request)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"LLM endpoint connection failed: {type(exc).__name__}") from None
    if response.status_code < 200 or response.status_code >= 300:
        raise RuntimeError(f"LLM endpoint test returned HTTP {response.status_code}")
    try:
        response_data = response.json()
    except ValueError:
        response_data = {}
    returned_model = response_data.get("model") if isinstance(response_data, dict) else None
    return LLMResult(
        content="",
        model=returned_model if isinstance(returned_model, str) else candidate.model,
    )


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


def _load_candidates() -> tuple[list[GatewayCandidate], list[LlmEndpoint]]:
    endpoints = _load_enabled_endpoints()
    if not endpoints:
        if settings.llm_api_key.strip() and settings.llm_default_model.strip():
            return (
                [
                    GatewayCandidate(
                        endpoint_id=None,
                        name="environment",
                        api_base=settings.llm_api_base,
                        api_key=settings.llm_api_key,
                        model=settings.llm_default_model,
                        source="environment",
                    )
                ],
                endpoints,
            )
        return [], endpoints

    now = datetime.now()
    candidates = [
        GatewayCandidate(
            endpoint_id=endpoint.id,
            name=endpoint.name,
            api_base=endpoint.api_base,
            api_key=None,
            model=endpoint.model,
            source="database",
            review_model=endpoint.review_model,
            provider=endpoint.provider,
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
    environment_configured = bool(
        settings.llm_api_key.strip() and settings.llm_default_model.strip()
    )
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
                            (
                                next_failures >= _COOLDOWN_FAILURES,
                                now + timedelta(seconds=_COOLDOWN_SECONDS),
                            ),
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
        calls.append(
            {
                "id": getattr(call, "id", ""),
                "name": getattr(function, "name", ""),
                "arguments": json.dumps(arguments, ensure_ascii=False)
                if isinstance(arguments, dict)
                else arguments,
            }
        )
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
        calls.append(
            {
                "id": _value(item, "call_id") or _value(item, "id", ""),
                "name": _value(item, "name", ""),
                "arguments": json.dumps(arguments, ensure_ascii=False)
                if isinstance(arguments, dict)
                else arguments,
            }
        )
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
    pending_calls: set[str] = set()
    for message in messages:
        item_type = message.get("type")
        role = message.get("role")
        if item_type == "function_call":
            item = _responses_function_call(message)
            if item["call_id"] in pending_calls:
                raise _malformed_responses_history()
            pending_calls.add(item["call_id"])
            response_input.append(item)
        elif item_type == "function_call_output":
            item = _responses_function_call_output(message, pending_calls)
            pending_calls.remove(item["call_id"])
            response_input.append(item)
        elif role == "system":
            content = message.get("content")
            if isinstance(content, str) and content:
                instructions.append(content)
            elif content is not None and content != "":
                raise _malformed_responses_history()
        elif role in {"user", "assistant", "developer"}:
            content = message.get("content")
            if content is not None and content != "":
                response_input.append(_responses_message(message, role, content))
            tool_calls = message.get("tool_calls")
            if tool_calls is not None:
                if role != "assistant" or not isinstance(tool_calls, list):
                    raise _malformed_responses_history()
                for tool_call in tool_calls:
                    item = _chat_tool_call(tool_call)
                    if item["call_id"] in pending_calls:
                        raise _malformed_responses_history()
                    pending_calls.add(item["call_id"])
                    response_input.append(item)
        elif role == "tool":
            item = _chat_tool_output(message, pending_calls)
            pending_calls.remove(item["call_id"])
            response_input.append(item)
        else:
            raise _malformed_responses_history()
    return response_input, "\n\n".join(instructions) or None


def _malformed_responses_history() -> ValueError:
    return ValueError("Malformed Responses tool history")


def _malformed_responses_content() -> ValueError:
    return ValueError("Malformed Responses message content")


def _responses_message(
    message: dict[str, Any], role: Literal["user", "assistant", "developer"], content: Any
) -> dict[str, Any]:
    if isinstance(content, str):
        return {"type": "message", "role": role, "content": content}
    if not isinstance(content, list) or not content:
        raise _malformed_responses_content()
    normalized = [_responses_content_part(role, part) for part in content]
    has_output_text = any(part["type"] == "output_text" for part in normalized)
    if has_output_text:
        if role != "assistant" or any(part["type"] != "output_text" for part in normalized):
            raise _malformed_responses_content()
        message_id = message.get("id")
        status = message.get("status")
        if (
            not isinstance(message_id, str)
            or not message_id
            or status
            not in {
                "in_progress",
                "completed",
                "incomplete",
            }
        ):
            raise _malformed_responses_content()
        return {
            "type": "message",
            "id": message_id,
            "status": status,
            "role": "assistant",
            "content": normalized,
        }
    return {"type": "message", "role": role, "content": normalized}


def _responses_content_part(
    role: Literal["user", "assistant", "developer"], part: Any
) -> dict[str, Any]:
    if not isinstance(part, dict):
        raise _malformed_responses_content()
    part_type = part.get("type")
    if part_type in {"text", "input_text"}:
        text = part.get("text")
        if not isinstance(text, str):
            raise _malformed_responses_content()
        return {"type": "input_text", "text": text}
    if part_type in {"image_url", "input_image"}:
        if role != "user":
            raise _malformed_responses_content()
        return _responses_image_part(part)
    if part_type == "output_text":
        if role != "assistant" or not isinstance(part.get("text"), str):
            raise _malformed_responses_content()
        annotations = part.get("annotations", [])
        if not isinstance(annotations, list):
            raise _malformed_responses_content()
        return {"type": "output_text", "text": part["text"], "annotations": annotations}
    raise _malformed_responses_content()


def _responses_image_part(part: dict[str, Any]) -> dict[str, Any]:
    image_value = part.get("image_url")
    detail = part.get("detail", "auto")
    if isinstance(image_value, dict):
        detail = image_value.get("detail", detail)
        image_value = image_value.get("url")
    if not isinstance(image_value, str) or not image_value:
        raise _malformed_responses_content()
    if detail not in {"low", "high", "auto", "original"}:
        raise _malformed_responses_content()
    return {"type": "input_image", "image_url": image_value, "detail": detail}


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        raise _malformed_responses_history() from None


def _responses_function_call(item: dict[str, Any]) -> dict[str, Any]:
    call_id = item.get("call_id")
    name = item.get("name")
    arguments = item.get("arguments")
    if not isinstance(call_id, str) or not call_id or not isinstance(name, str) or not name:
        raise _malformed_responses_history()
    if not isinstance(arguments, (str, dict)):
        raise _malformed_responses_history()
    return {
        "type": "function_call",
        "call_id": call_id,
        "name": name,
        "arguments": _json_text(arguments),
    }


def _responses_function_call_output(
    item: dict[str, Any], pending_calls: set[str]
) -> dict[str, Any]:
    call_id = item.get("call_id")
    if not isinstance(call_id, str) or call_id not in pending_calls or "output" not in item:
        raise _malformed_responses_history()
    return {
        "type": "function_call_output",
        "call_id": call_id,
        "output": _json_text(item["output"]),
    }


def _chat_tool_call(tool_call: Any) -> dict[str, Any]:
    if not isinstance(tool_call, dict):
        raise _malformed_responses_history()
    function = tool_call.get("function")
    if not isinstance(function, dict):
        raise _malformed_responses_history()
    return _responses_function_call(
        {
            "call_id": tool_call.get("id") or tool_call.get("call_id"),
            "name": function.get("name"),
            "arguments": function.get("arguments"),
        }
    )


def _chat_tool_output(message: dict[str, Any], pending_calls: set[str]) -> dict[str, Any]:
    if "content" not in message:
        raise _malformed_responses_history()
    return _responses_function_call_output(
        {
            "call_id": message.get("tool_call_id") or message.get("call_id"),
            "output": message.get("content"),
        },
        pending_calls,
    )


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
    model_role: Literal["primary", "review"] = "primary",
    follow_redirects: bool | None = None,
) -> LLMResult:
    """Call one explicit candidate without selection, failover, or health writes."""
    selected_model = (
        candidate.review_model
        if model_role == "review" and candidate.review_model
        else candidate.model
    )
    common_kwargs: dict[str, Any] = {
        "model": selected_model,
        "api_base": candidate.api_base,
        "api_key": candidate.api_key,
        "temperature": temperature,
        "timeout": settings.llm_timeout_seconds,
        "num_retries": 0,
    }
    if candidate.source == "database" and "/" not in selected_model:
        common_kwargs["custom_llm_provider"] = "openai"
    output_tokens = min(
        max_tokens or settings.llm_max_output_tokens, settings.llm_max_output_tokens
    )
    if candidate.disable_response_storage:
        common_kwargs["store"] = False

    with ExitStack() as stack:
        if follow_redirects is not None:
            import httpx
            from openai import OpenAI

            http_client = stack.enter_context(httpx.Client(follow_redirects=follow_redirects))
            common_kwargs["client"] = OpenAI(
                api_key=candidate.api_key or "not-provided",
                base_url=candidate.api_base,
                http_client=http_client,
                max_retries=0,
            )

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


async def complete_candidate(
    candidate: GatewayCandidate,
    messages: list[dict[str, Any]],
    *,
    system: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.2,
    max_tokens: int | None = None,
    model_role: Literal["primary", "review"] = "primary",
    follow_redirects: bool | None = None,
) -> LLMResult:
    return await asyncio.to_thread(
        complete_candidate_sync,
        candidate,
        messages,
        system=system,
        tools=tools,
        temperature=temperature,
        max_tokens=max_tokens,
        model_role=model_role,
        follow_redirects=follow_redirects,
    )


def complete_sync(
    messages: list[dict[str, Any]],
    *,
    system: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.2,
    max_tokens: int | None = None,
    model_role: Literal["primary", "review"] = "primary",
) -> LLMResult:
    candidates, _ = _load_candidates()
    if model_role == "review":
        candidates = [candidate for candidate in candidates if candidate.review_model] + [
            candidate for candidate in candidates if not candidate.review_model
        ]
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
            review_model=candidate.review_model,
            provider=candidate.provider,
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
                model_role=model_role,
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
