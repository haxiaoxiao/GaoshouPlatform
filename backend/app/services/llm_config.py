from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, Protocol

API_KEY_PLACEHOLDER = "__GAOSHOU_STORED_SECRET__"

WireApi = Literal["chat_completions", "responses"]
ReasoningEffort = Literal["none", "minimal", "low", "medium", "high", "xhigh"]

_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}
_WIRE_API_ALIASES: dict[str, WireApi] = {
    "chat": "chat_completions",
    "chat_completion": "chat_completions",
    "chat-completions": "chat_completions",
    "chat_completions": "chat_completions",
    "responses": "responses",
}
_ROOT_RUNTIME_FIELDS = {
    "OPENAI_API_KEY",
    "disable_response_storage",
    "env",
    "model",
    "model_provider",
    "model_providers",
    "model_reasoning_effort",
    "review_model",
}
_PROVIDER_RUNTIME_FIELDS = {"base_url", "name", "requires_openai_auth", "wire_api"}
_SECRET_FIELD_NAMES = {
    "api_key",
    "api_secret",
    "access_token",
    "authorization",
    "auth_token",
    "client_secret",
    "credential",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "token",
}
_SECRET_CONTAINER_NAMES = {"auth", "credentials", "secrets"}
_SECRET_FIELD_SUFFIXES = ("_api_key", "_password", "_secret", "_token")

ImmutableJson = Mapping[str, "ImmutableJson"] | tuple["ImmutableJson", ...] | str | int | float | bool | None


@dataclass(frozen=True)
class ParsedLlmConfig:
    provider: str
    name: str
    api_base: str
    api_key: str | None = field(repr=False)
    model: str
    review_model: str | None
    wire_api: WireApi
    reasoning_effort: ReasoningEffort | None
    disable_response_storage: bool
    requires_openai_auth: bool
    sanitized_config: Mapping[str, ImmutableJson]
    preserved_fields: tuple[str, ...]

    def to_json_config(self) -> dict[str, Any]:
        """Return a fresh mutable JSON-compatible copy for persistence."""
        return _thaw_json(self.sanitized_config)


class LegacyLlmEndpoint(Protocol):
    name: str
    api_base: str
    model: str


def parse_llm_config(
    config: Mapping[str, Any] | str, *, allow_placeholder: bool = False
) -> ParsedLlmConfig:
    document = _json_object_copy(config)
    provider = _required_string(document, "model_provider")
    model = _required_string(document, "model")
    review_model = _optional_string(document, "review_model")

    providers = document.get("model_providers")
    if not isinstance(providers, dict):
        raise ValueError("model_providers must be a JSON object")
    provider_config = providers.get(provider)
    if not isinstance(provider_config, dict):
        raise ValueError(f"model_provider {provider!r} must exist in model_providers")

    name = _optional_string(provider_config, "name") or provider
    api_base = _required_string(provider_config, "base_url")
    _validate_url(name=name, api_base=api_base, model=model)
    wire_api = _wire_api(provider_config.get("wire_api", "chat_completions"))
    reasoning_effort = _reasoning_effort(document.get("model_reasoning_effort"))
    disable_response_storage = _boolean(document, "disable_response_storage", default=False)
    requires_openai_auth = _boolean(provider_config, "requires_openai_auth", default=False)
    api_key = _extract_and_redact_key(document, allow_placeholder=allow_placeholder)
    sanitized_document = _sanitize_secrets(document)

    return ParsedLlmConfig(
        provider=provider,
        name=name,
        api_base=api_base,
        api_key=api_key,
        model=model,
        review_model=review_model,
        wire_api=wire_api,
        reasoning_effort=reasoning_effort,
        disable_response_storage=disable_response_storage,
        requires_openai_auth=requires_openai_auth,
        sanitized_config=_freeze_json(sanitized_document),
        preserved_fields=_preserved_fields(document, provider),
    )


def synthesize_legacy_config(endpoint: LegacyLlmEndpoint) -> dict[str, Any]:
    provider = endpoint.name
    return {
        "model_provider": provider,
        "model": endpoint.model,
        "model_providers": {
            provider: {
                "name": provider,
                "base_url": endpoint.api_base,
                "wire_api": "chat_completions",
                "requires_openai_auth": True,
            }
        },
        "env": {"OPENAI_API_KEY": API_KEY_PLACEHOLDER},
    }


def _json_object_copy(config: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(config, str):
        try:
            value = json.loads(config)
        except json.JSONDecodeError as exc:
            raise ValueError("config must contain valid JSON") from exc
    elif isinstance(config, Mapping):
        try:
            value = json.loads(json.dumps(config))
        except (TypeError, ValueError) as exc:
            raise ValueError("config must contain only JSON-compatible values") from exc
    else:
        raise ValueError("config must be a JSON object")
    if not isinstance(value, dict):
        raise ValueError("config must be a JSON object")
    return value


def _required_string(value: Mapping[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result.strip():
        raise ValueError(f"{field} must be a nonblank string")
    return result.strip()


def _optional_string(value: Mapping[str, Any], field: str) -> str | None:
    result = value.get(field)
    if result is None:
        return None
    if not isinstance(result, str) or not result.strip():
        raise ValueError(f"{field} must be a nonblank string or null")
    return result.strip()


def _boolean(value: Mapping[str, Any], field: str, *, default: bool) -> bool:
    result = value.get(field, default)
    if not isinstance(result, bool):
        raise ValueError(f"{field} must be a JSON boolean")
    return result


def _wire_api(value: Any) -> WireApi:
    if not isinstance(value, str) or value not in _WIRE_API_ALIASES:
        raise ValueError("wire_api must be responses or chat_completions")
    return _WIRE_API_ALIASES[value]


def _reasoning_effort(value: Any) -> ReasoningEffort | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in _REASONING_EFFORTS:
        raise ValueError("model_reasoning_effort has an unsupported value")
    return value  # type: ignore[return-value]


def _extract_and_redact_key(document: dict[str, Any], *, allow_placeholder: bool) -> str | None:
    env = document.get("env")
    if env is not None and not isinstance(env, dict):
        raise ValueError("env must be a JSON object")
    locations = []
    if isinstance(env, dict) and "OPENAI_API_KEY" in env:
        locations.append(env)
    if "OPENAI_API_KEY" in document:
        locations.append(document)

    plaintext_keys: set[str] = set()
    has_placeholder = False
    for location in locations:
        candidate = location["OPENAI_API_KEY"]
        if not isinstance(candidate, str) or not candidate.strip():
            raise ValueError("OPENAI_API_KEY must be a nonblank string")
        candidate = candidate.strip()
        if candidate == API_KEY_PLACEHOLDER:
            has_placeholder = True
        else:
            plaintext_keys.add(candidate)

    if len(plaintext_keys) > 1:
        raise ValueError("conflicting OPENAI_API_KEY credentials")
    if not plaintext_keys and not allow_placeholder:
        if has_placeholder:
            raise ValueError("OPENAI_API_KEY placeholder is only valid when updating")
        raise ValueError("OPENAI_API_KEY is required")
    for location in locations:
        location["OPENAI_API_KEY"] = API_KEY_PLACEHOLDER
    return next(iter(plaintext_keys), None)


def _preserved_fields(document: Mapping[str, Any], provider: str) -> tuple[str, ...]:
    preserved = [key for key in document if key not in _ROOT_RUNTIME_FIELDS]
    env = document.get("env")
    if isinstance(env, dict):
        preserved.extend(f"env.{key}" for key in env if key != "OPENAI_API_KEY")
    providers = document.get("model_providers")
    if isinstance(providers, dict):
        preserved.extend(f"model_providers.{key}" for key in providers if key != provider)
        selected = providers.get(provider)
        if isinstance(selected, dict):
            preserved.extend(
                f"model_providers.{provider}.{key}"
                for key in selected
                if key not in _PROVIDER_RUNTIME_FIELDS
            )
    return tuple(sorted(preserved))


def _sanitize_secrets(value: Any, *, redact_scalar_leaves: bool = False) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = key.casefold()
            secret_field = normalized_key in _SECRET_FIELD_NAMES or normalized_key.endswith(
                _SECRET_FIELD_SUFFIXES
            )
            secret_container = normalized_key in _SECRET_CONTAINER_NAMES
            if (secret_field or secret_container) and not isinstance(item, (dict, list)):
                sanitized[key] = API_KEY_PLACEHOLDER
            elif redact_scalar_leaves and not isinstance(item, (dict, list, bool)):
                sanitized[key] = API_KEY_PLACEHOLDER
            else:
                sanitized[key] = _sanitize_secrets(
                    item,
                    redact_scalar_leaves=redact_scalar_leaves or secret_container,
                )
        return sanitized
    if isinstance(value, list):
        return [
            API_KEY_PLACEHOLDER
            if redact_scalar_leaves and not isinstance(item, (dict, list, bool))
            else _sanitize_secrets(item, redact_scalar_leaves=redact_scalar_leaves)
            for item in value
        ]
    return value


def _freeze_json(value: Any) -> ImmutableJson:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: ImmutableJson) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _validate_url(*, name: str, api_base: str, model: str) -> None:
    # Lazy import keeps llm_endpoints free to import this parser in the persistence task.
    from app.services.llm_endpoints import LlmEndpointService

    try:
        LlmEndpointService._validate_fields(name=name, api_base=api_base, model=model)
    except ValueError as exc:
        raise ValueError(str(exc).replace("api_base", "base_url")) from None
