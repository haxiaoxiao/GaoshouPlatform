from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.services.llm_config import (
    API_KEY_PLACEHOLDER,
    ParsedLlmConfig,
    parse_llm_config,
    synthesize_legacy_config,
)


def _config(**overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "model_provider": "OpenAI",
        "model": "gpt-5.5",
        "model_providers": {
            "OpenAI": {
                "name": "OpenAI",
                "base_url": "https://api.example.com/v1",
                "wire_api": "responses",
                "requires_openai_auth": True,
            }
        },
        "env": {"OPENAI_API_KEY": "secret-key"},
    }
    config.update(overrides)
    return config


def test_parse_json_extracts_runtime_fields_and_removes_key() -> None:
    config = _config(
        review_model="gpt-5.5-review",
        model_reasoning_effort="xhigh",
        disable_response_storage=True,
        network_access="enabled",
        future_option={"mode": "keep"},
    )

    parsed = parse_llm_config(config)

    assert isinstance(parsed, ParsedLlmConfig)
    assert parsed.provider == "OpenAI"
    assert parsed.name == "OpenAI"
    assert parsed.api_base == "https://api.example.com/v1"
    assert parsed.api_key == "secret-key"
    assert parsed.model == "gpt-5.5"
    assert parsed.review_model == "gpt-5.5-review"
    assert parsed.wire_api == "responses"
    assert parsed.reasoning_effort == "xhigh"
    assert parsed.disable_response_storage is True
    assert parsed.requires_openai_auth is True
    assert parsed.sanitized_config["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert parsed.preserved_fields == ("future_option", "network_access")
    assert "secret-key" not in json.dumps(parsed.sanitized_config)


def test_parse_uses_selected_provider_and_preserves_unselected_provider_fields() -> None:
    config = _config()
    config["model_providers"]["Local"] = {
        "name": "Local gateway",
        "base_url": "http://localhost:11434/v1",
        "wire_api": "chat",
        "vendor_option": 3,
    }
    config["model_provider"] = "Local"
    config["OPENAI_API_KEY"] = "root-secret"
    config.pop("env")

    parsed = parse_llm_config(config)

    assert parsed.provider == "Local"
    assert parsed.name == "Local gateway"
    assert parsed.api_base == "http://localhost:11434/v1"
    assert parsed.api_key == "root-secret"
    assert parsed.wire_api == "chat_completions"
    assert parsed.requires_openai_auth is False
    assert parsed.sanitized_config["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert "model_providers.Local.vendor_option" in parsed.preserved_fields
    assert "model_providers.OpenAI" in parsed.preserved_fields


@pytest.mark.parametrize("alias", ["chat", "chat_completion", "chat-completions"])
def test_parse_normalizes_chat_wire_api_aliases(alias: str) -> None:
    config = _config()
    config["model_providers"]["OpenAI"]["wire_api"] = alias

    assert parse_llm_config(config).wire_api == "chat_completions"


def test_parse_uses_root_replacement_when_nested_key_is_placeholder() -> None:
    config = _config(
        env={"OPENAI_API_KEY": API_KEY_PLACEHOLDER},
        OPENAI_API_KEY="root-secret",
    )

    parsed = parse_llm_config(config)

    assert parsed.api_key == "root-secret"
    assert parsed.sanitized_config["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert parsed.sanitized_config["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert "root-secret" not in json.dumps(parsed.sanitized_config)


def test_parse_uses_nested_replacement_when_root_key_is_placeholder() -> None:
    config = _config(OPENAI_API_KEY=API_KEY_PLACEHOLDER)

    parsed = parse_llm_config(config)

    assert parsed.api_key == "secret-key"
    assert parsed.sanitized_config["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert parsed.sanitized_config["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert "secret-key" not in json.dumps(parsed.sanitized_config)


def test_parse_rejects_conflicting_plaintext_credentials() -> None:
    config = _config(OPENAI_API_KEY="different-secret")

    with pytest.raises(ValueError, match="conflicting OPENAI_API_KEY") as error:
        parse_llm_config(config)

    assert "secret-key" not in str(error.value)
    assert "different-secret" not in str(error.value)


def test_parse_rejects_invalid_non_selected_credential_type() -> None:
    config = _config(OPENAI_API_KEY=123)

    with pytest.raises(ValueError, match="OPENAI_API_KEY") as error:
        parse_llm_config(config)

    assert "secret-key" not in str(error.value)


def test_parse_placeholder_requires_explicit_update_mode() -> None:
    config = _config(env={"OPENAI_API_KEY": API_KEY_PLACEHOLDER})

    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        parse_llm_config(config)

    parsed = parse_llm_config(config, allow_placeholder=True)
    assert parsed.api_key is None
    assert parsed.sanitized_config["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER


def test_parse_deep_copies_sanitized_config() -> None:
    config = _config(future_option={"items": [1]})

    parsed = parse_llm_config(config)
    config["future_option"]["items"].append(2)

    assert parsed.sanitized_config["future_option"] == {"items": [1]}
    with pytest.raises(AttributeError):
        parsed.model = "changed"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_provider", 1),
        ("model", False),
        ("review_model", 4),
        ("model_reasoning_effort", "extreme"),
        ("disable_response_storage", "true"),
    ],
)
def test_parse_rejects_invalid_root_field_types(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=field):
        parse_llm_config(_config(**{field: value}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", 1),
        ("base_url", "ftp://api.example.com"),
        ("wire_api", "completions"),
        ("requires_openai_auth", "yes"),
    ],
)
def test_parse_rejects_invalid_provider_fields(field: str, value: object) -> None:
    config = _config()
    config["model_providers"]["OpenAI"][field] = value

    with pytest.raises(ValueError, match=field):
        parse_llm_config(config)


@pytest.mark.parametrize(
    "config",
    [
        "{bad json",
        "[]",
        [],
        {"model_provider": "missing", "model": "gpt", "model_providers": {}},
        _config(model_providers=[]),
        _config(env=[]),
    ],
)
def test_parse_rejects_malformed_non_object_or_invalid_structure(config: object) -> None:
    with pytest.raises(ValueError):
        parse_llm_config(config)


def test_parse_rejects_non_json_values_without_leaking_credentials() -> None:
    config = _config(future_option={"secret": object()})

    with pytest.raises(ValueError) as error:
        parse_llm_config(config)

    assert "secret-key" not in str(error.value)


def test_synthesize_legacy_config_uses_attributes_without_model_import() -> None:
    endpoint = SimpleNamespace(
        name="Legacy provider",
        api_base="https://legacy.example.com/v1",
        model="legacy-model",
    )

    config = synthesize_legacy_config(endpoint)

    assert config == {
        "model_provider": "Legacy provider",
        "model": "legacy-model",
        "model_providers": {
            "Legacy provider": {
                "name": "Legacy provider",
                "base_url": "https://legacy.example.com/v1",
                "wire_api": "chat_completions",
                "requires_openai_auth": True,
            }
        },
        "env": {"OPENAI_API_KEY": API_KEY_PLACEHOLDER},
    }
