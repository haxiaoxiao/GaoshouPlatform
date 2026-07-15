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
    assert "secret-key" not in json.dumps(parsed.to_json_config())


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
    assert "root-secret" not in json.dumps(parsed.to_json_config())


def test_parse_uses_nested_replacement_when_root_key_is_placeholder() -> None:
    config = _config(OPENAI_API_KEY=API_KEY_PLACEHOLDER)

    parsed = parse_llm_config(config)

    assert parsed.api_key == "secret-key"
    assert parsed.sanitized_config["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert parsed.sanitized_config["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert "secret-key" not in json.dumps(parsed.to_json_config())


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


def test_parse_create_requires_plaintext_api_key() -> None:
    config = _config(future_option="must-not-leak")
    config.pop("env")

    with pytest.raises(ValueError, match="OPENAI_API_KEY is required") as error:
        parse_llm_config(config)

    assert "must-not-leak" not in str(error.value)


def test_parsed_config_repr_never_contains_api_key() -> None:
    parsed = parse_llm_config(_config())

    assert "secret-key" not in repr(parsed)
    assert "api_key=" not in repr(parsed)


def test_parse_recursively_sanitizes_unselected_provider_and_nested_secrets() -> None:
    config = _config(
        env={
            "OPENAI_API_KEY": "secret-key",
            "VENDOR_TOKEN": "env-token",
            "CLIENT_SECRET": "env-secret",
            "DB_PASSWORD": "env-password",
            "HARMLESS_MODE": "keep",
        },
        nested={
            "Authorization": "Bearer nested-auth",
            "credentials": {"token": "nested-token", "label": "keep"},
            "requires_openai_auth": True,
        },
    )
    config["model_providers"]["Unused"] = {
        "name": "Unused",
        "base_url": "https://unused.example.com/v1",
        "api_key": "unused-key",
        "metadata": {"PASSWORD": "unused-password", "region": "keep"},
    }

    parsed = parse_llm_config(config)
    sanitized = parsed.to_json_config()

    assert sanitized["env"] == {
        "OPENAI_API_KEY": API_KEY_PLACEHOLDER,
        "VENDOR_TOKEN": API_KEY_PLACEHOLDER,
        "CLIENT_SECRET": API_KEY_PLACEHOLDER,
        "DB_PASSWORD": API_KEY_PLACEHOLDER,
        "HARMLESS_MODE": "keep",
    }
    assert sanitized["nested"]["Authorization"] == API_KEY_PLACEHOLDER
    assert sanitized["nested"]["credentials"] == {
        "token": API_KEY_PLACEHOLDER,
        "label": API_KEY_PLACEHOLDER,
    }
    assert sanitized["nested"]["requires_openai_auth"] is True
    assert sanitized["model_providers"]["Unused"]["api_key"] == API_KEY_PLACEHOLDER
    assert sanitized["model_providers"]["Unused"]["metadata"] == {
        "PASSWORD": API_KEY_PLACEHOLDER,
        "region": "keep",
    }
    serialized = json.dumps(sanitized)
    for secret in (
        "env-token",
        "env-secret",
        "env-password",
        "nested-auth",
        "nested-token",
        "unused-key",
        "unused-password",
    ):
        assert secret not in serialized


def test_parse_sanitizes_composite_names_and_credential_containers_at_any_depth() -> None:
    secrets = {
        "access_token": "access-value",
        "REFRESH_TOKEN": "refresh-value",
        "client_secret": "client-value",
        "Api_Secret": "api-secret-value",
        "private_key": "private-value",
        "auth_token": "auth-value",
        "service_TOKEN": "service-value",
        "service_SECRET": "service-secret-value",
        "service_PASSWORD": "password-value",
        "service_API_KEY": "api-key-value",
    }
    config = _config(
        nested={
            "level": secrets,
            "env": {"nested": {"DEEP_TOKEN": "deep-value"}},
            "auth": {
                "username": "user-value",
                "session": {"value": "session-value"},
                "requires_openai_auth": True,
                "enabled": False,
            },
            "secrets": ["list-value", {"label": "object-value"}],
            "credentials": {"account": "account-value", "port": 443},
        }
    )

    parsed = parse_llm_config(config)
    sanitized = parsed.to_json_config()
    nested = sanitized["nested"]

    assert set(nested["level"].values()) == {API_KEY_PLACEHOLDER}
    assert nested["env"]["nested"]["DEEP_TOKEN"] == API_KEY_PLACEHOLDER
    assert nested["auth"] == {
        "username": API_KEY_PLACEHOLDER,
        "session": {"value": API_KEY_PLACEHOLDER},
        "requires_openai_auth": API_KEY_PLACEHOLDER,
        "enabled": API_KEY_PLACEHOLDER,
    }
    assert nested["secrets"] == [
        API_KEY_PLACEHOLDER,
        {"label": API_KEY_PLACEHOLDER},
    ]
    assert nested["credentials"] == {
        "account": API_KEY_PLACEHOLDER,
        "port": API_KEY_PLACEHOLDER,
    }
    serialized = json.dumps(sanitized)
    rendered = repr(parsed)
    for value in (
        *secrets.values(),
        "deep-value",
        "user-value",
        "session-value",
        "list-value",
        "object-value",
        "account-value",
    ):
        assert value not in serialized
        assert value not in rendered


def test_parse_sanitizes_headers_cookies_authorization_variants_and_credentialed_urls() -> None:
    config = _config(
        transport={
            "headers": {
                "X-Custom-Authorization": "Bearer header-secret",
                "X-Trace": "trace-secret",
                "nested": {"value": "nested-header-secret"},
                "enabled": True,
            },
            "cookies": {"session": "cookie-secret", "flags": ["cookie-list-secret"]},
            "cookie": "single-cookie-secret",
            "Proxy-Authorization": "Basic proxy-auth-secret",
            "proxy_url": "socks5://proxy-user:proxy-pass@proxy.example.test:1080",
            "secondary_proxy": "second-user:second-pass@proxy.example.test:8080",
            "callback_url": "https://url-user:url-pass@callback.example.test/path",
            "public_url": "https://public.example.test/path",
            "region": "us-east-1",
        }
    )

    sanitized = parse_llm_config(config).to_json_config()["transport"]

    assert sanitized["headers"] == {
        "X-Custom-Authorization": API_KEY_PLACEHOLDER,
        "X-Trace": API_KEY_PLACEHOLDER,
        "nested": {"value": API_KEY_PLACEHOLDER},
        "enabled": API_KEY_PLACEHOLDER,
    }
    assert sanitized["cookies"] == {
        "session": API_KEY_PLACEHOLDER,
        "flags": [API_KEY_PLACEHOLDER],
    }
    assert sanitized["cookie"] == API_KEY_PLACEHOLDER
    assert sanitized["Proxy-Authorization"] == API_KEY_PLACEHOLDER
    assert sanitized["proxy_url"] == API_KEY_PLACEHOLDER
    assert sanitized["secondary_proxy"] == API_KEY_PLACEHOLDER
    assert sanitized["callback_url"] == API_KEY_PLACEHOLDER
    assert sanitized["public_url"] == "https://public.example.test/path"
    assert sanitized["region"] == "us-east-1"

    serialized = json.dumps(sanitized)
    for secret in (
        "header-secret",
        "trace-secret",
        "nested-header-secret",
        "cookie-secret",
        "cookie-list-secret",
        "single-cookie-secret",
        "proxy-auth-secret",
        "proxy-user",
        "proxy-pass",
        "second-user",
        "second-pass",
        "url-user",
        "url-pass",
    ):
        assert secret not in serialized


def test_parse_normalizes_secret_key_styles_and_redacts_secret_object_leaves() -> None:
    config = _config(
        nested={
            "apiKey": "camel-api-key",
            "ClientSecret": "pascal-client-secret",
            "client-secret": "hyphen-client-secret",
            "auth.token": "dot-auth-token",
            "private key": "space-private-key",
            "access_token": {
                "value": "object-access-token",
                "nested": {"label": "nested-access-value"},
                "requires_openai_auth": True,
            },
            "privateKey": [
                "list-private-key",
                {"part": "nested-private-part", "enabled": False},
            ],
        }
    )

    parsed = parse_llm_config(config)
    nested = parsed.to_json_config()["nested"]

    for key in ("apiKey", "ClientSecret", "client-secret", "auth.token", "private key"):
        assert nested[key] == API_KEY_PLACEHOLDER
    assert nested["access_token"] == {
        "value": API_KEY_PLACEHOLDER,
        "nested": {"label": API_KEY_PLACEHOLDER},
        "requires_openai_auth": API_KEY_PLACEHOLDER,
    }
    assert nested["privateKey"] == [
        API_KEY_PLACEHOLDER,
        {"part": API_KEY_PLACEHOLDER, "enabled": API_KEY_PLACEHOLDER},
    ]
    serialized = json.dumps(parsed.to_json_config())
    rendered = repr(parsed)
    for secret in (
        "camel-api-key",
        "pascal-client-secret",
        "hyphen-client-secret",
        "dot-auth-token",
        "space-private-key",
        "object-access-token",
        "nested-access-value",
        "list-private-key",
        "nested-private-part",
    ):
        assert secret not in serialized
        assert secret not in rendered


def test_parse_secret_sanitization_does_not_leak_values_in_later_errors() -> None:
    config = _config(
        nested={"token": "nested-token"},
        disable_response_storage="invalid",
    )

    with pytest.raises(ValueError, match="disable_response_storage") as error:
        parse_llm_config(config)

    assert "nested-token" not in str(error.value)
    assert "secret-key" not in str(error.value)


def test_parse_exposes_deeply_immutable_config_with_json_conversion() -> None:
    config = _config(future_option={"items": [1]})

    parsed = parse_llm_config(config)
    config["future_option"]["items"].append(2)

    assert parsed.to_json_config()["future_option"] == {"items": [1]}
    with pytest.raises(TypeError):
        parsed.sanitized_config["future_option"] = {"items": []}
    with pytest.raises(TypeError):
        parsed.sanitized_config["future_option"]["items"][0] = 2
    with pytest.raises(AttributeError):
        parsed.sanitized_config["future_option"]["items"].append(2)
    with pytest.raises(AttributeError):
        parsed.model = "changed"

    mutable = parsed.to_json_config()
    mutable["future_option"]["items"].append(2)
    assert parsed.to_json_config()["future_option"] == {"items": [1]}
    assert json.loads(json.dumps(mutable))["future_option"] == {"items": [1, 2]}


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
