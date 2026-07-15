from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import TypeAdapter
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.ai.gateway import LLMResult
from app.api.llm_endpoints import LlmEndpointCreate, LlmEndpointUpdate
from app.core.config import settings
from app.db.models.base import Base
from app.db.models.llm_endpoint import LlmEndpoint
from app.db.sqlite import get_async_session
from app.main import app


@pytest.fixture
async def endpoint_api(
    tmp_path: Path, monkeypatch
) -> AsyncIterator[tuple[AsyncClient, async_sessionmaker]]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'routes.db').as_posix()}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def session_override():
        async with sessions() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    monkeypatch.setattr(settings, "gaoshou_data_dir", str(tmp_path))
    monkeypatch.setattr(
        "litellm.litellm_core_utils.url_utils.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )
    app.dependency_overrides[get_async_session] = session_override
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client, sessions
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()


def _payload(**overrides):
    return {
        "name": "Primary",
        "api_base": "https://llm.example.test/v1",
        "api_key": "top-secret-1234",
        "model": "openai/test-model",
        **overrides,
    }


def _config(**overrides):
    config = {
        "model_provider": "OpenAI",
        "model": "gpt-5.5",
        "review_model": "gpt-5.5-review",
        "model_reasoning_effort": "high",
        "disable_response_storage": True,
        "future_option": {"keep": True},
        "model_providers": {
            "OpenAI": {
                "name": "Primary JSON",
                "base_url": "https://responses.example.test/v1",
                "wire_api": "responses",
                "requires_openai_auth": True,
                "vendor_option": "preserve-me",
            }
        },
        "env": {"OPENAI_API_KEY": "json-secret-5678"},
    }
    config.update(overrides)
    return config


def test_llm_endpoint_operations_routes_are_registered():
    paths = app.openapi()["paths"]
    assert "/api/system/llm-endpoints" in paths
    assert "/api/system/llm-endpoints/{endpoint_id}" in paths
    assert "/api/system/llm-endpoints/reorder" in paths
    assert "/api/system/llm-endpoints/{endpoint_id}/test" in paths


def test_llm_endpoint_request_schemas_have_planned_limits_and_no_priority():
    create_schema = TypeAdapter(LlmEndpointCreate).json_schema()
    update_schema = TypeAdapter(LlmEndpointUpdate).json_schema()

    assert len(create_schema["anyOf"]) == 2
    assert len(update_schema["anyOf"]) == 2
    legacy_create = create_schema["$defs"]["LlmEndpointLegacyCreate"]
    legacy_update = update_schema["$defs"]["LlmEndpointLegacyUpdate"]
    assert legacy_create["additionalProperties"] is False
    assert set(legacy_create["required"]) == {"name", "api_base", "api_key", "model"}
    name_schema = legacy_create["properties"]["name"]
    assert name_schema == {
        "maxLength": 100,
        "minLength": 1,
        "title": "Name",
        "type": "string",
    }
    api_base_schema = legacy_create["properties"]["api_base"]
    api_key_schema = legacy_create["properties"]["api_key"]
    model_schema = legacy_create["properties"]["model"]
    assert api_base_schema["minLength"] == 8
    assert api_base_schema["maxLength"] == 500
    assert api_key_schema["minLength"] == 1
    assert api_key_schema["maxLength"] == 2000
    assert model_schema["minLength"] == 1
    assert model_schema["maxLength"] == 200
    assert "priority" not in legacy_create["properties"]
    assert "priority" not in legacy_update["properties"]

    request_schema = app.openapi()["paths"]["/api/system/llm-endpoints"]["post"]["requestBody"][
        "content"
    ]["application/json"]["schema"]
    assert len(request_schema["anyOf"]) == 2


def test_llm_endpoint_request_schemas_require_complete_legacy_create_and_sparse_update():
    create_adapter = TypeAdapter(LlmEndpointCreate)
    update_adapter = TypeAdapter(LlmEndpointUpdate)
    assert create_adapter.validate_python({"config": _config()}).config == _config()
    assert update_adapter.validate_python({"config": _config()}).config == _config()

    with pytest.raises(ValueError, match="Field required"):
        create_adapter.validate_python({"name": "Incomplete"})
    assert update_adapter.validate_python({"name": "Renamed"}).name == "Renamed"
    assert update_adapter.validate_python({"enabled": False}).enabled is False

    with pytest.raises(ValueError, match="update must not be empty"):
        update_adapter.validate_python({})
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        update_adapter.validate_python({"config": _config(), "model": "mixed"})


@pytest.mark.asyncio
async def test_json_create_list_and_update_preserve_config_without_secrets(endpoint_api):
    client, _ = endpoint_api
    created_response = await client.post(
        "/api/system/llm-endpoints",
        json={"config": _config(), "enabled": False},
    )

    assert created_response.status_code == 201
    created = created_response.json()
    assert created["provider"] == "OpenAI"
    assert created["name"] == "Primary JSON"
    assert created["review_model"] == "gpt-5.5-review"
    assert created["wire_api"] == "responses"
    assert created["reasoning_effort"] == "high"
    assert created["disable_response_storage"] is True
    assert created["requires_openai_auth"] is True
    assert created["enabled"] is False
    assert created["config"]["env"]["OPENAI_API_KEY"] == "__GAOSHOU_STORED_SECRET__"
    assert created["config"]["future_option"] == {"keep": True}
    assert created["config"]["model_providers"]["OpenAI"]["vendor_option"] == "preserve-me"
    assert "future_option" in created["preserved_fields"]
    assert "model_providers.OpenAI.vendor_option" in created["preserved_fields"]
    assert "json-secret-5678" not in created_response.text
    assert "api_key_encrypted" not in created_response.text

    listed = await client.get("/api/system/llm-endpoints")
    assert listed.status_code == 200
    assert listed.json() == [created]

    update_config = created["config"]
    update_config["model"] = "gpt-5.6"
    updated_response = await client.patch(
        f"/api/system/llm-endpoints/{created['id']}",
        json={"config": update_config, "enabled": True},
    )
    assert updated_response.status_code == 200
    updated = updated_response.json()
    assert updated["model"] == "gpt-5.6"
    assert updated["enabled"] is True
    assert updated["api_key_hint"] == "********5678"


@pytest.mark.asyncio
async def test_json_response_supports_reasoning_effort_none(endpoint_api):
    client, _ = endpoint_api
    config = _config(model_reasoning_effort="none")

    response = await client.post("/api/system/llm-endpoints", json={"config": config})

    assert response.status_code == 201
    assert response.json()["reasoning_effort"] == "none"


@pytest.mark.asyncio
async def test_json_response_recursively_sanitizes_custom_credentials(endpoint_api):
    client, _ = endpoint_api
    config = _config(
        transport={
            "headers": {
                "X-Custom-Authorization": "Bearer api-header-secret",
                "X-Trace": "api-trace-secret",
            },
            "cookies": {"session": "api-cookie-secret"},
            "proxy_url": "http://api-user:api-pass@proxy.example.test:8080",
            "public_url": "https://public.example.test/path",
            "region": "us-east-1",
        }
    )

    response = await client.post("/api/system/llm-endpoints", json={"config": config})

    assert response.status_code == 201
    transport = response.json()["config"]["transport"]
    assert transport["headers"] == {
        "X-Custom-Authorization": "__GAOSHOU_STORED_SECRET__",
        "X-Trace": "__GAOSHOU_STORED_SECRET__",
    }
    assert transport["cookies"] == {"session": "__GAOSHOU_STORED_SECRET__"}
    assert transport["proxy_url"] == "__GAOSHOU_STORED_SECRET__"
    assert transport["public_url"] == "https://public.example.test/path"
    assert transport["region"] == "us-east-1"
    for secret in (
        "api-header-secret",
        "api-trace-secret",
        "api-cookie-secret",
        "api-user",
        "api-pass",
    ):
        assert secret not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"config": []},
        {"config": "not-json"},
        {"config": {"model": "missing-provider"}},
        {"name": "partial"},
    ],
)
async def test_create_rejects_invalid_config_types_and_incomplete_legacy_payload(
    endpoint_api,
    payload,
):
    client, _ = endpoint_api
    response = await client.post("/api/system/llm-endpoints", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("patch", "field", "expected"),
    [
        ({"name": "Renamed"}, "name", "Renamed"),
        ({"model": "openai/new-model"}, "model", "openai/new-model"),
        ({"api_key": "replacement-secret-9999"}, "api_key_hint", "********9999"),
    ],
)
async def test_legacy_update_accepts_sparse_discrete_fields(endpoint_api, patch, field, expected):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()

    response = await client.patch(
        f"/api/system/llm-endpoints/{endpoint['id']}",
        json=patch,
    )

    assert response.status_code == 200
    assert response.json()[field] == expected


@pytest.mark.asyncio
async def test_legacy_update_accepts_enabled_only_for_current_ui(endpoint_api):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()

    response = await client.patch(
        f"/api/system/llm-endpoints/{endpoint['id']}",
        json={"enabled": False},
    )

    assert response.status_code == 200
    assert response.json()["enabled"] is False


@pytest.mark.asyncio
async def test_update_rejects_empty_and_mixed_payloads_clearly(endpoint_api):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()

    empty = await client.patch(f"/api/system/llm-endpoints/{endpoint['id']}", json={})
    mixed = await client.patch(
        f"/api/system/llm-endpoints/{endpoint['id']}",
        json={"config": endpoint["config"], "name": "Mixed"},
    )

    assert empty.status_code == 422
    assert "update must not be empty" in empty.text
    assert mixed.status_code == 422
    assert "Extra inputs are not permitted" in mixed.text
    assert "LlmEndpointJsonUpdate" in mixed.text


@pytest.mark.asyncio
async def test_create_and_update_reject_priority(endpoint_api):
    client, _ = endpoint_api
    create_response = await client.post(
        "/api/system/llm-endpoints",
        json=_payload(priority=0),
    )
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    update_response = await client.patch(
        f"/api/system/llm-endpoints/{endpoint['id']}",
        json={"priority": 0},
    )

    assert create_response.status_code == 422
    assert update_response.status_code == 422


@pytest.mark.asyncio
async def test_create_list_update_and_delete_never_expose_api_key(endpoint_api):
    client, _ = endpoint_api
    created = await client.post("/api/system/llm-endpoints", json=_payload())
    assert created.status_code == 201
    endpoint = created.json()
    assert endpoint["api_key_hint"] == "********1234"
    assert "api_key" not in endpoint
    assert "api_key_encrypted" not in endpoint
    assert "top-secret" not in created.text

    listed = await client.get("/api/system/llm-endpoints")
    assert listed.status_code == 200
    assert listed.json() == [endpoint]
    assert "top-secret" not in listed.text

    updated = await client.patch(
        f"/api/system/llm-endpoints/{endpoint['id']}",
        json=_payload(name="Renamed"),
    )
    assert updated.status_code == 200
    assert updated.json()["name"] == "Renamed"
    assert updated.json()["api_key_hint"] == "********1234"

    deleted = await client.delete(f"/api/system/llm-endpoints/{endpoint['id']}")
    assert deleted.status_code == 204
    assert (await client.get("/api/system/llm-endpoints")).json() == []


@pytest.mark.asyncio
async def test_invalid_url_and_missing_endpoint_have_clear_client_errors(endpoint_api):
    client, _ = endpoint_api
    invalid = await client.post(
        "/api/system/llm-endpoints",
        json=_payload(api_base="not-a-provider-url"),
    )
    missing = await client.patch(
        "/api/system/llm-endpoints/missing",
        json=_payload(name="No endpoint"),
    )

    assert invalid.status_code == 422
    assert "api_base" in invalid.json()["detail"]
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_api_base_destination_change_requires_replacement_key(endpoint_api, monkeypatch):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    update_config = endpoint["config"]
    provider = update_config["model_provider"]
    update_config["model_providers"][provider]["base_url"] = "https://redirect.example.test/v1"

    rejected = await client.patch(
        f"/api/system/llm-endpoints/{endpoint['id']}",
        json={"config": update_config},
    )
    captured = {}

    async def fake_complete_candidate(candidate, *_args, **_kwargs):
        captured["candidate"] = candidate
        return LLMResult(content="ok", model=candidate.model)

    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", fake_complete_candidate)
    tested = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert rejected.status_code == 422
    assert "replacement OPENAI_API_KEY" in rejected.json()["detail"]
    assert tested.status_code == 200
    assert captured["candidate"].api_base == "https://llm.example.test/v1"
    assert captured["candidate"].api_key == "top-secret-1234"


@pytest.mark.asyncio
async def test_connection_uses_json_responses_candidate_options(endpoint_api, monkeypatch):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json={"config": _config()})).json()
    captured = {}

    async def fake_complete_candidate(candidate, messages, **kwargs):
        captured["candidate"] = candidate
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return LLMResult(content="ok", model=candidate.model)

    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", fake_complete_candidate)
    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert response.status_code == 200
    candidate = captured["candidate"]
    assert candidate.provider == "OpenAI"
    assert candidate.review_model == "gpt-5.5-review"
    assert candidate.wire_api == "responses"
    assert candidate.reasoning_effort == "high"
    assert candidate.disable_response_storage is True
    assert candidate.requires_openai_auth is True
    assert captured["messages"] == [{"role": "user", "content": "Reply with OK."}]
    assert captured["kwargs"] == {
        "temperature": 0,
        "max_tokens": 1,
        "follow_redirects": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolved_addresses",
    [
        ["127.0.0.1"],
        ["10.0.0.8"],
        ["169.254.169.254"],
        ["224.0.0.1"],
        ["0.0.0.0"],
        ["::1"],
        ["93.184.216.34", "10.0.0.8"],
    ],
)
async def test_connection_rejects_any_nonpublic_dns_result_before_key_decryption_or_provider_call(
    endpoint_api,
    monkeypatch,
    resolved_addresses,
):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    calls = []

    monkeypatch.setattr(
        "litellm.litellm_core_utils.url_utils.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", (address, 443)) for address in resolved_addresses],
    )

    async def fail_if_decrypted(*_args, **_kwargs):
        calls.append("decrypt")
        raise AssertionError("private destination key must not be decrypted")

    async def fail_if_called(*_args, **_kwargs):
        calls.append("provider")
        raise AssertionError("private destination provider must not be called")

    monkeypatch.setattr(
        "app.api.llm_endpoints.LlmEndpointService.decrypt_api_key",
        fail_if_decrypted,
    )
    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", fail_if_called)

    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert response.status_code == 422
    assert "public HTTP(S)" in response.json()["detail"]
    assert calls == []


@pytest.mark.asyncio
async def test_connection_rejects_private_destination_even_if_litellm_allowlists_host(
    endpoint_api,
    monkeypatch,
):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    calls = []
    monkeypatch.setattr("litellm.user_url_allowed_hosts", ["llm.example.test"])
    monkeypatch.setattr(
        "litellm.litellm_core_utils.url_utils.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("10.0.0.8", 443))],
    )

    async def fail_if_decrypted(*_args, **_kwargs):
        calls.append("decrypt")
        raise AssertionError("allowlist must not bypass public destination policy")

    monkeypatch.setattr(
        "app.api.llm_endpoints.LlmEndpointService.decrypt_api_key",
        fail_if_decrypted,
    )

    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert response.status_code == 422
    assert "public HTTP(S)" in response.json()["detail"]
    assert calls == []


@pytest.mark.asyncio
async def test_reorder_requires_every_endpoint_exactly_once(endpoint_api):
    client, _ = endpoint_api
    first = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    second = (
        await client.post(
            "/api/system/llm-endpoints",
            json=_payload(name="Backup", api_key="backup-secret-5678"),
        )
    ).json()

    incomplete = await client.post(
        "/api/system/llm-endpoints/reorder",
        json={"endpoint_ids": [first["id"]]},
    )
    reordered = await client.post(
        "/api/system/llm-endpoints/reorder",
        json={"endpoint_ids": [second["id"], first["id"]]},
    )

    assert incomplete.status_code == 409
    assert "every endpoint exactly once" in incomplete.json()["detail"]
    assert [item["id"] for item in reordered.json()] == [second["id"], first["id"]]
    assert [item["priority"] for item in reordered.json()] == [0, 1]


@pytest.mark.asyncio
async def test_connection_uses_decrypted_selected_candidate_and_records_success(
    endpoint_api, monkeypatch
):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    captured = {}

    async def fake_complete_candidate(candidate, messages, **kwargs):
        captured["candidate"] = candidate
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return LLMResult(content="ok", model="provider/returned-model")

    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", fake_complete_candidate)
    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["model"] == "provider/returned-model"
    assert isinstance(response.json()["latency_ms"], int)
    assert response.json()["latency_ms"] >= 0
    assert "api_key" not in response.json()
    assert captured["candidate"].endpoint_id == endpoint["id"]
    assert captured["candidate"].api_key == "top-secret-1234"
    assert captured["messages"] == [{"role": "user", "content": "Reply with OK."}]
    assert captured["kwargs"]["max_tokens"] == 1
    assert captured["kwargs"]["follow_redirects"] is False

    listed = (await client.get("/api/system/llm-endpoints")).json()[0]
    assert listed["last_success_at"] is not None
    assert listed["consecutive_failures"] == 0


@pytest.mark.asyncio
async def test_connection_failure_is_sanitized_and_records_health(endpoint_api, monkeypatch):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()

    async def fail_candidate(*_args, **_kwargs):
        raise RuntimeError("authorization: Bearer top-secret-1234")

    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", fail_candidate)
    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert "top-secret-1234" not in response.text
    assert "REDACTED" in response.json()["error"]
    listed = (await client.get("/api/system/llm-endpoints")).json()[0]
    assert listed["consecutive_failures"] == 1
    assert "top-secret-1234" not in listed["last_error"]


@pytest.mark.asyncio
async def test_connection_failure_survives_health_write_failure(endpoint_api, monkeypatch):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()
    health_errors = []

    async def fail_candidate(*_args, **_kwargs):
        raise RuntimeError("authorization: Bearer top-secret-1234")

    async def fail_health_write(_service, _endpoint_id, error):
        health_errors.append(str(error))
        raise RuntimeError("health database unavailable for top-secret-1234")

    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", fail_candidate)
    monkeypatch.setattr("app.api.llm_endpoints.LlmEndpointService.mark_failure", fail_health_write)
    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")

    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert "REDACTED" in response.json()["error"]
    assert health_errors == [response.json()["error"]]
    assert "top-secret-1234" not in response.text


@pytest.mark.asyncio
async def test_connection_success_survives_health_write_failure_and_rolls_back(
    endpoint_api,
    monkeypatch,
    caplog,
):
    client, _ = endpoint_api
    endpoint = (await client.post("/api/system/llm-endpoints", json=_payload())).json()

    async def succeed_candidate(candidate, *_args, **_kwargs):
        return LLMResult(content="ok", model=candidate.model)

    async def fail_success_write(service, endpoint_id):
        stored = await service.session.get(LlmEndpoint, endpoint_id)
        stored.last_error = "uncommitted health change"
        await service.session.flush()
        raise RuntimeError("health write failed with top-secret-1234")

    monkeypatch.setattr("app.api.llm_endpoints.complete_candidate", succeed_candidate)
    monkeypatch.setattr("app.api.llm_endpoints.LlmEndpointService.mark_success", fail_success_write)
    response = await client.post(f"/api/system/llm-endpoints/{endpoint['id']}/test")
    listed = (await client.get("/api/system/llm-endpoints")).json()[0]

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert listed["last_error"] is None
    assert "top-secret-1234" not in response.text
    assert "top-secret-1234" not in caplog.text
    assert "REDACTED" in caplog.text
