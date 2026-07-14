from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Literal, get_type_hints

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.ai.gateway import GatewayCandidate, _normalize_response, complete_sync, gateway_status
from app.ai.mcp_server import mcp
from app.ai.tools import get_tool, list_tools
from app.core.config import settings
from app.db.models.base import Base
from app.db.models.llm_endpoint import LlmEndpoint
from app.db.sqlite import async_session_factory
from app.services.ai_native import AINativeService, create_tool_approval
from app.services.llm_endpoints import LlmEndpointService
from app.services.runtime_tasks import (
    claim_task,
    get_task,
    mark_stale_runtime_tasks_failed,
    register_task,
)


def test_gateway_status_requires_key_and_model(monkeypatch):
    monkeypatch.setattr(settings, "llm_api_key", "")
    monkeypatch.setattr(settings, "llm_default_model", "")
    assert gateway_status()["state"] == "blocked"

    monkeypatch.setattr(settings, "llm_api_key", "secret")
    monkeypatch.setattr(settings, "llm_default_model", "openai/codex")
    status = gateway_status()
    assert status["state"] == "ready"
    assert "secret" not in str(status)


def _gateway_test_database(monkeypatch, tmp_path):
    database_path = tmp_path / "gateway.sqlite"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    monkeypatch.setattr(settings, "database_url", database_url)
    monkeypatch.setattr(settings, "gaoshou_data_dir", str(tmp_path))
    engine = create_engine(database_url.replace("+aiosqlite", ""))
    Base.metadata.create_all(engine)
    return engine


def _add_endpoint(
    engine,
    *,
    name,
    key,
    priority,
    cooldown_until=None,
    enabled=True,
    created_at=None,
    consecutive_failures=0,
    last_error=None,
):
    service = LlmEndpointService(SimpleNamespace())
    endpoint = LlmEndpoint(
        name=name,
        api_base=f"https://{name}.example/v1",
        api_key_encrypted=service._encrypt(key),
        api_key_hint=f"********{key[-4:]}",
        model=f"openai/{name}",
        priority=priority,
        enabled=enabled,
        cooldown_until=cooldown_until,
        created_at=created_at or datetime.now(),
        consecutive_failures=consecutive_failures,
        last_error=last_error,
    )
    with Session(engine) as session:
        session.add(endpoint)
        session.commit()
        endpoint_id = endpoint.id
    return endpoint_id


def _response(model):
    return SimpleNamespace(
        model=model,
        usage=SimpleNamespace(model_dump=lambda: {"total_tokens": 3}),
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok", tool_calls=[]))],
    )


def test_gateway_candidate_has_explicit_source_literal():
    assert get_type_hints(GatewayCandidate)["source"] == Literal["database", "environment"]


def test_gateway_first_healthy_endpoint_wins(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    primary_id = _add_endpoint(engine, name="primary", key="primary-secret", priority=0)
    backup_id = _add_endpoint(engine, name="backup", key="backup-secret", priority=1)
    calls = []

    def completion(**kwargs):
        calls.append(kwargs["model"])
        return _response(kwargs["model"])

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    result = complete_sync([{"role": "user", "content": "hello"}])

    assert result.model == "openai/primary"
    assert calls == ["openai/primary"]
    with Session(engine) as session:
        assert session.get(LlmEndpoint, primary_id).last_success_at is not None
        assert session.get(LlmEndpoint, backup_id).last_success_at is None


def test_gateway_orders_equal_priorities_by_created_at(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    with engine.begin() as connection:
        connection.execute(text("DROP INDEX ux_llm_endpoints_priority"))
    later = datetime(2026, 1, 2)
    earlier = datetime(2026, 1, 1)
    _add_endpoint(engine, name="later", key="later-secret", priority=0, created_at=later)
    _add_endpoint(engine, name="earlier", key="earlier-secret", priority=0, created_at=earlier)
    calls = []

    def completion(**kwargs):
        calls.append(kwargs["model"])
        return _response(kwargs["model"])

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    complete_sync([{"role": "user", "content": "hello"}])

    assert calls == ["openai/earlier"]


def test_gateway_three_failures_create_sixty_second_cooldown(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    endpoint_id = _add_endpoint(engine, name="primary", key="primary-secret", priority=0)

    def completion(**_kwargs):
        raise ConnectionError("offline")

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    started_at = datetime.now()

    for _ in range(3):
        with pytest.raises(RuntimeError):
            complete_sync([{"role": "user", "content": "hello"}])

    with Session(engine) as session:
        endpoint = session.get(LlmEndpoint, endpoint_id)
        assert endpoint.consecutive_failures == 3
        assert endpoint.cooldown_until is not None
        assert timedelta(seconds=55) <= endpoint.cooldown_until - started_at <= timedelta(seconds=65)


def test_gateway_success_clears_existing_failure_health(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    endpoint_id = _add_endpoint(
        engine,
        name="primary",
        key="primary-secret",
        priority=0,
        cooldown_until=datetime.now() - timedelta(seconds=1),
        consecutive_failures=2,
        last_error="old failure",
    )
    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(completion=lambda **kwargs: _response(kwargs["model"])),
    )

    complete_sync([{"role": "user", "content": "hello"}])

    with Session(engine) as session:
        endpoint = session.get(LlmEndpoint, endpoint_id)
        assert endpoint.consecutive_failures == 0
        assert endpoint.cooldown_until is None
        assert endpoint.last_error is None
        assert endpoint.last_success_at is not None


def test_gateway_all_failed_error_aggregates_names_and_sanitized_errors(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    _add_endpoint(engine, name="primary", key="primary-key", priority=0)
    _add_endpoint(engine, name="backup", key="backup-key", priority=1)

    def completion(**kwargs):
        name = kwargs["model"].removeprefix("openai/")
        key = kwargs["api_key"]
        raise ConnectionError(
            f"{name} failed with {key} at https://alice:hunter2@{name}.example/v1"
            f"?api_key={key}&access_token={name}-query-secret"
        )

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(RuntimeError) as caught:
        complete_sync([{"role": "user", "content": "hello"}])

    message = str(caught.value)
    assert "primary" in message
    assert "backup" in message
    assert "primary failed" in message
    assert "backup failed" in message
    for secret in (
        "primary-key",
        "backup-key",
        "alice:hunter2",
        "primary-query-secret",
        "backup-query-secret",
    ):
        assert secret not in message


def test_gateway_health_write_failure_is_nonfatal_and_does_not_chain_provider_error(
    monkeypatch,
    tmp_path,
):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    _add_endpoint(engine, name="primary", key="primary-key", priority=0)
    _add_endpoint(engine, name="backup", key="backup-key", priority=1)
    attempts = []

    def completion(**kwargs):
        attempts.append(kwargs["model"])
        raise ConnectionError(f"provider failed with {kwargs['api_key']}")

    def broken_health_write(*_args, **_kwargs):
        raise OSError("health database unavailable")

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setattr("app.ai.gateway._write_health", broken_health_write)

    with pytest.raises(RuntimeError) as caught:
        complete_sync([{"role": "user", "content": "hello"}])

    assert attempts == ["openai/primary", "openai/backup"]
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "primary-key" not in str(caught.value)
    assert "backup-key" not in str(caught.value)


def test_gateway_health_write_failure_logs_only_sanitized_endpoint_metadata(
    monkeypatch,
    tmp_path,
    caplog,
):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    primary_id = _add_endpoint(engine, name="primary", key="primary-key", priority=0)
    _add_endpoint(engine, name="backup", key="backup-key", priority=1)

    def completion(**kwargs):
        if kwargs["model"] == "openai/primary":
            raise ConnectionError("provider exploded with primary-key")
        return _response(kwargs["model"])

    def broken_health_write(endpoint_id, **_kwargs):
        if endpoint_id == primary_id:
            raise OSError(
                "health writer failed primary-key at "
                "https://admin:database-password@db.example/write?token=health-query-secret"
            )

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setattr("app.ai.gateway._write_health", broken_health_write)
    caplog.set_level(logging.WARNING, logger="app.ai.gateway")

    result = complete_sync([{"role": "user", "content": "hello"}])

    assert result.model == "openai/backup"
    warning = " ".join(record.getMessage() for record in caplog.records)
    assert "primary" in warning
    assert primary_id in warning
    assert "database" in warning
    assert "health writer failed" in warning
    for unsafe in (
        "provider exploded",
        "primary-key",
        "admin:database-password",
        "health-query-secret",
    ):
        assert unsafe not in warning


def test_older_attempt_success_does_not_clear_interleaved_newer_failure(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    endpoint_id = _add_endpoint(
        engine,
        name="primary",
        key="primary-key",
        priority=0,
        consecutive_failures=1,
        last_error="older failure",
    )
    newer_failure_at = None

    def completion(**kwargs):
        nonlocal newer_failure_at
        newer_failure_at = datetime.now()
        with Session(engine) as session:
            endpoint = session.get(LlmEndpoint, endpoint_id)
            endpoint.consecutive_failures = 2
            endpoint.last_failure_at = newer_failure_at
            endpoint.last_error = "newer failure"
            endpoint.cooldown_until = newer_failure_at + timedelta(seconds=60)
            session.commit()
        time.sleep(0.02)
        return _response(kwargs["model"])

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    complete_sync([{"role": "user", "content": "hello"}])

    with Session(engine) as session:
        endpoint = session.get(LlmEndpoint, endpoint_id)
        assert newer_failure_at is not None
        assert endpoint.consecutive_failures == 2
        assert endpoint.last_failure_at == newer_failure_at
        assert endpoint.last_error == "newer failure"
        assert endpoint.cooldown_until == newer_failure_at + timedelta(seconds=60)


def test_gateway_candidate_not_found_fails_over(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    _add_endpoint(engine, name="primary", key="primary-key", priority=0)
    _add_endpoint(engine, name="backup", key="backup-key", priority=1)
    attempts = []

    class NotFoundError(Exception):
        status_code = 404

    def completion(**kwargs):
        attempts.append(kwargs["model"])
        if kwargs["model"] == "openai/primary":
            raise NotFoundError("model is unavailable on this endpoint")
        return _response(kwargs["model"])

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    result = complete_sync([{"role": "user", "content": "hello"}])

    assert result.model == "openai/backup"
    assert attempts == ["openai/primary", "openai/backup"]


def test_gateway_request_wide_client_error_stops_failover(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    _add_endpoint(engine, name="primary", key="primary-key", priority=0)
    _add_endpoint(engine, name="backup", key="backup-key", priority=1)
    attempts = []

    class BadRequestError(Exception):
        status_code = 400

    def completion(**kwargs):
        attempts.append(kwargs["model"])
        raise BadRequestError("messages payload is invalid")

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(RuntimeError, match="messages payload is invalid") as caught:
        complete_sync([{"role": "user", "content": "hello"}])

    assert attempts == ["openai/primary"]
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_gateway_uses_fixed_db_priority_skips_cooldown_and_updates_health(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    _add_endpoint(
        engine,
        name="cooling",
        key="cooling-secret",
        priority=0,
        cooldown_until=datetime.now() + timedelta(minutes=5),
    )
    failed_id = _add_endpoint(engine, name="primary", key="primary-secret", priority=1)
    success_id = _add_endpoint(engine, name="backup", key="backup-secret", priority=2)
    monkeypatch.setattr(settings, "llm_api_key", "environment-secret")
    monkeypatch.setattr(settings, "llm_default_model", "openai/environment")
    calls = []

    def completion(**kwargs):
        calls.append(kwargs)
        if kwargs["model"] == "openai/primary":
            raise ConnectionError(
                "network failed primary-secret https://user:pass@primary.example/v1?api_key=primary-secret"
            )
        return _response(kwargs["model"])

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    result = complete_sync([{"role": "user", "content": "hello"}])

    assert result.model == "openai/backup"
    assert [call["model"] for call in calls] == ["openai/primary", "openai/backup"]
    assert all(call["num_retries"] == 0 for call in calls)
    assert all(call["api_key"] != "environment-secret" for call in calls)
    with Session(engine) as session:
        failed = session.get(LlmEndpoint, failed_id)
        succeeded = session.get(LlmEndpoint, success_id)
        assert failed is not None and failed.consecutive_failures == 1
        assert "primary-secret" not in str(failed.last_error)
        assert "user:pass" not in str(failed.last_error)
        assert succeeded is not None and succeeded.last_success_at is not None
        assert succeeded.consecutive_failures == 0


@pytest.mark.asyncio
async def test_complete_sync_needs_no_asyncio_bridge_and_env_is_only_empty_db_fallback(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    _add_endpoint(engine, name="disabled", key="disabled-secret", priority=0, enabled=False)
    monkeypatch.setattr(settings, "llm_api_base", "https://environment.example/v1")
    monkeypatch.setattr(settings, "llm_api_key", "environment-secret")
    monkeypatch.setattr(settings, "llm_default_model", "openai/environment")
    captured = {}

    def completion(**kwargs):
        captured.update(kwargs)
        return _response(kwargs["model"])

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    result = complete_sync([{"role": "user", "content": "hello"}])

    assert result.model == "openai/environment"
    assert captured["api_key"] == "environment-secret"
    status = gateway_status()
    assert status["configuration_mode"] == "environment"
    assert status["enabled_endpoint_count"] == 0
    assert status["primary_endpoint"] is None


def test_gateway_status_reports_db_primary_and_preserves_existing_fields(monkeypatch, tmp_path):
    engine = _gateway_test_database(monkeypatch, tmp_path)
    primary_id = _add_endpoint(engine, name="primary", key="primary-secret", priority=0)
    _add_endpoint(engine, name="backup", key="backup-secret", priority=1)
    monkeypatch.setattr(settings, "llm_api_key", "")
    monkeypatch.setattr(settings, "llm_default_model", "")

    status = gateway_status()

    assert status["configured"] is True
    assert status["state"] == "ready"
    assert status["configuration_mode"] == "database"
    assert status["enabled_endpoint_count"] == 2
    assert status["primary_endpoint"] == {
        "id": primary_id,
        "name": "primary",
        "model": "openai/primary",
        "api_base": "https://primary.example/v1",
    }
    assert "primary-secret" not in str(status)


def test_gateway_normalizes_tool_calls():
    response = SimpleNamespace(
        model="openai/codex",
        usage=SimpleNamespace(model_dump=lambda: {"total_tokens": 12}),
        choices=[SimpleNamespace(message=SimpleNamespace(
            content="",
            tool_calls=[SimpleNamespace(id="call-1", function=SimpleNamespace(name="stock_snapshot", arguments='{"symbol":"600519.SH"}'))],
        ))],
    )
    result = _normalize_response(response)
    assert result.tool_calls[0]["name"] == "stock_snapshot"
    assert result.usage["total_tokens"] == 12


def test_read_only_registry_never_exposes_write_tools():
    assert list_tools(read_only=True)
    assert all(tool.risk == "read" for tool in list_tools(read_only=True))
    assert get_tool("save_strategy_candidate").risk == "write"
    with pytest.raises(ValueError):
        get_tool("submit_live_order")


def test_mcp_registry_preserves_typed_tool_schema():
    tools = {tool.name: tool for tool in mcp._tool_manager.list_tools()}
    schema = tools["stock_snapshot"].parameters
    assert schema["required"] == ["symbol"]
    assert schema["properties"]["symbol"]["type"] == "string"
    assert "arguments" not in schema["properties"]
    factor_schema = tools["factor_list"].parameters
    assert factor_schema["additionalProperties"] is False
    assert factor_schema["properties"]["limit"]["maximum"] == 100


@pytest.mark.asyncio
async def test_write_approval_can_only_be_claimed_once(monkeypatch):
    execution_started = asyncio.Event()
    release_execution = asyncio.Event()
    executions = 0

    async def fake_execute_tool(_session, _name, _arguments):
        nonlocal executions
        executions += 1
        execution_started.set()
        await release_execution.wait()
        return {"status": "submitted"}

    approval_id = "approval-concurrent-test"
    register_task(
        task_id=approval_id,
        kind="ai_approval",
        title="Confirm test write",
        status="waiting_approval",
        meta={"tool": "run_candidate_backtest", "arguments": {}, "used": False},
    )
    monkeypatch.setattr("app.services.ai_native.execute_tool", fake_execute_tool)
    service = AINativeService(SimpleNamespace())

    first = asyncio.create_task(service.resolve_approval(approval_id, confirmed=True))
    await execution_started.wait()
    second = asyncio.create_task(service.resolve_approval(approval_id, confirmed=True))
    await asyncio.sleep(0)
    release_execution.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert executions == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1


def test_interrupted_running_approval_is_marked_failed_on_restart():
    approval_id = "approval-interrupted-test"
    register_task(
        task_id=approval_id,
        kind="ai_approval",
        title="Interrupted write",
        status="running",
    )

    changed = mark_stale_runtime_tasks_failed(
        kinds={"ai_approval"},
        older_than_seconds=0,
        message="Write outcome is indeterminate after restart",
    )

    assert changed == 1
    task = get_task(approval_id)
    assert task is not None
    assert task["status"] == "failed"
    assert task["error"] == "Write outcome is indeterminate after restart"


@pytest.mark.asyncio
async def test_reconcile_approval_states_updates_persisted_conversation():
    async with async_session_factory() as session:
        service = AINativeService(session)
        conversation = await service.create_conversation("Interrupted approval")
        approval = create_tool_approval(
            "save_strategy_candidate",
            {"name": "Candidate", "code": "def init(context):\n    pass"},
            owner_meta={"conversation_id": conversation.id},
            approval_id="approval-reconcile-test",
        )
        conversation.messages = [{"role": "assistant", "content": "confirm", "approvals": [approval]}]
        await session.commit()
        claim_task(
            approval["approval_id"],
            expected_kind="ai_approval",
            expected_status="waiting_approval",
            status="running",
        )
        mark_stale_runtime_tasks_failed(
            kinds={"ai_approval"},
            older_than_seconds=0,
            message="Write outcome is indeterminate after restart",
        )

        changed = await service.reconcile_approval_states()
        await session.refresh(conversation)

    assert changed == 1
    assert conversation.messages[0]["approvals"][0]["status"] == "failed"
