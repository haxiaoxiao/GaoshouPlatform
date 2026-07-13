from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.ai.gateway import _normalize_response, gateway_status
from app.ai.mcp_server import mcp
from app.ai.tools import get_tool, list_tools
from app.core.config import settings
from app.db.sqlite import async_session_factory
from app.services.ai_native import AINativeService, create_tool_approval
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
