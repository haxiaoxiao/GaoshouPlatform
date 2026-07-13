from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient

from app.ai.gateway import LLMResult
from app.api.ai import _execute_graph, _run_graph, resume_ai_workflows
from app.core.config import settings
from app.main import app
from app.services.runtime_tasks import get_task, register_task


@pytest.mark.asyncio
async def test_ai_conversation_persists_messages(monkeypatch):
    captured_system = ""

    async def fake_complete(*args, **kwargs):
        nonlocal captured_system
        captured_system = str(kwargs.get("system") or "")
        return LLMResult(content="基于现有数据继续研究。", model="fake")

    monkeypatch.setattr("app.services.ai_native.complete", fake_complete)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = (await client.post("/api/ai/conversations", json={"title": "测试会话", "context": {"route": "/factor"}})).json()
        response = await client.post("/api/ai/chat", json={
            "conversation_id": created["id"],
            "message": "查看因子",
            "context": {"route": "/factor"},
        })
        loaded = (await client.get(f"/api/ai/conversations/{created['id']}")).json()

    assert response.status_code == 200
    assert loaded["messages"][-1]["content"] == "基于现有数据继续研究。"
    assert loaded["messages"][0]["role"] == "user"
    assert '"route": "/factor"' in captured_system


@pytest.mark.asyncio
async def test_missing_conversation_returns_not_found():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        chat = await client.post("/api/ai/chat", json={
            "conversation_id": "missing",
            "message": "hello",
        })
        deleted = await client.delete("/api/ai/conversations/missing")

    assert chat.status_code == 404
    assert deleted.status_code == 404


@pytest.mark.asyncio
async def test_ai_api_is_not_marked_as_legacy_deprecated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/ai/status")

    assert response.status_code == 200
    assert "Deprecation" not in response.headers


@pytest.mark.asyncio
async def test_ai_write_tool_requires_single_use_confirmation(monkeypatch):
    async def fake_complete(*args, **kwargs):
        return LLMResult(
            content="",
            model="fake",
            tool_calls=[{
                "id": "call-write",
                "name": "save_strategy_candidate",
                "arguments": json.dumps({"name": "AI候选", "code": "def init(context):\n    pass"}),
            }],
        )

    monkeypatch.setattr("app.services.ai_native.complete", fake_complete)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = (await client.post("/api/ai/conversations", json={"title": "审批测试"})).json()
        chat = (await client.post("/api/ai/chat", json={
            "conversation_id": created["id"],
            "message": "保存这个策略",
        })).json()
        approval_id = chat["approvals"][0]["approval_id"]
        confirmed = await client.post(f"/api/ai/approvals/{approval_id}/confirm")
        repeated = await client.post(f"/api/ai/approvals/{approval_id}/confirm")

    assert confirmed.status_code == 200
    assert confirmed.json()["result"]["status"] == "candidate"
    assert repeated.status_code == 409

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        loaded = (await client.get(f"/api/ai/conversations/{created['id']}")).json()
    assert loaded["messages"][-1]["approvals"][0]["status"] == "completed"


@pytest.mark.asyncio
async def test_deleting_conversation_revokes_pending_write_approval(monkeypatch):
    executions = 0

    async def fake_complete(*args, **kwargs):
        return LLMResult(
            content="",
            model="fake",
            tool_calls=[{
                "id": "call-delete",
                "name": "save_strategy_candidate",
                "arguments": json.dumps({"name": "待撤销", "code": "def init(context):\n    pass"}),
            }],
        )

    async def fake_execute_tool(*_args, **_kwargs):
        nonlocal executions
        executions += 1
        return {"status": "candidate"}

    monkeypatch.setattr("app.services.ai_native.complete", fake_complete)
    monkeypatch.setattr("app.services.ai_native.execute_tool", fake_execute_tool)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = (await client.post("/api/ai/conversations", json={"title": "删除审批"})).json()
        chat = (await client.post("/api/ai/chat", json={
            "conversation_id": created["id"],
            "message": "保存策略",
        })).json()
        approval_id = chat["approvals"][0]["approval_id"]
        deleted = await client.delete(f"/api/ai/conversations/{created['id']}")
        confirmed = await client.post(f"/api/ai/approvals/{approval_id}/confirm")

    assert deleted.status_code == 200
    assert confirmed.status_code == 409
    assert executions == 0


@pytest.mark.asyncio
async def test_workflow_submission_returns_run_handle_before_execution_finishes(monkeypatch):
    execution_started = asyncio.Event()
    release_execution = asyncio.Event()

    async def fake_execute(*_args, **_kwargs):
        execution_started.set()
        await release_execution.wait()
        return {"run_id": "ignored", "status": "succeeded"}

    monkeypatch.setattr("app.api.ai._execute_graph", fake_execute)
    response = await asyncio.wait_for(_run_graph("quant_research", {"question": "test"}), timeout=0.1)

    assert response["status"] == "queued"
    assert response["result_ref"] == f"/api/ai/runs/{response['run_id']}"
    await asyncio.wait_for(execution_started.wait(), timeout=0.1)
    release_execution.set()
    await asyncio.sleep(0)


def test_workflow_resume_is_claimed_once(monkeypatch):
    scheduled: list[str] = []
    run_id = "ai-workflow-resume-claim"
    register_task(
        task_id=run_id,
        kind="ai_workflow",
        title="quant_research",
        status="running",
        meta={"workflow": "quant_research", "initial_state": {"question": "resume"}},
    )
    monkeypatch.setattr(
        "app.api.ai._schedule_graph",
        lambda task_id, *_args, **_kwargs: scheduled.append(task_id),
    )

    assert resume_ai_workflows() == 1
    assert resume_ai_workflows() == 0
    assert scheduled == [run_id]


@pytest.mark.asyncio
async def test_resume_workflow_uses_initial_state_when_checkpoint_is_missing(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class FakeSaver:
        async def aget_tuple(self, config):
            captured["checkpoint_config"] = config
            return None

    class FakeGraph:
        async def ainvoke(self, graph_input, config):
            captured["graph_input"] = graph_input
            captured["invoke_config"] = config
            return {"status": "complete"}

    @asynccontextmanager
    async def fake_saver_context(_path):
        yield FakeSaver()

    monkeypatch.setattr(
        "langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver.from_conn_string",
        fake_saver_context,
    )
    monkeypatch.setattr("app.api.ai.build_quant_research_graph", lambda _saver: FakeGraph())
    monkeypatch.setattr("app.api.ai.update_task", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(settings, "ai_checkpoint_db_path", str(tmp_path / "checkpoints.sqlite"))

    initial_state = {"question": "恢复量化研究", "context": {"route": "/factors"}}
    await _execute_graph(
        "ai-workflow-resume",
        "quant_research",
        initial_state,
        resume=True,
    )

    expected_config = {"configurable": {"thread_id": "ai-workflow-resume"}}
    assert captured["checkpoint_config"] == expected_config
    assert captured["invoke_config"] == expected_config
    assert captured["graph_input"] == initial_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("graph_name", "graph_result", "expected_tool"),
    [
        (
            "report_strategy",
            {
                "result": {
                    "name": "研报候选策略",
                    "code": "def init(context):\n    pass",
                    "summary": "候选策略",
                    "parameters": {},
                },
                "validation": {"valid": True, "errors": []},
            },
            "save_strategy_candidate",
        ),
        (
            "quant_research",
            {
                "report": "建议运行候选回测。",
                "backtest_request": {
                    "engine": "akquant",
                    "start_date": "2025-01-01",
                    "end_date": "2025-12-31",
                },
            },
            "run_candidate_backtest",
        ),
    ],
)
async def test_workflow_candidates_create_write_approvals(
    monkeypatch,
    tmp_path,
    graph_name,
    graph_result,
    expected_tool,
):
    class FakeSaver:
        pass

    class FakeGraph:
        async def ainvoke(self, _graph_input, _config):
            return graph_result

    @asynccontextmanager
    async def fake_saver_context(_path):
        yield FakeSaver()

    monkeypatch.setattr(
        "langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver.from_conn_string",
        fake_saver_context,
    )
    monkeypatch.setattr("app.api.ai.build_report_strategy_graph", lambda _saver: FakeGraph())
    monkeypatch.setattr("app.api.ai.build_quant_research_graph", lambda _saver: FakeGraph())
    monkeypatch.setattr(settings, "ai_checkpoint_db_path", str(tmp_path / f"{graph_name}.sqlite"))

    response = await _execute_graph(
        f"ai-workflow-{graph_name}-approval",
        graph_name,
        {"input": "test"},
    )

    approval = response["result"]["approvals"][0]
    persisted = get_task(approval["approval_id"])
    assert approval["tool"] == expected_tool
    assert persisted is not None
    assert persisted["status"] == "waiting_approval"
    assert persisted["meta"]["run_id"] == f"ai-workflow-{graph_name}-approval"
