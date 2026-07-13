from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.gateway import gateway_status
from app.ai.tools import list_tools
from app.ai.workflows import build_quant_research_graph, build_report_strategy_graph
from app.core.config import settings
from app.db.sqlite import get_async_session
from app.services.ai_native import (
    AINativeService,
    ConversationNotFoundError,
    create_tool_approval,
    serialize_conversation,
)
from app.services.runtime_tasks import claim_task, get_task, list_tasks, register_task, update_task

router = APIRouter()
_workflow_tasks: set[asyncio.Task[Any]] = set()


class ConversationCreate(BaseModel):
    title: str = "New conversation"
    context: dict[str, Any] = Field(default_factory=dict)


class ChatRequest(BaseModel):
    conversation_id: str
    message: str = Field(min_length=1, max_length=20000)
    context: dict[str, Any] = Field(default_factory=dict)


class QuantResearchRequest(BaseModel):
    question: str = Field(min_length=3, max_length=20000)
    context: dict[str, Any] = Field(default_factory=dict)


@router.get("/status")
async def ai_status() -> dict[str, Any]:
    return {**gateway_status(), "checkpoint_db": settings.ai_checkpoint_db_path, "retention_days": settings.ai_conversation_retention_days}


@router.get("/tools")
async def ai_tools() -> list[dict[str, Any]]:
    return [tool.public_dict() for tool in list_tools()]


@router.post("/conversations")
async def create_conversation(req: ConversationCreate, session: AsyncSession = Depends(get_async_session)) -> dict[str, Any]:
    return serialize_conversation(await AINativeService(session).create_conversation(req.title, req.context))


@router.get("/conversations")
async def list_conversations(session: AsyncSession = Depends(get_async_session)) -> list[dict[str, Any]]:
    return [serialize_conversation(row) for row in await AINativeService(session).list_conversations()]


@router.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str, session: AsyncSession = Depends(get_async_session)) -> dict[str, Any]:
    try:
        return serialize_conversation(await AINativeService(session).get_conversation(conversation_id))
    except ConversationNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str, session: AsyncSession = Depends(get_async_session)) -> dict[str, bool]:
    try:
        await AINativeService(session).delete_conversation(conversation_id)
    except ConversationNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"deleted": True}


@router.post("/chat")
async def chat(req: ChatRequest, session: AsyncSession = Depends(get_async_session)) -> dict[str, Any]:
    try:
        return await AINativeService(session).chat(req.conversation_id, req.message, req.context)
    except ConversationNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
async def ai_run(run_id: str) -> dict[str, Any]:
    task = get_task(run_id)
    if task is None:
        raise HTTPException(status_code=404, detail="AI run not found")
    return task


async def _task_events(run_id: str) -> AsyncIterator[str]:
    last_payload = ""
    for _ in range(600):
        task = get_task(run_id)
        if task is None:
            yield "event: error\ndata: {\"message\":\"run not found\"}\n\n"
            return
        payload = json.dumps(task, ensure_ascii=False, default=str)
        if payload != last_payload:
            yield f"event: status\ndata: {payload}\n\n"
            last_payload = payload
        if task["status"] in {"succeeded", "failed", "cancelled"}:
            yield f"event: done\ndata: {payload}\n\n"
            return
        await asyncio.sleep(1)


@router.get("/runs/{run_id}/events")
async def ai_run_events(run_id: str) -> StreamingResponse:
    return StreamingResponse(_task_events(run_id), media_type="text/event-stream")


@router.post("/approvals/{approval_id}/confirm")
async def confirm_approval(approval_id: str, session: AsyncSession = Depends(get_async_session)) -> dict[str, Any]:
    try:
        return await AINativeService(session).resolve_approval(approval_id, confirmed=True)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/approvals/{approval_id}/reject")
async def reject_approval(approval_id: str, session: AsyncSession = Depends(get_async_session)) -> dict[str, Any]:
    try:
        return await AINativeService(session).resolve_approval(approval_id, confirmed=False)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


async def _execute_graph(run_id: str, graph_name: str, initial_state: dict[str, Any], *, resume: bool = False) -> dict[str, Any]:
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    Path(settings.ai_checkpoint_db_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        if resume:
            update_task(run_id, progress=1)
        else:
            update_task(run_id, status="running", progress=1)
        async with AsyncSqliteSaver.from_conn_string(settings.ai_checkpoint_db_path) as saver:
            graph = build_report_strategy_graph(saver) if graph_name == "report_strategy" else build_quant_research_graph(saver)
            config = {"configurable": {"thread_id": run_id}}
            checkpoint = await saver.aget_tuple(config) if resume else None
            graph_input = None if checkpoint is not None else initial_state
            result = await graph.ainvoke(graph_input, config)
        approvals = _workflow_approvals(run_id, graph_name, result)
        result = {**result, "approvals": approvals}
        update_task(run_id, status="succeeded", progress=100, result_ref=f"/api/ai/runs/{run_id}", meta={"result": result})
        return {"run_id": run_id, "status": "succeeded", "result": result}
    except Exception as exc:
        update_task(run_id, status="failed", progress=100, error=str(exc))
        raise


def _workflow_approvals(run_id: str, graph_name: str, result: dict[str, Any]) -> list[dict[str, Any]]:
    tool_name = ""
    arguments: dict[str, Any] | None = None
    if graph_name == "report_strategy" and bool((result.get("validation") or {}).get("valid")):
        generated = dict(result.get("result") or {})
        code = str(generated.get("code") or "").strip()
        if code:
            tool_name = "save_strategy_candidate"
            arguments = {
                "name": str(generated.get("name") or "AI report strategy"),
                "code": code,
                "parameters": dict(generated.get("parameters") or {}),
                "description": str(generated.get("summary") or "AI-generated report strategy candidate"),
            }
    elif graph_name == "quant_research" and isinstance(result.get("backtest_request"), dict):
        tool_name = "run_candidate_backtest"
        arguments = dict(result["backtest_request"])
    if not tool_name or arguments is None:
        return []
    suffix = hashlib.sha256(f"{run_id}:{tool_name}".encode()).hexdigest()[:12]
    return [create_tool_approval(
        tool_name,
        arguments,
        owner_meta={"run_id": run_id},
        approval_id=f"approval-{suffix}",
    )]


async def _run_graph(graph_name: str, initial_state: dict[str, Any]) -> dict[str, Any]:
    run_id = f"ai-workflow-{uuid.uuid4().hex[:12]}"
    register_task(
        task_id=run_id,
        kind="ai_workflow",
        title=graph_name,
        status="queued",
        meta={"workflow": graph_name, "initial_state": initial_state, "owner_pid": os.getpid()},
    )
    _schedule_graph(run_id, graph_name, initial_state)
    return {"run_id": run_id, "status": "queued", "result_ref": f"/api/ai/runs/{run_id}"}


def _schedule_graph(run_id: str, graph_name: str, initial_state: dict[str, Any], *, resume: bool = False) -> None:
    task = asyncio.create_task(_execute_graph(run_id, graph_name, initial_state, resume=resume))
    _workflow_tasks.add(task)
    task.add_done_callback(_workflow_tasks.discard)


def _process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    return Path(f"/proc/{pid}").exists()


def resume_ai_workflows() -> int:
    resumed = 0
    for task in list_tasks(include_finished=False, kinds={"ai_workflow"}, limit=None):
        status = str(task.get("status") or "")
        if task.get("kind") != "ai_workflow" or status not in {"queued", "running", "resuming"}:
            continue
        meta = dict(task.get("meta") or {})
        if _process_is_running(int(meta.get("owner_pid") or 0)):
            continue
        workflow = str(meta.get("workflow") or "")
        initial_state = dict(meta.get("initial_state") or {})
        if workflow not in {"report_strategy", "quant_research"}:
            continue
        claimed = claim_task(
            str(task["task_id"]),
            expected_kind="ai_workflow",
            expected_status=status,
            status="resuming",
            meta={"owner_pid": os.getpid()},
            expected_updated_at=float(task["updated_at"]),
        )
        if claimed is None:
            continue
        _schedule_graph(str(task["task_id"]), workflow, initial_state, resume=True)
        resumed += 1
    return resumed


@router.post("/workflows/report-strategy")
async def report_strategy(file: UploadFile = File(...)) -> dict[str, Any]:
    suffix = Path(file.filename or "report.txt").suffix.lower()
    if suffix not in {".pdf", ".txt", ".md"}:
        raise HTTPException(status_code=400, detail="Only PDF/TXT/MD reports are supported")
    from app.services.report_to_strategy import parse_report

    runtime_dir = Path(settings.gaoshou_data_dir) / "ai-runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    path = runtime_dir / f"{uuid.uuid4().hex}{suffix}"
    path.write_bytes(await file.read())
    try:
        text = await asyncio.to_thread(parse_report, str(path))
    finally:
        path.unlink(missing_ok=True)
    return await _run_graph("report_strategy", {"report_text": text, "filename": file.filename or "report"})


@router.post("/workflows/quant-research")
async def quant_research(req: QuantResearchRequest) -> dict[str, Any]:
    return await _run_graph("quant_research", {"question": req.question, "context": req.context})
