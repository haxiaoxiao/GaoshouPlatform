from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.gateway import complete, gateway_status
from app.ai.tools import execute_tool, get_tool, list_tools
from app.core.config import settings
from app.db.models import AIConversation
from app.services.runtime_tasks import claim_task, get_task, list_tasks, register_task, update_task

SYSTEM_PROMPT = """You are GaoshouPlatform Copilot, an A-share quantitative research assistant.
Use registered tools for platform facts. Never claim a write succeeded before explicit user approval.
Do not place live orders, execute arbitrary code, or invent unavailable data. Answer in the user's language."""


class ConversationNotFoundError(ValueError):
    pass


def _public_approval_status(status: str) -> str:
    return {
        "cancelled": "rejected",
        "failed": "failed",
        "running": "running",
        "succeeded": "completed",
        "waiting_approval": "pending",
    }.get(status, status)


def _expires_at() -> datetime:
    return datetime.now() + timedelta(days=max(settings.ai_conversation_retention_days, 1))


def create_tool_approval(
    name: str,
    arguments: dict[str, Any],
    *,
    owner_meta: dict[str, Any],
    approval_id: str | None = None,
) -> dict[str, Any]:
    tool = get_tool(name)
    if tool.risk != "write":
        raise ValueError(f"Read-only tool does not require approval: {name}")
    from jsonschema import validate

    validate(instance=arguments, schema=tool.input_schema)
    digest = hashlib.sha256(json.dumps(arguments, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    task_id = approval_id or f"approval-{uuid.uuid4().hex[:12]}"
    existing = get_task(task_id)
    if existing is not None:
        meta = dict(existing.get("meta") or {})
        return {
            "approval_id": task_id,
            "tool": str(meta.get("tool") or name),
            "arguments": dict(meta.get("arguments") or arguments),
            "arguments_hash": str(meta.get("arguments_hash") or digest),
            "status": _public_approval_status(str(existing.get("status"))),
        }
    register_task(
        task_id=task_id,
        kind="ai_approval",
        title=f"Confirm {name}",
        status="waiting_approval",
        meta={
            **owner_meta,
            "tool": name,
            "arguments": arguments,
            "arguments_hash": digest,
            "used": False,
        },
    )
    return {
        "approval_id": task_id,
        "tool": name,
        "arguments": arguments,
        "arguments_hash": digest,
        "status": "pending",
    }


def _revoke_conversation_approvals(conversation_id: str) -> int:
    revoked = 0
    for task in list_tasks(include_finished=False, kinds={"ai_approval"}, limit=None):
        meta = dict(task.get("meta") or {})
        if task.get("kind") != "ai_approval" or meta.get("conversation_id") != conversation_id:
            continue
        claimed = claim_task(
            str(task["task_id"]),
            expected_kind="ai_approval",
            expected_status="waiting_approval",
            status="cancelled",
            meta={"used": True, "decision": "conversation_removed"},
        )
        if claimed is not None:
            update_task(str(task["task_id"]), progress=100)
            revoked += 1
    return revoked


class AINativeService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def cleanup_expired(self) -> int:
        expired_ids = list((await self.session.scalars(
            select(AIConversation.id).where(AIConversation.expires_at < datetime.now())
        )).all())
        for conversation_id in expired_ids:
            _revoke_conversation_approvals(str(conversation_id))
        result = await self.session.execute(delete(AIConversation).where(AIConversation.expires_at < datetime.now()))
        await self.session.commit()
        return int(result.rowcount or 0)

    async def reconcile_approval_states(self) -> int:
        tasks = {
            str(task["task_id"]): task
            for task in list_tasks(include_finished=True, kinds={"ai_approval"}, limit=None)
        }
        if not tasks:
            return 0
        rows = list((await self.session.scalars(select(AIConversation))).all())
        changed_rows = 0
        for row in rows:
            messages = json.loads(json.dumps(row.messages or [], ensure_ascii=False, default=str))
            changed = False
            for message in messages:
                approvals = list(message.get("approvals") or [])
                for index, approval in enumerate(approvals):
                    task = tasks.get(str(approval.get("approval_id") or ""))
                    if task is None:
                        continue
                    status = _public_approval_status(str(task.get("status") or ""))
                    if approval.get("status") == status:
                        continue
                    meta = dict(task.get("meta") or {})
                    approvals[index] = {
                        **approval,
                        "status": status,
                        **({"result": meta["result"]} if "result" in meta else {}),
                    }
                    message["approvals"] = approvals
                    changed = True
            if changed:
                row.messages = messages
                changed_rows += 1
        if changed_rows:
            await self.session.commit()
        return changed_rows

    async def create_conversation(self, title: str = "New conversation", context: dict[str, Any] | None = None) -> AIConversation:
        row = AIConversation(id=str(uuid.uuid4()), title=title[:200], messages=[], context=context or {}, expires_at=_expires_at())
        self.session.add(row)
        await self.session.commit()
        await self.session.refresh(row)
        return row

    async def list_conversations(self) -> list[AIConversation]:
        await self.cleanup_expired()
        return list((await self.session.scalars(select(AIConversation).order_by(AIConversation.updated_at.desc()).limit(100))).all())

    async def get_conversation(self, conversation_id: str) -> AIConversation:
        row = await self.session.get(AIConversation, conversation_id)
        if row is None or row.expires_at < datetime.now():
            raise ConversationNotFoundError("AI conversation not found or expired")
        return row

    async def delete_conversation(self, conversation_id: str) -> None:
        row = await self.get_conversation(conversation_id)
        _revoke_conversation_approvals(conversation_id)
        await self.session.delete(row)
        await self.session.commit()

    async def _record_approval_resolution(
        self,
        conversation_id: str,
        approval_id: str,
        status: str,
        result: Any = None,
    ) -> None:
        if not conversation_id:
            return
        row = await self.session.get(AIConversation, conversation_id)
        if row is None:
            return
        messages = json.loads(json.dumps(row.messages or [], ensure_ascii=False, default=str))
        changed = False
        normalized_result = json.loads(json.dumps(result, ensure_ascii=False, default=str)) if result is not None else None
        for message in messages:
            approvals = list(message.get("approvals") or [])
            for index, approval in enumerate(approvals):
                if approval.get("approval_id") != approval_id:
                    continue
                approvals[index] = {
                    **approval,
                    "status": status,
                    **({"result": normalized_result} if normalized_result is not None else {}),
                }
                message["approvals"] = approvals
                changed = True
        if changed:
            row.messages = messages
            await self.session.commit()

    async def chat(self, conversation_id: str, message: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        row = await self.get_conversation(conversation_id)
        messages = list(row.messages or [])
        messages.append({"role": "user", "content": message})
        if context:
            row.context = {**(row.context or {}), **context}
        model_system = SYSTEM_PROMPT
        if row.context:
            model_system += "\nCurrent page context: " + json.dumps(row.context, ensure_ascii=False, default=str)
        task_id = f"ai-{uuid.uuid4().hex[:12]}"
        register_task(task_id=task_id, kind="ai_chat", title=row.title, status="running", meta={"conversation_id": row.id})
        try:
            result = await complete(messages, system=model_system, tools=[tool.llm_schema() for tool in list_tools()])
            approvals: list[dict[str, Any]] = []
            tool_results: list[dict[str, Any]] = []
            for call in result.tool_calls:
                name = str(call.get("name") or "")
                arguments = json.loads(str(call.get("arguments") or "{}"))
                tool = get_tool(name)
                if tool.risk == "write":
                    approvals.append(create_tool_approval(name, arguments, owner_meta={"conversation_id": row.id}))
                    continue
                value = await execute_tool(self.session, name, arguments)
                tool_results.append({"tool": name, "result": value})
            if tool_results:
                messages.append({"role": "assistant", "content": result.content or "I checked the platform data."})
                messages.append({"role": "user", "content": "Tool results:\n" + json.dumps(tool_results, ensure_ascii=False, default=str)})
                result = await complete(messages, system=model_system)
            assistant = result.content or ("等待确认后执行写操作。" if approvals else "未生成回复。")
            messages.append({"role": "assistant", "content": assistant, "approvals": approvals, "tool_results": tool_results})
            row.messages = messages
            row.expires_at = _expires_at()
            await self.session.commit()
            update_task(task_id, status="succeeded", progress=100, result_ref=f"/api/ai/conversations/{row.id}", meta={"approvals": approvals, "usage": result.usage})
            return {"run_id": task_id, "conversation_id": row.id, "message": messages[-1], "approvals": approvals}
        except Exception as exc:
            update_task(task_id, status="failed", progress=100, error=str(exc))
            raise

    async def resolve_approval(self, approval_id: str, *, confirmed: bool) -> dict[str, Any]:
        task = get_task(approval_id)
        if not task or task.get("kind") != "ai_approval":
            raise ValueError("Approval not found")
        meta = dict(task.get("meta") or {})
        conversation_id = str(meta.get("conversation_id") or "")
        if conversation_id:
            try:
                await self.get_conversation(conversation_id)
            except ConversationNotFoundError as exc:
                _revoke_conversation_approvals(conversation_id)
                raise ValueError("Approval owner conversation is unavailable") from exc
        target_status = "running" if confirmed else "cancelled"
        claimed = claim_task(
            approval_id,
            expected_kind="ai_approval",
            expected_status="waiting_approval",
            status=target_status,
            meta={"used": True, "decision": "confirmed" if confirmed else "rejected"},
        )
        if claimed is None:
            raise ValueError("Approval is no longer available")
        meta = dict(claimed.get("meta") or {})
        if not confirmed:
            update_task(approval_id, progress=100)
            await self._record_approval_resolution(conversation_id, approval_id, "rejected")
            return {"approval_id": approval_id, "status": "rejected"}
        try:
            result = await execute_tool(self.session, str(meta["tool"]), dict(meta.get("arguments") or {}))
        except Exception as exc:
            update_task(approval_id, status="failed", progress=100, error=str(exc))
            await self._record_approval_resolution(conversation_id, approval_id, "failed", {"error": str(exc)})
            raise
        update_task(approval_id, status="succeeded", progress=100, meta={"used": True, "decision": "confirmed", "result": result})
        await self._record_approval_resolution(conversation_id, approval_id, "completed", result)
        return {"approval_id": approval_id, "status": "completed", "result": result}


def serialize_conversation(row: AIConversation) -> dict[str, Any]:
    return {
        "id": row.id,
        "title": row.title,
        "messages": row.messages or [],
        "context": row.context or {},
        "expires_at": row.expires_at.isoformat(),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


__all__ = [
    "AINativeService",
    "ConversationNotFoundError",
    "create_tool_approval",
    "gateway_status",
    "serialize_conversation",
]
