from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.ai import AIArtifact


def _new_artifact_id() -> str:
    return f"ai-{uuid.uuid4().hex[:12]}"


def _serialize_artifact(artifact: AIArtifact) -> dict[str, Any]:
    return {
        "artifact_id": artifact.artifact_id,
        "kind": artifact.kind,
        "status": artifact.status,
        "input_summary": artifact.input_summary,
        "tool_calls": artifact.tool_calls or [],
        "result_ref": artifact.result_ref,
        "key_outputs": artifact.key_outputs or {},
        "error": artifact.error,
        "created_at": artifact.created_at.isoformat() if artifact.created_at else None,
        "updated_at": artifact.updated_at.isoformat() if artifact.updated_at else None,
    }


async def create_artifact(
    session: AsyncSession,
    *,
    kind: str,
    status: str = "created",
    input_summary: str | None = None,
    tool_calls: list[dict[str, Any]] | None = None,
    result_ref: str | None = None,
    key_outputs: dict[str, Any] | None = None,
    error: str | None = None,
) -> AIArtifact:
    artifact = AIArtifact(
        artifact_id=_new_artifact_id(),
        kind=kind,
        status=status,
        input_summary=input_summary,
        tool_calls=tool_calls or [],
        result_ref=result_ref,
        key_outputs=key_outputs or {},
        error=error,
    )
    session.add(artifact)
    await session.flush()
    return artifact


async def update_artifact(
    session: AsyncSession,
    artifact_id: str,
    *,
    status: str | None = None,
    tool_calls: list[dict[str, Any]] | None = None,
    result_ref: str | None = None,
    key_outputs: dict[str, Any] | None = None,
    error: str | None = None,
) -> AIArtifact | None:
    artifact = await session.get(AIArtifact, artifact_id)
    if artifact is None:
        return None
    if status is not None:
        artifact.status = status
    if tool_calls is not None:
        artifact.tool_calls = tool_calls
    if result_ref is not None:
        artifact.result_ref = result_ref
    if key_outputs is not None:
        artifact.key_outputs = key_outputs
    if error is not None:
        artifact.error = error
    await session.flush()
    return artifact


async def get_artifact(session: AsyncSession, artifact_id: str) -> dict[str, Any] | None:
    artifact = await session.get(AIArtifact, artifact_id)
    return _serialize_artifact(artifact) if artifact else None


async def list_artifacts(
    session: AsyncSession,
    *,
    kind: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    stmt = select(AIArtifact).order_by(desc(AIArtifact.created_at)).limit(limit)
    if kind:
        stmt = stmt.where(AIArtifact.kind == kind)
    rows = (await session.execute(stmt)).scalars().all()
    return [_serialize_artifact(row) for row in rows]
