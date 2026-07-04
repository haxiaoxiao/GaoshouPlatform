from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.ai.artifacts import create_artifact, get_artifact, list_artifacts, update_artifact
from app.db.models.base import Base


@pytest.mark.asyncio
async def test_ai_artifact_store_round_trip():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        artifact = await create_artifact(
            session,
            kind="tool:system.status",
            status="running",
            input_summary="status",
            tool_calls=[{"tool_name": "system.status"}],
        )
        await update_artifact(
            session,
            artifact.artifact_id,
            status="completed",
            result_ref="/monitor",
            key_outputs={"summary": "ok"},
        )
        await session.commit()

    async with maker() as session:
        loaded = await get_artifact(session, artifact.artifact_id)
        items = await list_artifacts(session, limit=10)

    await engine.dispose()

    assert loaded is not None
    assert loaded["status"] == "completed"
    assert loaded["result_ref"] == "/monitor"
    assert loaded["key_outputs"]["summary"] == "ok"
    assert [item["artifact_id"] for item in items] == [artifact.artifact_id]
