from __future__ import annotations

from sqlalchemy import create_engine

from app.core.config import settings
from app.db.models.base import Base
from app.services.runtime_tasks import (
    get_task,
    register_task,
    reset_runtime_tasks,
    update_task,
)


def test_runtime_task_survives_process_memory_reset(tmp_path, monkeypatch):
    database = tmp_path / "tasks.db"
    url = f"sqlite+aiosqlite:///{database.as_posix()}"
    sync_engine = create_engine(url.replace("+aiosqlite", ""))
    Base.metadata.create_all(sync_engine)
    sync_engine.dispose()
    monkeypatch.setattr(settings, "database_url", url)
    reset_runtime_tasks(clear_persistent=False)

    register_task(task_id="job-1", kind="backtest", title="Backtest", meta={"release_id": "release-1"})
    reset_runtime_tasks(clear_persistent=False)

    restored = get_task("job-1")
    assert restored is not None
    assert restored["status"] == "running"
    assert restored["meta"] == {"release_id": "release-1"}

    update_task("job-1", status="succeeded", progress=1.0, result_ref="backtest:1")
    reset_runtime_tasks(clear_persistent=False)
    completed = get_task("job-1")
    assert completed is not None
    assert completed["status"] == "succeeded"
    assert completed["progress"] == 1.0
    assert completed["result_ref"] == "backtest:1"

    update_task("job-1", status="done", progress=1.0)
    reset_runtime_tasks(clear_persistent=False)
    normalized = get_task("job-1")
    assert normalized is not None
    assert normalized["status"] == "succeeded"

    reset_runtime_tasks(clear_persistent=True)
