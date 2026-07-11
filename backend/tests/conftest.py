from __future__ import annotations

import shutil

import pytest
from sqlalchemy import create_engine

from app.core.config import settings
from app.db.models.base import Base
from app.services.runtime_tasks import reset_runtime_tasks


@pytest.fixture(autouse=True)
def isolate_live_trading_settings(monkeypatch: pytest.MonkeyPatch):
    """Never let tests inherit live-order capability from the prod env file."""
    monkeypatch.setattr(settings, "live_trading_enable_order_submit", False)
    monkeypatch.setattr(settings, "live_trading_auto_execute_enabled", False)


@pytest.fixture(scope="session")
def test_database_template(tmp_path_factory):
    import app.db.models  # noqa: F401

    database = tmp_path_factory.mktemp("database-template") / "template.db"
    engine = create_engine(f"sqlite:///{database.as_posix()}")
    Base.metadata.create_all(engine)
    engine.dispose()
    return database


@pytest.fixture(autouse=True)
def isolate_test_database(tmp_path, test_database_template, monkeypatch: pytest.MonkeyPatch):
    """Every test gets a disposable SQLite database instead of the active environment DB."""
    database = tmp_path / "gaoshou-test.db"
    shutil.copyfile(test_database_template, database)
    url = f"sqlite+aiosqlite:///{database.as_posix()}"
    monkeypatch.setattr(settings, "database_url", url)
    reset_runtime_tasks(clear_persistent=False)
    yield
    reset_runtime_tasks(clear_persistent=False)
