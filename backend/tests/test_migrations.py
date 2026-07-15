from __future__ import annotations

import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def test_lineage_migration_upgrades_and_downgrades_legacy_database(tmp_path):
    database = tmp_path / "legacy.db"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE strategies (id INTEGER PRIMARY KEY, name TEXT NOT NULL, code TEXT NOT NULL);
        CREATE TABLE backtests (
            id INTEGER PRIMARY KEY,
            strategy_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL
        );
        """
    )
    connection.close()

    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option("script_location", str(backend_root / "migrations"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{database.as_posix()}")

    command.upgrade(config, "head")
    inspector = inspect(create_engine(f"sqlite:///{database.as_posix()}"))
    assert {"data_snapshots", "strategy_releases", "research_artifacts", "jobs", "job_events"} <= set(
        inspector.get_table_names()
    )
    assert {
        "run_id",
        "release_id",
        "data_snapshot_id",
        "engine",
        "result_schema_version",
        "code_hash",
        "warnings",
    } <= {column["name"] for column in inspector.get_columns("backtests")}

    command.downgrade(config, "base")
    inspector = inspect(create_engine(f"sqlite:///{database.as_posix()}"))
    assert "strategy_releases" not in inspector.get_table_names()
    assert "run_id" not in {column["name"] for column in inspector.get_columns("backtests")}


def test_llm_json_config_migration_preserves_legacy_row_across_roundtrip(tmp_path):
    database = tmp_path / "llm-json-roundtrip.db"
    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option("script_location", str(backend_root / "migrations"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{database.as_posix()}")

    command.upgrade(config, "20260713_0001")
    engine = create_engine(f"sqlite:///{database.as_posix()}")
    original = {
        "id": "legacy-roundtrip",
        "name": "Legacy endpoint",
        "api_base": "https://legacy.example.com/v1",
        "api_key_encrypted": "gAAAAABlegacy-encrypted-looking-ciphertext",
        "api_key_hint": "********text",
        "model": "legacy/model",
        "priority": 0,
        "enabled": 0,
        "consecutive_failures": 2,
        "cooldown_until": "2026-07-15 10:00:00",
        "last_success_at": "2026-07-14 09:00:00",
        "last_failure_at": "2026-07-15 09:30:00",
        "last_error": "legacy failure",
        "created_at": "2026-07-13 08:00:00",
        "updated_at": "2026-07-15 09:30:00",
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO llm_endpoints "
                "(id, name, api_base, api_key_encrypted, api_key_hint, model, priority, enabled, "
                "consecutive_failures, cooldown_until, last_success_at, last_failure_at, last_error, "
                "created_at, updated_at) VALUES "
                "(:id, :name, :api_base, :api_key_encrypted, :api_key_hint, :model, :priority, "
                ":enabled, :consecutive_failures, :cooldown_until, :last_success_at, "
                ":last_failure_at, :last_error, :created_at, :updated_at)"
            ),
            original,
        )

    command.upgrade(config, "20260714_0001")
    with engine.connect() as connection:
        upgraded = connection.execute(
            text("SELECT * FROM llm_endpoints WHERE id = :id"), {"id": original["id"]}
        ).mappings().one()
        assert upgraded["api_key_encrypted"] == original["api_key_encrypted"]
        assert upgraded["provider"] is None
        assert upgraded["review_model"] is None
        assert upgraded["wire_api"] == "chat_completions"
        assert upgraded["reasoning_effort"] is None
        assert upgraded["disable_response_storage"] == 0
        assert upgraded["requires_openai_auth"] == 0
        assert upgraded["config_json"] is None

    command.downgrade(config, "20260713_0001")
    with engine.connect() as connection:
        downgraded = connection.execute(
            text("SELECT * FROM llm_endpoints WHERE id = :id"), {"id": original["id"]}
        ).mappings().one()
        for field, value in original.items():
            assert str(downgraded[field]) == str(value)
        assert "wire_api" not in downgraded

    command.upgrade(config, "20260714_0001")
    with engine.connect() as connection:
        reupgraded = connection.execute(
            text("SELECT * FROM llm_endpoints WHERE id = :id"), {"id": original["id"]}
        ).mappings().one()
        for field, value in original.items():
            assert str(reupgraded[field]) == str(value)
        assert reupgraded["wire_api"] == "chat_completions"
        assert reupgraded["disable_response_storage"] == 0
        assert reupgraded["requires_openai_auth"] == 0
    engine.dispose()
