from __future__ import annotations

import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


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
