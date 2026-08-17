from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models.llm_endpoint import LlmEndpoint
from app.services.llm_endpoints import LlmEndpointService


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
    with Session(engine) as session:
        migrated = session.get(LlmEndpoint, original["id"])
        assert migrated is not None
        assert LlmEndpointService.serialize(migrated)["requires_openai_auth"] is True

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


def test_market_radar_migration_contract_and_partial_uniqueness(tmp_path):
    database = tmp_path / "market-radar.db"
    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option("script_location", str(backend_root / "migrations"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{database.as_posix()}")

    command.upgrade(config, "20260714_0001")
    engine = create_engine(f"sqlite:///{database.as_posix()}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE radar_migration_sentinel (id INTEGER PRIMARY KEY)"))
        connection.execute(text("INSERT INTO radar_migration_sentinel (id) VALUES (1)"))

    command.upgrade(config, "head")
    inspector = inspect(engine)
    radar_tables = {
        "market_radar_snapshots",
        "market_alert_rules",
        "market_alert_events",
    }
    assert radar_tables <= set(inspector.get_table_names())

    expected_columns = {
        "market_radar_snapshots": {
            "id",
            "snapshot_type",
            "as_of",
            "computed_at",
            "status",
            "confidence",
            "formula_version",
            "metrics_json",
            "source_freshness_json",
            "created_at",
            "updated_at",
        },
        "market_alert_rules": {
            "id",
            "rule_key",
            "version",
            "scope",
            "subject",
            "rule_type",
            "parameters_json",
            "severity",
            "cooldown_seconds",
            "enabled",
            "source",
            "created_at",
            "updated_at",
        },
        "market_alert_events": {
            "id",
            "rule_id",
            "snapshot_id",
            "scope",
            "subject",
            "direction",
            "severity",
            "status",
            "title",
            "explanation",
            "dedupe_key",
            "evidence_json",
            "triggered_at",
            "last_seen_at",
            "acknowledged_at",
            "dismissed_at",
            "resolved_at",
            "last_notified_at",
            "occurrence_count",
            "clear_streak",
            "created_at",
            "updated_at",
        },
    }
    forbidden_fragments = {
        "account",
        "account_id",
        "position",
        "quantity",
        "cost",
        "cookie",
        "token",
        "secret",
    }
    for table_name, columns in expected_columns.items():
        actual = {str(column["name"]): column for column in inspector.get_columns(table_name)}
        assert set(actual) == columns
        assert all(
            fragment not in name.lower()
            for name in actual
            for fragment in forbidden_fragments
        )

    snapshot_types = {
        str(column["name"]): str(column["type"]).upper()
        for column in inspector.get_columns("market_radar_snapshots")
    }
    rule_types = {
        str(column["name"]): str(column["type"]).upper()
        for column in inspector.get_columns("market_alert_rules")
    }
    event_types = {
        str(column["name"]): str(column["type"]).upper()
        for column in inspector.get_columns("market_alert_events")
    }
    assert snapshot_types["metrics_json"] == "TEXT"
    assert snapshot_types["source_freshness_json"] == "TEXT"
    assert rule_types["parameters_json"] == "TEXT"
    assert event_types["evidence_json"] == "TEXT"

    event_foreign_keys = {
        foreign_key["constrained_columns"][0]: foreign_key
        for foreign_key in inspector.get_foreign_keys("market_alert_events")
    }
    assert event_foreign_keys["rule_id"]["referred_table"] == "market_alert_rules"
    assert event_foreign_keys["snapshot_id"]["referred_table"] == "market_radar_snapshots"
    assert event_foreign_keys["snapshot_id"]["options"]["ondelete"] == "SET NULL"

    snapshot_indexes = {index["name"] for index in inspector.get_indexes("market_radar_snapshots")}
    rule_indexes = {index["name"] for index in inspector.get_indexes("market_alert_rules")}
    event_indexes = {index["name"] for index in inspector.get_indexes("market_alert_events")}
    assert "uq_market_radar_snapshot_identity" in snapshot_indexes
    assert "uq_market_alert_rule_key_version" in rule_indexes
    assert "ix_market_alert_rules_enabled_scope_subject" in rule_indexes
    assert "ux_market_alert_events_open_dedupe_key" in event_indexes
    assert "ix_market_alert_events_status_triggered_at" in event_indexes
    assert "ix_market_alert_events_scope_subject_status" in event_indexes

    with engine.connect() as connection:
        partial_index_sql = connection.execute(
            text(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'ux_market_alert_events_open_dedupe_key'"
            )
        ).scalar_one()
    normalized_index_sql = " ".join(str(partial_index_sql).lower().split())
    assert "where status in ('active', 'acknowledged', 'dismissed')" in normalized_index_sql

    now = "2026-07-18 10:00:00"
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO market_alert_rules "
                "(id, rule_key, version, scope, subject, rule_type, parameters_json, severity, "
                "cooldown_seconds, enabled, source, created_at, updated_at) VALUES "
                "(1, 'market.panic', 1, 'market', 'all_a', 'threshold', '{}', 'high', "
                "900, 1, 'system', :now, :now)"
            ),
            {"now": now},
        )
        connection.execute(
            text(
                "INSERT INTO market_alert_events "
                "(id, rule_id, scope, subject, direction, severity, status, title, explanation, "
                "dedupe_key, evidence_json, triggered_at, last_seen_at, occurrence_count, "
                "clear_streak, created_at, updated_at) VALUES "
                "(1, 1, 'market', 'all_a', 'negative', 'high', 'active', 'Market alert', "
                "'Evidence', 'market.panic:all_a', '{}', :now, :now, 1, 0, :now, :now)"
            ),
            {"now": now},
        )

    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO market_alert_events "
                    "(id, rule_id, scope, subject, direction, severity, status, title, explanation, "
                    "dedupe_key, evidence_json, triggered_at, last_seen_at, occurrence_count, "
                    "clear_streak, created_at, updated_at) VALUES "
                    "(2, 1, 'market', 'all_a', 'negative', 'high', 'acknowledged', "
                    "'Duplicate', 'Duplicate', 'market.panic:all_a', '{}', :now, :now, 1, 0, "
                    ":now, :now)"
                ),
                {"now": now},
            )

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE market_alert_events SET status = 'resolved', resolved_at = :now "
                "WHERE id = 1"
            ),
            {"now": now},
        )
        connection.execute(
            text(
                "INSERT INTO market_alert_events "
                "(id, rule_id, scope, subject, direction, severity, status, title, explanation, "
                "dedupe_key, evidence_json, triggered_at, last_seen_at, occurrence_count, "
                "clear_streak, created_at, updated_at) VALUES "
                "(2, 1, 'market', 'all_a', 'negative', 'high', 'active', 'New cycle', "
                "'Evidence', 'market.panic:all_a', '{}', :now, :now, 1, 0, :now, :now)"
            ),
            {"now": now},
        )

    command.downgrade(config, "20260714_0001")
    inspector = inspect(engine)
    assert radar_tables.isdisjoint(inspector.get_table_names())
    assert "llm_endpoints" in inspector.get_table_names()
    assert "radar_migration_sentinel" in inspector.get_table_names()
    with engine.connect() as connection:
        assert connection.execute(text("SELECT id FROM radar_migration_sentinel")).scalar_one() == 1
    engine.dispose()
