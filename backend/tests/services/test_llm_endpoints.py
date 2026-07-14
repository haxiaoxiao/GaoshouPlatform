from __future__ import annotations

import asyncio
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models.base import Base
from app.db.models.llm_endpoint import LlmEndpoint
from app.services.llm_endpoints import LlmEndpointService


@pytest.fixture
async def endpoint_service(tmp_path: Path):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        yield LlmEndpointService(session, data_dir=tmp_path), session, tmp_path
    await engine.dispose()


async def _create(
    service: LlmEndpointService,
    *,
    name: str,
    priority: int | None = None,
    enabled: bool = True,
):
    return await service.create(
        name=name,
        api_base="https://llm.example.test/v1",
        api_key="super-secret-1234",
        model="provider/model",
        priority=priority,
        enabled=enabled,
    )


@pytest.mark.asyncio
async def test_create_encrypts_key_and_serialization_masks_it(endpoint_service):
    service, session, data_dir = endpoint_service

    endpoint = await _create(service, name="Primary")
    await session.commit()
    stored = await session.get(LlmEndpoint, endpoint.id)

    assert stored is not None
    assert stored.api_key_encrypted != "super-secret-1234"
    assert "super-secret-1234" not in stored.api_key_encrypted
    assert (data_dir / ".secrets" / "llm-config.key").is_file()
    assert service.serialize(stored)["api_key_hint"] == "********1234"
    assert "api_key_encrypted" not in service.serialize(stored)
    assert await service.decrypt_api_key(endpoint.id) == "super-secret-1234"


@pytest.mark.asyncio
async def test_list_enabled_only_filters_disabled_endpoints_in_the_database(endpoint_service):
    service, _, _ = endpoint_service
    enabled = await _create(service, name="Enabled", enabled=True)
    await _create(service, name="Disabled", enabled=False)

    assert [endpoint.id for endpoint in await service.list(enabled_only=True)] == [enabled.id]


@pytest.mark.asyncio
async def test_first_key_creation_recovers_from_an_exclusive_create_collision(endpoint_service, monkeypatch):
    service, session, data_dir = endpoint_service
    competing_service = LlmEndpointService(session, data_dir=data_dir)
    winner_key = Fernet.generate_key()
    collision_calls = 0

    def create_winning_key(key_path: Path, generated_key: bytes) -> None:
        nonlocal collision_calls
        collision_calls += 1
        assert generated_key != winner_key
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(winner_key)
        raise FileExistsError

    monkeypatch.setattr(service, "_create_key_file", create_winning_key, raising=False)

    first_fernet = service._fernet(create=True)
    second_fernet = competing_service._fernet(create=True)

    assert collision_calls == 1
    assert second_fernet.decrypt(first_fernet.encrypt(b"endpoint-key")) == b"endpoint-key"


@pytest.mark.asyncio
async def test_existing_key_permissions_are_hardened_once_across_repeated_decrypts(
    endpoint_service, monkeypatch
):
    service, session, data_dir = endpoint_service
    key_path = data_dir / ".secrets" / "llm-config.key"
    key_path.parent.mkdir(parents=True)
    key = Fernet.generate_key()
    key_path.write_bytes(key)
    endpoint = LlmEndpoint(
        name="Primary",
        api_base="https://llm.example.test/v1",
        api_key_encrypted=Fernet(key).encrypt(b"endpoint-key").decode("utf-8"),
        api_key_hint="********-key",
        model="provider/model",
        priority=0,
    )
    session.add(endpoint)
    await session.flush()
    calls: list[Path] = []

    def record_hardening(path: Path) -> None:
        calls.append(path)

    LlmEndpointService._hardened_key_paths.clear()
    monkeypatch.setattr(
        LlmEndpointService,
        "_harden_key_file_permissions",
        staticmethod(record_hardening),
    )

    assert await service.decrypt_api_key(endpoint.id) == "endpoint-key"
    assert await service.decrypt_api_key(endpoint.id) == "endpoint-key"
    assert calls == [key_path]


def test_key_permissions_are_hardened_again_when_file_is_replaced_at_same_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    key_path = tmp_path / "llm-config.key"
    key_path.write_bytes(Fernet.generate_key())
    calls: list[Path] = []

    def record_hardening(path: Path) -> None:
        calls.append(path)

    LlmEndpointService._hardened_key_paths.clear()
    monkeypatch.setattr(
        LlmEndpointService,
        "_harden_key_file_permissions",
        staticmethod(record_hardening),
    )

    LlmEndpointService._ensure_key_file_permissions(key_path, force=False)
    key_path.unlink()
    key_path.write_bytes(Fernet.generate_key())
    LlmEndpointService._ensure_key_file_permissions(key_path, force=False)

    assert calls == [key_path, key_path]


def test_windows_key_hardening_uses_argument_lists_without_touching_real_acls(tmp_path: Path):
    key_path = tmp_path / "llm-config.key"
    key_path.write_bytes(Fernet.generate_key())
    calls: list[tuple[list[str], dict]] = []

    def fake_run(command: list[str], **kwargs) -> None:
        calls.append((command, kwargs))

    LlmEndpointService._harden_key_file_permissions(
        key_path,
        platform="nt",
        run_command=fake_run,
        current_user=lambda: "test-user",
    )

    assert [command for command, _ in calls] == [
        ["icacls", str(key_path), "/inheritance:r"],
        ["icacls", str(key_path), "/grant:r", "test-user:(R,W)"],
    ]
    assert all(kwargs == {"check": True, "capture_output": True, "text": True} for _, kwargs in calls)


def test_windows_key_hardening_fails_closed_when_icacls_fails(tmp_path: Path):
    key_path = tmp_path / "llm-config.key"
    key_path.write_bytes(Fernet.generate_key())

    def failing_run(command: list[str], **kwargs) -> None:
        raise OSError(f"Unable to run {command[0]}")

    with pytest.raises(ValueError, match="Unable to harden"):
        LlmEndpointService._harden_key_file_permissions(
            key_path,
            platform="nt",
            run_command=failing_run,
            current_user=lambda: "test-user",
        )


@pytest.mark.asyncio
async def test_short_api_key_hint_does_not_expose_the_complete_key(endpoint_service):
    service, _, _ = endpoint_service

    endpoint = await service.create(
        name="Short key",
        api_base="https://llm.example.test/v1",
        api_key="abcd",
        model="provider/model",
    )

    serialized = service.serialize(endpoint)
    assert endpoint.api_key_hint == "********"
    assert "abcd" not in endpoint.api_key_hint
    assert "abcd" not in str(serialized)


@pytest.mark.asyncio
async def test_update_preserves_blank_key_and_replaces_nonblank_key(endpoint_service):
    service, session, _ = endpoint_service
    endpoint = await _create(service, name="Primary")
    original_ciphertext = endpoint.api_key_encrypted

    preserved = await service.update(endpoint.id, api_key="")
    assert preserved.api_key_encrypted == original_ciphertext

    omitted = await service.update(endpoint.id, name="Primary renamed")
    assert omitted.api_key_encrypted == original_ciphertext

    replaced = await service.update(endpoint.id, api_key="replacement-9876")
    await session.commit()
    assert replaced.api_key_encrypted != original_ciphertext
    assert replaced.api_key_hint == "********9876"
    assert await service.decrypt_api_key(endpoint.id) == "replacement-9876"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"name": "", "api_base": "https://llm.example.test", "model": "a"}, "name"),
        ({"name": "a", "api_base": "ftp://llm.example.test", "model": "a"}, "api_base"),
        ({"name": "a", "api_base": "https:///v1", "model": "a"}, "api_base"),
        ({"name": "a", "api_base": "https://user:pass@llm.example.test", "model": "a"}, "api_base"),
        ({"name": "a", "api_base": "https://llm.example.test?v=1", "model": "a"}, "api_base"),
        ({"name": "a", "api_base": "https://llm.example.test#fragment", "model": "a"}, "api_base"),
        ({"name": "a", "api_base": "https://llm.example.test", "model": ""}, "model"),
    ],
)
async def test_create_rejects_invalid_required_values(endpoint_service, kwargs, message):
    service, _, _ = endpoint_service

    with pytest.raises(ValueError, match=message):
        await service.create(api_key="secret", **kwargs)


@pytest.mark.asyncio
async def test_reorder_requires_exact_endpoint_set_and_keeps_priorities_contiguous(endpoint_service):
    service, _, _ = endpoint_service
    first = await _create(service, name="First")
    second = await _create(service, name="Second")
    third = await _create(service, name="Third")

    with pytest.raises(ValueError, match="exactly"):
        await service.reorder([first.id, second.id])

    reordered = await service.reorder([third.id, first.id, second.id])
    assert [endpoint.id for endpoint in reordered] == [third.id, first.id, second.id]
    assert [endpoint.priority for endpoint in reordered] == [0, 1, 2]


@pytest.mark.asyncio
async def test_delete_reindexes_remaining_endpoints(endpoint_service):
    service, _, _ = endpoint_service
    first = await _create(service, name="First")
    second = await _create(service, name="Second")
    third = await _create(service, name="Third")

    await service.delete(second.id)
    listed = await service.list()

    assert [endpoint.id for endpoint in listed] == [first.id, third.id]
    assert [endpoint.priority for endpoint in listed] == [0, 1]


@pytest.mark.asyncio
async def test_failure_cooldown_and_success_reset_health(endpoint_service):
    service, _, _ = endpoint_service
    endpoint = await _create(service, name="Primary")

    for _ in range(2):
        endpoint = await service.mark_failure(endpoint.id, "request\nfailed")
        assert endpoint.cooldown_until is None
    endpoint = await service.mark_failure(endpoint.id, "x" * 2000)
    assert endpoint.consecutive_failures == 3
    assert endpoint.cooldown_until is not None
    assert endpoint.cooldown_until > datetime.now() + timedelta(seconds=55)
    assert len(endpoint.last_error) == 1000
    assert "\n" not in endpoint.last_error

    recovered = await service.mark_success(endpoint.id)
    assert recovered.consecutive_failures == 0
    assert recovered.cooldown_until is None
    assert recovered.last_error is None
    assert recovered.last_success_at is not None


@pytest.mark.asyncio
async def test_success_does_not_clear_a_failure_recorded_after_success_started(endpoint_service):
    service, session, _ = endpoint_service
    endpoint = await _create(service, name="Primary")
    endpoint.consecutive_failures = 3
    endpoint.cooldown_until = datetime.now() + timedelta(seconds=60)
    endpoint.last_error = "later failure"
    endpoint.last_failure_at = datetime.now() + timedelta(seconds=1)
    await session.flush()

    result = await service.mark_success(endpoint.id)

    assert result.consecutive_failures == 3
    assert result.cooldown_until is not None
    assert result.last_error == "later failure"
    assert result.last_success_at is None


@pytest.mark.asyncio
async def test_failure_error_never_persists_the_endpoint_plaintext_api_key(endpoint_service):
    service, _, data_dir = endpoint_service
    api_key = "raw-secret-4321"
    endpoint = await service.create(
        name="Primary",
        api_base="https://llm.example.test/v1",
        api_key=api_key,
        model="provider/model",
    )

    failed = await service.mark_failure(endpoint.id, f"provider rejected {api_key} during request")

    assert api_key not in failed.last_error
    assert api_key not in str(service.serialize(failed))

    (data_dir / ".secrets" / "llm-config.key").unlink()
    recorded = await service.mark_failure(endpoint.id, "provider is unavailable")
    assert recorded.last_error == "provider is unavailable"


@pytest.mark.asyncio
async def test_failure_error_redacts_json_quoted_secret_fields(endpoint_service):
    service, _, _ = endpoint_service
    endpoint = await service.create(
        name="Primary",
        api_base="https://llm.example.test/v1",
        api_key="endpoint-secret-4321",
        model="provider/model",
    )
    embedded_secret = "json-secret-4321"

    failed = await service.mark_failure(endpoint.id, f'{{"api_key": "{embedded_secret}"}}')

    assert embedded_secret not in failed.last_error
    assert embedded_secret not in str(service.serialize(failed))


@pytest.mark.asyncio
async def test_failure_error_structurally_redacts_escaped_json_secret_without_suffix_leakage(endpoint_service):
    service, _, _ = endpoint_service
    endpoint = await service.create(
        name="Primary",
        api_base="https://llm.example.test/v1",
        api_key="endpoint-secret-4321",
        model="provider/model",
    )

    failed = await service.mark_failure(
        endpoint.id,
        '{"nested":{"authorization_secret":"json\\u002dsecret\\u002d4321"}}',
    )

    assert "json-secret-4321" not in failed.last_error
    assert "4321" not in failed.last_error
    assert "[REDACTED]" in failed.last_error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        "client_secret=plain-text-secret",
        'secret: "plain-text-secret"',
        '"api_key"="plain-text-secret"',
    ],
)
async def test_failure_error_redacts_plaintext_secret_fields(endpoint_service, error: str):
    service, _, _ = endpoint_service
    endpoint = await _create(service, name="Primary")

    failed = await service.mark_failure(endpoint.id, error)

    assert "plain-text-secret" not in failed.last_error
    assert "[REDACTED]" in failed.last_error


@pytest.mark.asyncio
async def test_parallel_failures_use_atomic_persisted_increments(tmp_path: Path):
    database_path = tmp_path / "endpoint-concurrency.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path.as_posix()}", connect_args={"timeout": 10}
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        endpoint = await LlmEndpointService(session, data_dir=tmp_path).create(
            name="Primary",
            api_base="https://llm.example.test/v1",
            api_key="concurrency-secret",
            model="provider/model",
        )
        endpoint_id = endpoint.id
        await session.commit()

    async def record_failure(index: int) -> None:
        async with sessions() as session:
            service = LlmEndpointService(session, data_dir=tmp_path)
            await service.mark_failure(endpoint_id, f"failure {index}")
            await session.commit()

    await asyncio.gather(*(record_failure(index) for index in range(3)))

    async with sessions() as session:
        persisted = await session.get(LlmEndpoint, endpoint_id)
        assert persisted is not None
        assert persisted.consecutive_failures == 3
        assert persisted.cooldown_until is not None
    await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_creates_serialize_to_unique_contiguous_priorities(tmp_path: Path):
    database_path = tmp_path / "endpoint-create-concurrency.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path.as_posix()}", connect_args={"timeout": 10}
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def create_endpoint(name: str) -> str:
        async with sessions() as session:
            endpoint = await LlmEndpointService(session, data_dir=tmp_path).create(
                name=name,
                api_base="https://llm.example.test/v1",
                api_key=f"{name}-secret",
                model="provider/model",
            )
            await session.commit()
            return endpoint.id

    endpoint_ids = await asyncio.gather(create_endpoint("First"), create_endpoint("Second"))

    async with sessions() as session:
        endpoints = await LlmEndpointService(session).list()
        assert {endpoint.id for endpoint in endpoints} == set(endpoint_ids)
        assert [endpoint.priority for endpoint in endpoints] == [0, 1]
        assert len({endpoint.priority for endpoint in endpoints}) == 2
    await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_creates_after_prior_reads_serialize_to_contiguous_priorities(tmp_path: Path):
    database_path = tmp_path / "endpoint-prior-read-concurrency.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path.as_posix()}", connect_args={"timeout": 10}
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def list_then_create(name: str) -> str:
        async with sessions() as session:
            service = LlmEndpointService(session, data_dir=tmp_path)
            assert await service.list() == []
            endpoint = await service.create(
                name=name,
                api_base="https://llm.example.test/v1",
                api_key=f"{name}-secret",
                model="provider/model",
            )
            await session.commit()
            return endpoint.id

    endpoint_ids = await asyncio.gather(list_then_create("First"), list_then_create("Second"))

    async with sessions() as session:
        endpoints = await LlmEndpointService(session).list()
        assert {endpoint.id for endpoint in endpoints} == set(endpoint_ids)
        assert [endpoint.priority for endpoint in endpoints] == [0, 1]
    await engine.dispose()


@pytest.mark.asyncio
async def test_create_refuses_to_rollback_pending_caller_changes(endpoint_service):
    service, session, _ = endpoint_service
    await service.list()
    session.add(
        LlmEndpoint(
            name="Caller pending",
            api_base="https://llm.example.test/v1",
            api_key_encrypted="pending-ciphertext",
            api_key_hint="********ding",
            model="provider/model",
            priority=0,
        )
    )

    with pytest.raises(ValueError, match="pending changes"):
        await service.create(
            name="Service endpoint",
            api_base="https://llm.example.test/v1",
            api_key="service-secret",
            model="provider/model",
        )


@pytest.mark.asyncio
async def test_create_refuses_to_rollback_prior_flushed_orm_dml(endpoint_service):
    service, session, _ = endpoint_service
    caller_endpoint = LlmEndpoint(
        name="Caller flushed",
        api_base="https://llm.example.test/v1",
        api_key_encrypted="caller-ciphertext",
        api_key_hint="********ller",
        model="provider/model",
        priority=0,
    )
    session.add(caller_endpoint)
    await session.flush()

    with pytest.raises(ValueError, match="caller transaction"):
        await _create(service, name="Service endpoint")

    assert session.in_transaction()
    assert await session.get(LlmEndpoint, caller_endpoint.id) is caller_endpoint


@pytest.mark.asyncio
async def test_create_refuses_to_rollback_prior_core_dml(endpoint_service):
    service, session, _ = endpoint_service
    caller_id = "caller-core-dml"
    await session.execute(
        text(
            "INSERT INTO llm_endpoints "
            "(id, name, api_base, api_key_encrypted, api_key_hint, model, priority, enabled, "
            "consecutive_failures, created_at, updated_at) "
            "VALUES (:id, 'Caller Core', 'https://llm.example.test/v1', 'ciphertext', "
            "'********core', 'provider/model', 0, 1, 0, :now, :now)"
        ),
        {"id": caller_id, "now": datetime.now()},
    )

    with pytest.raises(ValueError, match="caller transaction"):
        await _create(service, name="Service endpoint")

    assert session.in_transaction()
    assert await session.scalar(
        text("SELECT COUNT(*) FROM llm_endpoints WHERE id = :id"), {"id": caller_id}
    ) == 1


@pytest.mark.asyncio
async def test_reused_service_reacquires_immediate_transaction_after_commit_and_read(
    endpoint_service, monkeypatch: pytest.MonkeyPatch
):
    service, session, _ = endpoint_service
    await _create(service, name="First")
    await session.commit()
    await service.list()

    executed_statements: list[str] = []
    original_execute = session.execute

    async def recording_execute(statement, *args, **kwargs):
        executed_statements.append(str(statement))
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(session, "execute", recording_execute)

    await _create(service, name="Second")

    assert executed_statements
    assert executed_statements[0] == "BEGIN IMMEDIATE"


@pytest.mark.asyncio
async def test_create_preserves_explicit_caller_transaction_context(endpoint_service):
    service, session, _ = endpoint_service

    async with session.begin():
        with pytest.raises(RuntimeError, match="caller-owned transaction"):
            await service.create(
                name="Service endpoint",
                api_base="https://llm.example.test/v1",
                api_key="service-secret",
                model="provider/model",
            )

        assert list((await session.scalars(select(LlmEndpoint))).all()) == []


@pytest.mark.asyncio
async def test_stale_admin_reorder_keeps_committed_priorities_unique_and_contiguous(tmp_path: Path):
    database_path = tmp_path / "endpoint-priorities.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path.as_posix()}", connect_args={"timeout": 10}
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        service = LlmEndpointService(session, data_dir=tmp_path)
        first = await _create(service, name="First")
        second = await _create(service, name="Second")
        third = await _create(service, name="Third")
        endpoint_ids = [first.id, second.id, third.id]
        await session.commit()

    async with sessions() as first_session, sessions() as stale_session:
        first_service = LlmEndpointService(first_session, data_dir=tmp_path)
        stale_service = LlmEndpointService(stale_session, data_dir=tmp_path)
        await stale_service.list()
        await stale_session.commit()

        await first_service.reorder(list(reversed(endpoint_ids)))
        await first_session.commit()
        await stale_service.reorder([endpoint_ids[1], endpoint_ids[0], endpoint_ids[2]])
        await stale_session.commit()

    async with sessions() as session:
        priorities = [endpoint.priority for endpoint in await LlmEndpointService(session).list()]
        assert priorities == [0, 1, 2]
        assert len(set(priorities)) == 3
    await engine.dispose()


def test_migration_creates_unique_priority_index(tmp_path: Path):
    migration_path = Path(__file__).parents[2] / "migrations" / "versions" / "20260713_0001_llm_endpoints.py"
    spec = importlib.util.spec_from_file_location("llm_endpoint_migration", migration_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    engine = create_engine(f"sqlite:///{(tmp_path / 'migration.db').as_posix()}")
    with engine.begin() as connection:
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()
        priority_index = next(
            index
            for index in inspect(connection).get_indexes("llm_endpoints")
            if index["name"] == "ux_llm_endpoints_priority"
        )
    engine.dispose()

    assert bool(priority_index["unique"]) is True


def test_migration_reindexes_legacy_duplicate_priorities_before_adding_unique_index(tmp_path: Path):
    migration_path = Path(__file__).parents[2] / "migrations" / "versions" / "20260713_0001_llm_endpoints.py"
    spec = importlib.util.spec_from_file_location("llm_endpoint_migration_legacy", migration_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    engine = create_engine(f"sqlite:///{(tmp_path / 'legacy-migration.db').as_posix()}")
    with engine.begin() as connection:
        Base.metadata.create_all(connection)
        connection.exec_driver_sql("DROP INDEX ux_llm_endpoints_priority")
        connection.execute(
            text(
                "INSERT INTO llm_endpoints "
                "(id, name, api_base, api_key_encrypted, api_key_hint, model, priority, enabled, "
                "consecutive_failures, created_at, updated_at) "
                "VALUES (:id, :name, :api_base, :encrypted, :hint, :model, 0, 1, 0, :created, :updated)"
            ),
            [
                {
                    "id": "legacy-1",
                    "name": "Legacy 1",
                    "api_base": "https://llm.example.test/v1",
                    "encrypted": "ciphertext-1",
                    "hint": "********1234",
                    "model": "provider/model",
                    "created": datetime.now().isoformat(),
                    "updated": datetime.now().isoformat(),
                },
                {
                    "id": "legacy-2",
                    "name": "Legacy 2",
                    "api_base": "https://llm.example.test/v1",
                    "encrypted": "ciphertext-2",
                    "hint": "********5678",
                    "model": "provider/model",
                    "created": datetime.now().isoformat(),
                    "updated": datetime.now().isoformat(),
                },
            ],
        )
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()
        priorities = connection.execute(
            text("SELECT priority FROM llm_endpoints ORDER BY priority, id")
        ).scalars().all()
    engine.dispose()

    assert priorities == [0, 1]


@pytest.mark.asyncio
async def test_decrypt_rejects_missing_or_corrupt_key_material(endpoint_service):
    service, _, data_dir = endpoint_service
    endpoint = await _create(service, name="Primary")
    key_path = data_dir / ".secrets" / "llm-config.key"
    key_path.unlink()

    with pytest.raises(ValueError, match="key"):
        await service.decrypt_api_key(endpoint.id)

    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_text("not-a-fernet-key", encoding="ascii")
    with pytest.raises(ValueError, match="key"):
        await service.decrypt_api_key(endpoint.id)


@pytest.mark.asyncio
async def test_create_inserts_at_requested_priority_and_reindexes(endpoint_service):
    service, _, _ = endpoint_service
    first = await _create(service, name="First")
    third = await _create(service, name="Third")
    second = await _create(service, name="Second", priority=1)

    listed = await service.list()
    assert [endpoint.id for endpoint in listed] == [first.id, second.id, third.id]
    assert [endpoint.priority for endpoint in listed] == [0, 1, 2]
