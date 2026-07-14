from __future__ import annotations

import getpass
import json
import os
import re
import stat
import subprocess
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import case, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import SessionTransactionOrigin

from app.core.config import settings
from app.db.models.llm_endpoint import LlmEndpoint

_COOLDOWN_FAILURES = 3
_COOLDOWN_SECONDS = 60
_MAX_ERROR_LENGTH = 1000
_KEY_READ_ATTEMPTS = 10
_KEY_READ_RETRY_SECONDS = 0.01
_SENSITIVE_ERROR_KEY = re.compile(r"api[_ -]?key|token|password|authorization|secret", re.IGNORECASE)


class LlmEndpointService:
    _hardened_key_paths: dict[Path, tuple[int, int, int, int]] = {}
    _key_permissions_lock = threading.Lock()

    def __init__(self, session: AsyncSession, *, data_dir: Path | None = None):
        self.session = session
        self._data_dir = data_dir
        self._immediate_write_transaction: Any | None = None

    async def list(self, enabled_only: bool = False) -> list[LlmEndpoint]:
        statement = select(LlmEndpoint).order_by(LlmEndpoint.priority, LlmEndpoint.id)
        if enabled_only:
            statement = statement.where(LlmEndpoint.enabled.is_(True))
        result = await self.session.scalars(statement.execution_options(populate_existing=True))
        return list(result)

    async def create(
        self,
        *,
        name: str,
        api_base: str,
        api_key: str,
        model: str,
        priority: int | None = None,
        enabled: bool = True,
    ) -> LlmEndpoint:
        await self._begin_write()
        name, api_base, model = self._validate_fields(name=name, api_base=api_base, model=model)
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("api_key is required")

        endpoints = await self.list()
        insertion_index = len(endpoints) if priority is None else self._validate_priority(priority, len(endpoints))
        endpoint = LlmEndpoint(
            name=name,
            api_base=api_base,
            api_key_encrypted=self._encrypt(api_key),
            api_key_hint=self._key_hint(api_key),
            model=model,
            priority=insertion_index,
            enabled=bool(enabled),
        )
        endpoints.insert(insertion_index, endpoint)
        self.session.add(endpoint)
        await self._reindex(endpoints)
        return endpoint

    async def update(
        self,
        endpoint_id: str,
        *,
        name: str | None = None,
        api_base: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        priority: int | None = None,
        enabled: bool | None = None,
    ) -> LlmEndpoint:
        await self._begin_write()
        endpoint = await self._get_fresh(endpoint_id)
        next_name = endpoint.name if name is None else name
        next_api_base = endpoint.api_base if api_base is None else api_base
        next_model = endpoint.model if model is None else model
        validated_name, validated_api_base, validated_model = self._validate_fields(
            name=next_name, api_base=next_api_base, model=next_model
        )
        if (
            self._normalized_destination(validated_api_base)
            != self._normalized_destination(endpoint.api_base)
            and not str(api_key or "").strip()
        ):
            raise ValueError("A nonblank replacement api_key is required when api_base destination changes")
        endpoint.name = validated_name
        endpoint.api_base = validated_api_base
        endpoint.model = validated_model
        if api_key is not None and api_key.strip():
            endpoint.api_key_encrypted = self._encrypt(api_key)
            endpoint.api_key_hint = self._key_hint(api_key)
        if enabled is not None:
            endpoint.enabled = bool(enabled)
        if priority is not None:
            endpoints = await self.list()
            endpoints.remove(endpoint)
            endpoints.insert(self._validate_priority(priority, len(endpoints)), endpoint)
            await self._reindex(endpoints)
        await self.session.flush()
        return endpoint

    async def delete(self, endpoint_id: str) -> None:
        await self._begin_write()
        endpoint = await self._get(endpoint_id)
        endpoints = await self.list()
        endpoints.remove(endpoint)
        await self.session.delete(endpoint)
        await self._reindex(endpoints)

    async def reorder(self, endpoint_ids: list[str]) -> list[LlmEndpoint]:
        await self._begin_write()
        endpoints = await self.list()
        existing_ids = {endpoint.id for endpoint in endpoints}
        requested_ids = set(endpoint_ids)
        if len(endpoint_ids) != len(existing_ids) or requested_ids != existing_ids:
            raise ValueError("endpoint_ids must contain every endpoint exactly once")
        endpoint_by_id = {endpoint.id: endpoint for endpoint in endpoints}
        ordered = [endpoint_by_id[endpoint_id] for endpoint_id in endpoint_ids]
        await self._reindex(ordered)
        return ordered

    async def decrypt_api_key(self, endpoint_id: str) -> str:
        endpoint = await self._get(endpoint_id)
        return self._decrypt_endpoint_key(endpoint)

    def _decrypt_endpoint_key(self, endpoint: LlmEndpoint) -> str:
        try:
            return self._fernet(create=False).decrypt(endpoint.api_key_encrypted.encode("utf-8")).decode("utf-8")
        except (InvalidToken, UnicodeDecodeError) as exc:
            raise ValueError("LLM endpoint API key cannot be decrypted") from exc

    async def mark_success(self, endpoint_id: str) -> LlmEndpoint:
        endpoint = await self._get(endpoint_id)
        success_started_at = datetime.now()
        await self.session.execute(
            update(LlmEndpoint)
            .where(
                LlmEndpoint.id == endpoint_id,
                or_(
                    LlmEndpoint.last_failure_at.is_(None),
                    LlmEndpoint.last_failure_at < success_started_at,
                ),
            )
            .values(
                consecutive_failures=0,
                cooldown_until=None,
                last_error=None,
                last_success_at=success_started_at,
            )
            .execution_options(synchronize_session=False)
        )
        await self.session.refresh(endpoint)
        return endpoint

    async def mark_failure(self, endpoint_id: str, error: object) -> LlmEndpoint:
        endpoint = await self._get(endpoint_id)
        try:
            secret = self._decrypt_endpoint_key(endpoint)
        except ValueError:
            secret = None
        now = datetime.now()
        next_failures = LlmEndpoint.consecutive_failures + 1
        await self.session.execute(
            update(LlmEndpoint)
            .where(LlmEndpoint.id == endpoint_id)
            .values(
                consecutive_failures=next_failures,
                cooldown_until=case(
                    (next_failures >= _COOLDOWN_FAILURES, now + timedelta(seconds=_COOLDOWN_SECONDS)),
                    else_=LlmEndpoint.cooldown_until,
                ),
                last_failure_at=now,
                last_error=self._sanitize_error(error, secret=secret),
            )
            .execution_options(synchronize_session=False)
        )
        await self.session.refresh(endpoint)
        return endpoint

    @staticmethod
    def serialize(endpoint: LlmEndpoint) -> dict[str, Any]:
        return {
            "id": endpoint.id,
            "name": endpoint.name,
            "api_base": endpoint.api_base,
            "api_key_hint": endpoint.api_key_hint,
            "model": endpoint.model,
            "priority": endpoint.priority,
            "enabled": endpoint.enabled,
            "consecutive_failures": endpoint.consecutive_failures,
            "cooldown_until": endpoint.cooldown_until,
            "last_success_at": endpoint.last_success_at,
            "last_failure_at": endpoint.last_failure_at,
            "last_error": endpoint.last_error,
            "created_at": endpoint.created_at,
            "updated_at": endpoint.updated_at,
        }

    async def _get(self, endpoint_id: str) -> LlmEndpoint:
        endpoint = await self.session.get(LlmEndpoint, endpoint_id)
        if endpoint is None:
            raise ValueError(f"LLM endpoint {endpoint_id} not found")
        return endpoint

    async def _get_fresh(self, endpoint_id: str) -> LlmEndpoint:
        endpoint = await self.session.scalar(
            select(LlmEndpoint)
            .where(LlmEndpoint.id == endpoint_id)
            .execution_options(populate_existing=True)
        )
        if endpoint is None:
            raise ValueError(f"LLM endpoint {endpoint_id} not found")
        return endpoint

    async def _begin_write(self) -> None:
        bind = self.session.get_bind()
        if bind.dialect.name != "sqlite":
            return
        transaction = self.session.get_transaction()
        if (
            transaction is not None
            and transaction.sync_transaction is self._immediate_write_transaction
        ):
            return
        self._immediate_write_transaction = None
        if transaction is not None:
            if self.session.new or self.session.dirty or self.session.deleted:
                raise ValueError("Cannot start endpoint write with pending changes in the caller transaction")
            if transaction.sync_transaction.origin is not SessionTransactionOrigin.AUTOBEGIN:
                raise RuntimeError("Cannot start endpoint write inside a caller-owned transaction")
            connection = await self.session.connection()
            sqlite_transaction_started = await connection.run_sync(
                lambda sync_connection: bool(
                    sync_connection.connection.driver_connection.in_transaction
                )
            )
            if sqlite_transaction_started:
                raise ValueError("Cannot start endpoint write inside a caller transaction")
            await self.session.rollback()
        await self.session.execute(text("BEGIN IMMEDIATE"))
        transaction = self.session.get_transaction()
        if transaction is None:
            raise RuntimeError("Unable to start endpoint write transaction")
        self._immediate_write_transaction = transaction.sync_transaction

    def _encrypt(self, api_key: str) -> str:
        return self._fernet(create=True).encrypt(api_key.encode("utf-8")).decode("utf-8")

    def _fernet(self, *, create: bool) -> Fernet:
        key_path = self._key_path
        created_key = False
        if not key_path.exists():
            if not create:
                raise ValueError("LLM endpoint encryption key is missing")
            key_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self._create_key_file(key_path, Fernet.generate_key())
                created_key = True
            except FileExistsError:
                pass
        self._ensure_key_file_permissions(key_path, force=created_key)
        return self._read_fernet_key(key_path)

    @classmethod
    def _ensure_key_file_permissions(cls, key_path: Path, *, force: bool) -> None:
        resolved_path = key_path.resolve()
        with cls._key_permissions_lock:
            file_stat = key_path.stat()
            identity = (file_stat.st_dev, file_stat.st_ino, file_stat.st_ctime_ns, file_stat.st_size)
            if force or cls._hardened_key_paths.get(resolved_path) != identity:
                cls._harden_key_file_permissions(key_path)
                hardened_stat = key_path.stat()
                cls._hardened_key_paths[resolved_path] = (
                    hardened_stat.st_dev,
                    hardened_stat.st_ino,
                    hardened_stat.st_ctime_ns,
                    hardened_stat.st_size,
                )

    @staticmethod
    def _create_key_file(key_path: Path, key: bytes) -> None:
        descriptor = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            remaining = memoryview(key)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError("Unable to write LLM endpoint encryption key")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _read_fernet_key(key_path: Path) -> Fernet:
        last_error: OSError | ValueError | None = None
        for attempt in range(_KEY_READ_ATTEMPTS):
            try:
                return Fernet(key_path.read_bytes().strip())
            except (OSError, ValueError) as exc:
                last_error = exc
                if attempt < _KEY_READ_ATTEMPTS - 1:
                    time.sleep(_KEY_READ_RETRY_SECONDS)
        raise ValueError("LLM endpoint encryption key is invalid") from last_error

    @staticmethod
    def _harden_key_file_permissions(
        key_path: Path,
        *,
        platform: str | None = None,
        run_command: Callable[..., Any] | None = None,
        current_user: Callable[[], str] | None = None,
    ) -> None:
        target_platform = platform or os.name
        if target_platform == "posix":
            try:
                os.chmod(key_path, 0o600)
                if stat.S_IMODE(key_path.stat().st_mode) != 0o600:
                    raise OSError("LLM endpoint encryption key permissions are not 0600")
            except OSError as exc:
                raise ValueError("Unable to harden LLM endpoint encryption key permissions") from exc
            return
        if target_platform == "nt":
            runner = run_command or subprocess.run
            try:
                user = (current_user or getpass.getuser)()
                runner(
                    ["icacls", str(key_path), "/inheritance:r"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                runner(
                    ["icacls", str(key_path), "/grant:r", f"{user}:(R,W)"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except (KeyError, OSError, subprocess.CalledProcessError) as exc:
                raise ValueError("Unable to harden LLM endpoint encryption key permissions") from exc
            return
        raise ValueError("Unsupported platform for LLM endpoint encryption key permissions")

    @property
    def _key_path(self) -> Path:
        data_dir = self._data_dir or Path(settings.gaoshou_data_dir)
        return data_dir / ".secrets" / "llm-config.key"

    @staticmethod
    def _validate_fields(*, name: str, api_base: str, model: str) -> tuple[str, str, str]:
        if not isinstance(name, str) or not (normalized_name := name.strip()):
            raise ValueError("name is required")
        if not isinstance(model, str) or not (normalized_model := model.strip()):
            raise ValueError("model is required")
        if not isinstance(api_base, str) or not (normalized_url := api_base.strip()):
            raise ValueError("api_base is required")
        parsed = urlparse(normalized_url)
        try:
            _parsed_port = parsed.port
        except ValueError:
            raise ValueError("api_base must be an http or https URL with a host and valid port") from None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("api_base must be an http or https URL with a host")
        return normalized_name, normalized_url, normalized_model

    @staticmethod
    def _normalized_destination(api_base: str) -> tuple[str, str, int]:
        parsed = urlparse(api_base)
        default_port = 443 if parsed.scheme.lower() == "https" else 80
        return parsed.scheme.lower(), str(parsed.hostname).lower(), parsed.port or default_port

    @staticmethod
    def _validate_priority(priority: int, endpoint_count: int) -> int:
        if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= endpoint_count:
            raise ValueError("priority is out of range")
        return priority

    async def _reindex(self, endpoints: list[LlmEndpoint]) -> None:
        for priority, endpoint in enumerate(endpoints):
            endpoint.priority = -(priority + 1)
        await self.session.flush()
        for priority, endpoint in enumerate(endpoints):
            endpoint.priority = priority
        await self.session.flush()

    @staticmethod
    def _key_hint(api_key: str) -> str:
        if len(api_key) <= 4:
            return "********"
        return f"********{api_key[-4:]}"

    @staticmethod
    def _sanitize_error(error: object, *, secret: str | None = None) -> str:
        message = str(error)
        try:
            parsed = json.loads(message)
        except (TypeError, ValueError):
            parsed = None
        else:
            sanitized = LlmEndpointService._redact_json_error(parsed, secret=secret)
            return json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))[:_MAX_ERROR_LENGTH]
        if secret:
            message = message.replace(secret, "[REDACTED]")
        message = re.sub(
            r"(?i)([\"']?(?:api[_ -]?key|client[_ -]?secret|token|password|authorization|secret)[\"']?\s*:\s*[\"'])[^\"']*",
            r"\1[REDACTED]",
            message,
        )
        message = re.sub(r"\s+", " ", message).strip()
        message = re.sub(
            r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?)[^\s,;]+",
            r"\1[REDACTED]",
            message,
        )
        message = re.sub(
            r"(?i)([\"']?(?:api[_ -]?key|client[_ -]?secret|token|password|authorization|secret)"
            r"[\"']?\s*[:=]\s*)([\"']?)[^\s,;\"']+\2",
            r"\1\2[REDACTED]\2",
            message,
        )
        return message[:_MAX_ERROR_LENGTH]

    @staticmethod
    def _redact_json_error(value: Any, *, secret: str | None) -> Any:
        if isinstance(value, dict):
            return {
                key: "[REDACTED]"
                if isinstance(key, str) and _SENSITIVE_ERROR_KEY.search(key)
                else LlmEndpointService._redact_json_error(item, secret=secret)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [LlmEndpointService._redact_json_error(item, secret=secret) for item in value]
        if isinstance(value, str) and secret:
            return value.replace(secret, "[REDACTED]")
        return value


LLMEndpointService = LlmEndpointService
