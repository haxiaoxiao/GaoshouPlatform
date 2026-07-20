from __future__ import annotations

import hmac
import secrets
import time
from dataclasses import dataclass
from threading import Lock
from typing import Callable

from app.core.config import settings


@dataclass(frozen=True)
class LiveControlSession:
    token: str
    account_mask: str
    expires_at: float


@dataclass(frozen=True, slots=True)
class LiveSubmissionContext:
    release_id: str
    strategy_id: int
    profile_key: str
    account_mask: str
    idempotency_hash: str
    reservation_id: str


@dataclass(frozen=True, slots=True)
class _LiveSubmissionAuthorization:
    seal: object
    nonce: str


@dataclass(frozen=True, slots=True)
class _PendingLiveSubmission:
    context: LiveSubmissionContext
    control_token: str


@dataclass(frozen=True, slots=True)
class _LiveBrokerPermit:
    seal: object
    context: LiveSubmissionContext
    control_token: str


_LIVE_SUBMIT_PATH = "/api/v1/live/orders/submit"


class LiveControlSessionManager:
    def __init__(
        self,
        *,
        secret: str,
        ttl_seconds: int,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._secret = secret
        self._ttl_seconds = max(60, int(ttl_seconds))
        self._clock = clock
        self._sessions: dict[str, LiveControlSession] = {}
        self._submission_seal = object()
        self._submission_contexts: dict[str, _PendingLiveSubmission] = {}
        self._submission_lock = Lock()

    def unlock(
        self,
        *,
        secret: str,
        expected_account_mask: str,
        actual_account_mask: str,
    ) -> LiveControlSession:
        if not self._secret:
            raise ValueError("LIVE_TRADING_CONTROL_SECRET is not configured")
        if not hmac.compare_digest(str(secret), self._secret):
            raise ValueError("Invalid live control secret")
        self._validate_account(expected_account_mask, actual_account_mask)
        now = self._clock()
        session = LiveControlSession(
            token=secrets.token_urlsafe(32),
            account_mask=actual_account_mask,
            expires_at=now + self._ttl_seconds,
        )
        self._sessions[session.token] = session
        self._remove_expired(now)
        return session

    def validate(
        self,
        *,
        token: str,
        expected_account_mask: str,
        actual_account_mask: str,
    ) -> LiveControlSession:
        session = self._sessions.get(str(token or ""))
        if session is None:
            raise ValueError("Invalid live control session")
        now = self._clock()
        if now > session.expires_at:
            self._sessions.pop(session.token, None)
            raise ValueError("Live control session expired")
        self._validate_account(expected_account_mask, actual_account_mask)
        if not hmac.compare_digest(session.account_mask, actual_account_mask):
            raise ValueError("Live control session account changed")
        return session

    def issue_submission_authorization(
        self,
        *,
        control_session: LiveControlSession,
        release_id: str,
        strategy_id: int,
        profile_key: str,
        account_mask: str,
        idempotency_hash: str,
        reservation_id: str,
    ) -> object:
        current = self._sessions.get(control_session.token)
        now = self._clock()
        if current is not control_session or now > control_session.expires_at:
            raise PermissionError("Live control session is not active")
        if not hmac.compare_digest(control_session.account_mask, str(account_mask or "")):
            raise PermissionError("Live submission account does not match control session")
        normalized_profile = str(profile_key or "").strip()
        normalized_release = str(release_id or "").strip()
        normalized_hash = str(idempotency_hash or "").strip()
        normalized_reservation = str(reservation_id or "").strip()
        if (
            not normalized_release
            or int(strategy_id) <= 0
            or not normalized_profile
            or not normalized_hash
            or normalized_reservation != f"live-submit:{normalized_hash}"
        ):
            raise ValueError("Validated live submission context is incomplete")
        context = LiveSubmissionContext(
            release_id=normalized_release,
            strategy_id=int(strategy_id),
            profile_key=normalized_profile,
            account_mask=control_session.account_mask,
            idempotency_hash=normalized_hash,
            reservation_id=normalized_reservation,
        )
        nonce = secrets.token_urlsafe(32)
        with self._submission_lock:
            self._submission_contexts[nonce] = _PendingLiveSubmission(
                context=context,
                control_token=control_session.token,
            )
        return _LiveSubmissionAuthorization(seal=self._submission_seal, nonce=nonce)

    def consume_submission_authorization(
        self,
        authorization: object | None,
    ) -> object:
        if (
            not isinstance(authorization, _LiveSubmissionAuthorization)
            or authorization.seal is not self._submission_seal
        ):
            raise PermissionError(f"Live order submission is authorized only through {_LIVE_SUBMIT_PATH}")
        with self._submission_lock:
            pending = self._submission_contexts.pop(authorization.nonce, None)
        if pending is None:
            raise PermissionError("Live submission authorization was already consumed or is invalid")
        control_session = self._sessions.get(pending.control_token)
        if control_session is None or self._clock() > control_session.expires_at:
            raise PermissionError("Live control session expired before order submission")
        if not hmac.compare_digest(control_session.account_mask, pending.context.account_mask):
            raise PermissionError("Live control session account changed before order submission")
        return _LiveBrokerPermit(
            seal=self._submission_seal,
            context=pending.context,
            control_token=pending.control_token,
        )

    def validate_broker_permit(self, permit: object | None) -> LiveSubmissionContext:
        if not isinstance(permit, _LiveBrokerPermit) or permit.seal is not self._submission_seal:
            raise PermissionError(f"Live broker access is authorized only through {_LIVE_SUBMIT_PATH}")
        control_session = self._sessions.get(permit.control_token)
        if control_session is None or self._clock() > control_session.expires_at:
            raise PermissionError("Live control session expired before broker access")
        if not hmac.compare_digest(control_session.account_mask, permit.context.account_mask):
            raise PermissionError("Live control session account changed before broker access")
        return permit.context

    def _remove_expired(self, now: float) -> None:
        expired = [token for token, session in self._sessions.items() if now > session.expires_at]
        for token in expired:
            self._sessions.pop(token, None)

    @staticmethod
    def _validate_account(expected: str, actual: str) -> None:
        if not expected or not actual or not hmac.compare_digest(str(expected), str(actual)):
            raise ValueError("Live account mask does not match")


live_control_sessions = LiveControlSessionManager(
    secret=settings.live_trading_control_secret,
    ttl_seconds=settings.live_trading_control_session_ttl_seconds,
)
