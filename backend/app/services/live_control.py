from __future__ import annotations

import hmac
import secrets
import time
from dataclasses import dataclass
from typing import Callable

from app.core.config import settings


@dataclass(frozen=True)
class LiveControlSession:
    token: str
    account_mask: str
    expires_at: float


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
