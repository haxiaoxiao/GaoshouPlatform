import pytest

from app.services.live_control import LiveControlSessionManager


def test_control_session_requires_secret_account_match_and_expiry():
    now = [100.0]
    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60, clock=lambda: now[0])

    with pytest.raises(ValueError, match="secret"):
        manager.unlock(secret="wrong", expected_account_mask="66***80", actual_account_mask="66***80")
    with pytest.raises(ValueError, match="account"):
        manager.unlock(
            secret="control-secret",
            expected_account_mask="11***22",
            actual_account_mask="66***80",
        )

    session = manager.unlock(
        secret="control-secret",
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )
    manager.validate(
        token=session.token,
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )

    now[0] = 161.0
    with pytest.raises(ValueError, match="expired"):
        manager.validate(
            token=session.token,
            expected_account_mask="66***80",
            actual_account_mask="66***80",
        )


def test_control_session_is_unavailable_without_configured_secret():
    manager = LiveControlSessionManager(secret="", ttl_seconds=60)

    with pytest.raises(ValueError, match="not configured"):
        manager.unlock(secret="anything", expected_account_mask="66***80", actual_account_mask="66***80")
