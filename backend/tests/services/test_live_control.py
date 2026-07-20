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


def test_live_submission_context_is_fully_bound_and_consumed_once():
    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60)
    control = manager.unlock(
        secret="control-secret",
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )

    authorization = manager.issue_submission_authorization(
        control_session=control,
        release_id="release-1",
        strategy_id=43,
        profile_key="stable-profile",
        account_mask="66***80",
        idempotency_hash="idempotency-hash",
        reservation_id="live-submit:idempotency-hash",
    )
    permit = manager.consume_submission_authorization(authorization)
    context = manager.validate_broker_permit(permit)

    assert context.release_id == "release-1"
    assert context.strategy_id == 43
    assert context.profile_key == "stable-profile"
    assert context.account_mask == "66***80"
    assert context.idempotency_hash == "idempotency-hash"
    assert context.reservation_id == "live-submit:idempotency-hash"
    with pytest.raises(PermissionError, match="already consumed"):
        manager.consume_submission_authorization(authorization)
    with pytest.raises(PermissionError, match="only through"):
        manager.validate_broker_permit(context)
