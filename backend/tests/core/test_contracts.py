from app.core.contracts import DatasetReadiness, Environment, JobStatus, ReleaseStatus


def test_platform_contract_enums_have_stable_wire_values():
    assert [item.value for item in Environment] == ["research", "paper", "live"]
    assert [item.value for item in DatasetReadiness] == ["ready", "stale", "missing", "invalid"]
    assert [item.value for item in ReleaseStatus] == [
        "draft",
        "validated",
        "paper_approved",
        "live_approved",
        "retired",
    ]
    assert [item.value for item in JobStatus] == [
        "queued",
        "running",
        "succeeded",
        "failed",
        "cancelled",
    ]
