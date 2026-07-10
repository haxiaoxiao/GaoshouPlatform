from enum import StrEnum


class Environment(StrEnum):
    RESEARCH = "research"
    PAPER = "paper"
    LIVE = "live"


class DatasetReadiness(StrEnum):
    READY = "ready"
    STALE = "stale"
    MISSING = "missing"
    INVALID = "invalid"


class ReleaseStatus(StrEnum):
    DRAFT = "draft"
    VALIDATED = "validated"
    PAPER_APPROVED = "paper_approved"
    LIVE_APPROVED = "live_approved"
    RETIRED = "retired"


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
