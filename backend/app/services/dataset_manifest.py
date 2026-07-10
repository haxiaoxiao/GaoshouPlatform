"""Small, durable metadata files for Parquet dataset readiness checks."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import duckdb

DatasetReadinessStatus = Literal["ready", "stale", "missing", "invalid"]


@dataclass(frozen=True)
class DatasetManifest:
    dataset: str
    generated_at: datetime
    file_count: int
    byte_size: int
    row_count: int
    partition_count: int
    min_date: str | None
    max_date: str | None
    schema_hash: str
    schema_version: int = 1
    validation_status: str = "valid"
    content_checksum: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["generated_at"] = self.generated_at.isoformat(timespec="seconds")
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> DatasetManifest:
        body = dict(payload)
        generated_at = body.get("generated_at")
        if not isinstance(generated_at, datetime):
            body["generated_at"] = datetime.fromisoformat(str(generated_at))
        return cls(**body)


@dataclass(frozen=True)
class DatasetReadiness:
    dataset: str | None
    status: DatasetReadinessStatus
    age_days: int | None
    max_date: str | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def manifest_path(dataset_root: str | Path) -> Path:
    return Path(dataset_root) / "_manifest.json"


def write_dataset_manifest(dataset_root: str | Path, manifest: DatasetManifest) -> Path:
    root = Path(dataset_root)
    root.mkdir(parents=True, exist_ok=True)
    target = manifest_path(root)
    temporary = root / f"._manifest-{uuid.uuid4().hex}.json.tmp"
    try:
        temporary.write_text(
            json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def read_dataset_manifest(dataset_root: str | Path) -> DatasetManifest | None:
    path = manifest_path(dataset_root)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        return DatasetManifest.from_dict(payload)
    except (OSError, ValueError, TypeError):
        return None


def evaluate_dataset_readiness(
    manifest: DatasetManifest | None,
    *,
    as_of: date,
    max_age_days: int,
) -> DatasetReadiness:
    if manifest is None:
        return DatasetReadiness(dataset=None, status="missing", age_days=None, max_date=None, reason="manifest missing")
    if manifest.validation_status != "valid":
        return DatasetReadiness(
            dataset=manifest.dataset,
            status="invalid",
            age_days=None,
            max_date=manifest.max_date,
            reason=f"validation_status={manifest.validation_status}",
        )
    if not manifest.max_date:
        return DatasetReadiness(
            dataset=manifest.dataset,
            status="invalid",
            age_days=None,
            max_date=None,
            reason="max_date missing",
        )
    try:
        latest = date.fromisoformat(str(manifest.max_date)[:10])
    except ValueError:
        return DatasetReadiness(
            dataset=manifest.dataset,
            status="invalid",
            age_days=None,
            max_date=manifest.max_date,
            reason="max_date invalid",
        )
    age_days = max(0, (as_of - latest).days)
    if age_days > max_age_days:
        return DatasetReadiness(
            dataset=manifest.dataset,
            status="stale",
            age_days=age_days,
            max_date=manifest.max_date,
            reason=f"age {age_days}d exceeds {max_age_days}d",
        )
    return DatasetReadiness(
        dataset=manifest.dataset,
        status="ready",
        age_days=age_days,
        max_date=manifest.max_date,
    )


def build_dataset_manifest(
    dataset_root: str | Path,
    *,
    dataset: str,
    date_column: str,
) -> DatasetManifest:
    root = Path(dataset_root)
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise ValueError(f"No Parquet files found in {root}")

    identifier = '"' + str(date_column).replace('"', '""') + '"'
    connection = duckdb.connect(":memory:")
    try:
        connection.read_parquet(
            [path.as_posix() for path in files],
            hive_partitioning=True,
            union_by_name=True,
        ).create_view("_dataset_manifest_source")
        row = connection.execute(
            f"SELECT count(*), min({identifier}), max({identifier}) "
            "FROM _dataset_manifest_source",
        ).fetchone()
        schema_rows = [
            (str(item[0]), str(item[1]))
            for item in connection.execute(
                "DESCRIBE SELECT * FROM _dataset_manifest_source"
            ).fetchall()
        ]
    finally:
        connection.close()
    schema_hash = hashlib.sha256(json.dumps(schema_rows, sort_keys=True).encode("utf-8")).hexdigest()
    row_count = int(row[0] or 0) if row else 0
    min_date = str(row[1]) if row and row[1] is not None else None
    max_date = str(row[2]) if row and row[2] is not None else None
    partitions = {path.parent.relative_to(root).as_posix() for path in files}
    checksum = hashlib.sha256(
        json.dumps([row_count, min_date, max_date, schema_hash]).encode("utf-8")
    ).hexdigest()
    return DatasetManifest(
        dataset=dataset,
        generated_at=datetime.now(),
        file_count=len(files),
        byte_size=sum(path.stat().st_size for path in files),
        row_count=row_count,
        partition_count=len(partitions),
        min_date=min_date,
        max_date=max_date,
        schema_hash=schema_hash,
        validation_status="valid",
        content_checksum=checksum,
    )
