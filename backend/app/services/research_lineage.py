from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.research_lineage import DataSnapshot, ResearchArtifact, StrategyRelease
from app.db.models.strategy import Strategy

_TRANSITIONS = {
    "draft": "validated",
    "validated": "paper_approved",
    "paper_approved": "live_approved",
    "live_approved": "retired",
}


class ResearchLineageService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_data_snapshot(
        self,
        *,
        environment: str,
        dataset_versions: dict[str, Any],
        freshness: dict[str, Any],
        status: str,
    ) -> DataSnapshot:
        payload = json.dumps(
            {"dataset_versions": dataset_versions, "freshness": freshness},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        snapshot = DataSnapshot(
            id=str(uuid.uuid4()),
            environment=environment,
            dataset_versions=dataset_versions,
            freshness=freshness,
            schema_hash=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            status=status,
        )
        self.session.add(snapshot)
        await self.session.flush()
        return snapshot

    async def create_strategy_release(
        self,
        *,
        strategy_id: int,
        data_snapshot_id: str,
        engine: str,
        engine_version: str,
        git_commit: str,
        parameters: dict[str, Any],
        universe: dict[str, Any],
        cost_model: dict[str, Any],
        factor_params_hashes: dict[str, str],
    ) -> StrategyRelease:
        strategy = await self.session.get(Strategy, strategy_id)
        if strategy is None:
            raise ValueError(f"Strategy {strategy_id} not found")
        snapshot = await self.session.get(DataSnapshot, data_snapshot_id)
        if snapshot is None or snapshot.status != "ready":
            raise ValueError("A ready data snapshot is required")
        release = StrategyRelease(
            id=str(uuid.uuid4()),
            strategy_id=strategy_id,
            data_snapshot_id=data_snapshot_id,
            code_hash=hashlib.sha256(strategy.code.encode("utf-8")).hexdigest(),
            git_commit=git_commit,
            engine=engine,
            engine_version=engine_version,
            parameters=parameters,
            universe=universe,
            cost_model=cost_model,
            factor_params_hashes=factor_params_hashes,
            status="draft",
        )
        self.session.add(release)
        await self.session.flush()
        return release

    async def add_artifact(
        self,
        *,
        release_id: str,
        kind: str,
        validation_status: str,
        metrics: dict[str, Any],
        start_date: date | None = None,
        end_date: date | None = None,
        report_path: str | None = None,
        checksum: str | None = None,
    ) -> ResearchArtifact:
        if await self.session.get(StrategyRelease, release_id) is None:
            raise ValueError(f"Release {release_id} not found")
        artifact = ResearchArtifact(
            id=str(uuid.uuid4()),
            release_id=release_id,
            kind=kind,
            validation_status=validation_status,
            start_date=start_date,
            end_date=end_date,
            metrics=metrics,
            report_path=report_path,
            checksum=checksum,
        )
        self.session.add(artifact)
        await self.session.flush()
        return artifact

    async def promote_release(self, release_id: str, target_status: str) -> StrategyRelease:
        release = await self.session.get(StrategyRelease, release_id)
        if release is None:
            raise ValueError(f"Release {release_id} not found")
        if _TRANSITIONS.get(release.status) != target_status:
            raise ValueError(f"Invalid release transition: {release.status} -> {target_status}")
        artifacts = list(
            (
                await self.session.scalars(
                    select(ResearchArtifact).where(
                        ResearchArtifact.release_id == release_id,
                        ResearchArtifact.validation_status == "valid",
                    )
                )
            ).all()
        )
        if target_status == "validated":
            validations = [item for item in artifacts if item.kind == "validation"]
            if not validations:
                raise ValueError("A valid validation artifact is required")
            if not any(self._validation_evidence_complete(item.metrics) for item in validations):
                raise ValueError("Validation artifact is missing required checks")
        if target_status == "paper_approved":
            backtests = [item for item in artifacts if item.kind == "backtest"]
            if not backtests:
                raise ValueError("A valid backtest artifact is required")
            if not any(int(item.metrics.get("result_schema_version") or 0) == 2 for item in backtests):
                raise ValueError("A schema v2 backtest artifact is required")
        if target_status == "live_approved":
            paper = next((item for item in reversed(artifacts) if item.kind == "paper_shadow"), None)
            if paper is None:
                raise ValueError("A valid paper shadow artifact is required")
            self._validate_paper_shadow(paper.metrics)
        release.status = target_status
        if target_status in {"paper_approved", "live_approved"}:
            release.approved_at = datetime.now()
        await self.session.flush()
        return release

    @staticmethod
    def _validation_evidence_complete(metrics: dict[str, Any]) -> bool:
        required = (
            "data_integrity",
            "point_in_time",
            "no_lookahead",
            "execution_consistency",
        )
        return all(metrics.get(key) is True for key in required)

    @staticmethod
    def _validate_paper_shadow(metrics: dict[str, Any]) -> None:
        if int(metrics.get("trading_days") or 0) < 5:
            raise ValueError("Paper shadow requires at least 5 trading days")
        if metrics.get("reconciliation_complete") is not True:
            raise ValueError("Paper shadow reconciliation is incomplete")
        if int(metrics.get("guardrail_violations") or 0) != 0:
            raise ValueError("Paper shadow contains guardrail violations")
        if int(metrics.get("duplicate_orders") or 0) != 0:
            raise ValueError("Paper shadow contains duplicate orders")
