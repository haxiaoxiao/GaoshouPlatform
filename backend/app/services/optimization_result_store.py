"""Persist optimization task results in the existing backtests table."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from loguru import logger
from sqlalchemy import select

from app.backtest.config import BacktestConfig
from app.db.models.strategy import Backtest, Strategy
from app.db.sqlite import async_session_factory


async def save_optimization_result(
    *,
    task_id: str,
    optimization_type: str,
    config: BacktestConfig,
    request_params: dict[str, Any],
    result: dict[str, Any],
    success: bool,
) -> int | None:
    """Save AKQuant optimization output as a completed/failed backtest record.

    The platform does not yet have a dedicated optimization history table. Using
    the existing backtests table keeps optimization tasks visible in the current
    history UI and avoids a migration for this phase.
    """
    try:
        async with async_session_factory() as session:
            strategy = None
            if config.strategy_id is not None:
                found = await session.execute(
                    select(Strategy).where(Strategy.id == config.strategy_id).limit(1)
                )
                strategy = found.scalars().first()

            code = config.strategy_code or config.factor_expression or config.buy_condition or ""
            if strategy is None and code:
                found = await session.execute(select(Strategy).where(Strategy.code == code).limit(1))
                strategy = found.scalars().first()

            if strategy is None:
                strategy = Strategy(
                    name=f"{optimization_type}-{task_id}",
                    code=code or f"# {optimization_type} optimization task {task_id}",
                    description=f"auto-created AKQuant {optimization_type} optimization strategy",
                )
                session.add(strategy)
                await session.flush()

            record = Backtest(
                strategy_id=strategy.id,
                status="completed" if success else "failed",
                start_date=config.start_date or date.today(),
                end_date=config.end_date or date.today(),
                initial_capital=Decimal(str(config.initial_capital)),
                parameters={
                    "record_type": "optimization",
                    "optimization_type": optimization_type,
                    "task_id": task_id,
                    "engine": config.engine,
                    "mode": config.mode,
                    "bar_type": config.bar_type,
                    "symbols": config.symbols,
                    "symbol_count": len(config.symbols),
                    "index_symbol": config.index_symbol,
                    "strategy_id": config.strategy_id,
                    "strategy_params": config.strategy_params,
                    **request_params,
                },
                result=result if success else {"error": result.get("error"), **result},
            )
            session.add(record)
            await session.commit()
            logger.info(
                "Optimization task {} persisted as backtest id={} ({})",
                task_id,
                record.id,
                optimization_type,
            )
            return int(record.id)
    except Exception as exc:
        logger.error("Failed to persist optimization task {}: {}", task_id, exc)
        return None
