# backend/app/scripts/init_demo_factors.py
"""Seed two typical expression-based demo factors and run initial analysis.

Factors:
  1. momentum_20d  — 20-day price momentum (classic cross-sectional factor)
  2. turnover_anomaly_20d — 20-day turnover anomaly (liquidity factor)

Usage:
  cd backend
  $env:PYTHONPATH='.'
  .venv\Scripts\python.exe app/scripts/init_demo_factors.py
  .venv\Scripts\python.exe app/scripts/init_demo_factors.py --analyze --start 2024-01-01 --end 2025-12-31
"""
import argparse
import asyncio
from datetime import date
from decimal import Decimal

import pandas as pd
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Factor, FactorAnalysis
from app.db.sqlite import async_session_factory

DEMO_FACTORS = [
    {
        "name": "momentum_20d",
        "category": "动量类因子",
        "expression": "$close / Delay($close, 20) - 1",
        "stock_pool": "hs300",
        "description": (
            "20日价格动量因子。计算过去20个交易日的累计收益率。\n"
            "经典横截面动量因子，在A股市场中期通常呈现正IC。\n"
            "公式: close_t / close_{t-20} - 1"
        ),
    },
    {
        "name": "volume_anomaly_20d",
        "category": "情绪类因子",
        "expression": "$volume / ts_mean($volume, 20) - 1",
        "stock_pool": "hs300",
        "description": (
            "20日成交量异常因子。当日成交量相对于过去20日均值的偏离度。\n"
            "高成交量异常通常伴随短期价格冲击或趋势加速。\n"
            "公式: volume_t / mean(volume, 20) - 1"
        ),
    },
]


async def seed_factors(session: AsyncSession) -> list[Factor]:
    """Insert demo factors if they don't already exist."""
    from app.services.factor_service import FactorCreate, FactorService

    service = FactorService(session)
    created: list[Factor] = []

    for spec in DEMO_FACTORS:
        existing = await service.get_factor_by_name(spec["name"])
        if existing:
            logger.info("Factor '{}' already exists (id={}), skipping", spec["name"], existing.id)
            created.append(existing)
            continue

        factor = await service.create_factor(FactorCreate(
            name=spec["name"],
            category=spec["category"],
            source="custom",
            code=spec["expression"],
            parameters={
                "expression": spec["expression"],
                "stock_pool": spec["stock_pool"],
                "kind": "factor",
                "engine": "builtin",
            },
            description=spec["description"],
        ))
        created.append(factor)
        logger.info("Created factor '{}' (id={})", factor.name, factor.id)

    await session.commit()
    return created


async def run_analysis(
    factor: Factor,
    symbols: list[str],
    start_date: date,
    end_date: date,
    n_groups: int = 5,
) -> dict:
    """Run IC + quantile analysis for a single factor."""
    from app.services.factor_evaluation import FactorEvaluationService

    params = factor.parameters or {}
    expression = factor.code or params.get("expression", "")
    if not expression:
        logger.warning("Factor '{}' has no expression, skipping analysis", factor.name)
        return {"error": "No expression"}

    logger.info(
        "Analyzing '{}' with {} symbols from {} to {}",
        factor.name, len(symbols), start_date, end_date,
    )

    service = FactorEvaluationService()

    ic_result = await service.run_ic_analysis(
        expression=expression,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    ic_stats = ic_result.get("ic_stats", {})
    logger.info(
        "  IC mean={:.4f}  std={:.4f}  ICIR={:.4f}  positive_rate={:.1%}",
        ic_stats.get("mean", 0),
        ic_stats.get("std", 0),
        ic_stats.get("icir", 0),
        ic_stats.get("positive_rate", 0),
    )

    # Best-effort quantile backtest
    qt_result = None
    try:
        qt_result = await service.run_quantile_backtest(
            expression=expression,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            n_groups=n_groups,
        )
    except Exception as e:
        logger.warning("  Quantile backtest skipped: {}", e)

    return {
        "expression": expression,
        "ic_analysis": ic_result,
        "quantile_backtest": qt_result,
        "summary": {
            "ic_mean": ic_stats.get("mean"),
            "icir": ic_stats.get("icir"),
            "long_short_annual_return": qt_result.get("annual_return") if qt_result else None,
        },
    }


async def resolve_symbols(stock_pool: str = "hs300") -> list[str]:
    """Resolve stock pool name to a list of symbols from market data."""
    from datetime import date as dt_date, timedelta
    from app.data_stores import get_market_data_store

    store = get_market_data_store()
    info = store.coverage(
        [],
        dt_date.today() - timedelta(days=730),
        dt_date.today(),
        dataset="klines_daily",
    )
    symbols = sorted(info.get("symbols_covered", []))
    if not symbols:
        logger.warning("No symbols found in market data store; falling back to top300")
        symbols = store.top_by_avg_amount(
            dt_date.today() - timedelta(days=365),
            dt_date.today(),
            limit=300,
        )
    logger.info("Resolved {} symbols from market data", len(symbols))
    return symbols


async def main():
    parser = argparse.ArgumentParser(description="Seed demo factors and optionally run analysis")
    parser.add_argument(
        "--analyze", action="store_true",
        help="Run IC analysis after seeding factors",
    )
    parser.add_argument(
        "--start", type=str, default="2024-01-01",
        help="Analysis start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end", type=str, default="2025-12-31",
        help="Analysis end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--symbols", type=str, default=None,
        help="Comma-separated symbol list (default: auto-resolve from market data)",
    )
    parser.add_argument(
        "--pool", type=str, default="hs300",
        help="Stock pool name for analysis",
    )
    args = parser.parse_args()

    async with async_session_factory() as session:
        # 1. Seed factors
        factors = await seed_factors(session)
        if not factors:
            logger.error("No factors seeded")
            return

        if not args.analyze:
            logger.info("Factors seeded. Use --analyze to run analysis.")
            return

        # 2. Resolve symbols
        if args.symbols:
            symbols = sorted({s.strip().upper() for s in args.symbols.split(",") if s.strip()})
        else:
            symbols = await resolve_symbols(args.pool)

        if not symbols:
            logger.error("No symbols resolved — cannot run analysis")
            return

        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)

        # 3. Run analysis for each factor
        for factor in factors:
            try:
                report = await run_analysis(factor, symbols, start_date, end_date)
            except Exception as e:
                logger.exception("Analysis failed for '{}': {}", factor.name, e)
                continue

            ic_stats = report.get("ic_analysis", {}).get("ic_stats", {})
            analysis = FactorAnalysis(
                factor_id=factor.id,
                start_date=start_date,
                end_date=end_date,
                ic_mean=Decimal(str(round(ic_stats.get("mean", 0) or 0, 4))),
                ic_std=Decimal(str(round(ic_stats.get("std", 0) or 0, 4))),
                ir=Decimal(str(round(ic_stats.get("icir", 0) or 0, 4))),
                details=report,
            )
            session.add(analysis)

        await session.commit()
        logger.info("Analysis results saved for {} factor(s)", len(factors))


if __name__ == "__main__":
    asyncio.run(main())
