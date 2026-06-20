"""AKQuant Polars factor evaluation helpers."""
from __future__ import annotations

import asyncio
from datetime import date
from typing import Any


class AkquantFactorUnavailableError(RuntimeError):
    """Raised when AKQuant factor dependencies are not available."""


async def evaluate_akquant_factor(
    expression: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, list[dict[str, Any]]]:
    """Evaluate an AKQuant/Polars factor expression over local daily bars."""
    return await asyncio.to_thread(
        _evaluate_akquant_factor_sync,
        expression,
        symbols,
        start_date,
        end_date,
    )


def _evaluate_akquant_factor_sync(
    expression: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, list[dict[str, Any]]]:
    try:
        import polars as pl
        from akquant.factor.engine import FactorEngine
    except Exception as exc:  # pragma: no cover - depends on optional packages
        raise AkquantFactorUnavailableError(
            "AKQuant factor engine requires akquant and polars"
        ) from exc

    from app.data_stores import get_market_data_store

    store = get_market_data_store()
    pdf = store.load_daily(symbols, start_date, end_date)
    if pdf.empty:
        return {}
    if "turnover_rate" not in pdf.columns:
        pdf["turnover_rate"] = float("nan")
    pdf = pdf.reset_index()
    pdf = pdf.rename(columns={"trade_date": "date"})

    schema = ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "turnover_rate"]
    df = pl.from_pandas(pdf[schema])
    df = df.with_columns(
        pl.col("date").cast(pl.Date).cast(pl.Datetime),
        pl.col(["open", "high", "low", "close", "amount", "turnover_rate"]).cast(
            pl.Float64
        ),
        pl.col("volume").cast(pl.Float64),
    )

    engine = FactorEngine(catalog=None)  # type: ignore[arg-type]
    result = engine.run_on_data(df, expression, str(start_date), str(end_date))
    if result.is_empty():
        return {}

    out: dict[str, list[dict[str, Any]]] = {}
    for row in result.sort(["symbol", "date"]).iter_rows(named=True):
        value = row.get("factor_value")
        dt = row.get("date")
        symbol = str(row.get("symbol"))
        out.setdefault(symbol, []).append(
            {
                "trade_date": dt.date().isoformat() if hasattr(dt, "date") else str(dt),
                "value": None if value is None else float(value),
            }
        )
    return out
