from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.data_stores.parquet_store import _list_param, _sql_literal
from app.db.duckdb import get_duckdb
from app.services.factor_precompute_runtime import release_precompute_memory, symbol_chunks
from app.services.factor_value_store import get_factor_value_store

RELAY_FACTOR_COLUMNS = {
    "moneyflow_net_mf_amount": ("moneyflow", "trade_date", "net_mf_amount"),
    "moneyflow_net_mf_vol": ("moneyflow", "trade_date", "net_mf_vol"),
    "auction_amount": ("auction_replay", "datetime", "amount"),
    "auction_vwap": ("auction_replay", "datetime", "vwap"),
}


def precompute_relay_factors(
    *,
    factor_names: list[str],
    start_date: date,
    end_date: date,
    symbols: list[str],
    progress_callback: Callable[[float, str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    store = get_factor_value_store()
    writer = store.batch_writer()
    errors: dict[str, str] = {}

    def report(progress: float, stage: str, **meta: Any) -> None:
        if progress_callback:
            progress_callback(progress, stage, meta)

    if not symbols:
        raise ValueError("No symbols supplied for Relay factor precompute")
    report(0.05, "load_relay_datasets", factor_names=factor_names, symbol_count=len(symbols))

    chunks = symbol_chunks(symbols)
    total_steps = max(len(factor_names) * max(len(chunks), 1), 1)
    current_step = 0
    for factor_name in factor_names:
        for symbol_chunk in chunks:
            current_step += 1
            try:
                if factor_name == "auction_gap_pct":
                    factor_rows = _load_auction_gap_pct(start_date, end_date, symbol_chunk)
                elif factor_name == "block_moneyflow_net_amount":
                    factor_rows = _load_block_moneyflow_net_amount(start_date, end_date, symbol_chunk)
                else:
                    factor_rows = _load_direct_factor(factor_name, start_date, end_date, symbol_chunk)
                writer.extend(factor_rows)
                writer.flush()
                del factor_rows
                release_precompute_memory()
            except Exception as exc:
                errors[factor_name] = str(exc)
                logger.warning("Relay factor {} precompute skipped: {}", factor_name, exc)
                break
            report(0.05 + 0.85 * current_step / total_steps, "compute_relay_factor", factor_name=factor_name, current=current_step, total=total_steps)

    report(0.94, "write_relay_factor_values", rows_buffered=writer.rows_buffered)
    writer.flush()
    report(1.0, "done", rows_written=writer.written)
    return {
        "factor_names": factor_names,
        "rows": {name: writer.counts.get(name, 0) for name in factor_names},
        "rows_written": writer.written,
        "errors": errors,
        "failed_factor_names": [name for name, error in errors.items() if error],
    }


def _load_direct_factor(factor_name: str, start_date: date, end_date: date, symbols: list[str]) -> list[dict[str, Any]]:
    if factor_name not in RELAY_FACTOR_COLUMNS:
        raise ValueError(f"Unsupported direct Relay factor: {factor_name}")
    dataset, date_col, value_col = RELAY_FACTOR_COLUMNS[factor_name]
    pattern = _dataset_pattern(dataset)
    if pattern is None:
        return []
    columns = _dataset_columns(pattern)
    if value_col not in columns:
        raise ValueError(f"Relay dataset {dataset} missing {value_col}; rerun tushare_relay sync")
    symbol_filter = f"symbol IN {_list_param(symbols)}"
    if date_col == "datetime":
        date_expr = "CAST(datetime AS DATE)"
    else:
        date_expr = date_col
    sql = f"""
        SELECT symbol, {date_expr} AS trade_date, {value_col} AS value
        FROM read_parquet({_sql_literal(pattern)}, hive_partitioning=true)
        WHERE {symbol_filter}
          AND {date_expr} >= {_sql_literal(start_date)}
          AND {date_expr} <= {_sql_literal(end_date)}
          AND {value_col} IS NOT NULL
    """
    df = get_duckdb().execute(sql).df()
    return _factor_rows(df, factor_name)


def _load_auction_gap_pct(start_date: date, end_date: date, symbols: list[str]) -> list[dict[str, Any]]:
    pattern = _dataset_pattern("auction_replay")
    if pattern is None:
        return []
    columns = _dataset_columns(pattern)
    price_col = "price" if "price" in columns else "close"
    if price_col not in columns or "open" not in columns:
        raise ValueError("auction_replay needs price/close and open columns for auction_gap_pct")
    sql = f"""
        SELECT
            symbol,
            CAST(datetime AS DATE) AS trade_date,
            CASE WHEN open IS NULL OR open = 0 THEN NULL ELSE ({price_col} - open) / open END AS value
        FROM read_parquet({_sql_literal(pattern)}, hive_partitioning=true)
        WHERE symbol IN {_list_param(symbols)}
          AND CAST(datetime AS DATE) >= {_sql_literal(start_date)}
          AND CAST(datetime AS DATE) <= {_sql_literal(end_date)}
    """
    df = get_duckdb().execute(sql).df()
    return _factor_rows(df, "auction_gap_pct")


def _load_block_moneyflow_net_amount(start_date: date, end_date: date, symbols: list[str]) -> list[dict[str, Any]]:
    block_pattern = _dataset_pattern("block_moneyflow")
    member_pattern = _dataset_pattern("ths_member")
    if block_pattern is None or member_pattern is None:
        return []
    columns = _dataset_columns(block_pattern)
    value_col = next((col for col in ("net_amount", "net_buy_amount", "net_mf_amount", "amount") if col in columns), None)
    if value_col is None:
        raise ValueError("block_moneyflow has no usable net amount column")
    sql = f"""
        WITH latest_member AS (
            SELECT ths_code, symbol
            FROM read_parquet({_sql_literal(member_pattern)}, hive_partitioning=true)
            WHERE symbol IN {_list_param(symbols)}
            QUALIFY row_number() OVER (PARTITION BY ths_code, symbol ORDER BY snapshot_date DESC) = 1
        )
        SELECT m.symbol, b.trade_date, b.{value_col} AS value
        FROM read_parquet({_sql_literal(block_pattern)}, hive_partitioning=true) b
        JOIN latest_member m ON m.ths_code = b.block_code
        WHERE b.trade_date >= {_sql_literal(start_date)}
          AND b.trade_date <= {_sql_literal(end_date)}
    """
    df = get_duckdb().execute(sql).df()
    return _factor_rows(df, "block_moneyflow_net_amount")


def _factor_rows(df: pd.DataFrame, factor_name: str) -> list[dict[str, Any]]:
    if df.empty:
        return []
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["symbol", "trade_date", "value"])
    if df.empty:
        return []
    now = datetime.now()
    return [
        {
            "symbol": str(row.symbol),
            "trade_date": pd.Timestamp(row.trade_date).date(),
            "factor_name": factor_name,
            "value": float(row.value),
            "source": "relay_precompute",
            "created_at": now,
        }
        for row in df.itertuples(index=False)
    ]


def _dataset_pattern(dataset: str) -> str | None:
    root = Path(settings.parquet_data_dir) / dataset
    if not root.exists() or not any(".tmp-" not in str(path) for path in root.rglob("*.parquet")):
        return None
    pattern = root / "year=*" / "month=??" / "*.parquet"
    if not any(root.glob("year=*/month=??/*.parquet")):
        pattern = root / "**" / "*.parquet"
    return str(pattern).replace("\\", "/")


def _dataset_columns(pattern: str) -> set[str]:
    df = get_duckdb().execute(
        f"SELECT * FROM read_parquet({_sql_literal(pattern)}, hive_partitioning=true) LIMIT 0"
    ).df()
    return set(df.columns)
