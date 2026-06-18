from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.duckdb import get_duckdb
from app.db.models.financial import FinancialData
from app.data_stores.parquet_store import _list_param, _sql_literal


STATEMENT_DATASETS = {
    "income": "financial_income",
    "balancesheet": "financial_balancesheet",
    "cashflow": "financial_cashflow",
}


@dataclass
class PITFinancialSnapshot:
    symbol: str
    as_of_date: date
    report_date: date | None = None
    ann_date: date | None = None
    sqlite_fields: dict[str, Any] = field(default_factory=dict)
    statements: dict[str, dict[str, Any]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "as_of_date": self.as_of_date.isoformat(),
            "report_date": self.report_date.isoformat() if self.report_date else None,
            "ann_date": self.ann_date.isoformat() if self.ann_date else None,
            "sqlite_fields": self.sqlite_fields,
            "statements": self.statements,
        }


class FinancialPITStore:
    """Point-in-time financial mart combining SQLite summary data and Relay statements."""

    def __init__(self, session: AsyncSession, data_dir: str | None = None):
        self.session = session
        self.data_dir = Path(data_dir or settings.parquet_data_dir)

    async def load_snapshots(
        self,
        symbols: Sequence[str],
        *,
        as_of_date: date,
    ) -> dict[str, PITFinancialSnapshot]:
        normalized_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            return {}

        snapshots = {
            symbol: PITFinancialSnapshot(symbol=symbol, as_of_date=as_of_date)
            for symbol in normalized_symbols
        }
        await self._attach_sqlite_financials(snapshots, as_of_date)
        self._attach_relay_statements(snapshots, as_of_date)
        return snapshots

    async def _attach_sqlite_financials(
        self,
        snapshots: dict[str, PITFinancialSnapshot],
        as_of_date: date,
    ) -> None:
        if not snapshots:
            return
        rows = (
            await self.session.execute(
                select(FinancialData)
                .where(FinancialData.symbol.in_(list(snapshots)))
                .where(FinancialData.ann_date.is_not(None))
                .where(FinancialData.ann_date <= as_of_date)
                .order_by(FinancialData.symbol, FinancialData.ann_date.desc(), FinancialData.report_date.desc())
            )
        ).scalars().all()

        seen: set[str] = set()
        for row in rows:
            if row.symbol in seen or row.symbol not in snapshots:
                continue
            seen.add(row.symbol)
            snapshot = snapshots[row.symbol]
            snapshot.report_date = row.report_date
            snapshot.ann_date = row.ann_date
            snapshot.sqlite_fields = {
                "report_type": row.report_type,
                "eps": row.eps,
                "bvps": row.bvps,
                "roe": row.roe,
                "revenue": row.revenue,
                "net_profit": row.net_profit,
                "revenue_yoy": row.revenue_yoy,
                "profit_yoy": row.profit_yoy,
                "gross_margin": row.gross_margin,
                "total_assets": row.total_assets,
                "total_liability": row.total_liability,
                "total_equity": row.total_equity,
                "pe_ttm": row.pe_ttm,
                "pb": row.pb,
            }

    def _attach_relay_statements(
        self,
        snapshots: dict[str, PITFinancialSnapshot],
        as_of_date: date,
    ) -> None:
        if not snapshots:
            return
        symbols = list(snapshots)
        for statement_name, dataset in STATEMENT_DATASETS.items():
            frame = self._load_latest_statement(dataset, symbols, as_of_date)
            if frame.empty:
                continue
            for row in frame.to_dict(orient="records"):
                symbol = str(row.get("symbol") or "")
                if symbol not in snapshots:
                    continue
                snapshots[symbol].statements[statement_name] = _clean_statement_row(row)

    def _load_latest_statement(
        self,
        dataset: str,
        symbols: Sequence[str],
        as_of_date: date,
    ) -> pd.DataFrame:
        root = self.data_dir / dataset
        if not root.exists() or not any(".tmp-" not in str(file) for file in root.rglob("*.parquet")):
            return pd.DataFrame()

        pattern = str(root / "year=*" / "month=??" / "*.parquet")
        if not any(root.glob("year=*/month=??/*.parquet")):
            pattern = str(root / "**" / "*.parquet")
        pattern = pattern.replace("\\", "/")

        sql = f"""
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY f_ann_date DESC NULLS LAST, end_date DESC NULLS LAST
                    ) AS row_num
                FROM read_parquet({_sql_literal(pattern)}, hive_partitioning=true, union_by_name=true)
                WHERE symbol IN {_list_param(symbols)}
                  AND f_ann_date <= {_sql_literal(as_of_date)}
            )
            WHERE row_num = 1
        """
        frame = get_duckdb().execute(sql).df()
        if frame.empty:
            return frame
        return frame.drop(columns=["row_num"], errors="ignore")


def _clean_statement_row(row: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, pd.Timestamp):
            cleaned[key] = value.date().isoformat() if not pd.isna(value) else None
        elif pd.isna(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned
