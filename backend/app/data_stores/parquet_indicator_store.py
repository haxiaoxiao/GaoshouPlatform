"""Parquet 指标数据存储实现"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from app.data_stores.indicator_base import IndicatorStore
from app.data_stores.parquet_store import _sql_literal, _list_param
from app.db.duckdb import get_duckdb


class ParquetIndicatorStore(IndicatorStore):
    """基于 Parquet 文件 + DuckDB 查询的指标存储"""

    def __init__(self, data_dir: str | None = None):
        from app.core.config import settings

        self._data_dir = Path(data_dir or settings.parquet_data_dir)

    def _dataset_path(self, dataset: str) -> Path:
        return self._data_dir / dataset

    def _glob_pattern(self, dataset: str) -> str:
        return str(self._dataset_path(dataset) / "**" / "*.parquet").replace("\\", "/")

    def _exists(self, dataset: str) -> bool:
        root = self._dataset_path(dataset)
        return root.exists() and any(root.rglob("*.parquet"))

    def load_cross_section(
        self,
        names: list[str],
        trade_date: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        if not self._exists("stock_indicators"):
            return pd.DataFrame()

        conditions = f"trade_date = '{trade_date}'"
        if names:
            conditions += f" AND indicator_name IN {_list_param(names)}"
        if symbols:
            conditions += f" AND symbol IN {_list_param(symbols)}"

        sql = f"""
            SELECT symbol, indicator_name, trade_date, value, updated_at
            FROM read_parquet('{self._glob_pattern("stock_indicators")}', hive_partitioning=true)
            WHERE {conditions}
            ORDER BY symbol, indicator_name
        """
        db = get_duckdb()
        return db.execute(sql).df()

    def load_timeseries(
        self,
        names: list[str],
        start: date,
        end: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        if not names or not self._exists("indicator_timeseries"):
            return pd.DataFrame()

        name_list = _list_param(names)
        conditions = f"""indicator_name IN {name_list}
          AND datetime >= '{start}'
          AND datetime < '{end}'"""
        if symbols:
            conditions += f" AND symbol IN {_list_param(symbols)}"

        sql = f"""
            SELECT symbol, indicator_name, datetime, value, updated_at
            FROM read_parquet('{self._glob_pattern("indicator_timeseries")}', hive_partitioning=true)
            WHERE {conditions}
            ORDER BY symbol, indicator_name, datetime
        """
        db = get_duckdb()
        return db.execute(sql).df()

    def write_cross_section(self, df: pd.DataFrame) -> int:
        return self._write_partitioned(
            df, dataset="stock_indicators", date_col="trade_date",
            indicator_col="indicator_name",
        )

    def write_timeseries(self, df: pd.DataFrame) -> int:
        return self._write_partitioned(
            df, dataset="indicator_timeseries", date_col="datetime",
            indicator_col="indicator_name",
        )

    def _write_partitioned(self, df: pd.DataFrame, *, dataset: str, date_col: str, indicator_col: str) -> int:
        if df.empty:
            return 0
        df = df.copy()
        dt = pd.to_datetime(df[date_col])
        df["year"] = dt.dt.year.astype(str)
        df["month"] = dt.dt.strftime("%m")

        root = self._dataset_path(dataset)
        root.mkdir(parents=True, exist_ok=True)

        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(root),
            partition_cols=[indicator_col, "year", "month"],
            existing_data_behavior="overwrite_or_ignore",
        )
        return len(df)
