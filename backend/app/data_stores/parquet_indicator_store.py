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
        self._cross_section_dataset = self._first_existing_dataset(
            "stock_indicators",
            "feature_values",
        )
        self._timeseries_dataset = self._first_existing_dataset(
            "indicator_timeseries",
            "feature_timeseries",
        )

    def _dataset_path(self, dataset: str) -> Path:
        return self._data_dir / dataset

    def _glob_pattern(self, dataset: str) -> str:
        return str(self._dataset_path(dataset) / "**" / "*.parquet").replace("\\", "/")

    def _exists(self, dataset: str) -> bool:
        root = self._dataset_path(dataset)
        return root.exists() and any(root.rglob("*.parquet"))

    def _first_existing_dataset(self, *datasets: str) -> str:
        for dataset in datasets:
            if self._exists(dataset):
                return dataset
        return datasets[0]

    def _indicator_col(self, dataset: str) -> str:
        return "feature_name" if dataset == "feature_values" else "indicator_name"

    def _updated_col(self, dataset: str) -> str:
        return "created_at" if dataset == "feature_values" else "updated_at"

    def load_cross_section(
        self,
        names: list[str],
        trade_date: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        dataset = self._cross_section_dataset
        if not self._exists(dataset):
            return pd.DataFrame()

        indicator_col = self._indicator_col(dataset)
        updated_col = self._updated_col(dataset)
        conditions = f"trade_date = '{trade_date}'"
        if names:
            conditions += f" AND {indicator_col} IN {_list_param(names)}"
        if symbols:
            conditions += f" AND symbol IN {_list_param(symbols)}"

        sql = f"""
            SELECT symbol,
                   {indicator_col} AS indicator_name,
                   trade_date,
                   value,
                   {updated_col} AS updated_at
            FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
            WHERE {conditions}
            ORDER BY symbol, {indicator_col}
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
        dataset = self._timeseries_dataset
        if not names or not self._exists(dataset):
            return pd.DataFrame()

        indicator_col = self._indicator_col(dataset)
        updated_col = self._updated_col(dataset)
        name_list = _list_param(names)
        conditions = f"""{indicator_col} IN {name_list}
          AND datetime >= '{start}'
          AND datetime < '{end}'"""
        if symbols:
            conditions += f" AND symbol IN {_list_param(symbols)}"

        sql = f"""
            SELECT symbol,
                   {indicator_col} AS indicator_name,
                   datetime,
                   value,
                   {updated_col} AS updated_at
            FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
            WHERE {conditions}
            ORDER BY symbol, {indicator_col}, datetime
        """
        db = get_duckdb()
        return db.execute(sql).df()

    def latest_trade_date(
        self,
        names: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> date | None:
        dataset = self._cross_section_dataset
        if not self._exists(dataset):
            return None

        indicator_col = self._indicator_col(dataset)
        conditions = ["trade_date IS NOT NULL"]
        if names:
            conditions.append(f"{indicator_col} IN {_list_param(names)}")
        if symbols:
            conditions.append(f"symbol IN {_list_param(symbols)}")

        sql = f"""
            SELECT max(trade_date) AS trade_date
            FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
            WHERE {' AND '.join(conditions)}
        """
        row = get_duckdb().execute(sql).fetchone()
        return row[0] if row and row[0] is not None else None

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
