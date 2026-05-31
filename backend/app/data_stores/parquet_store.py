"""Parquet + DuckDB 行情数据存储实现"""
from __future__ import annotations

import shutil
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

import pandas as pd
from loguru import logger

from app.data_stores.base import MarketDataStore
from app.db.duckdb import get_duckdb

_DAILY_COLS = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
_MINUTE_COLS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
_DATASET_KEY_COLS = {
    "factor_cache": ["symbol", "trade_date", "expr_hash"],
    "adj_factors": ["symbol", "trade_date"],
    "moneyflow": ["symbol", "trade_date"],
    "block_moneyflow": ["block_code", "trade_date"],
    "auction_replay": ["symbol", "datetime", "mode"],
    "ths_index": ["ths_code", "snapshot_date"],
    "ths_member": ["ths_code", "symbol", "snapshot_date"],
    "announcements": ["symbol", "ann_date", "title_hash"],
    "research_reports": ["symbol", "report_date", "title_hash"],
    "market_news": ["source_api", "publish_time", "title_hash"],
}


def _list_param(values: Sequence) -> str:
    """将序列转为 DuckDB tuple 字面量。

    DuckDB 对 IN 子句的 list 参数绑定兼容性不稳定；这里仍生成字面量，
    但先做单引号转义，避免 symbol/expression 中的引号破坏 SQL。
    """
    quoted = ", ".join(_sql_literal(v) for v in values)
    return f"({quoted})"


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


class ParquetMarketDataStore(MarketDataStore):
    """基于 Parquet 文件 + DuckDB 查询的行情数据存储"""

    def __init__(self, data_dir: str | None = None):
        from app.core.config import settings

        self._data_dir = Path(data_dir or settings.parquet_data_dir)

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def _dataset_path(self, dataset: str) -> Path:
        return self._data_dir / dataset

    def _glob_pattern(self, dataset: str) -> str:
        p = self._dataset_path(dataset)
        if self._has_year_month_partitions(dataset):
            return str(p / "year=*" / "month=??" / "*.parquet").replace("\\", "/")
        return str(p / "**" / "*.parquet").replace("\\", "/")

    def _exists(self, dataset: str) -> bool:
        # 检查是否有任何 parquet 文件
        root = self._dataset_path(dataset)
        if not root.exists():
            return False
        if any(root.glob("year=*/month=??/*.parquet")):
            return True
        return any(".tmp-" not in str(file) for file in root.rglob("*.parquet"))

    def _has_year_month_partitions(self, dataset: str) -> bool:
        root = self._dataset_path(dataset)
        return root.exists() and any(root.glob("year=*/month=??"))

    def _year_month_filter(self, dataset: str, start: date | datetime, end: date | datetime) -> str:
        if not self._has_year_month_partitions(dataset):
            return ""

        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        if pd.isna(start_ts) or pd.isna(end_ts) or end_ts < start_ts:
            return ""

        current = date(int(start_ts.year), int(start_ts.month), 1)
        last = date(int(end_ts.year), int(end_ts.month), 1)
        terms: list[str] = []
        while current <= last:
            terms.append(f"(year = {current.year} AND month = '{current.month:02d}')")
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        if not terms:
            return ""
        return "\n              AND (" + " OR ".join(terms) + ")"

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------

    def load_daily(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        if not symbols or not self._exists("klines_daily"):
            return pd.DataFrame()

        cols = list(columns) if columns else _DAILY_COLS
        col_str = ", ".join(cols)
        sym_list = _list_param(symbols)
        partition_filter = self._year_month_filter("klines_daily", start_date, end_date)

        sql = f"""
            SELECT {col_str}
            FROM read_parquet('{self._glob_pattern("klines_daily")}', hive_partitioning=true)
            WHERE symbol IN {sym_list}
              AND trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
              {partition_filter}
            ORDER BY symbol, trade_date
        """
        db = get_duckdb()
        df = db.execute(sql).df()
        if df.empty:
            return df
        for c in ["open", "high", "low", "close", "amount"]:
            if c in df.columns:
                df[c] = df[c].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="first")
        return df.set_index("trade_date").sort_index()

    def load_minute(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        columns: Sequence[str] | None = None,
        timer_times: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        # timer 查询优先读稀疏表；若稀疏表存在但所查区间为空，再回退到完整分钟线。
        datasets = ["klines_minute_timer", "klines_minute"] if timer_times else ["klines_minute", "klines_minute_timer"]
        datasets = [dataset for dataset in datasets if self._exists(dataset)]
        if not datasets:
            return pd.DataFrame()

        cols = list(columns) if columns else _MINUTE_COLS
        col_str = ", ".join(cols)
        sym_list = _list_param(symbols)

        conditions = f"""symbol IN {sym_list}
          AND datetime >= '{start}'
          AND datetime < '{end}'"""

        if timer_times:
            timer_minutes = []
            for text in timer_times:
                try:
                    h, m, *_ = str(text).split(":")
                    timer_minutes.append(int(h) * 60 + int(m))
                except Exception:
                    continue
            if timer_minutes:
                mins_str = ", ".join(str(m) for m in timer_minutes)
                conditions += f"\n          AND (hour(datetime) * 60 + minute(datetime)) IN ({mins_str})"

        db = get_duckdb()
        df = pd.DataFrame()
        for dataset in datasets:
            partition_filter = self._year_month_filter(dataset, start, end)
            sql = f"""
                SELECT {col_str}
                FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
                WHERE {conditions}
                  {partition_filter}
                ORDER BY symbol, datetime
            """
            df = db.execute(sql).df()
            if not df.empty:
                break
        if df.empty:
            return df
        for c in ["open", "high", "low", "close", "amount"]:
            if c in df.columns:
                df[c] = df[c].astype(float)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.drop_duplicates(subset=["symbol", "datetime"], keep="first")
        return df.set_index("datetime").sort_index()

    def load_minute_volume_sum(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, float]:
        if not symbols:
            return {}
        datasets = [dataset for dataset in ["klines_minute", "klines_minute_timer"] if self._exists(dataset)]
        if not datasets:
            return {}

        sym_list = _list_param(symbols)
        conditions = f"""symbol IN {sym_list}
          AND datetime >= '{start}'
          AND datetime < '{end}'"""
        db = get_duckdb()
        for dataset in datasets:
            partition_filter = self._year_month_filter(dataset, start, end)
            sql = f"""
                SELECT symbol, sum(volume) AS volume
                FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
                WHERE {conditions}
                  {partition_filter}
                GROUP BY symbol
            """
            rows = db.execute(sql).fetchall()
            if rows:
                return {str(symbol): float(volume or 0.0) for symbol, volume in rows}
        return {}

    def load_minute_cum_volume(
        self,
        symbols: Sequence[str],
        as_of: datetime,
    ) -> dict[str, float]:
        if not symbols:
            return {}
        as_of = as_of.replace(second=0, microsecond=0)
        dataset = "klines_minute_cum_timer"
        if not self._exists(dataset):
            return super().load_minute_cum_volume(symbols, as_of)

        sym_list = _list_param(symbols)
        partition_filter = self._year_month_filter(dataset, as_of, as_of)
        sql = f"""
            SELECT symbol, volume
            FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
            WHERE symbol IN {sym_list}
              AND datetime = '{as_of}'
              {partition_filter}
        """
        db = get_duckdb()
        rows = db.execute(sql).fetchall()
        if not rows:
            return super().load_minute_cum_volume(symbols, as_of)
        return {str(symbol): float(volume or 0.0) for symbol, volume in rows}

    def load_latest_daily_values(
        self,
        symbols: Sequence[str],
        field: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, float]:
        if not symbols or field not in {"open", "high", "low", "close", "volume", "amount"}:
            return {}
        if not self._exists("klines_daily"):
            return {}

        sym_list = _list_param(symbols)
        partition_filter = self._year_month_filter("klines_daily", start_date, end_date)
        sql = f"""
            SELECT symbol, arg_max({field}, trade_date) AS value
            FROM read_parquet('{self._glob_pattern("klines_daily")}', hive_partitioning=true)
            WHERE symbol IN {sym_list}
              AND trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
              AND {field} IS NOT NULL
              {partition_filter}
            GROUP BY symbol
        """
        rows = get_duckdb().execute(sql).fetchall()
        return {str(symbol): float(value or 0.0) for symbol, value in rows if value is not None}

    def has_data(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
    ) -> bool:
        if not symbols:
            return False
        if bar_type == "minute_timer":
            dataset = "klines_minute_timer" if self._exists("klines_minute_timer") else "klines_minute"
        elif bar_type == "minute":
            dataset = "klines_minute" if self._exists("klines_minute") else "klines_minute_timer"
        else:
            dataset = "klines_daily"
        if not self._exists(dataset):
            return False
        sym_list = _list_param(symbols)
        col = "datetime" if bar_type in {"minute", "minute_timer"} else "trade_date"
        sql = f"""
            SELECT 1 FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
            WHERE symbol IN {sym_list}
              AND {col} >= '{start_date}'
              AND {col} <= '{end_date}'
            LIMIT 1
        """
        db = get_duckdb()
        rows = db.execute(sql).fetchall()
        return bool(rows)

    def load_trading_dates(
        self, symbols: Sequence[str], start_date: date, end_date: date
    ) -> list[date]:
        if not symbols or not self._exists("klines_daily"):
            return []
        sym_list = _list_param(symbols)
        sql = f"""
            SELECT DISTINCT trade_date
            FROM read_parquet('{self._glob_pattern("klines_daily")}', hive_partitioning=true)
            WHERE symbol IN {sym_list}
              AND trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """
        db = get_duckdb()
        rows = db.execute(sql).fetchall()
        return [r[0] for r in rows]

    def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        if not self._exists("klines_daily"):
            return pd.Series(dtype=float, name="benchmark")
        sql = f"""
            SELECT trade_date, close
            FROM read_parquet('{self._glob_pattern("klines_daily")}', hive_partitioning=true)
            WHERE symbol = {_sql_literal(symbol)}
              AND trade_date >= {_sql_literal(start_date)}
              AND trade_date <= {_sql_literal(end_date)}
            ORDER BY trade_date
        """
        db = get_duckdb()
        df = db.execute(sql).df()
        if df.empty:
            return pd.Series(dtype=float, name="benchmark")
        df["close"] = df["close"].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.set_index("trade_date")
        returns = df["close"].pct_change().fillna(0.0)
        returns.name = symbol
        return returns

    def top_by_avg_amount(
        self, start_date: date, end_date: date, limit: int
    ) -> list[str]:
        if not self._exists("klines_daily"):
            return []
        sql = f"""
            SELECT symbol, avg(amount) as avg_amount
            FROM read_parquet('{self._glob_pattern("klines_daily")}', hive_partitioning=true)
            WHERE trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
              AND amount > 0 AND close > 0
            GROUP BY symbol
            ORDER BY avg_amount DESC
            LIMIT {limit}
        """
        db = get_duckdb()
        rows = db.execute(sql).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------

    def write_daily(self, df: pd.DataFrame) -> int:
        return self._write_partitioned(df, dataset="klines_daily", date_col="trade_date")

    def write_minute(self, df: pd.DataFrame, *, dataset: str = "klines_minute") -> int:
        from app.core.config import settings

        append_only = settings.parquet_minute_append_only
        if dataset == "klines_minute" and append_only:
            return self._append_partitioned(df, dataset=dataset, date_col="datetime")
        return self._write_partitioned(df, dataset=dataset, date_col="datetime")

    def _append_partitioned(self, df: pd.DataFrame, *, dataset: str, date_col: str) -> int:
        if df.empty:
            return 0
        df = df.copy()
        if date_col not in df.columns:
            if df.index.name == date_col:
                df = df.reset_index()
            else:
                raise KeyError(f"DataFrame missing {date_col} column")

        dt = pd.to_datetime(df[date_col])
        df["year"] = dt.dt.year.astype(str)
        df["month"] = dt.dt.strftime("%m")

        root = self._dataset_path(dataset)
        root.mkdir(parents=True, exist_ok=True)

        import pyarrow as pa
        import pyarrow.parquet as pq

        for (year, month), part in df.groupby(["year", "month"], sort=False):
            partition_dir = root / f"year={year}" / f"month={month}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            body = part.drop(columns=["year", "month"], errors="ignore")
            if dataset == "klines_minute" and "source" not in body.columns:
                body["source"] = "qmt"
            if date_col in body.columns:
                body = body.sort_values(["symbol", date_col] if "symbol" in body.columns else [date_col])
            table = pa.Table.from_pandas(body, preserve_index=False)
            pq.write_table(table, partition_dir / f"part-{uuid.uuid4().hex}.parquet")
        return len(df)

    def _write_partitioned(self, df: pd.DataFrame, *, dataset: str, date_col: str) -> int:
        if df.empty:
            return 0
        df = df.copy()
        if date_col not in df.columns:
            if df.index.name == date_col:
                df = df.reset_index()
            else:
                raise KeyError(f"DataFrame missing {date_col} column")

        dt = pd.to_datetime(df[date_col])
        df["year"] = dt.dt.year.astype(str)
        df["month"] = dt.dt.strftime("%m")

        root = self._dataset_path(dataset)
        root.mkdir(parents=True, exist_ok=True)

        import pyarrow as pa
        import pyarrow.parquet as pq

        if dataset in _DATASET_KEY_COLS:
            key_cols = _DATASET_KEY_COLS[dataset]
        elif date_col == "trade_date":
            key_cols = ["symbol", "trade_date"]
        else:
            key_cols = ["symbol", "datetime"]

        for (year, month), part in df.groupby(["year", "month"], sort=False):
            partition_dir = root / f"year={year}" / f"month={month}"
            existing_frames = []
            if partition_dir.exists():
                for file in partition_dir.glob("*.parquet"):
                    try:
                        existing_frames.append(pd.read_parquet(file))
                    except Exception as exc:
                        logger.warning("Failed to read existing parquet {}: {}", file, exc)
            body = part.drop(columns=["year", "month"], errors="ignore")
            if existing_frames:
                body = pd.concat(existing_frames + [body], ignore_index=True)
            present_keys = [c for c in key_cols if c in body.columns]
            if present_keys:
                body = body.drop_duplicates(subset=present_keys, keep="last")
            if date_col in body.columns:
                body = body.sort_values(["symbol", date_col] if "symbol" in body.columns else [date_col])

            tmp_dir = partition_dir.with_name(f"{partition_dir.name}.tmp-{uuid.uuid4().hex}")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(body, preserve_index=False)
            pq.write_table(table, tmp_dir / "part-0.parquet")

            if partition_dir.exists():
                shutil.rmtree(partition_dir)
            tmp_dir.replace(partition_dir)
        return len(df)

    def write_dataset(self, df: pd.DataFrame, *, dataset: str, date_col: str) -> int:
        """Write a generic partitioned Parquet dataset with dataset-specific dedupe keys."""
        return self._write_partitioned(df, dataset=dataset, date_col=date_col)

    # ------------------------------------------------------------------
    # 覆盖摘要
    # ------------------------------------------------------------------

    def coverage(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        *,
        dataset: str,
        timer_times: Sequence[str] | None = None,
    ) -> dict:
        if not self._exists(dataset):
            return {"total_rows": 0, "symbols_covered": [], "date_range": None}

        col = "datetime" if "minute" in dataset else "trade_date"
        symbol_filter = f"symbol IN {_list_param(symbols)} AND" if symbols else ""
        sql = f"""
            SELECT symbol, MIN({col}) as min_dt, MAX({col}) as max_dt, COUNT(*) as cnt
            FROM read_parquet('{self._glob_pattern(dataset)}', hive_partitioning=true)
            WHERE {symbol_filter}
              {col} >= {_sql_literal(start_date)}
              AND {col} <= {_sql_literal(end_date)}
            GROUP BY symbol
        """
        db = get_duckdb()
        rows = db.execute(sql).fetchall()
        symbols_covered = [r[0] for r in rows]
        total = sum(r[3] for r in rows)
        return {
            "total_rows": total,
            "symbols_covered": symbols_covered,
            "date_range": f"{start_date} ~ {end_date}",
        }
