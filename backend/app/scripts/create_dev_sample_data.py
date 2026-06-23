#!/usr/bin/env python
"""Create an isolated development data slice from production data.

The source SQLite database is opened in read-only mode and source Parquet files
are only scanned by DuckDB. All generated files are written under the target
directory so frontend/backend refactors can run against real-but-small data
without touching production storage.
"""
from __future__ import annotations

import argparse
import json
import shutil
import socket
import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_PUBLIC_DATA_DIR = _REPO_ROOT.parent / "Data"
_DEFAULT_SOURCE_DATA_DIR = _DEFAULT_PUBLIC_DATA_DIR
_DEFAULT_TARGET_DATA_DIR = _DEFAULT_PUBLIC_DATA_DIR / "dev_sample"
_DEFAULT_INDEX_SYMBOL = "399101.SZ"
_DEFAULT_EXTRA_SYMBOLS = ("600519.SH", "000001.SZ", "002117.SZ", "000821.SZ", "600651.SH")


def _parse_date(value: str) -> date:
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"Invalid date: {value!r}; use YYYY-MM-DD or YYYYMMDD")


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _read_only_sqlite(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"Source SQLite database not found: {path}")
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    return con


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in con.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()]


def _in_clause(column: str, values: Sequence[str]) -> tuple[str, list[str]]:
    if not values:
        return "1 = 0", []
    placeholders = ",".join("?" for _ in values)
    return f"{_quote_ident(column)} IN ({placeholders})", list(values)


def _copy_schema(src: sqlite3.Connection, dst: sqlite3.Connection) -> None:
    rows = src.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
        ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, name
        """
    ).fetchall()
    for row in rows:
        try:
            dst.execute(str(row["sql"]))
        except sqlite3.OperationalError:
            if row["type"] == "table":
                raise
            # Existing auto indexes or partial legacy objects are not required
            # for the sample database; table schemas are the important contract.
            continue
    dst.commit()


def _copy_rows(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
    *,
    where: str | None = None,
    params: Sequence[object] = (),
    suffix: str = "",
) -> int:
    if not _table_exists(src, table):
        return 0
    columns = _table_columns(src, table)
    if not columns:
        return 0

    column_sql = ", ".join(_quote_ident(col) for col in columns)
    query = f"SELECT {column_sql} FROM {_quote_ident(table)}"
    if where:
        query += f" WHERE {where}"
    if suffix:
        query += f" {suffix}"

    placeholders = ", ".join("?" for _ in columns)
    insert_sql = (
        f"INSERT OR IGNORE INTO {_quote_ident(table)} ({column_sql}) "
        f"VALUES ({placeholders})"
    )

    count = 0
    cursor = src.execute(query, tuple(params))
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        dst.executemany(insert_sql, [tuple(row[col] for col in columns) for row in rows])
        count += len(rows)
    dst.commit()
    return count


def _latest_rows_suffix(src: sqlite3.Connection, table: str, limit: int) -> str:
    if not _table_exists(src, table):
        return ""
    columns = _table_columns(src, table)
    if "id" in columns:
        return f"ORDER BY id DESC LIMIT {limit}"
    if "created_at" in columns:
        return f"ORDER BY created_at DESC LIMIT {limit}"
    return f"LIMIT {limit}"


def _latest_index_date(src: sqlite3.Connection, index_symbol: str, as_of: date) -> str | None:
    if not _table_exists(src, "index_components"):
        return None
    row = src.execute(
        """
        SELECT MAX(trade_date) AS trade_date
        FROM index_components
        WHERE index_symbol = ?
          AND trade_date <= ?
        """,
        (index_symbol, as_of.isoformat()),
    ).fetchone()
    return str(row["trade_date"]) if row and row["trade_date"] is not None else None


def _select_symbols(
    src: sqlite3.Connection,
    *,
    index_symbol: str,
    as_of: date,
    max_symbols: int,
    extra_symbols: Sequence[str],
) -> list[str]:
    symbols: list[str] = []
    latest_date = _latest_index_date(src, index_symbol, as_of)
    if latest_date:
        rows = src.execute(
            """
            SELECT symbol
            FROM index_components
            WHERE index_symbol = ?
              AND trade_date = ?
            ORDER BY COALESCE(weight, 0) DESC, symbol
            LIMIT ?
            """,
            (index_symbol, latest_date, max_symbols),
        ).fetchall()
        symbols.extend(str(row["symbol"]) for row in rows)

    if not symbols and _table_exists(src, "stocks"):
        rows = src.execute(
            """
            SELECT symbol
            FROM stocks
            WHERE COALESCE(is_delist, 0) = 0
              AND COALESCE(is_st, 0) = 0
            ORDER BY COALESCE(circ_mv, total_mv, 999999999999), symbol
            LIMIT ?
            """,
            (max_symbols,),
        ).fetchall()
        symbols.extend(str(row["symbol"]) for row in rows)

    if extra_symbols and _table_exists(src, "stocks"):
        where, params = _in_clause("symbol", list(extra_symbols))
        rows = src.execute(
            f"SELECT symbol FROM stocks WHERE {where} ORDER BY symbol",
            params,
        ).fetchall()
        symbols.extend(str(row["symbol"]) for row in rows)

    seen: set[str] = set()
    unique: list[str] = []
    for symbol in symbols:
        if symbol not in seen:
            seen.add(symbol)
            unique.append(symbol)
    return unique


def _month_starts(start: date, end: date) -> list[tuple[int, str]]:
    months: list[tuple[int, str]] = []
    current = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while current <= last:
        months.append((current.year, f"{current.month:02d}"))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _parquet_files_for_months(dataset_root: Path, start: date, end: date) -> list[Path]:
    if not dataset_root.exists():
        return []
    months = set(_month_starts(start, end))
    files: list[Path] = []
    for file in dataset_root.rglob("*.parquet"):
        text = str(file)
        if ".tmp-" in text or ".tmp" in text:
            continue
        year: int | None = None
        month: str | None = None
        for part in file.parts:
            if part.startswith("year="):
                try:
                    year = int(part.split("=", 1)[1])
                except ValueError:
                    year = None
            elif part.startswith("month="):
                value = part.split("=", 1)[1]
                if len(value) == 2 and value.isdigit():
                    month = value
        if year is None or month is None or (year, month) in months:
            files.append(file)
    return files


def _parquet_files(dataset_root: Path) -> list[Path]:
    if not dataset_root.exists():
        return []
    return [
        file
        for file in dataset_root.rglob("*.parquet")
        if ".tmp-" not in str(file) and ".tmp" not in str(file)
    ]


def _duckdb_list_literal(files: Iterable[Path]) -> str:
    return "[" + ", ".join(_sql_string(file.resolve().as_posix()) for file in files) + "]"


def _describe_parquet(con: duckdb.DuckDBPyConnection, files: Sequence[Path]) -> list[str]:
    if not files:
        return []
    file_list = _duckdb_list_literal(files[:1])
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({file_list}, hive_partitioning=true, union_by_name=true)"
    ).fetchall()
    return [str(row[0]) for row in rows]


def _copy_parquet_dataset(
    con: duckdb.DuckDBPyConnection,
    source_parquet: Path,
    target_parquet: Path,
    dataset: str,
    *,
    date_column: str,
    start_dt: datetime,
    end_dt: datetime,
    symbols: Sequence[str],
    symbol_column: str = "symbol",
) -> int:
    dataset_root = source_parquet / dataset
    files = _parquet_files_for_months(dataset_root, start_dt.date(), (end_dt - timedelta(days=1)).date())
    if not files:
        return 0

    columns = _describe_parquet(con, files)
    if date_column not in columns:
        return 0

    conditions = [
        f"CAST({_quote_ident(date_column)} AS TIMESTAMP) >= TIMESTAMP {_sql_string(start_dt.isoformat(sep=' '))}",
        f"CAST({_quote_ident(date_column)} AS TIMESTAMP) < TIMESTAMP {_sql_string(end_dt.isoformat(sep=' '))}",
    ]
    if symbol_column in columns and symbols:
        symbol_list = ", ".join(_sql_string(symbol) for symbol in symbols)
        conditions.append(f"{_quote_ident(symbol_column)} IN ({symbol_list})")

    excludes = [col for col in ("year", "month") if col in columns]
    select_prefix = "*"
    if excludes:
        select_prefix = "* EXCLUDE (" + ", ".join(_quote_ident(col) for col in excludes) + ")"

    date_expr = f"CAST({_quote_ident(date_column)} AS TIMESTAMP)"
    files_literal = _duckdb_list_literal(files)
    where_sql = " AND ".join(conditions)
    target_dir = target_parquet / dataset
    target_dir.mkdir(parents=True, exist_ok=True)

    count_sql = f"""
        SELECT COUNT(*)
        FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
        WHERE {where_sql}
    """
    row_count = int(con.execute(count_sql).fetchone()[0])
    if row_count == 0:
        return 0

    copy_sql = f"""
        COPY (
            SELECT
                {select_prefix},
                year({date_expr}) AS year,
                strftime({date_expr}, '%m') AS month
            FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
            WHERE {where_sql}
        )
        TO {_sql_string(target_dir.as_posix())}
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE true)
    """
    con.execute(copy_sql)
    return row_count


def _copy_parquet_latest_dataset(
    con: duckdb.DuckDBPyConnection,
    source_parquet: Path,
    target_parquet: Path,
    dataset: str,
    *,
    date_column: str,
    symbols: Sequence[str] = (),
    symbol_column: str = "symbol",
    max_rows: int | None = None,
) -> int:
    files = _parquet_files(source_parquet / dataset)
    if not files:
        return 0

    columns = _describe_parquet(con, files)
    if date_column not in columns:
        return 0

    files_literal = _duckdb_list_literal(files)
    symbol_condition = ""
    if symbol_column in columns and symbols:
        symbol_list = ", ".join(_sql_string(symbol) for symbol in symbols)
        symbol_condition = f"WHERE {_quote_ident(symbol_column)} IN ({symbol_list})"

    latest_date = con.execute(
        f"""
        SELECT MAX(CAST({_quote_ident(date_column)} AS DATE))
        FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
        {symbol_condition}
        """
    ).fetchone()[0]
    if latest_date is None and symbol_condition:
        latest_date = con.execute(
            f"""
            SELECT MAX(CAST({_quote_ident(date_column)} AS DATE))
            FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
            """
        ).fetchone()[0]
        symbol_condition = ""
    if latest_date is None:
        return 0

    conditions = [f"CAST({_quote_ident(date_column)} AS DATE) = DATE {_sql_string(latest_date)}"]
    if symbol_condition:
        symbol_list = ", ".join(_sql_string(symbol) for symbol in symbols)
        conditions.append(f"{_quote_ident(symbol_column)} IN ({symbol_list})")

    excludes = [col for col in ("year", "month") if col in columns]
    select_prefix = "*"
    if excludes:
        select_prefix = "* EXCLUDE (" + ", ".join(_quote_ident(col) for col in excludes) + ")"

    where_sql = " AND ".join(conditions)
    limit_sql = f"\n            LIMIT {max_rows}" if max_rows else ""
    date_expr = f"CAST({_quote_ident(date_column)} AS TIMESTAMP)"
    target_dir = target_parquet / dataset
    target_dir.mkdir(parents=True, exist_ok=True)

    count_sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT 1
            FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
            WHERE {where_sql}
            {limit_sql}
        )
    """
    row_count = int(con.execute(count_sql).fetchone()[0])
    if row_count == 0:
        return 0

    copy_sql = f"""
        COPY (
            SELECT
                {select_prefix},
                year({date_expr}) AS year,
                strftime({date_expr}, '%m') AS month
            FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
            WHERE {where_sql}
            {limit_sql}
        )
        TO {_sql_string(target_dir.as_posix())}
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE true)
    """
    con.execute(copy_sql)
    return row_count


def _copy_minute_timer_dataset(
    con: duckdb.DuckDBPyConnection,
    source_parquet: Path,
    target_parquet: Path,
    *,
    start_dt: datetime,
    end_dt: datetime,
    symbols: Sequence[str],
    timer_times: Sequence[str],
) -> int:
    source_dataset = "klines_minute"
    files = _parquet_files_for_months(source_parquet / source_dataset, start_dt.date(), (end_dt - timedelta(days=1)).date())
    if not files:
        return 0

    columns = _describe_parquet(con, files)
    if "datetime" not in columns:
        return 0

    timer_minutes: list[int] = []
    for text in timer_times:
        try:
            hour, minute, *_ = str(text).split(":")
            timer_minutes.append(int(hour) * 60 + int(minute))
        except Exception:
            continue
    if not timer_minutes:
        return 0

    symbol_list = ", ".join(_sql_string(symbol) for symbol in symbols)
    minute_list = ", ".join(str(value) for value in sorted(set(timer_minutes)))
    files_literal = _duckdb_list_literal(files)
    conditions = [
        f"CAST(datetime AS TIMESTAMP) >= TIMESTAMP {_sql_string(start_dt.isoformat(sep=' '))}",
        f"CAST(datetime AS TIMESTAMP) < TIMESTAMP {_sql_string(end_dt.isoformat(sep=' '))}",
        f"symbol IN ({symbol_list})",
        f"(hour(datetime) * 60 + minute(datetime)) IN ({minute_list})",
    ]
    where_sql = " AND ".join(conditions)
    excludes = [col for col in ("year", "month") if col in columns]
    select_prefix = "*"
    if excludes:
        select_prefix = "* EXCLUDE (" + ", ".join(_quote_ident(col) for col in excludes) + ")"

    row_count = int(con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
        WHERE {where_sql}
        """
    ).fetchone()[0])
    if row_count == 0:
        return 0

    target_dir = target_parquet / "klines_minute_timer"
    target_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                {select_prefix},
                year(CAST(datetime AS TIMESTAMP)) AS year,
                strftime(CAST(datetime AS TIMESTAMP), '%m') AS month
            FROM read_parquet({files_literal}, hive_partitioning=true, union_by_name=true)
            WHERE {where_sql}
        )
        TO {_sql_string(target_dir.as_posix())}
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE true)
        """
    )
    return row_count


def _copy_sqlite_sample(
    source_db: Path,
    target_db: Path,
    *,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    index_symbol: str,
    max_meta_rows: int,
) -> dict[str, int]:
    target_db.parent.mkdir(parents=True, exist_ok=True)
    if target_db.exists():
        target_db.unlink()

    src = _read_only_sqlite(source_db)
    dst = sqlite3.connect(target_db)
    dst.execute("PRAGMA foreign_keys = OFF")
    _copy_schema(src, dst)

    counts: dict[str, int] = {}
    symbol_where, symbol_params = _in_clause("symbol", list(symbols))
    date_range = (start_date.isoformat(), end_date.isoformat())

    for table in ("watchlist_groups", "factors", "factor_analysis", "strategies", "backtests", "orders", "trades"):
        counts[table] = _copy_rows(src, dst, table)

    for table in ("sync_runs", "sync_logs", "factor_research_runs", "factor_research_run_items"):
        counts[table] = _copy_rows(src, dst, table, suffix=_latest_rows_suffix(src, table, max_meta_rows))

    for table in ("stocks", "financial_data", "stock_name_changes", "stock_concept_memberships"):
        if _table_exists(src, table) and "symbol" in _table_columns(src, table):
            counts[table] = _copy_rows(src, dst, table, where=symbol_where, params=symbol_params)

    for table in ("stock_daily_basic", "stock_limit_prices"):
        if _table_exists(src, table):
            where = f"{symbol_where} AND trade_date >= ? AND trade_date <= ?"
            counts[table] = _copy_rows(src, dst, table, where=where, params=[*symbol_params, *date_range])

    if _table_exists(src, "index_components"):
        latest_date = _latest_index_date(src, index_symbol, end_date)
        index_where = (
            f"index_symbol = ? AND {symbol_where} "
            "AND ((trade_date >= ? AND trade_date <= ?) OR trade_date = ?)"
        )
        counts["index_components"] = _copy_rows(
            src,
            dst,
            "index_components",
            where=index_where,
            params=[index_symbol, *symbol_params, *date_range, latest_date or end_date.isoformat()],
        )

    if _table_exists(src, "watchlist_stocks"):
        counts["watchlist_stocks"] = _copy_rows(
            src,
            dst,
            "watchlist_stocks",
            where=symbol_where,
            params=symbol_params,
        )

    if _table_exists(src, "sentiment_posts"):
        where = f"{symbol_where} AND (published_at IS NULL OR date(published_at) >= ?)"
        counts["sentiment_posts"] = _copy_rows(
            src,
            dst,
            "sentiment_posts",
            where=where,
            params=[*symbol_params, (start_date - timedelta(days=30)).isoformat()],
            suffix=f"ORDER BY published_at DESC LIMIT {max_meta_rows}",
        )

    if _table_exists(src, "sentiment_threads"):
        counts["sentiment_threads"] = _copy_rows(
            src,
            dst,
            "sentiment_threads",
            where="last_reply_at IS NULL OR date(last_reply_at) >= ?",
            params=[(start_date - timedelta(days=30)).isoformat()],
            suffix=f"ORDER BY COALESCE(last_reply_at, published_at) DESC LIMIT {max_meta_rows}",
        )

    dst.execute(
        """
        CREATE TABLE IF NOT EXISTS dev_sample_manifest (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    dst.executemany(
        "INSERT OR REPLACE INTO dev_sample_manifest(key, value) VALUES (?, ?)",
        [
            ("created_at", datetime.now().isoformat(timespec="seconds")),
            ("source_db", str(source_db.resolve())),
            ("date_range", f"{start_date.isoformat()}..{end_date.isoformat()}"),
            ("symbol_count", str(len(symbols))),
            ("index_symbol", index_symbol),
        ],
    )
    dst.commit()
    dst.close()
    src.close()
    return counts


def _assert_safe_paths(source_data_dir: Path, target_data_dir: Path, *, allow_external_target: bool) -> None:
    source = source_data_dir.resolve()
    target = target_data_dir.resolve()
    repo = _REPO_ROOT.resolve()
    public_data = _DEFAULT_PUBLIC_DATA_DIR.resolve()
    target_is_public_sample = target.parent == source and target.name == "dev_sample"
    if source == target or target in source.parents or (source in target.parents and not target_is_public_sample):
        raise ValueError(f"Source and target must be independent: source={source}, target={target}")
    if not allow_external_target and not (target.is_relative_to(repo) or target.is_relative_to(public_data)):
        raise ValueError(f"Target must stay inside this dev workspace or public data root by default: {target}")


def _write_host_env(target_data_dir: Path) -> Path:
    host = socket.gethostname().split(".")[0]
    env_path = _REPO_ROOT / f".env.{host}.local"
    sqlite_path = (target_data_dir / "gaoshou.db").resolve().as_posix()
    parquet_path = (target_data_dir / "parquet").resolve().as_posix()
    env_path.write_text(
        "\n".join(
            [
                "# Generated by backend/app/scripts/create_dev_sample_data.py",
                "# Local-only override: keep dev storage isolated from production data.",
                "MARKET_DATA_BACKEND=parquet",
                f"PARQUET_DATA_DIR={parquet_path}",
                f"DATABASE_URL=sqlite+aiosqlite:///{sqlite_path}",
                "DUCKDB_PATH=:memory:",
                "ENABLE_SYNC_SCHEDULER=false",
                "LIVE_TRADING_ENABLE_ORDER_SUBMIT=false",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return env_path


def create_sample(args: argparse.Namespace) -> dict[str, object]:
    source_data_dir = Path(args.source_data_dir)
    target_data_dir = Path(args.target_data_dir)
    source_db = source_data_dir / "gaoshou.db"
    source_parquet = source_data_dir / "parquet"
    target_db = target_data_dir / "gaoshou.db"
    target_parquet = target_data_dir / "parquet"

    _assert_safe_paths(source_data_dir, target_data_dir, allow_external_target=args.allow_external_target)
    if target_data_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"Target already exists; pass --overwrite to replace it: {target_data_dir}")
        if target_data_dir.resolve() == _REPO_ROOT.resolve():
            raise ValueError("Refusing to delete repository root")
        shutil.rmtree(target_data_dir)
    target_data_dir.mkdir(parents=True, exist_ok=True)

    with _read_only_sqlite(source_db) as src:
        symbols = _select_symbols(
            src,
            index_symbol=args.index_symbol,
            as_of=args.end_date,
            max_symbols=args.max_symbols,
            extra_symbols=args.extra_symbols,
        )
    if not symbols:
        raise RuntimeError("No symbols selected from source database")

    sqlite_counts = _copy_sqlite_sample(
        source_db,
        target_db,
        symbols=symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        index_symbol=args.index_symbol,
        max_meta_rows=args.max_meta_rows,
    )

    con = duckdb.connect(":memory:")
    end_exclusive = datetime.combine(args.end_date + timedelta(days=1), time.min)
    start_datetime = datetime.combine(args.start_date, time.min)
    parquet_counts = {
        "klines_daily": _copy_parquet_dataset(
            con,
            source_parquet,
            target_parquet,
            "klines_daily",
            date_column="trade_date",
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
        ),
        "klines_minute": _copy_parquet_dataset(
            con,
            source_parquet,
            target_parquet,
            "klines_minute",
            date_column="datetime",
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
        ),
        "klines_minute_timer": _copy_minute_timer_dataset(
            con,
            source_parquet,
            target_parquet,
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
            timer_times=args.timer_times,
        ),
        "klines_minute_cum_timer": _copy_parquet_dataset(
            con,
            source_parquet,
            target_parquet,
            "klines_minute_cum_timer",
            date_column="datetime",
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
        ),
        "factor_values": _copy_parquet_dataset(
            con,
            source_parquet,
            target_parquet,
            "factor_values",
            date_column="trade_date",
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
        ),
        "stock_indicators": _copy_parquet_dataset(
            con,
            source_parquet,
            target_parquet,
            "stock_indicators",
            date_column="trade_date",
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
        ),
        "indicator_timeseries": _copy_parquet_dataset(
            con,
            source_parquet,
            target_parquet,
            "indicator_timeseries",
            date_column="datetime",
            start_dt=start_datetime,
            end_dt=end_exclusive,
            symbols=symbols,
        ),
        "stock_indicators_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "stock_indicators",
            date_column="trade_date",
            symbols=symbols,
        ),
        "indicator_timeseries_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "indicator_timeseries",
            date_column="datetime",
            symbols=symbols,
        ),
        "ths_index_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "ths_index",
            date_column="snapshot_date",
            max_rows=1000,
        ),
        "ths_member_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "ths_member",
            date_column="snapshot_date",
            symbols=symbols,
        ),
        "adj_factors_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "adj_factors",
            date_column="trade_date",
            max_rows=1000,
        ),
        "moneyflow_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "moneyflow",
            date_column="trade_date",
            max_rows=1000,
        ),
        "auction_replay_latest": _copy_parquet_latest_dataset(
            con,
            source_parquet,
            target_parquet,
            "auction_replay",
            date_column="datetime",
            max_rows=1000,
        ),
    }
    con.close()

    env_path = _write_host_env(target_data_dir) if args.write_host_env else None
    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_data_dir": str(source_data_dir.resolve()),
        "target_data_dir": str(target_data_dir.resolve()),
        "sqlite_db": str(target_db.resolve()),
        "parquet_data_dir": str(target_parquet.resolve()),
        "date_range": [args.start_date.isoformat(), args.end_date.isoformat()],
        "index_symbol": args.index_symbol,
        "symbols": symbols,
        "sqlite_counts": sqlite_counts,
        "parquet_counts": parquet_counts,
        "host_env": str(env_path.resolve()) if env_path else None,
        "safety": {
            "source_sqlite_mode": "read-only",
            "market_data_backend": "parquet",
            "target_isolated": True,
        },
    }
    (target_data_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create isolated dev sample data from production storage.")
    parser.add_argument("--source-data-dir", default=str(_DEFAULT_SOURCE_DATA_DIR), help="Production data root to read from.")
    parser.add_argument("--target-data-dir", default=str(_DEFAULT_TARGET_DATA_DIR), help="Dev sample data root to write.")
    parser.add_argument("--start-date", type=_parse_date, default=date(2026, 5, 1), help="Inclusive sample start date.")
    parser.add_argument("--end-date", type=_parse_date, default=date(2026, 5, 15), help="Inclusive sample end date.")
    parser.add_argument("--index-symbol", default=_DEFAULT_INDEX_SYMBOL, help="Index pool used to select representative symbols.")
    parser.add_argument("--max-symbols", type=int, default=60, help="Maximum index constituents to include before extra symbols.")
    parser.add_argument("--extra-symbols", nargs="*", default=list(_DEFAULT_EXTRA_SYMBOLS), help="Additional symbols to include if present.")
    parser.add_argument("--timer-times", nargs="*", default=["09:35", "14:50"], help="Timer minutes to materialize into klines_minute_timer.")
    parser.add_argument("--max-meta-rows", type=int, default=100, help="Max rows for UI metadata/history tables.")
    parser.add_argument("--overwrite", action="store_true", help="Replace the existing target sample directory.")
    parser.add_argument("--write-host-env", action="store_true", help="Write ignored .env.<hostname>.local to point dev at the sample.")
    parser.add_argument("--allow-external-target", action="store_true", help="Allow target outside this dev workspace.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    manifest = create_sample(args)
    print(json.dumps({
        "target_data_dir": manifest["target_data_dir"],
        "sqlite_counts": manifest["sqlite_counts"],
        "parquet_counts": manifest["parquet_counts"],
        "symbol_count": len(manifest["symbols"]),
        "host_env": manifest["host_env"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
