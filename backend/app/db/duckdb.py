"""DuckDB 连接管理 — 供 Parquet store 查询使用"""
from __future__ import annotations

import threading
from pathlib import Path

import duckdb

from app.core.config import settings

_local = threading.local()


def get_duckdb(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """获取线程本地 DuckDB 连接"""
    key = f"_duckdb_{'ro' if read_only else 'rw'}"
    conn = getattr(_local, key, None)
    if conn is None:
        db_path = settings.duckdb_path
        if db_path == ":memory:":
            conn = duckdb.connect(":memory:")
        else:
            p = Path(db_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            conn = duckdb.connect(str(p), read_only=read_only)
        setattr(_local, key, conn)
    return conn


def query_parquet(
    sql: str,
    params: dict | None = None,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[tuple]:
    """执行 DuckDB 查询并返回行列表"""
    db = conn or get_duckdb()
    if params:
        return db.execute(sql, params).fetchall()
    return db.execute(sql).fetchall()


def query_parquet_df(
    sql: str,
    params: dict | None = None,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
):
    """执行 DuckDB 查询并返回 DataFrame"""
    import pandas as pd

    db = conn or get_duckdb()
    if params:
        return db.execute(sql, params).df()
    return db.execute(sql).df()


def close_duckdb() -> None:
    """关闭当前线程的 DuckDB 连接"""
    for suffix in ("_duckdb_ro", "_duckdb_rw"):
        conn = getattr(_local, suffix, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            setattr(_local, suffix, None)
