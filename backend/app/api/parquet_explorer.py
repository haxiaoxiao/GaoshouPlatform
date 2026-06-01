import asyncio
import calendar
from datetime import date
from typing import Any

from fastapi import APIRouter, Query

from app.core.config import settings
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.duckdb import get_duckdb

router = APIRouter(prefix="/explorer/parquet", tags=["parquet"])

_DATASETS = [
    "klines_daily",
    "klines_minute",
    "klines_minute_timer",
    "klines_minute_cum_timer",
    "factor_cache",
    "factor_values",
    "stock_indicators",
    "indicator_timeseries",
]

_DATASET_DATE_COLUMNS = {
    "klines_daily": "trade_date",
    "klines_minute": "datetime",
    "klines_minute_timer": "datetime",
    "klines_minute_cum_timer": "datetime",
    "factor_cache": "trade_date",
    "factor_values": "trade_date",
    "stock_indicators": "trade_date",
    "indicator_timeseries": "datetime",
}


def _get_store():
    return ParquetMarketDataStore(settings.parquet_data_dir)


def _partition_summary(path, dataset: str) -> dict[str, Any] | None:
    partitions: list[tuple[int, int]] = []
    for year_dir in path.glob("year=*"):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        for month_dir in year_dir.glob("month=??"):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            if any(".tmp-" not in str(file) for file in month_dir.glob("*.parquet")):
                partitions.append((year, month))
    if not partitions and not any(".tmp-" not in str(file) for file in path.glob("*.parquet")):
        return None

    min_date = max_date = None
    if partitions:
        min_year, min_month = min(partitions)
        max_year, max_month = max(partitions)
        max_day = calendar.monthrange(max_year, max_month)[1]
        min_date = f"{min_year:04d}-{min_month:02d}-01"
        max_date = f"{max_year:04d}-{max_month:02d}-{max_day:02d}"
        if _DATASET_DATE_COLUMNS.get(dataset) == "datetime":
            min_date = f"{min_date} 00:00:00"
            max_date = f"{max_date} 23:59:59"

    return {
        "partition_count": len(partitions),
        "row_count": None,
        "estimated": True,
        "min_date": min_date,
        "max_date": max_date,
        "date_column": _DATASET_DATE_COLUMNS.get(dataset),
    }


def _estimated_total(offset: int, row_count: int, limit: int) -> int:
    return offset + row_count + (1 if row_count >= limit else 0)


@router.get("/datasets")
async def list_datasets():
    """List Parquet datasets without scanning large file trees."""
    store = _get_store()
    datasets = []
    for name in _DATASETS:
        path = store._dataset_path(name)
        if path.exists():
            summary = _partition_summary(path, name)
            if summary is None:
                continue
            datasets.append({"name": name, "path": str(path), **summary})
    return {"code": 0, "data": datasets}


@router.get("/{dataset}/schema")
async def dataset_schema(dataset: str):
    """Return the Parquet dataset schema."""
    store = _get_store()
    if not store._exists(dataset):
        return {"code": 1, "message": f"Dataset '{dataset}' not found"}
    pattern = store._glob_pattern(dataset)
    try:
        def _read_schema():
            return get_duckdb().execute(
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true) LIMIT 1"
            ).df()

        df = await asyncio.to_thread(_read_schema)
        return {"code": 0, "data": {
            "columns": df.columns.tolist(),
            "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        }}
    except Exception as e:
        return {"code": 1, "message": str(e)}


@router.get("/{dataset}/preview")
async def dataset_preview(
    dataset: str,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    include_total: bool = Query(default=False),
):
    """Preview rows; exact totals are opt-in because large Parquet counts are expensive."""
    store = _get_store()
    if not store._exists(dataset):
        return {"code": 1, "message": f"Dataset '{dataset}' not found"}
    pattern = store._glob_pattern(dataset)
    try:
        def _read_preview():
            db = get_duckdb()
            df = db.execute(
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true) LIMIT {limit} OFFSET {offset}"
            ).df()
            count = None
            if include_total:
                count = db.execute(
                    f"SELECT count(*) FROM read_parquet('{pattern}', hive_partitioning=true)"
                ).fetchone()[0]
            return df, count

        df, count = await asyncio.to_thread(_read_preview)
        total_rows = int(count or 0) if include_total else _estimated_total(offset, len(df), limit)
        return {"code": 0, "data": {
            "total_rows": total_rows,
            "total_estimated": not include_total,
            "rows": df.fillna("").to_dict(orient="records"),
        }}
    except Exception as e:
        return {"code": 1, "message": str(e)}


@router.get("/{dataset}/coverage")
async def dataset_coverage(
    dataset: str,
    symbols: str = Query(default="", description="股票代码，逗号分隔"),
    start: str = Query(default="2000-01-01"),
    end: str = Query(default="2099-12-31"),
):
    """查询指定 dataset 中 symbol 的覆盖日期范围"""
    store = _get_store()
    if not store._exists(dataset):
        return {"code": 1, "message": f"Dataset '{dataset}' not found"}
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    info = await asyncio.to_thread(
        store.coverage,
        sym_list if sym_list else [],
        date.fromisoformat(start),
        date.fromisoformat(end),
        dataset=dataset,
    )
    return {"code": 0, "data": info}
