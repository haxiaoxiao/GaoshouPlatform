"""Parquet 数据浏览器 API — /api/explorer/parquet"""
from datetime import date

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
    "stock_indicators",
    "feature_values",
    "indicator_timeseries",
]


def _get_store():
    return ParquetMarketDataStore(settings.parquet_data_dir)


@router.get("/datasets")
async def list_datasets():
    """列出所有 Parquet dataset 及其基本信息"""
    store = _get_store()
    datasets = []
    for name in _DATASETS:
        if store._exists(name):
            path = store._dataset_path(name)
            # Count files
            n_files = len(list(path.rglob("*.parquet")))
            datasets.append({"name": name, "path": str(path), "files": n_files})
    return {"code": 0, "data": datasets}


@router.get("/{dataset}/schema")
async def dataset_schema(dataset: str):
    """返回 Parquet dataset 的字段 schema"""
    store = _get_store()
    if not store._exists(dataset):
        return {"code": 1, "message": f"Dataset '{dataset}' not found"}
    pattern = store._glob_pattern(dataset)
    db = get_duckdb()
    try:
        df = db.execute(
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true) LIMIT 1"
        ).df()
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
):
    """预览 Parquet dataset 数据"""
    store = _get_store()
    if not store._exists(dataset):
        return {"code": 1, "message": f"Dataset '{dataset}' not found"}
    pattern = store._glob_pattern(dataset)
    db = get_duckdb()
    try:
        count = db.execute(
            f"SELECT count(*) FROM read_parquet('{pattern}', hive_partitioning=true)"
        ).fetchone()[0]
        df = db.execute(
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true) LIMIT {limit} OFFSET {offset}"
        ).df()
        return {"code": 0, "data": {
            "total_rows": count,
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
    info = store.coverage(
        sym_list if sym_list else [],
        date.fromisoformat(start),
        date.fromisoformat(end),
        dataset=dataset,
    )
    return {"code": 0, "data": info}
