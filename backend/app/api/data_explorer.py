"""Data explorer API for local Parquet/DuckDB."""
import calendar
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.config import settings
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.duckdb import get_duckdb
from app.services.parquet_dataset_catalog import (
    PARQUET_DATE_COLUMNS,
    build_dataset_summary,
    get_parquet_dataset_spec,
    iter_parquet_dataset_specs,
)
from app.services.tushare_relay_sync import dataset_coverage

router = APIRouter()

_ALLOWED_FILTER_OPS = {
    "=",
    "!=",
    "contains",
    "in",
    "between",
    ">",
    ">=",
    "<",
    "<=",
    "is null",
    "not null",
}


class ExplorerFilter(BaseModel):
    column: str
    op: str = Field(default="=")
    value: Any = None
    value_to: Any = None
    values: list[Any] | None = None


class ExplorerSearchRequest(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)
    order_by: str | None = None
    order_dir: str = Field(default="ASC")
    columns: list[str] | None = None
    filters: list[ExplorerFilter] = Field(default_factory=list)
    quick_search: dict[str, Any] = Field(default_factory=dict)
    include_total: bool = False


def _use_parquet() -> bool:
    return True


def _parquet_store() -> ParquetMarketDataStore:
    return ParquetMarketDataStore(settings.parquet_data_dir)


def _parquet_pattern(dataset: str) -> str:
    store = _parquet_store()
    if not _is_safe_dataset_name(dataset) or not store._exists(dataset):
        raise ValueError(f"Dataset '{dataset}' not found")
    return store._glob_pattern(dataset)


def _is_safe_dataset_name(dataset: str) -> bool:
    return bool(dataset) and "/" not in dataset and "\\" not in dataset and dataset not in {".", ".."}


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return "'" + str(value).replace("'", "''") + "'"


def _safe_identifier(name: str, columns: list[str]) -> str | None:
    return name if name in columns else None


def _quoted_identifier(name: str, *, backend: str) -> str:
    return f'"{name}"'


def _validate_table(table_name: str) -> None:
    _parquet_pattern(table_name)


def _schema_columns(table_name: str) -> list[str]:
    return [col["name"] for col in _parquet_schema(table_name)]


def _build_filter_sql(filter_rule: ExplorerFilter, columns: list[str], *, backend: str) -> str | None:
    column = _safe_identifier(filter_rule.column, columns)
    if not column:
        raise HTTPException(status_code=400, detail=f"Unknown column: {filter_rule.column}")

    op = str(filter_rule.op or "=").strip().lower()
    if op not in _ALLOWED_FILTER_OPS:
        raise HTTPException(status_code=400, detail=f"Unsupported operator: {filter_rule.op}")

    ident = _quoted_identifier(column, backend=backend)
    if op == "is null":
        return f"{ident} IS NULL"
    if op == "not null":
        return f"{ident} IS NOT NULL"
    if op == "contains":
        value = str(filter_rule.value or "").strip()
        if not value:
            return None
        text_expr = f"CAST({ident} AS VARCHAR)"
        return f"{text_expr} LIKE {_sql_literal('%' + value + '%')}"
    if op == "in":
        values = filter_rule.values if filter_rule.values is not None else filter_rule.value
        if isinstance(values, str):
            values = [item.strip() for item in values.split(",") if item.strip()]
        if not isinstance(values, list) or not values:
            return None
        return f"{ident} IN ({', '.join(_sql_literal(value) for value in values)})"
    if op == "between":
        if filter_rule.value in (None, "") or filter_rule.value_to in (None, ""):
            return None
        return f"{ident} BETWEEN {_sql_literal(filter_rule.value)} AND {_sql_literal(filter_rule.value_to)}"
    if filter_rule.value in (None, ""):
        return None
    return f"{ident} {op.upper()} {_sql_literal(filter_rule.value)}"


def _quick_filters(table_name: str, quick_search: dict[str, Any], columns: list[str]) -> list[ExplorerFilter]:
    filters: list[ExplorerFilter] = []
    symbol_value = str(quick_search.get("symbol") or "").strip()
    if symbol_value and "symbol" in columns:
        symbols = [item.strip().upper() for item in symbol_value.replace("，", ",").split(",") if item.strip()]
        filters.append(ExplorerFilter(column="symbol", op="in" if len(symbols) > 1 else "=", value=symbols[0] if len(symbols) == 1 else None, values=symbols if len(symbols) > 1 else None))

    catalog_date_col = PARQUET_DATE_COLUMNS.get(table_name)
    date_col = catalog_date_col if catalog_date_col in columns else next(
        (
            col
            for col in (
                "trade_date_1",
                "trade_date",
                "datetime",
                "available_date",
                "date",
                "snapshot_date",
                "ann_date",
                "f_ann_date",
                "end_date",
                "report_date",
                "publish_time",
                "time",
            )
            if col in columns
        ),
        None,
    )
    start_date = str(quick_search.get("start_date") or "").strip()
    end_date = str(quick_search.get("end_date") or "").strip()
    if date_col and start_date and end_date:
        filters.append(ExplorerFilter(column=date_col, op="between", value=start_date, value_to=end_date))
    elif date_col and start_date:
        filters.append(ExplorerFilter(column=date_col, op=">=", value=start_date))
    elif date_col and end_date:
        filters.append(ExplorerFilter(column=date_col, op="<=", value=end_date))

    for key in ("factor_name", "indicator_name", "as_of_time", "source"):
        value = str(quick_search.get(key) or "").strip()
        if value and key in columns:
            filters.append(ExplorerFilter(column=key, op="=", value=value))
    return filters


def _build_search_sql(
    table_name: str,
    request: ExplorerSearchRequest,
    *,
    backend: str,
) -> tuple[str, str, list[str], str]:
    columns = _schema_columns(table_name)
    selected_columns = request.columns or columns
    invalid_columns = [column for column in selected_columns if column not in columns]
    if invalid_columns:
        raise HTTPException(status_code=400, detail=f"Unknown columns: {invalid_columns}")

    source = (
        f"read_parquet({_sql_literal(_parquet_pattern(table_name))}, hive_partitioning=true)"
        if backend == "parquet"
        else f"`{table_name}`"
    )
    conditions = [
        condition
        for rule in [*_quick_filters(table_name, request.quick_search or {}, columns), *request.filters]
        if (condition := _build_filter_sql(rule, columns, backend=backend))
    ]
    where_sql = "WHERE " + " AND ".join(conditions) if conditions else ""
    safe_dir = "DESC" if str(request.order_dir).upper() == "DESC" else "ASC"
    order_clause = ""
    if request.order_by:
        safe_order = _safe_identifier(request.order_by, columns)
        if not safe_order:
            raise HTTPException(status_code=400, detail=f"Unknown order_by: {request.order_by}")
        order_clause = f"ORDER BY {_quoted_identifier(safe_order, backend=backend)} {safe_dir}"
    select_sql = ", ".join(_quoted_identifier(column, backend=backend) for column in selected_columns)
    return source, f"SELECT {select_sql} FROM {source} {where_sql} {order_clause}", selected_columns, where_sql


def _normalize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _parquet_schema(table_name: str) -> list[dict[str, Any]]:
    pattern = _parquet_pattern(table_name)
    df = get_duckdb().execute(
        f"SELECT * FROM read_parquet({_sql_literal(pattern)}, hive_partitioning=true) LIMIT 0"
    ).df()
    return [
        {"name": name, "type": str(dtype), "default": None, "comment": None}
        for name, dtype in df.dtypes.items()
    ]


def _partition_bounds(root) -> tuple[int, str | None, str | None]:
    partitions: list[tuple[int, int]] = []
    for year_dir in root.glob("year=*"):
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
    if not partitions:
        return 0, None, None
    min_year, min_month = min(partitions)
    max_year, max_month = max(partitions)
    max_day = calendar.monthrange(max_year, max_month)[1]
    return (
        len(partitions),
        f"{min_year:04d}-{min_month:02d}-01",
        f"{max_year:04d}-{max_month:02d}-{max_day:02d}",
    )


def _fast_parquet_table_summary(store: ParquetMarketDataStore, name: str) -> dict[str, Any] | None:
    root = store._dataset_path(name)
    if not root.exists():
        return None
    partition_count, min_date, max_date = _partition_bounds(root)
    if partition_count == 0 and not any(".tmp-" not in str(file) for file in root.glob("*.parquet")):
        return None

    spec = get_parquet_dataset_spec(name)
    date_col = PARQUET_DATE_COLUMNS.get(name)
    if date_col == "datetime" and min_date and max_date:
        min_date = f"{min_date} 00:00:00"
        max_date = f"{max_date} 23:59:59"

    row_count: int | None = None
    estimated = True
    if spec and spec.exact_summary and date_col:
        try:
            exact = dataset_coverage(name, date_col, exact=True)
            if not exact.get("error"):
                row_count = int(exact.get("row_count") or 0)
                min_date = _normalize_value(exact.get("min_date")) or min_date
                max_date = _normalize_value(exact.get("max_date")) or max_date
                estimated = False
        except Exception:
            row_count = None
            estimated = True

    summary = {
        "row_count": row_count,
        "rows": row_count,
        "count": row_count,
        "min_date": min_date,
        "max_date": max_date,
        "date_column": date_col,
        "estimated": estimated,
        "partition_count": partition_count,
    }
    if spec is not None:
        return build_dataset_summary(spec, **summary)
    return {"name": name, **summary}


def _estimated_total(offset: int, row_count: int, page_size: int) -> int:
    return offset + row_count + (1 if row_count >= page_size else 0)


@router.get("/tables", summary="List available data tables")
def list_tables():
    store = _parquet_store()
    tables = []
    for spec in iter_parquet_dataset_specs(settings.parquet_data_dir):
        summary = _fast_parquet_table_summary(store, spec.name)
        if summary is not None:
            tables.append(summary)
    return {"code": 0, "data": tables}


@router.get("/tables/{table_name}/schema", summary="Get table schema")
def get_table_schema(table_name: str):
    try:
        return {"code": 0, "data": _parquet_schema(table_name)}
    except Exception as e:
        return {"code": 1, "message": str(e)}


@router.get("/tables/{table_name}/preview", summary="Preview table rows")
def preview_table(
    table_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    order_by: str | None = Query(None),
    order_dir: str = Query("ASC"),
    where: str | None = Query(None),
    include_total: bool = Query(False),
):
    try:
        pattern = _parquet_pattern(table_name)
        columns = [col["name"] for col in _parquet_schema(table_name)]
        safe_dir = "ASC" if order_dir.upper() == "ASC" else "DESC"
        where_clause = f"WHERE {where}" if where else ""
        safe_order = _safe_identifier(order_by, columns) if order_by else None
        order_clause = f'ORDER BY "{safe_order}" {safe_dir}' if safe_order else ""
        offset = (page - 1) * page_size
        source = f"read_parquet({_sql_literal(pattern)}, hive_partitioning=true)"
        db = get_duckdb()
        raw_rows = db.execute(
            f"SELECT * FROM {source} {where_clause} {order_clause} LIMIT {page_size} OFFSET {offset}"
        ).fetchall()
        total_estimated = not include_total
        if include_total:
            total = int(db.execute(f"SELECT count(*) FROM {source} {where_clause}").fetchone()[0] or 0)
        else:
            total = _estimated_total(offset, len(raw_rows), page_size)
        rows = [
            {col: _normalize_value(raw[i]) for i, col in enumerate(columns)}
            for raw in raw_rows
        ]
        return {
            "code": 0,
            "data": {
                "columns": columns,
                "rows": rows,
                "total": int(total or 0),
                "total_estimated": total_estimated,
                "page": page,
                "page_size": page_size,
                "total_pages": (int(total or 0) + page_size - 1) // page_size,
            },
        }
    except Exception as e:
        return {"code": 1, "message": str(e)}


@router.post("/tables/{table_name}/search", summary="Search table rows")
def search_table(
    table_name: str,
    request: ExplorerSearchRequest = Body(...),
):
    try:
        _validate_table(table_name)
    except ValueError as exc:
        return {
            "code": 1,
            "message": str(exc),
            "data": {
                "columns": request.columns or [],
                "rows": [],
                "total": 0,
                "page": request.page,
                "page_size": request.page_size,
                "total_pages": 0,
            },
        }
    backend = "parquet"
    source, select_sql, selected_columns, where_sql = _build_search_sql(table_name, request, backend=backend)
    offset = (request.page - 1) * request.page_size
    data_sql = f"{select_sql} LIMIT {request.page_size} OFFSET {offset}"
    try:
        db = get_duckdb()
        raw_rows = db.execute(data_sql).fetchall()
        total = (
            int(db.execute(f"SELECT count(*) FROM {source} {where_sql}").fetchone()[0] or 0)
            if request.include_total
            else _estimated_total(offset, len(raw_rows), request.page_size)
        )
        rows = [
            {col: _normalize_value(raw[index]) for index, col in enumerate(selected_columns)}
            for raw in raw_rows
        ]
        return {
            "code": 0,
            "data": {
                "columns": selected_columns,
                "rows": rows,
                "total": int(total or 0),
                "total_estimated": not request.include_total,
                "page": request.page,
                "page_size": request.page_size,
                "total_pages": (int(total or 0) + request.page_size - 1) // request.page_size,
                "generated_sql": data_sql,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tables/{table_name}/distinct", summary="Get distinct column values")
def get_distinct_values(
    table_name: str,
    column: str = Query(...),
    q: str | None = Query(default=None),
    limit: int = Query(100, ge=1, le=1000),
):
    try:
        pattern = _parquet_pattern(table_name)
        columns = [col["name"] for col in _parquet_schema(table_name)]
        safe_column = _safe_identifier(column, columns)
        if not safe_column:
            return {"code": 1, "message": f"Unknown column: {column}"}
        where_sql = ""
        if q:
            where_sql = f'WHERE CAST("{safe_column}" AS VARCHAR) LIKE {_sql_literal("%" + q + "%")}'
        rows = get_duckdb().execute(
            f'SELECT DISTINCT "{safe_column}" FROM read_parquet({_sql_literal(pattern)}, hive_partitioning=true) {where_sql} LIMIT {limit}'
        ).fetchall()
        return {"code": 0, "data": [_normalize_value(row[0]) for row in rows]}
    except Exception as e:
        return {"code": 1, "message": str(e)}


@router.post("/query", summary="Run a read-only SQL query")
def execute_query(
    sql: str = Query(..., description="SQL query", max_length=2000),
    limit: int = Query(200, ge=1, le=1000),
):
    upper = sql.strip().upper()
    if not upper.startswith("SELECT"):
        return {"code": 1, "message": "Only SELECT queries are allowed"}

    if "LIMIT" not in upper:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    try:
        relation = get_duckdb().execute(sql)
        columns = [desc[0] for desc in (relation.description or [])]
        raw_rows = relation.fetchall()
        rows = [
            {column: _normalize_value(raw[index]) for index, column in enumerate(columns)}
            for raw in raw_rows
        ]
        return {"code": 0, "data": {"columns": columns, "rows": rows, "total": len(rows)}}
    except Exception as e:
        return {"code": 1, "message": str(e)}
