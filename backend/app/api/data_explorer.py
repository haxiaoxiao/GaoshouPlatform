"""Data explorer API for ClickHouse or local Parquet/DuckDB."""
import calendar
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.config import settings
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.clickhouse import get_ch_client
from app.db.duckdb import get_duckdb

router = APIRouter()

_PARQUET_DATASETS = [
    "klines_daily",
    "klines_minute",
    "klines_minute_timer",
    "klines_minute_cum_timer",
    "factor_cache",
    "factor_values",
    "stock_indicators",
    "indicator_timeseries",
    "adj_factors",
    "moneyflow",
    "block_moneyflow",
    "auction_replay",
    "ths_index",
    "ths_member",
    "announcements",
    "research_reports",
    "market_news",
]

_PARQUET_DATE_COLUMNS = {
    "klines_daily": "trade_date",
    "klines_minute": "datetime",
    "klines_minute_timer": "datetime",
    "klines_minute_cum_timer": "datetime",
    "factor_cache": "trade_date",
    "factor_values": "trade_date",
    "stock_indicators": "trade_date",
    "indicator_timeseries": "datetime",
    "adj_factors": "trade_date",
    "moneyflow": "trade_date",
    "block_moneyflow": "trade_date",
    "auction_replay": "datetime",
    "ths_index": "snapshot_date",
    "ths_member": "snapshot_date",
    "announcements": "ann_date",
    "research_reports": "report_date",
    "market_news": "publish_time",
}

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
    return settings.market_data_backend == "parquet" and not settings.clickhouse_enabled


def _parquet_store() -> ParquetMarketDataStore:
    return ParquetMarketDataStore(settings.parquet_data_dir)


def _parquet_pattern(dataset: str) -> str:
    store = _parquet_store()
    if dataset not in _PARQUET_DATASETS or not store._exists(dataset):
        raise ValueError(f"Dataset '{dataset}' not found")
    return store._glob_pattern(dataset)


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return "'" + str(value).replace("'", "''") + "'"


def _safe_identifier(name: str, columns: list[str]) -> str | None:
    return name if name in columns else None


def _quoted_identifier(name: str, *, backend: str) -> str:
    if backend == "clickhouse":
        return f"`{name}`"
    return f'"{name}"'


def _validate_table(table_name: str) -> None:
    if _use_parquet():
        _parquet_pattern(table_name)
        return
    available = {row[0] for row in get_ch_client().execute("SHOW TABLES")}
    if table_name not in available:
        raise HTTPException(status_code=404, detail=f"Unknown table: {table_name}")


def _schema_columns(table_name: str) -> list[str]:
    if _use_parquet():
        return [col["name"] for col in _parquet_schema(table_name)]
    rows = get_ch_client().execute(f"DESCRIBE TABLE `{table_name}`")
    return [row[0] for row in rows]


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
        text_expr = f"toString({ident})" if backend == "clickhouse" else f"CAST({ident} AS VARCHAR)"
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


def _quick_filters(quick_search: dict[str, Any], columns: list[str]) -> list[ExplorerFilter]:
    filters: list[ExplorerFilter] = []
    symbol_value = str(quick_search.get("symbol") or "").strip()
    if symbol_value and "symbol" in columns:
        symbols = [item.strip().upper() for item in symbol_value.replace("，", ",").split(",") if item.strip()]
        filters.append(ExplorerFilter(column="symbol", op="in" if len(symbols) > 1 else "=", value=symbols[0] if len(symbols) == 1 else None, values=symbols if len(symbols) > 1 else None))

    date_col = next((col for col in ("trade_date", "datetime", "date", "snapshot_date", "ann_date", "report_date", "publish_time") if col in columns), None)
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
        for rule in [*_quick_filters(request.quick_search or {}, columns), *request.filters]
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

    date_col = _PARQUET_DATE_COLUMNS.get(name)
    if date_col == "datetime" and min_date and max_date:
        min_date = f"{min_date} 00:00:00"
        max_date = f"{max_date} 23:59:59"

    return {
        "name": name,
        "row_count": None,
        "rows": None,
        "count": None,
        "min_date": min_date,
        "max_date": max_date,
        "date_column": date_col,
        "estimated": True,
        "partition_count": partition_count,
    }


def _estimated_total(offset: int, row_count: int, page_size: int) -> int:
    return offset + row_count + (1 if row_count >= page_size else 0)


@router.get("/tables", summary="List available data tables")
def list_tables():
    if _use_parquet():
        store = _parquet_store()
        tables = []
        for name in _PARQUET_DATASETS:
            summary = _fast_parquet_table_summary(store, name)
            if summary is not None:
                tables.append(summary)
        return {"code": 0, "data": tables}

    ch = get_ch_client()
    rows = ch.execute("SHOW TABLES")
    tables = []
    for row in rows:
        name = row[0]
        count_row = ch.execute(f"SELECT count() FROM `{name}`")[0][0]
        tables.append({"name": name, "row_count": count_row})
    return {"code": 0, "data": tables}


@router.get("/tables/{table_name}/schema", summary="Get table schema")
def get_table_schema(table_name: str):
    if _use_parquet():
        try:
            return {"code": 0, "data": _parquet_schema(table_name)}
        except Exception as e:
            return {"code": 1, "message": str(e)}

    ch = get_ch_client()
    rows = ch.execute(f"DESCRIBE TABLE `{table_name}`")
    columns = []
    for row in rows:
        columns.append({
            "name": row[0],
            "type": row[1],
            "default": row[2] if len(row) > 2 else None,
            "comment": row[3] if len(row) > 3 else None,
        })
    return {"code": 0, "data": columns}


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
    if _use_parquet():
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

    ch = get_ch_client()

    safe_dir = "ASC" if order_dir.upper() == "ASC" else "DESC"

    where_clause = f"WHERE {where}" if where else ""
    order_clause = f"ORDER BY `{order_by}` {safe_dir}" if order_by else ""
    offset = (page - 1) * page_size

    data_sql = f"SELECT * FROM `{table_name}` {where_clause} {order_clause} LIMIT {page_size} OFFSET {offset}"
    raw_rows = ch.execute(data_sql)
    if include_total:
        count_sql = f"SELECT count() FROM `{table_name}` {where_clause}"
        total = int(ch.execute(count_sql)[0][0] or 0)
    else:
        total = _estimated_total(offset, len(raw_rows), page_size)

    col_names = [desc[0] for desc in ch.execute(f"DESCRIBE TABLE `{table_name}`")]

    rows = []
    for raw in raw_rows:
        row = {}
        for i, col in enumerate(col_names):
            val = raw[i]
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            elif isinstance(val, (bytes,)):
                val = val.decode("utf-8", errors="replace")
            row[col] = val
        rows.append(row)

    return {
        "code": 0,
        "data": {
            "columns": col_names,
            "rows": rows,
            "total": total,
            "total_estimated": not include_total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        },
    }


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
    backend = "parquet" if _use_parquet() else "clickhouse"
    source, select_sql, selected_columns, where_sql = _build_search_sql(table_name, request, backend=backend)
    offset = (request.page - 1) * request.page_size
    data_sql = f"{select_sql} LIMIT {request.page_size} OFFSET {offset}"
    try:
        if backend == "parquet":
            db = get_duckdb()
            raw_rows = db.execute(data_sql).fetchall()
            total = (
                int(db.execute(f"SELECT count(*) FROM {source} {where_sql}").fetchone()[0] or 0)
                if request.include_total
                else _estimated_total(offset, len(raw_rows), request.page_size)
            )
        else:
            ch = get_ch_client()
            raw_rows = ch.execute(data_sql)
            total = (
                int(ch.execute(f"SELECT count(*) FROM {source} {where_sql}")[0][0] or 0)
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
    if _use_parquet():
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

    ch = get_ch_client()
    columns = _schema_columns(table_name)
    safe_column = _safe_identifier(column, columns)
    if not safe_column:
        return {"code": 1, "message": f"Unknown column: {column}"}
    where_sql = f"WHERE toString(`{safe_column}`) LIKE {_sql_literal('%' + q + '%')}" if q else ""
    rows = ch.execute(f"SELECT DISTINCT `{safe_column}` FROM `{table_name}` {where_sql} LIMIT {limit}")
    values = [row[0] for row in rows]
    if values and hasattr(values[0], "isoformat"):
        values = [v.isoformat() for v in values]
    return {"code": 0, "data": values}


@router.post("/query", summary="Run a read-only SQL query")
def execute_query(
    sql: str = Query(..., description="SQL query", max_length=2000),
    limit: int = Query(200, ge=1, le=1000),
):
    ch = get_ch_client()

    upper = sql.strip().upper()
    if not upper.startswith("SELECT") and not upper.startswith("SHOW") and not upper.startswith("DESCRIBE"):
        return {"code": 1, "message": "Only SELECT/SHOW/DESCRIBE queries are allowed"}

    if "LIMIT" not in upper:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    try:
        raw_rows = ch.execute(sql)
        if not raw_rows:
            return {"code": 0, "data": {"columns": [], "rows": [], "total": 0}}

        col_names = [desc[0] for desc in ch.execute(f"DESCRIBE TABLE `{_extract_table(sql)}`")] if raw_rows else []

        if not col_names:
            col_names = [f"col_{i}" for i in range(len(raw_rows[0]))]

        rows = []
        for raw in raw_rows:
            row = {}
            for i, col in enumerate(col_names):
                val = raw[i] if i < len(raw) else None
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                elif isinstance(val, bytes):
                    val = val.decode("utf-8", errors="replace")
                row[col] = val
            rows.append(row)

        return {"code": 0, "data": {"columns": col_names, "rows": rows, "total": len(rows)}}
    except Exception as e:
        return {"code": 1, "message": str(e)}


def _extract_table(sql: str) -> str:
    import re
    m = re.search(r'FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
    return m.group(1) if m else "unknown"
