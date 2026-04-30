# backend/app/api/data_explorer.py
"""ClickHouse数据浏览器API"""
from fastapi import APIRouter, Query
from typing import Any

from app.db.clickhouse import get_ch_client

router = APIRouter()


@router.get("/tables", summary="获取所有表")
def list_tables():
    ch = get_ch_client()
    rows = ch.execute("SHOW TABLES")
    tables = []
    for row in rows:
        name = row[0]
        count_row = ch.execute(f"SELECT count() FROM `{name}`")[0][0]
        tables.append({"name": name, "row_count": count_row})
    return {"code": 0, "data": tables}


@router.get("/tables/{table_name}/schema", summary="获取表结构")
def get_table_schema(table_name: str):
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


@router.get("/tables/{table_name}/preview", summary="预览表数据")
def preview_table(
    table_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    order_by: str | None = Query(None),
    order_dir: str = Query("ASC"),
    where: str | None = Query(None),
):
    ch = get_ch_client()

    safe_dir = "ASC" if order_dir.upper() == "ASC" else "DESC"

    where_clause = f"WHERE {where}" if where else ""
    order_clause = f"ORDER BY `{order_by}` {safe_dir}" if order_by else ""
    offset = (page - 1) * page_size

    count_sql = f"SELECT count() FROM `{table_name}` {where_clause}"
    total = ch.execute(count_sql)[0][0]

    data_sql = f"SELECT * FROM `{table_name}` {where_clause} {order_clause} LIMIT {page_size} OFFSET {offset}"
    raw_rows = ch.execute(data_sql)

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
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        },
    }


@router.get("/tables/{table_name}/distinct", summary="获取某列的唯一值")
def get_distinct_values(
    table_name: str,
    column: str = Query(...),
    limit: int = Query(100, ge=1, le=1000),
):
    ch = get_ch_client()
    rows = ch.execute(
        f"SELECT DISTINCT `{column}` FROM `{table_name}` LIMIT {limit}"
    )
    values = [row[0] for row in rows]
    if values and hasattr(values[0], "isoformat"):
        values = [v.isoformat() for v in values]
    return {"code": 0, "data": values}


@router.post("/query", summary="执行自定义SQL查询")
def execute_query(
    sql: str = Query(..., description="SQL查询语句", max_length=2000),
    limit: int = Query(200, ge=1, le=1000),
):
    ch = get_ch_client()

    upper = sql.strip().upper()
    if not upper.startswith("SELECT") and not upper.startswith("SHOW") and not upper.startswith("DESCRIBE"):
        return {"code": 1, "message": "只允许SELECT/SHOW/DESCRIBE查询"}

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