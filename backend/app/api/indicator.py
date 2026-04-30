# backend/app/api/indicator.py
"""指标库 API 接口"""
from datetime import date
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path as FPath, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.clickhouse import get_ch_client
from app.db.models.financial import FinancialData
from app.db.sqlite import get_async_session
from app.indicators import IndicatorRegistry
from sqlalchemy import select, func

router = APIRouter()

_CATEGORY_LABELS: dict[str, str] = {
    "valuation": "估值",
    "growth": "成长",
    "quality": "质量",
    "momentum": "动量",
    "volatility": "波动",
    "liquidity": "流动性",
    "technical": "技术",
    "theme": "主题",
}


@router.get("/categories", summary="获取指标分类列表")
async def get_categories() -> dict[str, Any]:
    categories = IndicatorRegistry.categories()
    return {"code": 0, "message": "success", "data": categories}


@router.get("/list", summary="获取指标列表")
async def list_indicators(
    category: str | None = Query(default=None, description="分类筛选"),
) -> dict[str, Any]:
    if category:
        indicator_classes = IndicatorRegistry.by_category(category)
    else:
        indicator_classes = IndicatorRegistry.all()

    items = [
        {
            "name": cls.name,
            "display_name": cls.display_name,
            "category": cls.category,
            "category_label": _CATEGORY_LABELS.get(cls.category, cls.category),
            "tags": cls.tags,
            "data_type": cls.data_type,
            "is_precomputed": cls.is_precomputed,
            "dependencies": cls.dependencies,
            "description": cls.description,
            "unit": cls.unit or "",
        }
        for cls in indicator_classes
    ]
    return {"code": 0, "message": "success", "data": items}


@router.get("/{name}/description", summary="获取指标详情")
async def get_indicator_description(
    name: str = FPath(description="指标名称"),
) -> dict[str, Any]:
    cls = IndicatorRegistry.get(name)
    if cls is None:
        raise HTTPException(status_code=404, detail=f"指标 '{name}' 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": {
            "name": cls.name,
            "display_name": cls.display_name,
            "category": cls.category,
            "category_label": _CATEGORY_LABELS.get(cls.category, cls.category),
            "tags": cls.tags,
            "data_type": cls.data_type,
            "is_precomputed": cls.is_precomputed,
            "dependencies": cls.dependencies,
            "description": cls.description,
        },
    }


@router.get("/query", summary="查询指标值")
async def query_indicators(
    symbols: str = Query(description="股票代码,逗号分隔"),
    indicator_names: str = Query(description="指标名称,逗号分隔"),
    trade_date: str | None = Query(default=None, description="交易日期 YYYY-MM-DD"),
) -> dict[str, Any]:
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    name_list = [n.strip() for n in indicator_names.split(",") if n.strip()]

    if not symbol_list or not name_list:
        raise HTTPException(status_code=400, detail="股票代码和指标名称不能为空")

    target_date = trade_date or date.today().isoformat()

    try:
        ch = get_ch_client()
        actual_date_row = ch.execute(
            "SELECT max(trade_date) FROM stock_indicators WHERE indicator_name IN %(names)s",
            params={"names": name_list},
        )
        if not actual_date_row or not actual_date_row[0]:
            return {"code": 0, "message": "success", "data": {"trade_date": target_date, "items": [
                {"symbol": s, "name": None, "indicators": {n: None for n in name_list}} for s in symbol_list
            ]}}
        use_date = str(actual_date_row[0][0])

        rows = ch.execute(
            """
            SELECT symbol, indicator_name, value
            FROM stock_indicators
            WHERE trade_date = %(td)s
              AND symbol IN %(syms)s
              AND indicator_name IN %(names)s
            """,
            params={"td": use_date, "syms": symbol_list, "names": name_list},
        )
    except Exception:
        rows = []

    import pandas as pd

    if rows:
        df = pd.DataFrame(rows, columns=["symbol", "indicator_name", "value"])
        for symbol in symbol_list:
            sub = df[df["symbol"] == symbol]
            indicators: dict[str, float | None] = {}
            for name in name_list:
                row = sub[sub["indicator_name"] == name]
                indicators[name] = float(row["value"].iloc[0]) if not row.empty else None
            items.append({"symbol": symbol, "name": None, "indicators": indicators})
    else:
        for symbol in symbol_list:
            items.append({
                "symbol": symbol,
                "name": None,
                "indicators": {n: None for n in name_list},
            })

    return {"code": 0, "message": "success", "data": {"trade_date": target_date, "items": items}}


@router.post("/compute", summary="触发指标计算")
async def compute_indicators(
    indicator_names: list[str] | None = Body(default=None, description="指标名称列表"),
    symbols: list[str] | None = Body(default=None, description="股票代码列表"),
    full_compute: bool = Body(default=False, description="是否全量计算"),
) -> dict[str, Any]:
    from app.indicators.scheduler import indicator_scheduler

    try:
        results = indicator_scheduler.compute_indicators(
            indicator_names=indicator_names,
            symbols=symbols,
            full_compute=full_compute,
        )
        return {
            "code": 0,
            "message": "success",
            "data": {"results": results, "message": "计算完成"},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"指标计算失败: {str(e)}")


@router.post("/screen", summary="选股筛选")
async def screen_stocks(
    filters: list[dict[str, Any]] = Body(description="筛选条件"),
    trade_date: str | None = Body(default=None, description="交易日期"),
    sort_by: str | None = Body(default=None, description="排序指标"),
    sort_order: str = Body(default="desc", description="排序方向: asc/desc"),
    limit: int = Body(default=50, description="返回数量限制"),
) -> dict[str, Any]:
    from datetime import date as date_cls

    target_date = trade_date or date_cls.today().isoformat()

    try:
        ch = get_ch_client()
        filter_names = [f.get("indicator_name", "") for f in filters if f.get("indicator_name")]
        if not filter_names:
            raise HTTPException(status_code=400, detail="至少需要一个筛选条件")

        actual_date = ch.execute(
            "SELECT max(trade_date) FROM stock_indicators WHERE indicator_name IN %(names)s",
            params={"names": filter_names},
        )
        if not actual_date or not actual_date[0]:
            return {
                "code": 0,
                "message": "success",
                "data": {"items": [], "total": 0, "trade_date": target_date},
            }
        use_date = str(actual_date[0][0])

        rows = ch.execute(
            """
            SELECT symbol, indicator_name, value
            FROM stock_indicators
            WHERE trade_date = %(td)s
              AND indicator_name IN %(names)s
            """,
            params={"td": use_date, "names": filter_names},
        )
    except HTTPException:
        raise
    except Exception:
        return {
            "code": 0,
            "message": "success",
            "data": {"items": [], "total": 0, "trade_date": target_date},
        }

    if not rows:
        return {
            "code": 0,
            "message": "success",
            "data": {"items": [], "total": 0, "trade_date": target_date},
        }

    import pandas as pd

    df = pd.DataFrame(rows, columns=["symbol", "indicator_name", "value"])
    pivot = df.pivot_table(index="symbol", columns="indicator_name", values="value", aggfunc="first")
    for f in filters:
        ind_name = f.get("indicator_name", "")
        op = f.get("op", ">=")
        val = f.get("value")
        if ind_name not in pivot.columns or val is None:
            continue
        col = pivot[ind_name]
        if op == ">=":
            pivot = pivot[col >= val]
        elif op == "<=":
            pivot = pivot[col <= val]
        elif op == ">":
            pivot = pivot[col > val]
        elif op == "<":
            pivot = pivot[col < val]
        elif op == "==" and isinstance(val, (int, float)):
            pivot = pivot[col == val]
        elif op == "between" and isinstance(val, list) and len(val) == 2:
            pivot = pivot[(col >= val[0]) & (col <= val[1])]

    if sort_by and sort_by in pivot.columns:
        ascending = sort_order == "asc"
        pivot = pivot.sort_values(sort_by, ascending=ascending)

    pivot = pivot.head(limit)

    items = [
        {
            "symbol": symbol,
            "name": None,
            "indicators": {col: float(val) if pd.notna(val) else None for col, val in row.items()},
        }
        for symbol, row in pivot.iterrows()
    ]

    return {
        "code": 0,
        "message": "success",
        "data": {"items": items, "total": len(items), "trade_date": target_date},
    }


@router.get("/financial/{symbol}", summary="获取股票季度财务数据")
async def get_financial_data(
    symbol: str = FPath(description="股票代码"),
    report_count: int = Query(default=8, ge=1, le=20, description="返回季度数"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    query = (
        select(FinancialData)
        .where(FinancialData.symbol == symbol)
        .order_by(FinancialData.report_date.desc())
        .limit(report_count)
    )
    result = await session.execute(query)
    rows = result.scalars().all()

    items = [
        {
            "report_date": r.report_date.isoformat(),
            "report_type": r.report_type,
            "eps": r.eps,
            "bvps": r.bvps,
            "roe": r.roe,
            "revenue": r.revenue,
            "net_profit": r.net_profit,
            "revenue_yoy": r.revenue_yoy,
            "profit_yoy": r.profit_yoy,
            "gross_margin": r.gross_margin,
            "total_assets": r.total_assets,
            "total_liability": r.total_liability,
            "total_equity": r.total_equity,
            "total_shares": r.total_shares,
            "float_shares": r.float_shares,
            "total_mv": r.total_mv,
            "circ_mv": r.circ_mv,
            "pe_ttm": r.pe_ttm,
            "pb": r.pb,
        }
        for r in rows
    ]

    return {"code": 0, "message": "success", "data": items}
