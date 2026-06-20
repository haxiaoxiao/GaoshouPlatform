"""主题类指标 - 从SQLite theme_annotations表读取人工标注"""
from functools import lru_cache

from sqlalchemy import create_engine, inspect, text

from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


@lru_cache(maxsize=1)
def _load_theme_annotations() -> dict[str, dict[str, object | None]]:
    """批量读取主题标注，避免同步时每只股票都单独开库。"""
    try:
        from app.core.config import settings

        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        try:
            with engine.connect() as conn:
                if not inspect(conn).has_table("theme_annotations"):
                    return {}
                result = conn.execute(
                    text(
                        "SELECT symbol, business_purity, chain_position, revenue_ratio "
                        "FROM theme_annotations"
                    )
                )
                annotations: dict[str, dict[str, object | None]] = {}
                for row in result.mappings().all():
                    symbol = str(row.get("symbol") or "").strip()
                    if not symbol:
                        continue
                    annotations[symbol] = {
                        "business_purity": row.get("business_purity"),
                        "chain_position": row.get("chain_position"),
                        "revenue_ratio": row.get("revenue_ratio"),
                    }
                return annotations
        finally:
            engine.dispose()
    except Exception:
        return {}


def _get_theme_annotation(symbol: str) -> dict | None:
    """从SQLite读取主题标注。"""
    return _load_theme_annotations().get(symbol)


@IndicatorRegistry.register
class BusinessPurity(IndicatorBase):
    name = "business_purity"
    display_name = "业务纯度"
    category = "theme"
    tags = ["主题", "人工标注"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "股票与主题的相关程度(0~1)，人工标注"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        annotation = _get_theme_annotation(context.symbol)
        if annotation and annotation["business_purity"] is not None:
            return round(annotation["business_purity"] * 100, 4)
        return None


@IndicatorRegistry.register
class ChainPosition(IndicatorBase):
    name = "chain_position"
    display_name = "产业链定位"
    category = "theme"
    tags = ["主题", "人工标注"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "股票在产业链中的位置(上游/中游/下游)，人工标注"

    def compute(self, context: IndicatorContext) -> float | None:
        annotation = _get_theme_annotation(context.symbol)
        if annotation and annotation["chain_position"]:
            mapping = {"上游": 1, "中游": 2, "下游": 3}
            return float(mapping.get(annotation["chain_position"], 0))
        return None


@IndicatorRegistry.register
class RevenueRatio(IndicatorBase):
    name = "revenue_ratio"
    display_name = "主题营收占比"
    category = "theme"
    tags = ["主题", "人工标注"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "主题相关业务营收占总营收比重(0~1)，人工标注"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        annotation = _get_theme_annotation(context.symbol)
        if annotation and annotation["revenue_ratio"] is not None:
            return round(annotation["revenue_ratio"] * 100, 4)
        return None
