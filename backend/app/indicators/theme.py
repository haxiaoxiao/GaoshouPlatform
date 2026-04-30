"""主题类指标 - 从SQLite theme_annotations表读取人工标注"""
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


def _get_theme_annotation(symbol: str) -> dict | None:
    """从SQLite读取主题标注"""
    try:
        from sqlalchemy import create_engine, select
        from app.db.models.stock import ThemeAnnotation
        from app.core.config import settings

        sync_url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(sync_url)
        try:
            with engine.connect() as conn:
                query = select(ThemeAnnotation).where(ThemeAnnotation.symbol == symbol)
                result = conn.execute(query)
                row = result.first()
                if row:
                    return {
                        "business_purity": row.business_purity,
                        "chain_position": row.chain_position,
                        "revenue_ratio": row.revenue_ratio,
                    }
        finally:
            engine.dispose()
    except (ImportError, Exception):
        pass
    return None


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