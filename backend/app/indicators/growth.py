"""成长类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


@IndicatorRegistry.register
class RevenueGrowth(IndicatorBase):
    name = "revenue_growth"
    display_name = "营收增速"
    category = "growth"
    tags = ["成长", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "营业收入同比增长率"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        revenue_yoy = info.get("revenue_yoy")
        if revenue_yoy is not None:
            return round(float(revenue_yoy), 4)
        return None


@IndicatorRegistry.register
class ProfitGrowth(IndicatorBase):
    name = "profit_growth"
    display_name = "净利润增速"
    category = "growth"
    tags = ["成长", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "净利润同比增长率"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        profit_yoy = info.get("profit_yoy")
        if profit_yoy is not None:
            return round(float(profit_yoy), 4)
        return None