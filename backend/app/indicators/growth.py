"""成长类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class RevenueGrowth(IndicatorBase):
    name = "revenue_growth"
    display_name = "营收增速"
    category = "growth"
    tags = ["成长", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "营业收入同比增长率"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        revenue_growth = info.get("revenue_growth")
        if revenue_growth is not None:
            return round(float(revenue_growth), 4)
        return None


class ProfitGrowth(IndicatorBase):
    name = "profit_growth"
    display_name = "净利润增速"
    category = "growth"
    tags = ["成长", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "净利润同比增长率"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        profit_growth = info.get("profit_growth")
        if profit_growth is not None:
            return round(float(profit_growth), 4)
        return None
