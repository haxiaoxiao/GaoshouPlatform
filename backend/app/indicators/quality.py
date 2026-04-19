"""质量类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class ROE(IndicatorBase):
    name = "roe"
    display_name = "净资产收益率"
    category = "quality"
    tags = ["质量", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "净利润 / 净资产"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        roe = info.get("roe")
        if roe is not None:
            return round(float(roe), 4)
        return None


class DebtRatio(IndicatorBase):
    name = "debt_ratio"
    display_name = "资产负债率"
    category = "quality"
    tags = ["质量", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总负债 / 总资产"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_liability = info.get("total_liability")
        total_assets = info.get("total_assets")
        if total_liability is not None and total_assets and total_assets != 0:
            return round(float(total_liability) / float(total_assets), 4)
        return None


class GrossMargin(IndicatorBase):
    name = "gross_margin"
    display_name = "毛利率"
    category = "quality"
    tags = ["质量", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "(营业收入-营业成本) / 营业收入"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        gross_margin = info.get("gross_margin")
        if gross_margin is not None:
            return round(float(gross_margin), 4)
        return None
