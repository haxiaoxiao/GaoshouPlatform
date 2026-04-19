"""估值类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext


class PETTM(IndicatorBase):
    name = "pe_ttm"
    display_name = "市盈率TTM"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 净利润(TTM)"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_mv = info.get("total_mv") or info.get("totalValue")
        net_profit = info.get("net_profit")
        if total_mv and net_profit and net_profit != 0:
            return round(total_mv / net_profit, 4)
        return None


class PB(IndicatorBase):
    name = "pb"
    display_name = "市净率"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 净资产"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_mv = info.get("total_mv") or info.get("totalValue")
        total_equity = info.get("total_equity")
        if total_mv and total_equity and total_equity != 0:
            return round(total_mv / total_equity, 4)
        return None


class PSTTM(IndicatorBase):
    name = "ps_ttm"
    display_name = "市销率TTM"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 营业收入(TTM)"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        total_mv = info.get("total_mv") or info.get("totalValue")
        revenue = info.get("revenue")
        if total_mv and revenue and revenue != 0:
            return round(total_mv / revenue, 4)
        return None


class DividendYield(IndicatorBase):
    name = "dividend_yield"
    display_name = "股息率"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "近12个月分红 / 总市值"

    def compute(self, context: IndicatorContext) -> float | None:
        return None
