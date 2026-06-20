"""估值类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


@IndicatorRegistry.register
class PETTM(IndicatorBase):
    name = "pe_ttm"
    display_name = "市盈率TTM"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 净利润(TTM)"
    unit = "x"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        # 优先使用已计算的 pe_ttm
        pe_ttm = info.get("pe_ttm")
        if pe_ttm is not None:
            return round(float(pe_ttm), 4)
        # 计算: total_mv(万元) * 10000 / net_profit(元) = PE
        total_mv = info.get("total_mv")
        net_profit = info.get("net_profit")
        if total_mv and net_profit and net_profit != 0:
            return round(total_mv * 10000 / net_profit, 4)
        return None


@IndicatorRegistry.register
class PB(IndicatorBase):
    name = "pb"
    display_name = "市净率"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 净资产"
    unit = "x"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        pb = info.get("pb")
        if pb is not None:
            return round(float(pb), 4)
        # 计算: total_mv(万元) * 10000 / total_equity(元) = PB
        total_mv = info.get("total_mv")
        total_equity = info.get("total_equity")
        if total_mv and total_equity and total_equity != 0:
            return round(total_mv * 10000 / total_equity, 4)
        return None


@IndicatorRegistry.register
class PSTTM(IndicatorBase):
    name = "ps_ttm"
    display_name = "市销率TTM"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "总市值 / 营业收入(TTM)"
    unit = "x"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        # 计算: total_mv(万元) * 10000 / revenue(元) = PS
        total_mv = info.get("total_mv")
        revenue = info.get("revenue")
        if total_mv and revenue and revenue != 0:
            return round(total_mv * 10000 / revenue, 4)
        return None


@IndicatorRegistry.register
class PriceToMA250W(IndicatorBase):
    name = "price_to_ma250w"
    display_name = "价格/250周线"
    category = "valuation"
    tags = ["估值", "行情"]
    data_type = "截面"
    is_precomputed = False
    dependencies = ["ma250_weekly"]
    description = "最新收盘价 / 250周均线，< 1 表示低于周线"
    unit = "x"

    def compute(self, context: IndicatorContext) -> float | None:
        try:
            closes = [float(item.get("close") or 0) for item in context.kline_data[:250]]
            closes = [value for value in closes if value > 0]
            if len(closes) < 20:
                return None
            close = closes[0]
            ma250 = sum(closes) / len(closes)
            if ma250 <= 0:
                return None
            return round(close / ma250, 4)
        except Exception:
            pass
        return None


@IndicatorRegistry.register
class DividendYield(IndicatorBase):
    name = "dividend_yield"
    display_name = "股息率"
    category = "valuation"
    tags = ["估值", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "近12个月分红 / 总市值"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        try:
            from app.data_stores import get_indicator_store

            store = get_indicator_store()
            latest_date = store.latest_trade_date(["dividend_yield"], [context.symbol])
            if latest_date is None:
                return None
            df = store.load_cross_section(["dividend_yield"], latest_date, [context.symbol])
            if not df.empty:
                return float(df["value"].iloc[0])
        except Exception:
            pass
        return None
