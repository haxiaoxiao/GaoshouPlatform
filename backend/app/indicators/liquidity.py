"""流动性类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


@IndicatorRegistry.register
class TurnoverRate(IndicatorBase):
    name = "turnover_rate"
    display_name = "换手率"
    category = "liquidity"
    tags = ["流动性", "行情"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "成交量 / 流通股本"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        volume = info.get("volume")
        float_shares = info.get("a_float_shares") or info.get("float_shares")
        if volume and float_shares and float_shares != 0:
            return round(float(volume) / float(float_shares) * 100, 4)
        return None


@IndicatorRegistry.register
class AvgAmount20d(IndicatorBase):
    name = "avg_amount_20d"
    display_name = "20日均成交额"
    category = "liquidity"
    tags = ["流动性", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20日成交额均值(万元)"
    unit = "10k CNY"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if not data:
            return None
        amounts = [d.get("amount", 0) for d in data]
        valid = [a for a in amounts if a > 0]
        if not valid:
            return None
        return round(sum(valid) / len(valid) / 10000, 2)


@IndicatorRegistry.register
class FreeFloatMV(IndicatorBase):
    name = "free_float_mv"
    display_name = "自由流通市值"
    category = "liquidity"
    tags = ["流动性", "基本面"]
    data_type = "截面"
    is_precomputed = True
    dependencies = []
    description = "流通市值(万元)"
    unit = "10k CNY"

    def compute(self, context: IndicatorContext) -> float | None:
        info = context.stock_info
        if not info:
            return None
        circ_mv = info.get("circ_mv")
        if circ_mv is not None:
            return round(float(circ_mv), 2)
        return None
