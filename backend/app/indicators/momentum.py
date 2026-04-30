"""动量类指标"""
import math
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


def _calc_return(kline_data: list[dict], n: int) -> float | None:
    """计算N日涨幅"""
    if not kline_data or len(kline_data) < 2:
        return None
    if len(kline_data) <= n:
        latest = kline_data[0].get("close", 0)
        oldest = kline_data[-1].get("close", 0)
    else:
        latest = kline_data[0].get("close", 0)
        oldest = kline_data[n].get("close", 0)
    if oldest == 0:
        return None
    return round((latest - oldest) / oldest * 100, 4)


@IndicatorRegistry.register
class Return5d(IndicatorBase):
    name = "return_5d"
    display_name = "5日涨幅"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近5个交易日涨幅"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        return _calc_return(context.kline_data, 5)


@IndicatorRegistry.register
class Return20d(IndicatorBase):
    name = "return_20d"
    display_name = "20日涨幅"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20个交易日涨幅"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        return _calc_return(context.kline_data, 20)


@IndicatorRegistry.register
class Return60d(IndicatorBase):
    name = "return_60d"
    display_name = "60日涨幅"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近60个交易日涨幅"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        return _calc_return(context.kline_data, 60)


@IndicatorRegistry.register
class MA5Slope(IndicatorBase):
    name = "ma5_slope"
    display_name = "5日均线斜率"
    category = "momentum"
    tags = ["动量", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "MA5线性回��斜率"
    unit = "%"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:5] if len(context.kline_data) >= 5 else context.kline_data
        if len(data) < 3:
            return None
        closes = [d.get("close", 0) for d in reversed(data)]
        if any(c == 0 for c in closes):
            return None
        n = len(closes)
        x_mean = (n - 1) / 2
        y_mean = sum(closes) / n
        numerator = sum((i - x_mean) * (c - y_mean) for i, c in enumerate(closes))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        if denominator == 0:
            return None
        slope = numerator / denominator
        return round(slope / y_mean * 100, 4)