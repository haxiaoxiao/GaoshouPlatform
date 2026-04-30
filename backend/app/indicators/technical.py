"""技术类指标"""
from app.indicators.base import IndicatorBase, IndicatorContext, IndicatorRegistry


@IndicatorRegistry.register
class MA5(IndicatorBase):
    name = "ma5"
    display_name = "5日均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "5日移动平均线"
    unit = "CNY"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:5]
        if len(data) < 5:
            return None
        closes = [d.get("close", 0) for d in data]
        if any(c == 0 for c in closes):
            return None
        return round(sum(closes) / len(closes), 4)


@IndicatorRegistry.register
class MA10(IndicatorBase):
    name = "ma10"
    display_name = "10日均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "10日移动平均线"
    unit = "CNY"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:10]
        if len(data) < 10:
            return None
        closes = [d.get("close", 0) for d in data]
        if any(c == 0 for c in closes):
            return None
        return round(sum(closes) / len(closes), 4)


@IndicatorRegistry.register
class MA20(IndicatorBase):
    name = "ma20"
    display_name = "20日均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "20日移动平均线"
    unit = "CNY"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if len(data) < 20:
            return None
        closes = [d.get("close", 0) for d in data]
        if any(c == 0 for c in closes):
            return None
        return round(sum(closes) / len(closes), 4)


@IndicatorRegistry.register
class RSI14(IndicatorBase):
    name = "rsi_14"
    display_name = "14日RSI"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "14日相对强弱指标"
    unit = ""

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:15]
        if len(data) < 15:
            return None
        closes = [d.get("close", 0) for d in reversed(data)]
        if any(c == 0 for c in closes):
            return None
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [c for c in changes if c > 0]
        losses = [-c for c in changes if c < 0]
        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 4)