"""波动类指标"""
import math

from app.indicators.base import IndicatorBase, IndicatorContext


class Volatility20d(IndicatorBase):
    name = "volatility_20d"
    display_name = "20日波动率"
    category = "volatility"
    tags = ["波动", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20日日收益率标准差(年化)"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if len(data) < 5:
            return None
        closes = [d.get("close", 0) for d in reversed(data)]
        if any(c == 0 for c in closes):
            return None
        returns = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
        ]
        if not returns:
            return None
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        return round(math.sqrt(variance) * math.sqrt(252), 4)


class AvgAmplitude(IndicatorBase):
    name = "avg_amplitude"
    display_name = "平均振幅"
    category = "volatility"
    tags = ["波动", "行情"]
    data_type = "时序"
    is_precomputed = True
    dependencies = []
    description = "近20日振幅均值"

    def compute(self, context: IndicatorContext) -> float | None:
        data = context.kline_data[:20]
        if not data:
            return None
        amplitudes = []
        for d in data:
            high = d.get("high", 0)
            low = d.get("low", 0)
            prev_close = d.get("prev_close", 0) or d.get("open", 0)
            if prev_close > 0:
                amplitudes.append((high - low) / prev_close)
        if not amplitudes:
            return None
        return round(sum(amplitudes) / len(amplitudes), 4)
