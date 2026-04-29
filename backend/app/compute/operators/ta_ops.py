"""L3 技术指标算子 — TA-Lib 封装"""
import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class _TAOperator(Operator):
    """TA-Lib 算子基类 — 自动处理 NaN"""

    level: int = 3
    category: str = "technical"
    _func = None  # talib 函数引用

    def _to_numpy(self, series: pd.Series) -> np.ndarray:
        return series.values.astype(np.float64)


class SMAOp(_TAOperator):
    name: str = "SMA"
    signature: str = "SMA(series, period)"
    description: str = "简单移动平均"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        r = talib.SMA(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class EMAOp(_TAOperator):
    name: str = "EMA"
    signature: str = "EMA(series, period)"
    description: str = "指数移动平均"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        r = talib.EMA(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class RSIOp(_TAOperator):
    name: str = "RSI"
    signature: str = "RSI(series, period)"
    description: str = "相对强弱指标"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 14))
        r = talib.RSI(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class MACDOp(Operator):
    name: str = "MACD"
    level: int = 3
    category: str = "technical"
    signature: str = "MACD(series, fast, slow, signal)"
    description: str = "MACD 柱 (DIF - DEA)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = kwargs["series"].values.astype(np.float64)
        fast = int(kwargs.get("fast", 12))
        slow = int(kwargs.get("slow", 26))
        sig = int(kwargs.get("signal", 9))
        _dif, _dea, macd = talib.MACD(s, fastperiod=fast, slowperiod=slow, signalperiod=sig)
        return pd.Series(macd, index=kwargs["series"].index)


class BBANDSUpperOp(_TAOperator):
    name: str = "BBANDS_upper"
    signature: str = "BBANDS_upper(series, period, nbdev)"
    description: str = "布林带上轨"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        nd = int(kwargs.get("nbdev", 2))
        upper, _mid, _low = talib.BBANDS(s, timeperiod=p, nbdevup=nd, nbdevdn=nd)
        return pd.Series(upper, index=kwargs["series"].index)


class BBANDSLowerOp(_TAOperator):
    name: str = "BBANDS_lower"
    signature: str = "BBANDS_lower(series, period, nbdev)"
    description: str = "布林带下轨"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        nd = int(kwargs.get("nbdev", 2))
        _up, _mid, low = talib.BBANDS(s, timeperiod=p, nbdevup=nd, nbdevdn=nd)
        return pd.Series(low, index=kwargs["series"].index)


class ATROp(_TAOperator):
    name: str = "ATR"
    signature: str = "ATR(high_series, low_series, close_series, period)"
    description: str = "平均真实波幅"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high_series"].values.astype(np.float64)
        low = kwargs["low_series"].values.astype(np.float64)
        close = kwargs["close_series"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = talib.ATR(high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["high_series"].index)


class KDJOp(Operator):
    name: str = "KDJ_K"
    level: int = 3
    category: str = "technical"
    signature: str = "KDJ_K(high, low, close, period)"
    description: str = "KDJ K值 (通过 Stochastic 慢速实现)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 9))
        k, _d = talib.STOCH(high, low, close, fastk_period=p, slowk_period=3,
                            slowd_period=3, slowd_matype=0)
        return pd.Series(k, index=kwargs["close"].index)


class CCIOp(_TAOperator):
    name: str = "CCI"
    signature: str = "CCI(high, low, close, period)"
    description: str = "商品通道指数"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = talib.CCI(high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["close"].index)


class WILLROp(_TAOperator):
    name: str = "WILLR"
    signature: str = "WILLR(high, low, close, period)"
    description: str = "威廉指标"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = talib.WILLR(high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["close"].index)


class OBVOp(Operator):
    name: str = "OBV"
    level: int = 3
    category: str = "technical"
    signature: str = "OBV(close, volume)"
    description: str = "能量潮"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        close = kwargs["close"].values.astype(np.float64)
        volume = kwargs["volume"].values.astype(np.float64)
        r = talib.OBV(close, volume)
        return pd.Series(r, index=kwargs["close"].index)


class MOMOp(_TAOperator):
    name: str = "MOM"
    signature: str = "MOM(series, period)"
    description: str = "动量"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        import talib
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 10))
        r = talib.MOM(s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


for op in [
    SMAOp(), EMAOp(), RSIOp(), MACDOp(), BBANDSUpperOp(), BBANDSLowerOp(),
    ATROp(), KDJOp(), CCIOp(), WILLROp(), OBVOp(), MOMOp(),
]:
    OperatorRegistry.register(op)
