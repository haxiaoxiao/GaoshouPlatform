"""L3 技术指标算子 — AKQuant TA-Lib preferred, Python TA-Lib fallback."""
import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry

STABLE_TA_FUNCTIONS = [
    "SMA", "EMA", "RSI",
    "MACD", "MACD_DIF", "MACD_DEA", "MACD_HIST",
    "BBANDS_UPPER", "BBANDS_MIDDLE", "BBANDS_LOWER",
    "ATR", "STOCH_K", "STOCH_D", "CCI", "WILLR", "OBV", "MOM", "MIN", "MAX",
]


def get_ta_capabilities() -> dict:
    provider = None
    detected: list[str] = []
    missing: list[str] = []
    akquant_available = False
    talib_available = False
    try:
        import akquant.talib as aq_talib

        akquant_available = True
        provider = "akquant.talib"
        detected = list(getattr(aq_talib, "__all__", []))
    except Exception:
        missing.append("akquant")
    try:
        import talib

        talib_available = True
        if provider is None:
            provider = "talib"
            detected = list(getattr(talib, "get_functions", lambda: [])())
    except Exception:
        missing.append("ta-lib")
    if provider is None:
        provider = "builtin"
    return {
        "ta_provider": provider,
        "akquant_available": akquant_available,
        "talib_available": talib_available,
        "stable_ta_functions": STABLE_TA_FUNCTIONS,
        "detected_ta_functions": detected,
        "missing_packages": missing if provider is None else [],
    }


def _talib_module():
    try:
        import akquant.talib as aq_talib

        return aq_talib
    except Exception:
        try:
            import talib

            return talib
        except Exception as exc:
            raise ImportError("TA functions require akquant.talib or ta-lib") from exc


def _call_ta(name: str, *args, **kwargs):
    try:
        fn = getattr(_talib_module(), name)
        try:
            return fn(*args, **kwargs, backend="python")
        except TypeError:
            return fn(*args, **kwargs)
    except ImportError:
        return _call_builtin_ta(name, *args, **kwargs)


def _period(kwargs: dict, default: int) -> int:
    return int(kwargs.get("timeperiod") or kwargs.get("period") or default)


def _call_builtin_ta(name: str, *args, **kwargs):
    """Small deterministic fallback for the stable expression whitelist."""
    upper = name.upper()
    if upper == "SMA":
        return pd.Series(args[0]).rolling(_period(kwargs, 20)).mean().to_numpy()
    if upper == "EMA":
        return pd.Series(args[0]).ewm(span=_period(kwargs, 20), adjust=False).mean().to_numpy()
    if upper == "RSI":
        series = pd.Series(args[0], dtype="float64")
        period = _period(kwargs, 14)
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.where(loss != 0, 100.0)
        return rsi.to_numpy()
    if upper == "MACD":
        series = pd.Series(args[0], dtype="float64")
        fast = int(kwargs.get("fastperiod", 12))
        slow = int(kwargs.get("slowperiod", 26))
        signal = int(kwargs.get("signalperiod", 9))
        dif = series.ewm(span=fast, adjust=False).mean() - series.ewm(span=slow, adjust=False).mean()
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = dif - dea
        return dif.to_numpy(), dea.to_numpy(), hist.to_numpy()
    if upper == "BBANDS":
        series = pd.Series(args[0], dtype="float64")
        period = _period(kwargs, 20)
        nbdevup = float(kwargs.get("nbdevup", 2))
        nbdevdn = float(kwargs.get("nbdevdn", 2))
        mid = series.rolling(period).mean()
        std = series.rolling(period).std()
        return (mid + nbdevup * std).to_numpy(), mid.to_numpy(), (mid - nbdevdn * std).to_numpy()
    if upper == "ATR":
        high = pd.Series(args[0], dtype="float64")
        low = pd.Series(args[1], dtype="float64")
        close = pd.Series(args[2], dtype="float64")
        period = _period(kwargs, 14)
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)
        return tr.rolling(period).mean().to_numpy()
    if upper == "STOCH":
        high = pd.Series(args[0], dtype="float64")
        low = pd.Series(args[1], dtype="float64")
        close = pd.Series(args[2], dtype="float64")
        fastk = int(kwargs.get("fastk_period", 9))
        slowk = int(kwargs.get("slowk_period", 3))
        slowd = int(kwargs.get("slowd_period", 3))
        lowest = low.rolling(fastk).min()
        highest = high.rolling(fastk).max()
        raw_k = 100 * (close - lowest) / (highest - lowest).replace(0, np.nan)
        k = raw_k.rolling(slowk).mean()
        d = k.rolling(slowd).mean()
        return k.to_numpy(), d.to_numpy()
    if upper == "CCI":
        high = pd.Series(args[0], dtype="float64")
        low = pd.Series(args[1], dtype="float64")
        close = pd.Series(args[2], dtype="float64")
        period = _period(kwargs, 14)
        tp = (high + low + close) / 3
        ma = tp.rolling(period).mean()
        mad = tp.rolling(period).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
        return ((tp - ma) / (0.015 * mad.replace(0, np.nan))).to_numpy()
    if upper == "WILLR":
        high = pd.Series(args[0], dtype="float64")
        low = pd.Series(args[1], dtype="float64")
        close = pd.Series(args[2], dtype="float64")
        period = _period(kwargs, 14)
        highest = high.rolling(period).max()
        lowest = low.rolling(period).min()
        return (-100 * (highest - close) / (highest - lowest).replace(0, np.nan)).to_numpy()
    if upper == "OBV":
        close = pd.Series(args[0], dtype="float64")
        volume = pd.Series(args[1], dtype="float64")
        direction = np.sign(close.diff().fillna(0))
        return (direction * volume).cumsum().to_numpy()
    if upper == "MOM":
        return pd.Series(args[0], dtype="float64").diff(_period(kwargs, 10)).to_numpy()
    if upper == "MIN":
        return pd.Series(args[0], dtype="float64").rolling(_period(kwargs, 10)).min().to_numpy()
    if upper == "MAX":
        return pd.Series(args[0], dtype="float64").rolling(_period(kwargs, 10)).max().to_numpy()
    raise ImportError("TA functions require akquant.talib or ta-lib")


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
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        r = _call_ta("SMA", s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class EMAOp(_TAOperator):
    name: str = "EMA"
    signature: str = "EMA(series, period)"
    description: str = "指数移动平均"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        r = _call_ta("EMA", s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class RSIOp(_TAOperator):
    name: str = "RSI"
    signature: str = "RSI(series, period)"
    description: str = "相对强弱指标"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 14))
        r = _call_ta("RSI", s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class _MACDBaseOp(Operator):
    level: int = 3
    category: str = "technical"
    output_index: int = 2

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = kwargs["series"].values.astype(np.float64)
        fast = int(kwargs.get("fast", 12))
        slow = int(kwargs.get("slow", 26))
        sig = int(kwargs.get("signal", 9))
        result = _call_ta("MACD", s, fastperiod=fast, slowperiod=slow, signalperiod=sig)
        return pd.Series(result[self.output_index], index=kwargs["series"].index)


class MACDOp(_MACDBaseOp):
    name: str = "MACD"
    signature: str = "MACD(series, fast, slow, signal)"
    description: str = "MACD 柱 (DIF - DEA)"
    output_index: int = 2


class MACDDifOp(_MACDBaseOp):
    name: str = "MACD_DIF"
    signature: str = "MACD_DIF(series, fast, slow, signal)"
    description: str = "MACD DIF"
    output_index: int = 0


class MACDDeaOp(_MACDBaseOp):
    name: str = "MACD_DEA"
    signature: str = "MACD_DEA(series, fast, slow, signal)"
    description: str = "MACD DEA"
    output_index: int = 1


class MACDHistOp(_MACDBaseOp):
    name: str = "MACD_HIST"
    signature: str = "MACD_HIST(series, fast, slow, signal)"
    description: str = "MACD 柱"
    output_index: int = 2


class _BBANDSBaseOp(_TAOperator):
    output_index: int = 0

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 20))
        nd = int(kwargs.get("nbdev", 2))
        result = _call_ta("BBANDS", s, timeperiod=p, nbdevup=nd, nbdevdn=nd)
        return pd.Series(result[self.output_index], index=kwargs["series"].index)


class BBANDSUpperOp(_BBANDSBaseOp):
    name: str = "BBANDS_upper"
    signature: str = "BBANDS_upper(series, period, nbdev)"
    description: str = "布林带上轨"
    output_index: int = 0


class BBANDSUpperAliasOp(BBANDSUpperOp):
    name: str = "BBANDS_UPPER"


class BBANDSMiddleOp(_BBANDSBaseOp):
    name: str = "BBANDS_MIDDLE"
    signature: str = "BBANDS_MIDDLE(series, period, nbdev)"
    description: str = "布林带中轨"
    output_index: int = 1


class BBANDSLowerOp(_BBANDSBaseOp):
    name: str = "BBANDS_lower"
    signature: str = "BBANDS_lower(series, period, nbdev)"
    description: str = "布林带下轨"
    output_index: int = 2


class BBANDSLowerAliasOp(BBANDSLowerOp):
    name: str = "BBANDS_LOWER"


class ATROp(_TAOperator):
    name: str = "ATR"
    signature: str = "ATR(high_series, low_series, close_series, period)"
    description: str = "平均真实波幅"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        high = kwargs["high_series"].values.astype(np.float64)
        low = kwargs["low_series"].values.astype(np.float64)
        close = kwargs["close_series"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = _call_ta("ATR", high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["high_series"].index)


class KDJOp(Operator):
    name: str = "KDJ_K"
    level: int = 3
    category: str = "technical"
    signature: str = "KDJ_K(high, low, close, period)"
    description: str = "KDJ K值 (通过 Stochastic 慢速实现)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 9))
        k, _d = _call_ta("STOCH", high, low, close, fastk_period=p, slowk_period=3,
                         slowd_period=3, slowd_matype=0)
        return pd.Series(k, index=kwargs["close"].index)


class STOCHKOp(KDJOp):
    name: str = "STOCH_K"
    signature: str = "STOCH_K(high, low, close, period)"
    description: str = "Stochastic K 值"


class STOCHDOp(Operator):
    name: str = "STOCH_D"
    level: int = 3
    category: str = "technical"
    signature: str = "STOCH_D(high, low, close, period)"
    description: str = "Stochastic D 值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 9))
        _k, d = _call_ta("STOCH", high, low, close, fastk_period=p, slowk_period=3,
                         slowd_period=3, slowd_matype=0)
        return pd.Series(d, index=kwargs["close"].index)


class CCIOp(_TAOperator):
    name: str = "CCI"
    signature: str = "CCI(high, low, close, period)"
    description: str = "商品通道指数"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = _call_ta("CCI", high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["close"].index)


class WILLROp(_TAOperator):
    name: str = "WILLR"
    signature: str = "WILLR(high, low, close, period)"
    description: str = "威廉指标"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        high = kwargs["high"].values.astype(np.float64)
        low = kwargs["low"].values.astype(np.float64)
        close = kwargs["close"].values.astype(np.float64)
        p = int(kwargs.get("period", 14))
        r = _call_ta("WILLR", high, low, close, timeperiod=p)
        return pd.Series(r, index=kwargs["close"].index)


class OBVOp(Operator):
    name: str = "OBV"
    level: int = 3
    category: str = "technical"
    signature: str = "OBV(close, volume)"
    description: str = "能量潮"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        close = kwargs["close"].values.astype(np.float64)
        volume = kwargs["volume"].values.astype(np.float64)
        r = _call_ta("OBV", close, volume)
        return pd.Series(r, index=kwargs["close"].index)


class MOMOp(_TAOperator):
    name: str = "MOM"
    signature: str = "MOM(series, period)"
    description: str = "动量"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 10))
        r = _call_ta("MOM", s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class MINOp(_TAOperator):
    name: str = "MIN"
    signature: str = "MIN(series, period)"
    description: str = "滚动最小值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 10))
        r = _call_ta("MIN", s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


class MAXOp(_TAOperator):
    name: str = "MAX"
    signature: str = "MAX(series, period)"
    description: str = "滚动最大值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._to_numpy(kwargs["series"])
        p = int(kwargs.get("period", 10))
        r = _call_ta("MAX", s, timeperiod=p)
        return pd.Series(r, index=kwargs["series"].index)


for op in [
    SMAOp(), EMAOp(), RSIOp(),
    MACDOp(), MACDDifOp(), MACDDeaOp(), MACDHistOp(),
    BBANDSUpperOp(), BBANDSUpperAliasOp(), BBANDSMiddleOp(),
    BBANDSLowerOp(), BBANDSLowerAliasOp(),
    ATROp(), KDJOp(), STOCHKOp(), STOCHDOp(), CCIOp(), WILLROp(),
    OBVOp(), MOMOp(), MINOp(), MAXOp(),
]:
    OperatorRegistry.register(op)
