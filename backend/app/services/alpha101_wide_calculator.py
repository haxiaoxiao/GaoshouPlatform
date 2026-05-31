from __future__ import annotations

import numpy as np
import pandas as pd

SUPPORTED_WIDE_ALPHA101: frozenset[str] = frozenset(
    f"alpha101_{index:03d}" for index in range(1, 102)
)


def is_alpha101_wide_supported(factor_name: str) -> bool:
    return str(factor_name) in SUPPORTED_WIDE_ALPHA101


def compute_alpha101_wide_factor_series(data: pd.DataFrame, factor_name: str) -> pd.Series:
    return WideAlphas(data).compute_series(factor_name)


class WideAlphas:
    """Wide-matrix Alpha101 implementation for formulas migrated from Alphas."""

    def __init__(self, data: pd.DataFrame):
        if data.empty:
            raise ValueError("Alpha101 input panel is empty")
        if not isinstance(data.index, pd.MultiIndex) or set(data.index.names) != {"symbol", "date"}:
            raise ValueError("Alpha101 wide input must use a MultiIndex with symbol/date levels")
        self.data = data.sort_index()
        self.eps = 1e-8
        self.fields = self._build_fields()
        self.industry_by_symbol = self._build_industry_map()

    def _build_fields(self) -> dict[str, pd.DataFrame]:
        fields: dict[str, pd.DataFrame] = {}
        for field in ("open", "high", "low", "close", "volume", "amount", "vwap", "return", "market_value"):
            if field in self.data.columns:
                fields[field] = self.data[field].unstack("symbol").sort_index()
        return fields

    def _build_industry_map(self) -> dict[str, str]:
        if "industry" not in self.data.columns:
            return {}
        industries = (
            self.data.reset_index()[["symbol", "industry"]]
            .drop_duplicates("symbol")
            .set_index("symbol")["industry"]
            .fillna("UNKNOWN")
            .astype(str)
        )
        return industries.to_dict()

    def compute(self, factor_name: str) -> pd.DataFrame:
        name = str(factor_name)
        if not is_alpha101_wide_supported(name):
            raise ValueError(f"Alpha101 wide factor is not implemented: {name}")
        method = getattr(self, f"alpha_{int(name.rsplit('_', 1)[1])}", None)
        if method is None:
            raise ValueError(f"Alpha101 wide factor is not implemented: {name}")
        return method()

    def compute_series(self, factor_name: str) -> pd.Series:
        frame = self.compute(factor_name)
        return self._to_series(frame)

    @staticmethod
    def _to_series(frame: pd.DataFrame) -> pd.Series:
        series = frame.stack().rename("value")
        return series.reorder_levels(["symbol", "date"]).sort_index()

    def _rank(self, x: pd.DataFrame) -> pd.DataFrame:
        return x.rank(axis=1, pct=True)

    @staticmethod
    def _delay(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.shift(d)

    @staticmethod
    def _delta(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.diff(d)

    @staticmethod
    def _correlation(x: pd.DataFrame, y: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).corr(y).replace([np.inf, -np.inf], np.nan)

    @staticmethod
    def _covariance(x: pd.DataFrame, y: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).cov(y)

    def _scale(self, x: pd.DataFrame, a: float = 1.0) -> pd.DataFrame:
        return x.mul(a).div(x.abs().sum(axis=1) + self.eps, axis=0)

    @staticmethod
    def _ts_rank(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).rank(pct=True)

    @staticmethod
    def _ts_min(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).min()

    @staticmethod
    def _ts_max(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).max()

    @staticmethod
    def _ts_argmin(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).apply(lambda values: np.argmin(values), raw=True)

    @staticmethod
    def _ts_argmax(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).apply(lambda values: np.argmax(values), raw=True)

    @staticmethod
    def _sum(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).sum()

    @staticmethod
    def _product(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).apply(np.prod, raw=True)

    @staticmethod
    def _stddev(x: pd.DataFrame, d: int) -> pd.DataFrame:
        return x.rolling(d).std()

    @staticmethod
    def _decay_linear(x: pd.DataFrame, d: int) -> pd.DataFrame:
        weights = np.arange(1, d + 1)
        return x.rolling(d).apply(lambda values: np.average(values, weights=weights), raw=True)

    def _adv(self, d: int) -> pd.DataFrame:
        return self.fields["volume"].rolling(d).mean()

    def _ind_neutralize(self, x: pd.DataFrame) -> pd.DataFrame:
        if not self.industry_by_symbol:
            return x
        result = x.copy()
        grouped_symbols: dict[str, list[str]] = {}
        for symbol in x.columns:
            grouped_symbols.setdefault(self.industry_by_symbol.get(str(symbol), "UNKNOWN"), []).append(symbol)
        for symbols in grouped_symbols.values():
            result[symbols] = x[symbols].sub(x[symbols].mean(axis=1), axis=0)
        return result

    def _field(self, name: str) -> pd.DataFrame:
        try:
            return self.fields[name]
        except KeyError as exc:
            raise ValueError(f"Alpha101 input is missing required field: {name}") from exc

    @staticmethod
    def _max_pair(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        result = left.where(left >= right, right)
        result = result.where(~left.isna(), right)
        return result.where(~right.isna(), left)

    @staticmethod
    def _min_pair(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        result = left.where(left <= right, right)
        result = result.where(~left.isna(), right)
        return result.where(~right.isna(), left)

    def alpha_1(self) -> pd.DataFrame:
        returns = self._field("return")
        close = self._field("close")
        base = self._stddev(returns, 20).where(returns < 0, close) ** 2
        return self._rank(self._ts_argmax(base, 5)) - 0.5

    def alpha_2(self) -> pd.DataFrame:
        volume = self._field("volume")
        x = self._rank(self._delta(np.log(volume.replace(0, np.nan)), 2))
        y = self._rank((self._field("close") - self._field("open")) / (self._field("open") + self.eps))
        return -self._correlation(x, y, 6)

    def alpha_3(self) -> pd.DataFrame:
        return -self._correlation(self._rank(self._field("open")), self._rank(self._field("volume")), 10)

    def alpha_4(self) -> pd.DataFrame:
        return -self._ts_rank(self._rank(self._field("low")), 9)

    def alpha_5(self) -> pd.DataFrame:
        left = self._rank(self._field("open") - self._sum(self._field("vwap"), 10) / 10)
        right = -self._rank(self._field("close") - self._field("vwap")).abs()
        return left * right

    def alpha_6(self) -> pd.DataFrame:
        return -self._correlation(self._field("open"), self._field("volume"), 10)

    def alpha_7(self) -> pd.DataFrame:
        volume = self._field("volume")
        close_delta = self._delta(self._field("close"), 7)
        adv20 = self._adv(20)
        value = self._ts_rank(close_delta.abs(), 60) * np.sign(close_delta)
        alpha = value.where(adv20 < volume, -1)
        return alpha.mask(adv20.isna())

    def alpha_8(self) -> pd.DataFrame:
        left = self._sum(self._field("open"), 5) * self._sum(self._field("return"), 5)
        return -self._rank(left - self._delay(left, 10))

    def alpha_9(self) -> pd.DataFrame:
        close_delta = self._delta(self._field("close"), 1)
        ts_min = self._ts_min(close_delta, 5)
        ts_max = self._ts_max(close_delta, 5)
        alpha = close_delta.where(ts_min > 0, close_delta.where(ts_max < 0, -close_delta))
        return alpha.mask(ts_min.isna())

    def alpha_10(self) -> pd.DataFrame:
        close_delta = self._delta(self._field("close"), 1)
        ts_min = self._ts_min(close_delta, 4)
        ts_max = self._ts_max(close_delta, 4)
        alpha = close_delta.where(ts_min > 0, close_delta.where(ts_max < 0, -close_delta))
        return alpha.mask(ts_min.isna())

    def alpha_11(self) -> pd.DataFrame:
        inner = self._field("vwap") - self._field("close")
        return self._rank(self._ts_max(inner, 3)) + self._rank(
            self._ts_min(inner, 3)
        ) * self._rank(self._delta(self._field("volume"), 3))

    def alpha_12(self) -> pd.DataFrame:
        return np.sign(self._delta(self._field("volume"), 1)) * (-self._delta(self._field("close"), 1))

    def alpha_13(self) -> pd.DataFrame:
        return -self._rank(
            self._covariance(self._rank(self._field("close")), self._rank(self._field("volume")), 5)
        )

    def alpha_14(self) -> pd.DataFrame:
        return -self._rank(self._delta(self._field("return"), 3)) * self._correlation(
            self._field("open"), self._field("volume"), 10
        )

    def alpha_15(self) -> pd.DataFrame:
        return -self._sum(
            self._rank(
                self._correlation(
                    self._rank(self._field("high")), self._rank(self._field("volume")), 3
                )
            ),
            3,
        )

    def alpha_16(self) -> pd.DataFrame:
        return -self._rank(
            self._covariance(self._rank(self._field("high")), self._rank(self._field("volume")), 5)
        )

    def alpha_17(self) -> pd.DataFrame:
        close = self._field("close")
        volume = self._field("volume")
        part_0 = -self._rank(self._ts_rank(close, 10))
        part_1 = self._rank(self._delta(self._delta(close, 1), 1))
        part_2 = self._rank(self._ts_rank(volume / (self._adv(20) + self.eps), 5))
        return part_0 * part_1 * part_2

    def alpha_18(self) -> pd.DataFrame:
        close = self._field("close")
        open_ = self._field("open")
        return -self._rank(self._stddev((close - open_).abs(), 5) + (close - open_)) + self._correlation(
            close, open_, 10
        )

    def alpha_19(self) -> pd.DataFrame:
        close = self._field("close")
        left = -np.sign(close - self._delay(close, 7) + self._delta(close, 7))
        right = 1 + self._rank(1 + self._sum(self._field("return"), 250))
        return left * right

    def alpha_20(self) -> pd.DataFrame:
        open_ = self._field("open")
        return (
            -self._rank(open_ - self._delay(self._field("high"), 1))
            * self._rank(open_ - self._delay(self._field("close"), 1))
            * self._rank(open_ - self._delay(self._field("low"), 1))
        )

    def alpha_21(self) -> pd.DataFrame:
        close = self._field("close")
        sma8 = self._sum(close, 8) / 8
        std8 = self._stddev(close, 8)
        sma2 = self._sum(close, 2) / 2
        volume_ratio = self._field("volume") / (self._adv(20) + self.eps)
        values = np.where(
            (sma8 + std8) < sma2,
            -1.0,
            np.where(sma2 < (sma8 - std8), 1.0, np.where(volume_ratio >= 1.0, 1.0, -1.0)),
        )
        alpha = pd.DataFrame(values, index=close.index, columns=close.columns)
        return alpha.mask(sma8.isna() | std8.isna() | sma2.isna())

    def alpha_22(self) -> pd.DataFrame:
        left = -self._delta(self._correlation(self._field("high"), self._field("volume"), 5), 5)
        right = self._rank(self._stddev(self._field("close"), 20))
        return left * right

    def alpha_23(self) -> pd.DataFrame:
        high = self._field("high")
        high_sum = self._sum(high, 20)
        alpha = (-self._delta(high, 2)).where((high_sum / 20) < high, 0)
        return alpha.mask(high_sum.isna())

    def alpha_24(self) -> pd.DataFrame:
        close = self._field("close")
        left = self._delta(self._sum(close, 100) / 100, 100) / (self._delay(close, 100) + self.eps)
        alpha = (-(close - self._ts_min(close, 100))).where(left <= 0.05, -self._delta(close, 3))
        return alpha.mask(left.isna())

    def alpha_25(self) -> pd.DataFrame:
        return self._rank(
            -self._field("return")
            * self._adv(20)
            * self._field("vwap")
            * (self._field("high") - self._field("close"))
        )

    def alpha_26(self) -> pd.DataFrame:
        return -self._ts_max(
            self._correlation(
                self._ts_rank(self._field("volume"), 5),
                self._ts_rank(self._field("high"), 5),
                5,
            ),
            3,
        )

    def alpha_27(self) -> pd.DataFrame:
        signal = self._rank(
            self._sum(
                self._correlation(self._rank(self._field("volume")), self._rank(self._field("vwap")), 6),
                2,
            )
            / 2.0
        )
        values = np.where(signal > 0.5, -1.0, 1.0)
        alpha = pd.DataFrame(values, index=signal.index, columns=signal.columns)
        return alpha.mask(signal.isna())

    def alpha_28(self) -> pd.DataFrame:
        return self._scale(
            self._correlation(self._adv(20), self._field("low"), 5)
            + (self._field("high") + self._field("low")) / 2
            - self._field("close")
        )

    def alpha_29(self) -> pd.DataFrame:
        close = self._field("close")
        left = self._ts_min(
            self._rank(
                self._rank(
                    self._scale(
                        np.log(
                            self._sum(
                                self._ts_min(
                                    self._rank(self._rank(-self._rank(self._delta(close - 1, 5)))),
                                    2,
                                ),
                                1,
                            )
                        )
                    )
                )
            ),
            5,
        )
        right = self._ts_rank(self._delay(-self._field("return"), 6), 5)
        return left + right

    def alpha_30(self) -> pd.DataFrame:
        close = self._field("close")
        direction = (
            np.sign(close - self._delay(close, 1))
            + np.sign(self._delay(close, 1) - self._delay(close, 2))
            + np.sign(self._delay(close, 2) - self._delay(close, 3))
        )
        return (
            (1 - self._rank(direction))
            * self._sum(self._field("volume"), 5)
            / (self._sum(self._field("volume"), 20) + self.eps)
        )

    def alpha_31(self) -> pd.DataFrame:
        close = self._field("close")
        part_0 = self._rank(
            self._rank(self._rank(self._decay_linear(-self._rank(self._rank(self._delta(close, 10))), 10)))
        )
        part_1 = self._rank(-self._delta(close, 3))
        part_2 = np.sign(self._scale(self._correlation(self._adv(20), self._field("low"), 12)))
        return part_0 + part_1 + part_2

    def alpha_32(self) -> pd.DataFrame:
        close = self._field("close")
        left = self._scale((self._sum(close, 7) / 7) - close)
        right = 20 * self._scale(self._correlation(self._field("vwap"), self._delay(close, 5), 230))
        return left + right

    def alpha_33(self) -> pd.DataFrame:
        return self._rank(-(1 - (self._field("open") / (self._field("close") + self.eps))))

    def alpha_34(self) -> pd.DataFrame:
        left = 1 - self._rank(
            self._stddev(self._field("return"), 2) / (self._stddev(self._field("return"), 5) + self.eps)
        )
        right = 1 - self._rank(self._delta(self._field("close"), 1))
        return self._rank(left + right)

    def alpha_35(self) -> pd.DataFrame:
        return (
            self._ts_rank(self._field("volume"), 32)
            * (1 - self._ts_rank(self._field("close") + self._field("high") - self._field("low"), 16))
            * (1 - self._ts_rank(self._field("return"), 32))
        )

    def alpha_36(self) -> pd.DataFrame:
        close = self._field("close")
        open_ = self._field("open")
        part_0 = 2.21 * self._rank(
            self._correlation(close - open_, self._delay(self._field("volume"), 1), 15)
        )
        part_1 = 0.7 * self._rank(open_ - close)
        part_2 = 0.73 * self._rank(self._ts_rank(self._delay(-self._field("return"), 6), 5))
        part_3 = self._rank(self._correlation(self._field("vwap"), self._adv(20), 6).abs())
        part_4 = 0.6 * self._rank((self._sum(close, 200) / 200 - open_) * (close - open_))
        return part_0 + part_1 + part_2 + part_3 + part_4

    def alpha_37(self) -> pd.DataFrame:
        open_close = self._field("open") - self._field("close")
        return self._rank(self._correlation(self._delay(open_close, 1), self._field("close"), 200)) + self._rank(
            open_close
        )

    def alpha_38(self) -> pd.DataFrame:
        left = -self._rank(self._ts_rank(self._field("close"), 10))
        right = self._rank(self._field("close") / (self._field("open") + self.eps))
        return left * right

    def alpha_39(self) -> pd.DataFrame:
        close = self._field("close")
        left = -self._rank(
            self._delta(close, 7)
            * (1 - self._rank(self._decay_linear(self._field("volume") / (self._adv(20) + self.eps), 9)))
        )
        right = 1 + self._rank(self._sum(self._field("return"), 250))
        return left * right

    def alpha_40(self) -> pd.DataFrame:
        return -self._rank(self._stddev(self._field("high"), 10)) * self._correlation(
            self._field("high"), self._field("volume"), 10
        )

    def alpha_41(self) -> pd.DataFrame:
        return (self._field("high") * self._field("low")) ** 0.5 - self._field("vwap")

    def alpha_42(self) -> pd.DataFrame:
        return self._rank(self._field("vwap") - self._field("close")) / (
            self._rank(self._field("vwap") + self._field("close")) + self.eps
        )

    def alpha_43(self) -> pd.DataFrame:
        return self._ts_rank(self._field("volume") / (self._adv(20) + self.eps), 20) * self._ts_rank(
            -self._delta(self._field("close"), 7), 8
        )

    def alpha_44(self) -> pd.DataFrame:
        return -self._correlation(self._field("high"), self._rank(self._field("volume")), 5)

    def alpha_45(self) -> pd.DataFrame:
        close = self._field("close")
        part_0 = self._rank(self._sum(self._delay(close, 5), 20) / 20)
        part_1 = self._correlation(close, self._field("volume"), 2)
        part_2 = self._rank(self._correlation(self._sum(close, 5), self._sum(close, 20), 2))
        return -part_0 * part_1 * part_2

    def alpha_46(self) -> pd.DataFrame:
        close = self._field("close")
        left = ((self._delay(close, 20) - self._delay(close, 10)) / 10) - (
            (self._delay(close, 10) - close) / 10
        )
        fallback = -(close - self._delay(close, 1))
        alpha = pd.DataFrame(
            np.where(left > 0.25, -1.0, np.where(left < 0.0, 1.0, fallback)),
            index=close.index,
            columns=close.columns,
        )
        return alpha.mask(left.isna())

    def alpha_47(self) -> pd.DataFrame:
        close = self._field("close")
        high = self._field("high")
        part_0 = self._rank(1 / (close + self.eps)) * self._field("volume") / (self._adv(20) + self.eps)
        part_1 = high * self._rank(high - close) / (self._sum(high, 5) / 5)
        part_2 = self._rank(self._field("vwap") - self._delay(self._field("vwap"), 5))
        return part_0 * part_1 - part_2

    def alpha_48(self) -> pd.DataFrame:
        close = self._field("close")
        left = self._ind_neutralize(
            self._correlation(self._delta(close, 1), self._delta(self._delay(close, 1), 1), 250)
            * self._delta(close, 1)
            / (close + self.eps)
        )
        right = self._sum(self._delta(close, 1) / (self._delay(close, 1) ** 2 + self.eps), 250)
        return left / (right + self.eps)

    def alpha_49(self) -> pd.DataFrame:
        close = self._field("close")
        left = (self._delay(close, 20) - self._delay(close, 10)) / 10 - (
            (self._delay(close, 10) - close) / 10
        )
        alpha = pd.DataFrame(
            np.where(left < -0.1, 1, -(close - self._delay(close, 1))),
            index=close.index,
            columns=close.columns,
        )
        return alpha.mask(left.isna())

    def alpha_50(self) -> pd.DataFrame:
        return -self._ts_max(
            self._rank(self._correlation(self._rank(self._field("volume")), self._rank(self._field("vwap")), 5)),
            5,
        )

    def alpha_51(self) -> pd.DataFrame:
        close = self._field("close")
        left = (self._delay(close, 20) - self._delay(close, 10)) / 10 - (
            self._delay(close, 10) - close
        ) / 10
        alpha = pd.DataFrame(
            np.where(left < -0.05, 1, -(close - self._delay(close, 1))),
            index=close.index,
            columns=close.columns,
        )
        return alpha.mask(left.isna())

    def alpha_52(self) -> pd.DataFrame:
        low = self._field("low")
        left = -self._ts_min(low, 5)
        right = (
            self._delay(self._ts_min(low, 5), 5)
            * self._rank((self._sum(self._field("return"), 240) - self._sum(self._field("return"), 20)) / 220)
            * self._ts_rank(self._field("volume"), 5)
        )
        return left + right

    def alpha_53(self) -> pd.DataFrame:
        close = self._field("close")
        low = self._field("low")
        high = self._field("high")
        return -self._delta(((close - low) - (high - close)) / (close - low + self.eps), 9)

    def alpha_54(self) -> pd.DataFrame:
        low = self._field("low")
        close = self._field("close")
        return -((low - close) * self._field("open") ** 5) / (
            (low - self._field("high")) * close**5 + self.eps
        )

    def alpha_55(self) -> pd.DataFrame:
        low = self._field("low")
        high = self._field("high")
        return -self._correlation(
            self._rank(
                (self._field("close") - self._ts_min(low, 12))
                / (self._ts_max(high, 12) - self._ts_min(low, 12) + self.eps)
            ),
            self._rank(self._field("volume")),
            6,
        )

    def alpha_56(self) -> pd.DataFrame:
        left = self._rank(
            self._sum(self._field("return"), 10) / self._sum(self._sum(self._field("return"), 2), 3)
        )
        right = self._rank(self._field("return") * self._field("market_value"))
        return -left * right

    def alpha_57(self) -> pd.DataFrame:
        close = self._field("close")
        return -(close - self._field("vwap")) / (self._decay_linear(self._rank(self._ts_argmax(close, 30)), 2) + self.eps)

    def alpha_58(self) -> pd.DataFrame:
        return -self._ts_rank(
            self._decay_linear(
                self._correlation(self._ind_neutralize(self._field("vwap")), self._field("volume"), 4),
                8,
            ),
            6,
        )

    def alpha_59(self) -> pd.DataFrame:
        return -self._ts_rank(
            self._decay_linear(
                self._correlation(self._ind_neutralize(self._field("vwap")), self._field("volume"), 4),
                16,
            ),
            8,
        )

    def alpha_60(self) -> pd.DataFrame:
        close = self._field("close")
        low = self._field("low")
        high = self._field("high")
        left = 2 * self._scale(
            self._rank((((close - low) - (high - close)) / (high - low + self.eps)) * self._field("volume"))
        )
        right = self._scale(self._rank(self._ts_argmax(close, 10)))
        return -(left - right)

    def alpha_61(self) -> pd.DataFrame:
        left = self._rank(self._field("vwap") - self._ts_min(self._field("vwap"), 16))
        right = self._rank(self._correlation(self._field("vwap"), self._adv(180), 18))
        alpha = (left < right).astype(float)
        return alpha.mask(left.isna() | right.isna())

    def alpha_62(self) -> pd.DataFrame:
        left = self._rank(self._correlation(self._field("vwap"), self._sum(self._adv(20), 22), 10))
        right = self._rank(
            (self._rank(self._field("open")) + self._rank(self._field("open")))
            < (self._rank((self._field("high") + self._field("low")) / 2) + self._rank(self._field("high")))
        )
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_63(self) -> pd.DataFrame:
        left = self._rank(self._decay_linear(self._delta(self._ind_neutralize(self._field("close")), 2), 8))
        right = self._rank(
            self._decay_linear(
                self._correlation(
                    self._field("vwap") * 0.318108 + self._field("open") * (1 - 0.318108),
                    self._sum(self._adv(180), 37),
                    14,
                ),
                12,
            )
        )
        return -(left - right)

    def alpha_64(self) -> pd.DataFrame:
        left = self._rank(
            self._correlation(
                self._sum(self._field("open") * 0.178404 + self._field("low") * (1 - 0.178404), 13),
                self._sum(self._adv(120), 13),
                17,
            )
        )
        right = self._rank(
            self._delta(((self._field("high") + self._field("low")) / 2) * 0.178404 + self._field("vwap") * (1 - 0.178404), 4)
        )
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_65(self) -> pd.DataFrame:
        left = self._rank(
            self._correlation(
                self._field("open") * 0.00817205 + self._field("vwap") * (1 - 0.00817205),
                self._sum(self._adv(60), 9),
                6,
            )
        )
        right = self._rank(self._field("open") - self._ts_min(self._field("open"), 14))
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_66(self) -> pd.DataFrame:
        left = self._rank(self._decay_linear(self._delta(self._field("vwap"), 4), 7))
        right = self._ts_rank(
            self._decay_linear(
                (self._field("low") - self._field("vwap"))
                / (self._field("open") - (self._field("high") + self._field("low")) / 2 + self.eps),
                11,
            ),
            7,
        )
        return -(left + right)

    def alpha_67(self) -> pd.DataFrame:
        left = self._rank(self._field("high") - self._ts_min(self._field("high"), 2))
        right = self._rank(
            self._correlation(
                self._ind_neutralize(self._field("vwap")),
                self._ind_neutralize(self._adv(20)),
                6,
            )
        )
        return -(left**right)

    def alpha_68(self) -> pd.DataFrame:
        left = self._ts_rank(self._correlation(self._rank(self._field("high")), self._rank(self._adv(15)), 9), 14)
        right = self._rank(self._delta(self._field("close") * 0.518371 + self._field("low") * (1 - 0.518371), 1))
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_69(self) -> pd.DataFrame:
        left = self._rank(self._ts_max(self._delta(self._ind_neutralize(self._field("vwap")), 3), 5))
        right = self._ts_rank(
            self._correlation(
                self._field("close") * 0.490655 + self._field("vwap") * (1 - 0.490655),
                self._adv(20),
                5,
            ),
            9,
        )
        return -(left**right)

    def alpha_70(self) -> pd.DataFrame:
        left = self._rank(self._delta(self._field("vwap"), 1))
        right = self._ts_rank(self._correlation(self._ind_neutralize(self._field("close")), self._adv(50), 18), 18)
        return -(left**right)

    def alpha_71(self) -> pd.DataFrame:
        left = self._ts_rank(
            self._decay_linear(
                self._correlation(self._ts_rank(self._field("close"), 3), self._ts_rank(self._adv(180), 12), 18),
                4,
            ),
            16,
        )
        right = self._ts_rank(
            self._decay_linear(self._rank((self._field("low") + self._field("open")) - 2 * self._field("vwap")) ** 2, 16),
            4,
        )
        return self._max_pair(left, right)

    def alpha_72(self) -> pd.DataFrame:
        left = self._rank(
            self._decay_linear(self._correlation((self._field("high") + self._field("low")) / 2, self._adv(40), 9), 10)
        )
        right = self._rank(
            self._decay_linear(
                self._correlation(self._ts_rank(self._field("vwap"), 4), self._ts_rank(self._field("volume"), 19), 7),
                3,
            )
        )
        return left / (right + self.eps)

    def alpha_73(self) -> pd.DataFrame:
        left = self._rank(self._decay_linear(self._delta(self._field("vwap"), 5), 3))
        base = self._field("open") * 0.147155 + self._field("low") * (1 - 0.147155)
        right = self._ts_rank(self._decay_linear(-self._delta(base, 2) / (base + self.eps), 3), 17)
        return -self._max_pair(left, right)

    def alpha_74(self) -> pd.DataFrame:
        left = self._rank(self._correlation(self._field("close"), self._sum(self._adv(30), 37), 15))
        right = self._rank(
            self._correlation(
                self._rank(self._field("high") * 0.0261661 + self._field("vwap") * (1 - 0.0261661)),
                self._rank(self._field("volume")),
                11,
            )
        )
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_75(self) -> pd.DataFrame:
        left = self._rank(self._correlation(self._field("vwap"), self._field("volume"), 4))
        right = self._rank(self._correlation(self._rank(self._field("low")), self._rank(self._adv(50)), 12))
        alpha = (left < right).astype(float)
        return alpha.mask(left.isna() | right.isna())

    def alpha_76(self) -> pd.DataFrame:
        left = self._rank(self._decay_linear(self._delta(self._field("vwap"), 1), 12))
        right = self._ts_rank(
            self._decay_linear(
                self._ts_rank(self._correlation(self._ind_neutralize(self._field("low")), self._adv(81), 8), 20),
                17,
            ),
            19,
        )
        return -self._max_pair(left, right)

    def alpha_77(self) -> pd.DataFrame:
        left = self._rank(self._decay_linear((self._field("high") + self._field("low")) / 2 - self._field("vwap"), 20))
        right = self._rank(
            self._decay_linear(self._correlation((self._field("high") + self._field("low")) / 2, self._adv(40), 3), 6)
        )
        return self._min_pair(left, right)

    def alpha_78(self) -> pd.DataFrame:
        left = self._rank(
            self._correlation(
                self._sum(self._field("low") * 0.352233 + self._field("vwap") * (1 - 0.352233), 20),
                self._sum(self._adv(40), 20),
                7,
            )
        )
        right = self._rank(self._correlation(self._rank(self._field("vwap")), self._rank(self._field("volume")), 6))
        return left**right

    def alpha_79(self) -> pd.DataFrame:
        left = self._rank(
            self._delta(self._ind_neutralize(self._field("close") * 0.60733 + self._field("open") * (1 - 0.60733)), 1)
        )
        right = self._rank(self._correlation(self._ts_rank(self._field("vwap"), 4), self._ts_rank(self._adv(150), 9), 15))
        alpha = (left < right).astype(float)
        return alpha.mask(left.isna() | right.isna())

    def alpha_80(self) -> pd.DataFrame:
        left = self._rank(
            np.sign(
                self._delta(
                    self._ind_neutralize(self._field("open") * 0.868128 + self._field("high") * (1 - 0.868128)),
                    4,
                )
            )
        )
        right = self._ts_rank(self._correlation(self._field("high"), self._adv(10), 5), 6)
        return -(left**right)

    def alpha_81(self) -> pd.DataFrame:
        left = self._rank(
            np.log(
                self._product(
                    self._rank(
                        self._rank(self._correlation(self._field("vwap"), self._sum(self._adv(10), 50), 8)) ** 4
                    ),
                    15,
                ).clip(lower=self.eps)
            )
        )
        right = self._rank(self._correlation(self._rank(self._field("vwap")), self._rank(self._field("volume")), 5))
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_82(self) -> pd.DataFrame:
        left = self._rank(self._decay_linear(self._delta(self._field("open"), 1), 15))
        right = self._ts_rank(
            self._decay_linear(self._correlation(self._ind_neutralize(self._field("volume")), self._field("open"), 17), 7),
            13,
        )
        return -self._min_pair(left, right)

    def alpha_83(self) -> pd.DataFrame:
        close = self._field("close")
        high_low_ratio = (self._field("high") - self._field("low")) / (self._sum(close, 5) / 5 + self.eps)
        part_0 = self._rank(self._delay(high_low_ratio, 2))
        part_1 = self._rank(self._rank(self._field("volume")))
        part_2 = high_low_ratio / (self._field("vwap") - close + self.eps)
        return part_0 * part_1 / (part_2 + self.eps)

    def alpha_84(self) -> pd.DataFrame:
        left = self._ts_rank(self._field("vwap") - self._ts_max(self._field("vwap"), 15), 21)
        return left ** self._delta(self._field("close"), 5)

    def alpha_85(self) -> pd.DataFrame:
        left = self._rank(
            self._correlation(
                self._field("high") * 0.876703 + self._field("close") * (1 - 0.876703),
                self._adv(30),
                10,
            )
        )
        right = self._rank(
            self._correlation(
                self._ts_rank((self._field("high") + self._field("low")) / 2, 4),
                self._ts_rank(self._field("volume"), 10),
                7,
            )
        )
        return left**right

    def alpha_86(self) -> pd.DataFrame:
        left = self._ts_rank(self._correlation(self._field("close"), self._sum(self._adv(20), 15), 6), 20)
        right = self._rank((self._field("open") + self._field("close")) - (self._field("vwap") + self._field("open")))
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_87(self) -> pd.DataFrame:
        left = self._rank(
            self._decay_linear(self._delta(self._field("close") * 0.369701 + self._field("vwap") * (1 - 0.369701), 2), 3)
        )
        right = self._ts_rank(
            self._decay_linear(self._correlation(self._ind_neutralize(self._adv(81)), self._field("close"), 13).abs(), 5),
            14,
        )
        return -self._max_pair(left, right)

    def alpha_88(self) -> pd.DataFrame:
        left = self._rank(
            self._decay_linear(
                (self._rank(self._field("open")) + self._rank(self._field("low")))
                - (self._rank(self._field("high")) + self._rank(self._field("close"))),
                8,
            )
        )
        right = self._ts_rank(
            self._decay_linear(
                self._correlation(self._ts_rank(self._field("close"), 8), self._ts_rank(self._adv(60), 21), 8),
                7,
            ),
            3,
        )
        return self._min_pair(left, right)

    def alpha_89(self) -> pd.DataFrame:
        left = self._ts_rank(self._decay_linear(self._correlation(self._field("low"), self._adv(10), 7), 6), 4)
        right = self._ts_rank(self._decay_linear(self._delta(self._ind_neutralize(self._field("vwap")), 3), 10), 15)
        return left - right

    def alpha_90(self) -> pd.DataFrame:
        left = self._rank(self._field("close") - self._ts_max(self._field("close"), 5))
        right = self._ts_rank(self._correlation(self._ind_neutralize(self._adv(40)), self._field("low"), 5), 3)
        return -(left**right)

    def alpha_91(self) -> pd.DataFrame:
        left = self._ts_rank(
            self._decay_linear(
                self._decay_linear(self._correlation(self._ind_neutralize(self._field("close")), self._field("volume"), 10), 16),
                4,
            ),
            5,
        )
        right = self._rank(self._decay_linear(self._correlation(self._field("vwap"), self._adv(30), 4), 3))
        return -(left - right)

    def alpha_92(self) -> pd.DataFrame:
        left = self._ts_rank(
            self._decay_linear(
                (((self._field("high") + self._field("low")) / 2 + self._field("close")) < (self._field("low") + self._field("open"))).astype(float),
                15,
            ),
            19,
        )
        right = self._ts_rank(
            self._decay_linear(self._correlation(self._rank(self._field("low")), self._rank(self._adv(30)), 8), 7),
            7,
        )
        return self._min_pair(left, right)

    def alpha_93(self) -> pd.DataFrame:
        left = self._ts_rank(
            self._decay_linear(self._correlation(self._ind_neutralize(self._field("vwap")), self._adv(81), 17), 20),
            8,
        )
        right = self._rank(
            self._decay_linear(self._delta(self._field("close") * 0.524434 + self._field("vwap") * (1 - 0.524434), 3), 16)
        )
        return left / (right + self.eps)

    def alpha_94(self) -> pd.DataFrame:
        left = self._rank(self._field("vwap") - self._ts_min(self._field("vwap"), 12))
        right = self._ts_rank(
            self._correlation(self._ts_rank(self._field("vwap"), 20), self._ts_rank(self._adv(60), 4), 18),
            3,
        )
        return -(left**right)

    def alpha_95(self) -> pd.DataFrame:
        left = self._rank(self._field("open") - self._ts_min(self._field("open"), 12))
        right = self._ts_rank(
            self._rank(
                self._correlation(
                    self._sum((self._field("high") + self._field("low")) / 2, 19),
                    self._sum(self._adv(40), 19),
                    13,
                )
            )
            ** 5,
            12,
        )
        alpha = (left < right).astype(float)
        return alpha.mask(left.isna() | right.isna())

    def alpha_96(self) -> pd.DataFrame:
        left = self._ts_rank(
            self._decay_linear(self._correlation(self._rank(self._field("vwap")), self._rank(self._field("volume")), 4), 4),
            8,
        )
        right = self._ts_rank(
            self._decay_linear(
                self._ts_argmax(
                    self._correlation(self._ts_rank(self._field("close"), 7), self._ts_rank(self._adv(60), 4), 4),
                    13,
                ),
                14,
            ),
            13,
        )
        return -self._max_pair(left, right)

    def alpha_97(self) -> pd.DataFrame:
        left = self._rank(
            self._decay_linear(
                self._delta(self._ind_neutralize(self._field("low") * 0.721001 + self._field("vwap") * (1 - 0.721001)), 3),
                20,
            )
        )
        right = self._ts_rank(
            self._decay_linear(
                self._ts_rank(self._correlation(self._ts_rank(self._field("low"), 8), self._ts_rank(self._adv(60), 17), 5), 19),
                16,
            ),
            7,
        )
        return -(left - right)

    def alpha_98(self) -> pd.DataFrame:
        left = self._rank(
            self._decay_linear(self._correlation(self._field("vwap"), self._sum(self._adv(5), 26), 5), 7)
        )
        right = self._rank(
            self._decay_linear(
                self._ts_rank(
                    self._ts_argmin(self._correlation(self._rank(self._field("open")), self._rank(self._adv(15)), 21), 9),
                    7,
                ),
                8,
            )
        )
        return left - right

    def alpha_99(self) -> pd.DataFrame:
        left = self._rank(
            self._correlation(
                self._sum((self._field("high") + self._field("low")) / 2, 20),
                self._sum(self._adv(60), 20),
                9,
            )
        )
        right = self._rank(self._correlation(self._field("low"), self._field("volume"), 6))
        alpha = -((left < right).astype(float))
        return alpha.mask(left.isna() | right.isna())

    def alpha_100(self) -> pd.DataFrame:
        close = self._field("close")
        low = self._field("low")
        high = self._field("high")
        part_0 = 1.5 * self._scale(
            self._ind_neutralize(
                self._ind_neutralize(self._rank((((close - low) - (high - close)) / (high - low + self.eps)) * self._field("volume")))
            )
        )
        left = self._correlation(close, self._rank(self._adv(20)), 5)
        right = self._rank(self._ts_argmin(close, 30))
        part_1 = self._scale(self._ind_neutralize(left - right))
        part_2 = self._field("volume") / (self._adv(20) + self.eps)
        return -(part_0 - part_1) * part_2

    def alpha_101(self) -> pd.DataFrame:
        return (self._field("close") - self._field("open")) / (self._field("high") - self._field("low") + 0.001)
