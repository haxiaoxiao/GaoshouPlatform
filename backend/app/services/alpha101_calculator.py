from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.services.factor_precompute_runtime import (
    precompute_memory_policy,
    release_precompute_memory,
)
from app.services.factor_value_store import (
    FactorValueStore,
    factor_params_hash,
    get_factor_value_store,
)


class Alphas:
    def __init__(self, data: pd.DataFrame):
        self.data = data
        self.eps = 1e-8

    def _rank(self, x: pd.Series) -> pd.Series:
        """cross-sectional rank"""
        return x.groupby("date").rank(pct=True)

    def _delay(self, x: pd.Series, d: int) -> pd.Series:
        """value of x d days ago"""
        return x.groupby("symbol").shift(d)

    def _correlation(self, x: pd.Series, y: pd.Series, d: int) -> pd.Series:
        """time-serial correlation of x and y for the past d days"""
        return (
            pd.concat([x, y], axis=1, ignore_index=True)
            .groupby("symbol")
            .apply(lambda df: df[0].rolling(d).corr(df[1]))
            .droplevel(0)
            .replace([-np.inf, np.inf], np.nan)
        )  # type: ignore

    def _covariance(self, x: pd.Series, y: pd.Series, d: int) -> pd.Series:
        """time-serial covariance of x and y for the past d days"""
        return (
            pd.concat([x, y], axis=1, ignore_index=True)
            .groupby("symbol")
            .apply(lambda df: df[0].rolling(d).cov(df[1]))
            .droplevel(0)
        )  # type: ignore

    def _scale(self, x: pd.Series, a: float = 1.0) -> pd.Series:
        """rescaled x such that sum(abs(x)) = a (the default is a = 1)"""
        denom = x.abs().groupby("date").transform("sum")
        return x * a / (denom + self.eps)

    def _delta(self, x: pd.Series, d: int) -> pd.Series:
        """today's value of x minus the value of x d days ago"""
        return x.groupby("symbol").diff(d)

    def _decay_linear(self, x: pd.Series, d: int) -> pd.Series:
        """weighted moving average over the past d days with linearly decaying
        weights d, d – 1, …, 1 (rescaled to sum up to 1)"""
        weights = np.arange(1, d + 1)
        return (
            x.groupby("symbol")
            .rolling(d)
            .apply(lambda s: np.average(s, weights=weights), raw=True)
            .droplevel(0)
        )

    def _ind_neutralize(self, x: pd.Series) -> pd.Series:
        """x cross-sectionally neutralized against groups g (subindustries, industries,
        sectors, etc.), i.e., x is cross-sectionally demeaned within each group g"""
        return x - x.groupby(["date", self.data["industry"]]).transform("mean")

    def _ts_min(self, x: pd.Series, d: int) -> pd.Series:
        """time-series min over the past d days"""
        return x.groupby("symbol").rolling(d).min().droplevel(0)

    def _ts_max(self, x: pd.Series, d: int) -> pd.Series:
        """time-series max over the past d days"""
        return x.groupby("symbol").rolling(d).max().droplevel(0)

    def _ts_argmin(self, x: pd.Series, d: int) -> pd.Series:
        """which day ts_min(x, d) occurred on"""
        return (
            x.groupby("symbol")
            .rolling(d)
            .apply(lambda s: np.argmin(s), raw=True)
            .droplevel(0)
        )

    def _ts_argmax(self, x: pd.Series, d: int) -> pd.Series:
        """which day ts_max(x, d) occurred on"""
        return (
            x.groupby("symbol")
            .rolling(d)
            .apply(lambda s: np.argmax(s), raw=True)
            .droplevel(0)
        )

    def _ts_rank(self, x: pd.Series, d: int) -> pd.Series:
        """time-series rank in the past d days"""
        return x.groupby("symbol").rolling(d).rank(pct=True).droplevel(0)

    def _sum(self, x: pd.Series, d: int) -> pd.Series:
        """time-series sum over the past d days"""
        return x.groupby("symbol").rolling(d).sum().droplevel(0)

    def _product(self, x: pd.Series, d: int) -> pd.Series:
        """time-series product over the past d days"""
        return x.groupby("symbol").rolling(d).apply(np.prod, raw=True).droplevel(0)

    def _stddev(self, x: pd.Series, d: int) -> pd.Series:
        """moving time-series standard deviation over the past d days"""
        return x.groupby("symbol").rolling(d).std().droplevel(0)

    def _adv(self, d: int) -> pd.Series:
        return self.data.groupby("symbol")["volume"].rolling(d).mean().droplevel(0)

    def alpha_1(self) -> pd.Series:
        """Alpha#1: (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) -
        0.5)"""
        return (
            self._rank(
                self._ts_argmax(
                    self._stddev(self.data["return"], 20).where(
                        self.data["return"] < 0, self.data["close"]
                    )
                    ** 2,
                    5,
                )
            )
            - 0.5
        )

    def alpha_2(self) -> pd.Series:
        """Alpha#2: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))"""
        x = self._rank(self._delta(self.data["volume"].apply(np.log), 2))
        y = self._rank(
            (self.data["close"] - self.data["open"]) / (self.data["open"] + self.eps)
        )

        return -self._correlation(x, y, 6)

    def alpha_3(self) -> pd.Series:
        """Alpha#3: (-1 * correlation(rank(open), rank(volume), 10))"""
        x = self._rank(self.data["open"])
        y = self._rank(self.data["volume"])

        return -self._correlation(x, y, 10)

    def alpha_4(self) -> pd.Series:
        """Alpha#4: (-1 * Ts_Rank(rank(low), 9))"""
        return -self._ts_rank(self._rank(self.data["low"]), 9)

    def alpha_5(self) -> pd.Series:
        """Alpha#5: (rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))"""
        left = self._rank(self.data["open"] - (self._sum(self.data["vwap"], 10) / 10))
        right = -self._rank(self.data["close"] - self.data["vwap"]).abs()

        return left * right

    def alpha_6(self) -> pd.Series:
        """Alpha#6: (-1 * correlation(open, volume, 10))"""
        return -self._correlation(self.data["open"], self.data["volume"], 10)

    def alpha_7(self) -> pd.Series:
        """Alpha#7: ((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1
        * 1))"""
        x = self._ts_rank(self._delta(self.data["close"], 7).abs(), 60) * self._delta(
            self.data["close"], 7
        ).apply(np.sign)

        alpha = x.where(self._adv(20) < self.data["volume"], -1)
        nan_mask = self._adv(20).isna()
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_8(self) -> pd.Series:
        """Alpha#8: (-1 * rank(((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)),
        10)))"""
        left = self._sum(self.data["open"], 5) * self._sum(self.data["return"], 5)
        return -self._rank(left - self._delay(left, 10))

    def alpha_9(self) -> pd.Series:
        """Alpha#9: ((0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) : ((ts_max(delta(close, 1), 5) < 0) ?
        delta(close, 1) : (-1 * delta(close, 1))))"""
        inner = self._delta(self.data["close"], 1)
        cond_0 = self._ts_min(inner, 5) > 0
        x = self._delta(self.data["close"], 1)
        cond_1 = self._ts_max(inner, 5) < 0

        alpha = x.where(cond_0, x.where(cond_1, -x))

        nan_mask = self._ts_min(inner, 5).isna()
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_10(self) -> pd.Series:
        """Alpha#10: rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) : ((ts_max(delta(close, 1), 4) < 0)
        ? delta(close, 1) : (-1 * delta(close, 1))))"""
        inner = self._delta(self.data["close"], 1)
        cond_0 = self._ts_min(inner, 4) > 0
        x = self._delta(self.data["close"], 1)
        cond_1 = self._ts_max(inner, 4) < 0

        alpha = x.where(cond_0, x.where(cond_1, -x))
        nan_mask = self._ts_min(inner, 4).isna()
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_11(self) -> pd.Series:
        """Alpha#11: ((rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) *
        rank(delta(volume, 3)))"""
        inner = self.data["vwap"] - self.data["close"]

        return self._rank(self._ts_max(inner, 3)) + self._rank(
            self._ts_min(inner, 3)
        ) * self._rank(self._delta(self.data["volume"], 3))

    def alpha_12(self) -> pd.Series:
        """Alpha#12: (sign(delta(volume, 1)) * (-1 * delta(close, 1)))"""
        return self._delta(self.data["volume"], 1).apply(np.sign) * (
            -self._delta(self.data["close"], 1)
        )

    def alpha_13(self) -> pd.Series:
        """Alpha#13: (-1 * rank(covariance(rank(close), rank(volume), 5)))"""
        x = self._rank(self.data["close"])
        y = self._rank(self.data["volume"])

        return -self._rank(self._covariance(x, y, 5))

    def alpha_14(self) -> pd.Series:
        """Alpha#14: ((-1 * rank(delta(returns, 3))) * correlation(open, volume, 10))"""
        return -self._rank(self._delta(self.data["return"], 3)) * self._correlation(
            self.data["open"], self.data["volume"], 10
        )

    def alpha_15(self) -> pd.Series:
        """Alpha#15: (-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3))"""
        return -self._sum(
            self._rank(
                self._correlation(
                    self._rank(self.data["high"]), self._rank(self.data["volume"]), 3
                )
            ),
            3,
        )

    def alpha_16(self) -> pd.Series:
        """Alpha#16: (-1 * rank(covariance(rank(high), rank(volume), 5)))"""
        return -self._rank(
            self._covariance(
                self._rank(self.data["high"]), self._rank(self.data["volume"]), 5
            )
        )

    def alpha_17(self) -> pd.Series:
        """Alpha#17: (((-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1))) *
        rank(ts_rank((volume / adv20), 5)))"""
        part_0 = -self._rank(self._ts_rank(self.data["close"], 10))
        part_1 = self._rank(self._delta(self._delta(self.data["close"], 1), 1))
        part_2 = self._rank(
            self._ts_rank(self.data["volume"] / (self._adv(20) + self.eps), 5)
        )

        return part_0 * part_1 * part_2

    def alpha_18(self) -> pd.Series:
        """Alpha#18: (-1 * rank(((stddev(abs((close - open)), 5) + (close - open)) + correlation(close, open,
        10))))"""
        return -self._rank(
            self._stddev((self.data["close"] - self.data["open"]).abs(), 5)
            + (self.data["close"] - self.data["open"])
        ) + self._correlation(self.data["close"], self.data["open"], 10)

    def alpha_19(self) -> pd.Series:
        """Alpha#19: ((-1 * sign(((close - delay(close, 7)) + delta(close, 7)))) * (1 + rank((1 + sum(returns,
        250)))))"""
        left = -(
            self.data["close"]
            - self._delay(self.data["close"], 7)
            + self._delta(self.data["close"], 7)
        ).apply(np.sign)

        right = 1 + self._rank(1 + self._sum(self.data["return"], 250))

        return left * right

    def alpha_20(self) -> pd.Series:
        """Alpha#20: (((-1 * rank((open - delay(high, 1)))) * rank((open - delay(close, 1)))) * rank((open -
        delay(low, 1))))"""
        return (
            -self._rank(self.data["open"] - self._delay(self.data["high"], 1))
            * self._rank(self.data["open"] - self._delay(self.data["close"], 1))
            * self._rank(self.data["open"] - self._delay(self.data["low"], 1))
        )

    def alpha_21(self) -> None:
        """Alpha#21: ((((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? (-1 * 1) : (((sum(close,
        2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 : (((1 < (volume / adv20)) || ((volume /
        adv20) == 1)) ? 1 : (-1 * 1))))"""
        sma8 = self._sum(self.data["close"], 8) / 8
        std8 = self._stddev(self.data["close"], 8)
        sma2 = self._sum(self.data["close"], 2) / 2
        volume_ratio = self.data["volume"] / (self._adv(20) + self.eps)
        alpha = pd.Series(
            np.where(
                (sma8 + std8) < sma2,
                -1.0,
                np.where(sma2 < (sma8 - std8), 1.0, np.where(volume_ratio >= 1.0, 1.0, -1.0)),
            ),
            index=self.data.index,
        )
        alpha.loc[(sma8.isna()) | (std8.isna()) | (sma2.isna())] = np.nan
        return alpha

    def alpha_22(self) -> pd.Series:
        """Alpha#22: (-1 * (delta(correlation(high, volume, 5), 5) * rank(stddev(close, 20))))"""
        left = -self._delta(
            self._correlation(self.data["high"], self.data["volume"], 5), 5
        )
        right = self._rank(self._stddev(self.data["close"], 20))

        return left * right

    def alpha_23(self) -> pd.Series:
        """Alpha#23: (((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)"""
        alpha = (-self._delta(self.data["high"], 2)).where(
            (self._sum(self.data["high"], 20) / 20) < self.data["high"], 0
        )
        nan_mask = self._sum(self.data["high"], 20).isna()
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_24(self) -> pd.Series:
        """Alpha#24: ((((delta((sum(close, 100) / 100), 100) / delay(close, 100)) < 0.05) ||
        ((delta((sum(close, 100) / 100), 100) / delay(close, 100)) == 0.05)) ? (-1 * (close - ts_min(close,
        100))) : (-1 * delta(close, 3)))"""
        left = self._delta(self._sum(self.data["close"], 100) / 100, 100) / (
            self._delay(self.data["close"], 100) + self.eps
        )
        cond = left <= 0.05
        x = -(self.data["close"] - self._ts_min(self.data["close"], 100))
        y = -self._delta(self.data["close"], 3)

        alpha = x.where(cond, y)
        nan_mask = (left).isna()
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_25(self) -> pd.Series:
        """Alpha#25: rank(((((-1 * returns) * adv20) * vwap) * (high - close)))"""
        return self._rank(
            -self.data["return"]
            * self._adv(20)
            * self.data["vwap"]
            * (self.data["high"] - self.data["close"])
        )

    def alpha_26(self) -> pd.Series:
        """Alpha#26: (-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3))"""
        return -self._ts_max(
            self._correlation(
                self._ts_rank(self.data["volume"], 5),
                self._ts_rank(self.data["high"], 5),
                5,
            ),
            3,
        )

    def alpha_27(self) -> None:
        """Alpha#27: ((0.5 < rank((sum(correlation(rank(volume), rank(vwap), 6), 2) / 2.0))) ? (-1 * 1) : 1)"""
        signal = self._rank(self._sum(self._correlation(self._rank(self.data["volume"]), self._rank(self.data["vwap"]), 6), 2) / 2.0)
        alpha = pd.Series(np.where(signal > 0.5, -1.0, 1.0), index=self.data.index)
        alpha.loc[signal.isna()] = np.nan
        return alpha

    def alpha_28(self) -> pd.Series:
        """Alpha#28: scale(((correlation(adv20, low, 5) + ((high + low) / 2)) - close))"""
        return self._scale(
            self._correlation(self._adv(20), self.data["low"], 5)
            + (self.data["high"] + self.data["low"]) / 2
            - self.data["close"]
        )

    def alpha_29(self) -> pd.Series:
        """Alpha#29: (min(product(rank(rank(scale(log(sum(ts_min(rank(rank((-1 * rank(delta((close - 1),
        5))))), 2), 1))))), 1), 5) + ts_rank(delay((-1 * returns), 6), 5))"""
        left = self._ts_min(
            self._rank(
                self._rank(
                    self._scale(
                        self._sum(
                            self._ts_min(
                                self._rank(
                                    self._rank(
                                        -self._rank(
                                            self._delta(self.data["close"] - 1, 5)
                                        )
                                    )
                                ),
                                2,
                            ),
                            1,
                        ).apply(np.log)
                    )
                )
            ),
            5,
        )
        right = self._ts_rank(self._delay(-self.data["return"], 6), 5)

        return left + right

    def alpha_30(self) -> pd.Series:
        """Alpha#30: (((1.0 - rank(((sign((close - delay(close, 1))) + sign((delay(close, 1) - delay(close, 2)))) +
        sign((delay(close, 2) - delay(close, 3)))))) * sum(volume, 5)) / sum(volume, 20))"""
        left = 1 - self._rank(
            (self.data["close"] - self._delay(self.data["close"], 1)).apply(np.sign)
            + (
                self._delay(self.data["close"], 1) - self._delay(self.data["close"], 2)
            ).apply(np.sign)
            + (
                self._delay(self.data["close"], 2) - self._delay(self.data["close"], 3)
            ).apply(np.sign)
        )

        return (
            left
            * self._sum(self.data["volume"], 5)
            / (self._sum(self.data["volume"], 20) + self.eps)
        )

    def alpha_31(self) -> pd.Series:
        """Alpha#31: ((rank(rank(rank(decay_linear((-1 * rank(rank(delta(close, 10)))), 10)))) + rank((-1 *
        delta(close, 3)))) + sign(scale(correlation(adv20, low, 12))))"""
        part_0 = self._rank(
            self._rank(
                self._rank(
                    self._decay_linear(
                        -self._rank(self._rank(self._delta(self.data["close"], 10))), 10
                    )
                )
            )
        )
        part_1 = self._rank(-self._delta(self.data["close"], 3))
        part_2 = self._scale(
            self._correlation(self._adv(20), self.data["low"], 12)
        ).apply(np.sign)

        return part_0 + part_1 + part_2

    def alpha_32(self) -> pd.Series:
        """Alpha#32: (scale(((sum(close, 7) / 7) - close)) + (20 * scale(correlation(vwap, delay(close, 5),
        230))))"""
        left = self._scale((self._sum(self.data["close"], 7) / 7) - self.data["close"])
        right = 20 * self._scale(
            self._correlation(
                self.data["vwap"], self._delay(self.data["close"], 5), 230
            )
        )

        return left + right

    def alpha_33(self) -> pd.Series:
        """Alpha#33: rank((-1 * ((1 - (open / close))^1)))"""
        return self._rank(-(1 - (self.data["open"] / (self.data["close"] + self.eps))))

    def alpha_34(self) -> pd.Series:
        """Alpha#34: rank(((1 - rank((stddev(returns, 2) / stddev(returns, 5)))) + (1 - rank(delta(close, 1)))))"""
        left = 1 - self._rank(
            self._stddev(self.data["return"], 2)
            / (self._stddev(self.data["return"], 5) + self.eps)
        )
        right = 1 - self._rank(self._delta(self.data["close"], 1))

        return self._rank(left + right)

    def alpha_35(self) -> pd.Series:
        """Alpha#35: ((Ts_Rank(volume, 32) * (1 - Ts_Rank(((close + high) - low), 16))) * (1 -
        Ts_Rank(returns, 32)))"""
        return (
            self._ts_rank(self.data["volume"], 32)
            * (
                1
                - self._ts_rank(
                    self.data["close"] + self.data["high"] - self.data["low"], 16
                )
            )
            * (1 - self._ts_rank(self.data["return"], 32))
        )

    def alpha_36(self) -> pd.Series:
        """Alpha#36: (((((2.21 * rank(correlation((close - open), delay(volume, 1), 15))) + (0.7 * rank((open
        - close)))) + (0.73 * rank(Ts_Rank(delay((-1 * returns), 6), 5)))) + rank(abs(correlation(vwap,
        adv20, 6)))) + (0.6 * rank((((sum(close, 200) / 200) - open) * (close - open)))))"""
        part_0 = 2.21 * self._rank(
            self._correlation(
                self.data["close"] - self.data["open"],
                self._delay(self.data["volume"], 1),
                15,
            )
        )
        part_1 = 0.7 * self._rank(self.data["open"] - self.data["close"])
        part_2 = 0.73 * self._rank(
            self._ts_rank(self._delay(-self.data["return"], 6), 5)
        )
        part_3 = self._rank(
            self._correlation(self.data["vwap"], self._adv(20), 6).abs()
        )
        part_4 = 0.6 * self._rank(
            (self._sum(self.data["close"], 200) / 200 - self.data["open"])
            * (self.data["close"] - self.data["open"])
        )

        return part_0 + part_1 + part_2 + part_3 + part_4

    def alpha_37(self) -> pd.Series:
        """Alpha#37: (rank(correlation(delay((open - close), 1), close, 200)) + rank((open - close)))"""
        left = self._rank(
            self._correlation(
                self._delay(self.data["open"] - self.data["close"], 1),
                self.data["close"],
                200,
            )
        )
        right = self._rank(self.data["open"] - self.data["close"])

        return left + right

    def alpha_38(self) -> pd.Series:
        """Alpha#38: ((-1 * rank(Ts_Rank(close, 10))) * rank((close / open)))"""
        left = -self._rank(self._ts_rank(self.data["close"], 10))
        right = self._rank(self.data["close"] / (self.data["open"] + self.eps))

        return left * right

    def alpha_39(self) -> pd.Series:
        """Alpha#39: ((-1 * rank((delta(close, 7) * (1 - rank(decay_linear((volume / adv20), 9)))))) * (1 +
        rank(sum(returns, 250))))"""
        left = -self._rank(
            self._delta(self.data["close"], 7)
            * (
                1
                - self._rank(
                    self._decay_linear(
                        self.data["volume"] / (self._adv(20) + self.eps), 9
                    )
                )
            )
        )
        right = 1 + self._rank(self._sum(self.data["return"], 250))

        return left * right

    def alpha_40(self) -> pd.Series:
        """Alpha#40: ((-1 * rank(stddev(high, 10))) * correlation(high, volume, 10))"""
        return -self._rank(self._stddev(self.data["high"], 10)) * self._correlation(
            self.data["high"], self.data["volume"], 10
        )

    def alpha_41(self) -> pd.Series:
        """Alpha#41: (((high * low)^0.5) - vwap)"""
        return (self.data["high"] * self.data["low"]) ** 0.5 - self.data["vwap"]

    def alpha_42(self) -> pd.Series:
        """Alpha#42: (rank((vwap - close)) / rank((vwap + close)))"""
        return self._rank(self.data["vwap"] - self.data["close"]) / (
            self._rank(self.data["vwap"] + self.data["close"]) + self.eps
        )

    def alpha_43(self) -> pd.Series:
        """Alpha#43: (ts_rank((volume / adv20), 20) * ts_rank((-1 * delta(close, 7)), 8))"""
        return self._ts_rank(
            self.data["volume"] / (self._adv(20) + self.eps), 20
        ) * self._ts_rank(-self._delta(self.data["close"], 7), 8)

    def alpha_44(self) -> pd.Series:
        """Alpha#44: (-1 * correlation(high, rank(volume), 5))"""
        return -self._correlation(self.data["high"], self._rank(self.data["volume"]), 5)

    def alpha_45(self) -> pd.Series:
        """Alpha#45: (-1 * ((rank((sum(delay(close, 5), 20) / 20)) * correlation(close, volume, 2)) *
        rank(correlation(sum(close, 5), sum(close, 20), 2))))"""
        part_0 = self._rank(self._sum(self._delay(self.data["close"], 5), 20) / 20)
        part_1 = self._correlation(self.data["close"], self.data["volume"], 2)
        part_2 = self._rank(
            self._correlation(
                self._sum(self.data["close"], 5), self._sum(self.data["close"], 20), 2
            )
        )

        return -part_0 * part_1 * part_2

    def alpha_46(self) -> None:
        """Alpha#46: ((0.25 < (((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10))) ?
        (-1 * 1) : (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < 0) ? 1 :
        ((-1 * 1) * (close - delay(close, 1)))))"""
        # left = (
        #     self._delay(self.data["close"], 20) - self._delay(self.data["close"], 10)
        # ) / 10 - (self._delay(self.data["close"], 10) - self.data["close"]) / 10
        # y = -(self.data["close"] - self._delay(self.data["close"], 1))

        # alpha = pd.Series(
        #     np.where(left > 0.25, -1, np.where(left < 0, 1, y)), index=self.data.index
        # )
        # nan_mask = left.isna()
        # alpha.loc[nan_mask] = np.nan

        left = (
            (self._delay(self.data["close"], 20) - self._delay(self.data["close"], 10)) / 10
            - (self._delay(self.data["close"], 10) - self.data["close"]) / 10
        )
        fallback = -(self.data["close"] - self._delay(self.data["close"], 1))
        alpha = pd.Series(
            np.where(left > 0.25, -1.0, np.where(left < 0.0, 1.0, fallback)),
            index=self.data.index,
        )
        alpha.loc[left.isna()] = np.nan
        return alpha

    def alpha_47(self) -> pd.Series:
        """Alpha#47: ((((rank((1 / close)) * volume) / adv20) * ((high * rank((high - close))) / (sum(high, 5) /
        5))) - rank((vwap - delay(vwap, 5))))"""
        part_0 = (
            self._rank(1 / (self.data["close"] + self.eps))
            * self.data["volume"]
            / (self._adv(20) + self.eps)
        )
        part_1 = (
            self.data["high"]
            * self._rank(self.data["high"] - self.data["close"])
            / (self._sum(self.data["high"], 5) / 5)
        )
        part_2 = self._rank(self.data["vwap"] - self._delay(self.data["vwap"], 5))

        return part_0 * part_1 - part_2

    def alpha_48(self) -> pd.Series:
        """Alpha#48: (indneutralize(((correlation(delta(close, 1), delta(delay(close, 1), 1), 250) *
        delta(close, 1)) / close), IndClass.subindustry) / sum(((delta(close, 1) / delay(close, 1))^2), 250))"""
        left = self._ind_neutralize(
            self._correlation(
                self._delta(self.data["close"], 1),
                self._delta(self._delay(self.data["close"], 1), 1),
                250,
            )
            * self._delta(self.data["close"], 1)
            / (self.data["close"] + self.eps)
        )
        right = self._sum(
            self._delta(self.data["close"], 1)
            / (self._delay(self.data["close"], 1) ** 2 + self.eps),
            250,
        )

        return left / (right + self.eps)

    def alpha_49(self) -> pd.Series:
        """Alpha#49: (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 *
        0.1)) ? 1 : ((-1 * 1) * (close - delay(close, 1))))"""
        left = (
            self._delay(self.data["close"], 20) - self._delay(self.data["close"], 10)
        ) / 10 - ((self._delay(self.data["close"], 10) - self.data["close"]) / 10)
        y = -(self.data["close"] - self._delay(self.data["close"], 1))

        alpha = pd.Series(np.where(left < -0.1, 1, y), index=self.data.index)
        nan_mask = left.isna()
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_50(self) -> pd.Series:
        """Alpha#50: (-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))"""
        return -self._ts_max(
            self._rank(
                self._correlation(
                    self._rank(self.data["volume"]), self._rank(self.data["vwap"]), 5
                ),
            ),
            5,
        )

    def alpha_51(self) -> pd.Series:
        """Alpha#51: (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 *
        0.05)) ? 1 : ((-1 * 1) * (close - delay(close, 1))))"""
        left = (
            self._delay(self.data["close"], 20) - self._delay(self.data["close"], 10)
        ) / 10 - (self._delay(self.data["close"], 10) - self.data["close"]) / 10
        y = -(self.data["close"] - self._delay(self.data["close"], 1))

        alpha = pd.Series(np.where(left < -0.05, 1, y), index=self.data.index)
        nan_mask = np.isnan(left)
        alpha.loc[nan_mask] = np.nan

        return alpha

    def alpha_52(self) -> pd.Series:
        """Alpha#52: ((((-1 * ts_min(low, 5)) + delay(ts_min(low, 5), 5)) * rank(((sum(returns, 240) -
        sum(returns, 20)) / 220))) * ts_rank(volume, 5))"""
        left = -self._ts_min(self.data["low"], 5)
        right = (
            self._delay(self._ts_min(self.data["low"], 5), 5)
            * self._rank(
                (
                    self._sum(self.data["return"], 240)
                    - self._sum(self.data["return"], 20)
                )
                / 220
            )
            * self._ts_rank(self.data["volume"], 5)
        )

        return left + right

    def alpha_53(self) -> pd.Series:
        """Alpha#53: (-1 * delta((((close - low) - (high - close)) / (close - low)), 9))"""
        return -self._delta(
            (
                (self.data["close"] - self.data["low"])
                - (self.data["high"] - self.data["close"])
            )
            / (self.data["close"] - self.data["low"] + self.eps),
            9,
        )

    def alpha_54(self) -> pd.Series:
        """Alpha#54: ((-1 * ((low - close) * (open^5))) / ((low - high) * (close^5)))"""
        return -((self.data["low"] - self.data["close"]) * self.data["open"] ** 5) / (
            (self.data["low"] - self.data["high"]) * self.data["close"] ** 5 + self.eps
        )

    def alpha_55(self) -> pd.Series:
        """Alpha#55: (-1 * correlation(rank(((close - ts_min(low, 12)) / (ts_max(high, 12) - ts_min(low,
        12)))), rank(volume), 6))"""
        return -self._correlation(
            self._rank(
                (self.data["close"] - self._ts_min(self.data["low"], 12))
                / (
                    self._ts_max(self.data["high"], 12)
                    - self._ts_min(self.data["low"], 12)
                    + self.eps
                )
            ),
            self._rank(self.data["volume"]),
            6,
        )

    def alpha_56(self) -> pd.Series:
        """Alpha#56: (0 - (1 * (rank((sum(returns, 10) / sum(sum(returns, 2), 3))) * rank((returns * cap)))))"""
        left = self._rank(
            self._sum(self.data["return"], 10)
            / self._sum(self._sum(self.data["return"], 2), 3)
        )
        right = self._rank(self.data["return"] * self.data["market_value"])

        return -left * right

    def alpha_57(self) -> pd.Series:
        """Alpha#57: (0 - (1 * ((close - vwap) / decay_linear(rank(ts_argmax(close, 30)), 2))))"""
        return -(self.data["close"] - self.data["vwap"]) / (
            self._decay_linear(self._rank(self._ts_argmax(self.data["close"], 30)), 2)
            + self.eps
        )

    def alpha_58(self) -> pd.Series:
        """Alpha#58: (-1 * Ts_Rank(decay_linear(correlation(IndNeutralize(vwap, IndClass.sector), volume,
        3.92795), 7.89291), 5.50322))"""
        return -self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ind_neutralize(self.data["vwap"]), self.data["volume"], 4
                ),
                8,
            ),
            6,
        )

    def alpha_59(self) -> pd.Series:
        """Alpha#59: (-1 * Ts_Rank(decay_linear(correlation(IndNeutralize(((vwap * 0.728317) + (vwap *
        (1 - 0.728317))), IndClass.industry), volume, 4.25197), 16.2289), 8.19648))"""
        return -self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ind_neutralize(self.data["vwap"]), self.data["volume"], 4
                ),
                16,
            ),
            8,
        )

    def alpha_60(self) -> pd.Series:
        """Alpha#60: (0 - (1 * ((2 * scale(rank(((((close - low) - (high - close)) / (high - low)) * volume)))) -
        scale(rank(ts_argmax(close, 10))))))"""
        left = 2 * self._scale(
            self._rank(

                    (
                        (self.data["close"] - self.data["low"])
                        - (self.data["high"] - self.data["close"])
                    )
                    / (self.data["high"] - self.data["low"] + self.eps)
                    * self.data["volume"]

            )
        )
        right = self._scale(self._rank(self._ts_argmax(self.data["close"], 10)))

        return -(left - right)

    def alpha_61(self) -> None:
        """Alpha#61: (rank((vwap - ts_min(vwap, 16.1219))) < rank(correlation(vwap, adv180, 17.9282)))"""
        left = self._rank(self.data["vwap"] - self._ts_min(self.data["vwap"], 16))
        right = self._rank(self._correlation(self.data["vwap"], self._adv(180), 18))
        alpha = (left < right).astype(float)
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_62(self) -> None:
        """Alpha#62: ((rank(correlation(vwap, sum(adv20, 22.4101), 9.91009)) < rank(((rank(open) +
        rank(open)) < (rank(((high + low) / 2)) + rank(high))))) * -1)"""
        left = self._rank(self._correlation(self.data["vwap"], self._sum(self._adv(20), 22), 10))
        right = self._rank(
            (self._rank(self.data["open"]) + self._rank(self.data["open"]))
            < (self._rank((self.data["high"] + self.data["low"]) / 2) + self._rank(self.data["high"]))
        )
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_63(self) -> pd.Series:
        """Alpha#63: ((rank(decay_linear(delta(IndNeutralize(close, IndClass.industry), 2.25164), 8.22237))
        - rank(decay_linear(correlation(((vwap * 0.318108) + (open * (1 - 0.318108))), sum(adv180,
        37.2467), 13.557), 12.2883))) * -1)"""
        left = self._rank(
            self._decay_linear(
                self._delta(self._ind_neutralize(self.data["close"]), 2), 8
            )
        )
        right = self._rank(
            self._decay_linear(
                self._correlation(
                    self.data["vwap"] * 0.318108 + self.data["open"] * (1 - 0.318108),
                    self._sum(self._adv(180), 37),
                    14,
                ),
                12,
            )
        )

        return -(left - right)

    def alpha_64(self) -> None:
        """Alpha#64: ((rank(correlation(sum(((open * 0.178404) + (low * (1 - 0.178404))), 12.7054),
        sum(adv120, 12.7054), 16.6208)) < rank(delta(((((high + low) / 2) * 0.178404) + (vwap * (1 -
        0.178404))), 3.69741))) * -1)"""
        left = self._rank(
            self._correlation(
                self._sum(self.data["open"] * 0.178404 + self.data["low"] * (1 - 0.178404), 13),
                self._sum(self._adv(120), 13),
                17,
            )
        )
        right = self._rank(
            self._delta((((self.data["high"] + self.data["low"]) / 2) * 0.178404) + (self.data["vwap"] * (1 - 0.178404)), 4)
        )
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_65(self) -> None:
        """Alpha#65: ((rank(correlation(((open * 0.00817205) + (vwap * (1 - 0.00817205))), sum(adv60,
        8.6911), 6.40374)) < rank((open - ts_min(open, 13.635)))) * -1)"""
        left = self._rank(
            self._correlation(
                self.data["open"] * 0.00817205 + self.data["vwap"] * (1 - 0.00817205),
                self._sum(self._adv(60), 9),
                6,
            )
        )
        right = self._rank(self.data["open"] - self._ts_min(self.data["open"], 14))
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_66(self) -> pd.Series:
        """Alpha#66: ((rank(decay_linear(delta(vwap, 3.51013), 7.23052)) + Ts_Rank(decay_linear(((((low
        * 0.96633) + (low * (1 - 0.96633))) - vwap) / (open - ((high + low) / 2))), 11.4157), 6.72611)) * -1)"""
        left = self._rank(self._decay_linear(self._delta(self.data["vwap"], 4), 7))
        right = self._ts_rank(
            self._decay_linear(
                (self.data["low"] - self.data["vwap"])
                / (
                    self.data["open"]
                    - (self.data["high"] + self.data["low"]) / 2
                    + self.eps
                ),
                11,
            ),
            7,
        )

        return -(left + right)

    def alpha_67(self) -> pd.Series:
        """Alpha#67: ((rank((high - ts_min(high, 2.14593)))^rank(correlation(IndNeutralize(vwap,
        IndClass.sector), IndNeutralize(adv20, IndClass.subindustry), 6.02936))) * -1)"""
        left = self._rank(self.data["high"] - self._ts_min(self.data["high"], 2))
        right = self._rank(
            self._correlation(
                self._ind_neutralize(self.data["vwap"]),
                self._ind_neutralize(self._adv(20)),
                6,
            )
        )

        return -(left**right)

    def alpha_68(self) -> None:
        """Alpha#68: ((Ts_Rank(correlation(rank(high), rank(adv15), 8.91644), 13.9333) <
        rank(delta(((close * 0.518371) + (low * (1 - 0.518371))), 1.06157))) * -1)"""
        left = self._ts_rank(self._correlation(self._rank(self.data["high"]), self._rank(self._adv(15)), 9), 14)
        right = self._rank(self._delta(self.data["close"] * 0.518371 + self.data["low"] * (1 - 0.518371), 1))
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_69(self) -> pd.Series:
        """Alpha#69: ((rank(ts_max(delta(IndNeutralize(vwap, IndClass.industry), 2.72412),
        4.79344))^Ts_Rank(correlation(((close * 0.490655) + (vwap * (1 - 0.490655))), adv20, 4.92416),
        9.0615)) * -1)"""
        left = self._rank(
            self._ts_max(self._delta(self._ind_neutralize(self.data["vwap"]), 3), 5)
        )
        right = self._ts_rank(
            self._correlation(
                self.data["close"] * 0.490655 + self.data["vwap"] * (1 - 0.490655),
                self._adv(20),
                5,
            ),
            9,
        )

        return -(left**right)

    def alpha_70(self) -> pd.Series:
        """Alpha#70: ((rank(delta(vwap, 1.29456))^Ts_Rank(correlation(IndNeutralize(close,
        IndClass.industry), adv50, 17.8256), 17.9171)) * -1)"""
        left = self._rank(self._delta(self.data["vwap"], 1))
        right = self._ts_rank(
            self._correlation(
                self._ind_neutralize(self.data["close"]), self._adv(50), 18
            ),
            18,
        )

        return -(left**right)

    def alpha_71(self) -> pd.Series:
        """Alpha#71: max(Ts_Rank(decay_linear(correlation(Ts_Rank(close, 3.43976), Ts_Rank(adv180,
        12.0647), 18.0175), 4.20501), 15.6948), Ts_Rank(decay_linear((rank(((low + open) - (vwap +
        vwap)))^2), 16.4662), 4.4388))"""
        left = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ts_rank(self.data["close"], 3),
                    self._ts_rank(self._adv(180), 12),
                    18,
                ),
                4,
            ),
            16,
        )
        right = self._ts_rank(
            self._decay_linear(
                self._rank(
                    (self.data["low"] + self.data["open"]) - 2 * self.data["vwap"]
                )
                ** 2,
                16,
            ),
            4,
        )

        return pd.concat([left, right], axis=1).max(axis=1)

    def alpha_72(self) -> pd.Series:
        """Alpha#72: (rank(decay_linear(correlation(((high + low) / 2), adv40, 8.93345), 10.1519)) /
        rank(decay_linear(correlation(Ts_Rank(vwap, 3.72469), Ts_Rank(volume, 18.5188), 6.86671),
        2.95011)))"""
        left = self._rank(
            self._decay_linear(
                self._correlation(
                    (self.data["high"] + self.data["low"]) / 2, self._adv(40), 9
                ),
                10,
            )
        )
        right = self._rank(
            self._decay_linear(
                self._correlation(
                    self._ts_rank(self.data["vwap"], 4),
                    self._ts_rank(self.data["volume"], 19),
                    7,
                ),
                3,
            )
        )

        return left / (right + self.eps)

    def alpha_73(self) -> pd.Series:
        """Alpha#73: (max(rank(decay_linear(delta(vwap, 4.72775), 2.91864)),
        Ts_Rank(decay_linear(((delta(((open * 0.147155) + (low * (1 - 0.147155))), 2.03608) / ((open *
        0.147155) + (low * (1 - 0.147155)))) * -1), 3.33829), 16.7411)) * -1)"""
        left = self._rank(self._decay_linear(self._delta(self.data["vwap"], 5), 3))
        right = self._ts_rank(
            self._decay_linear(
                -self._delta(
                    self.data["open"] * 0.147155 + self.data["low"] * (1 - 0.147155), 2
                )
                / (
                    self.data["open"] * 0.147155
                    + self.data["low"] * (1 - 0.147155)
                    + self.eps
                ),
                3,
            ),
            17,
        )

        return -pd.concat([left, right], axis=1).max(axis=1)

    def alpha_74(self) -> None:
        """Alpha#74: ((rank(correlation(close, sum(adv30, 37.4843), 15.1365)) <
        rank(correlation(rank(((high * 0.0261661) + (vwap * (1 - 0.0261661)))), rank(volume), 11.4791)))
        * -1)"""
        left = self._rank(self._correlation(self.data["close"], self._sum(self._adv(30), 37), 15))
        right = self._rank(
            self._correlation(
                self._rank(self.data["high"] * 0.0261661 + self.data["vwap"] * (1 - 0.0261661)),
                self._rank(self.data["volume"]),
                11,
            )
        )
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_75(self) -> None:
        """Alpha#75: (rank(correlation(vwap, volume, 4.24304)) < rank(correlation(rank(low), rank(adv50),
        12.4413)))"""
        left = self._rank(self._correlation(self.data["vwap"], self.data["volume"], 4))
        right = self._rank(self._correlation(self._rank(self.data["low"]), self._rank(self._adv(50)), 12))
        alpha = (left < right).astype(float)
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_76(self) -> pd.Series:
        """Alpha#76: (max(rank(decay_linear(delta(vwap, 1.24383), 11.8259)),
        Ts_Rank(decay_linear(Ts_Rank(correlation(IndNeutralize(low, IndClass.sector), adv81,
        8.14941), 19.569), 17.1543), 19.383)) * -1)"""
        left = self._rank(self._decay_linear(self._delta(self.data["vwap"], 1), 12))
        right = self._ts_rank(
            self._decay_linear(
                self._ts_rank(
                    self._correlation(
                        self._ind_neutralize(self.data["low"]), self._adv(81), 8
                    ),
                    20,
                ),
                17,
            ),
            19,
        )

        return -pd.concat([left, right], axis=1).max(axis=1)

    def alpha_77(self) -> pd.Series:
        """Alpha#77: min(rank(decay_linear(((((high + low) / 2) + high) - (vwap + high)), 20.0451)),
        rank(decay_linear(correlation(((high + low) / 2), adv40, 3.1614), 5.64125)))"""
        left = self._rank(
            self._decay_linear(
                ((self.data["high"] + self.data["low"]) / 2 - self.data["vwap"]),
                20,
            )
        )
        right = self._rank(
            self._decay_linear(
                self._correlation(
                    (self.data["high"] + self.data["low"]) / 2, self._adv(40), 3
                ),
                6,
            )
        )

        return pd.concat([left, right], axis=1).min(axis=1)

    def alpha_78(self) -> pd.Series:
        """Alpha#78: (rank(correlation(sum(((low * 0.352233) + (vwap * (1 - 0.352233))), 19.7428),
        sum(adv40, 19.7428), 6.83313))^rank(correlation(rank(vwap), rank(volume), 5.77492)))"""
        left = self._rank(
            self._correlation(
                self._sum(
                    self.data["low"] * 0.352233 + self.data["vwap"] * (1 - 0.352233), 20
                ),
                self._sum(self._adv(40), 20),
                7,
            )
        )
        right = self._rank(
            self._correlation(
                self._rank(self.data["vwap"]), self._rank(self.data["volume"]), 6
            )
        )

        return left**right

    def alpha_79(self) -> None:
        """Alpha#79: (rank(delta(IndNeutralize(((close * 0.60733) + (open * (1 - 0.60733))),
        IndClass.sector), 1.23438)) < rank(correlation(Ts_Rank(vwap, 3.60973), Ts_Rank(adv150,
        9.18637), 14.6644)))"""
        left = self._rank(
            self._delta(self._ind_neutralize(self.data["close"] * 0.60733 + self.data["open"] * (1 - 0.60733)), 1)
        )
        right = self._rank(
            self._correlation(self._ts_rank(self.data["vwap"], 4), self._ts_rank(self._adv(150), 9), 15)
        )
        alpha = (left < right).astype(float)
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_80(self) -> pd.Series:
        """Alpha#80: ((rank(Sign(delta(IndNeutralize(((open * 0.868128) + (high * (1 - 0.868128))),
        IndClass.industry), 4.04545)))^Ts_Rank(correlation(high, adv10, 5.11456), 5.53756)) * -1)"""
        left = self._rank(
            self._delta(
                self._ind_neutralize(
                    self.data["open"] * 0.868128 + self.data["high"] * (1 - 0.868128)
                ),
                4,
            ).apply(np.sign)
        )
        right = self._ts_rank(self._correlation(self.data["high"], self._adv(10), 5), 6)

        return -(left**right)

    def alpha_81(self) -> None:
        """Alpha#81: ((rank(Log(product(rank((rank(correlation(vwap, sum(adv10, 49.6054),
        8.47743))^4)), 14.9655))) < rank(correlation(rank(vwap), rank(volume), 5.07914))) * -1)"""
        left = self._rank(
            np.log(
                self._product(
                    self._rank(
                        self._rank(self._correlation(self.data["vwap"], self._sum(self._adv(10), 50), 8)) ** 4
                    ),
                    15,
                ).clip(lower=self.eps)
            )
        )
        right = self._rank(self._correlation(self._rank(self.data["vwap"]), self._rank(self.data["volume"]), 5))
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_82(self) -> pd.Series:
        """Alpha#82: (min(rank(decay_linear(delta(open, 1.46063), 14.8717)),
        Ts_Rank(decay_linear(correlation(IndNeutralize(volume, IndClass.sector), ((open * 0.634196) +
        (open * (1 - 0.634196))), 17.4842), 6.92131), 13.4283)) * -1)"""
        left = self._rank(self._decay_linear(self._delta(self.data["open"], 1), 15))
        right = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ind_neutralize(self.data["volume"]), self.data["open"], 17
                ),
                7,
            ),
            13,
        )

        return -pd.concat([left, right], axis=1).min(axis=1)

    def alpha_83(self) -> pd.Series:
        """Alpha#83: ((rank(delay(((high - low) / (sum(close, 5) / 5)), 2)) * rank(rank(volume))) / (((high -
        low) / (sum(close, 5) / 5)) / (vwap - close)))"""
        part_0 = self._rank(
            self._delay(
                (self.data["high"] - self.data["low"])
                / (self._sum(self.data["close"], 5) / 5 + self.eps),
                2,
            )
        )
        part_1 = self._rank(self._rank(self.data["volume"]))
        part_2 = (
            (self.data["high"] - self.data["low"])
            / (self._sum(self.data["close"], 5) / 5 + self.eps)
            / (self.data["vwap"] - self.data["close"] + self.eps)
        )

        return part_0 * part_1 / (part_2 + self.eps)

    def alpha_84(self) -> pd.Series:
        """Alpha#84: SignedPower(Ts_Rank((vwap - ts_max(vwap, 15.3217)), 20.7127), delta(close,
        4.96796))"""
        left = self._ts_rank(
            self.data["vwap"] - self._ts_max(self.data["vwap"], 15), 21
        )

        return left ** self._delta(self.data["close"], 5)

    def alpha_85(self) -> pd.Series:
        """Alpha#85: (rank(correlation(((high * 0.876703) + (close * (1 - 0.876703))), adv30,
        9.61331))^rank(correlation(Ts_Rank(((high + low) / 2), 3.70596), Ts_Rank(volume, 10.1595),
        7.11408)))"""
        left = self._rank(
            self._correlation(
                self.data["high"] * 0.876703 + self.data["close"] * (1 - 0.876703),
                self._adv(30),
                10,
            )
        )
        right = self._rank(
            self._correlation(
                self._ts_rank((self.data["high"] + self.data["low"]) / 2, 4),
                self._ts_rank(self.data["volume"], 10),
                7,
            )
        )

        return left**right

    def alpha_86(self) -> None:
        """Alpha#86: ((Ts_Rank(correlation(close, sum(adv20, 14.7444), 6.00049), 20.4195) < rank(((open
        + close) - (vwap + open)))) * -1)"""
        left = self._ts_rank(self._correlation(self.data["close"], self._sum(self._adv(20), 15), 6), 20)
        right = self._rank((self.data["open"] + self.data["close"]) - (self.data["vwap"] + self.data["open"]))
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_87(self) -> pd.Series:
        """Alpha#87: (max(rank(decay_linear(delta(((close * 0.369701) + (vwap * (1 - 0.369701))),
        1.91233), 2.65461)), Ts_Rank(decay_linear(abs(correlation(IndNeutralize(adv81,
        IndClass.industry), close, 13.4132)), 4.89768), 14.4535)) * -1)"""
        left = self._rank(
            self._decay_linear(
                self._delta(
                    self.data["close"] * 0.369701 + self.data["vwap"] * (1 - 0.369701),
                    2,
                ),
                3,
            )
        )
        right = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ind_neutralize(self._adv(81)), self.data["close"], 13
                ).abs(),
                5,
            ),
            14,
        )

        return -pd.concat([left, right], axis=1).max(axis=1)

    def alpha_88(self) -> pd.Series:
        """Alpha#88: min(rank(decay_linear(((rank(open) + rank(low)) - (rank(high) + rank(close))),
        8.06882)), Ts_Rank(decay_linear(correlation(Ts_Rank(close, 8.44728), Ts_Rank(adv60,
        20.6966), 8.01266), 6.65053), 2.61957))"""
        left = self._rank(
            self._decay_linear(
                (self._rank(self.data["open"]) + self._rank(self.data["low"]))
                - (self._rank(self.data["high"]) + self._rank(self.data["close"])),
                8,
            )
        )
        right = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ts_rank(self.data["close"], 8),
                    self._ts_rank(self._adv(60), 21),
                    8,
                ),
                7,
            ),
            3,
        )

        return pd.concat([left, right], axis=1).min(axis=1)

    def alpha_89(self) -> pd.Series:
        """Alpha#89: (Ts_Rank(decay_linear(correlation(((low * 0.967285) + (low * (1 - 0.967285))), adv10,
        6.94279), 5.51607), 3.79744) - Ts_Rank(decay_linear(delta(IndNeutralize(vwap,
        IndClass.industry), 3.48158), 10.1466), 15.3012))"""
        left = self._ts_rank(
            self._decay_linear(
                self._correlation(self.data["low"], self._adv(10), 7), 6
            ),
            4,
        )
        right = self._ts_rank(
            self._decay_linear(
                self._delta(self._ind_neutralize(self.data["vwap"]), 3), 10
            ),
            15,
        )

        return left - right

    def alpha_90(self) -> pd.Series:
        """Alpha#90: ((rank((close - ts_max(close, 4.66719)))^Ts_Rank(correlation(IndNeutralize(adv40,
        IndClass.subindustry), low, 5.38375), 3.21856)) * -1)"""
        left = self._rank(self.data["close"] - self._ts_max(self.data["close"], 5))
        right = self._ts_rank(
            self._correlation(self._ind_neutralize(self._adv(40)), self.data["low"], 5),
            3,
        )

        return -(left**right)

    def alpha_91(self) -> pd.Series:
        """Alpha#91: ((Ts_Rank(decay_linear(decay_linear(correlation(IndNeutralize(close,
        IndClass.industry), volume, 9.74928), 16.398), 3.83219), 4.8667) -
        rank(decay_linear(correlation(vwap, adv30, 4.01303), 2.6809))) * -1)"""
        left = self._ts_rank(
            self._decay_linear(
                self._decay_linear(
                    self._correlation(
                        self._ind_neutralize(self.data["close"]),
                        self.data["volume"],
                        10,
                    ),
                    16,
                ),
                4,
            ),
            5,
        )
        right = self._rank(
            self._decay_linear(
                self._correlation(self.data["vwap"], self._adv(30), 4), 3
            )
        )

        return -(left - right)

    def alpha_92(self) -> pd.Series:
        """Alpha#92: min(Ts_Rank(decay_linear(((((high + low) / 2) + close) < (low + open)), 14.7221),
        18.8683), Ts_Rank(decay_linear(correlation(rank(low), rank(adv30), 7.58555), 6.94024),
        6.80584))"""
        left = self._ts_rank(
            self._decay_linear(
                (
                    ((self.data["high"] + self.data["low"]) / 2 + self.data["close"])
                    < (self.data["low"] + self.data["open"])
                ).astype(float),
                15,
            ),
            19,
        )
        right = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._rank(self.data["low"]), self._rank(self._adv(30)), 8
                ),
                7,
            ),
            7,
        )

        return pd.concat([left, right], axis=1).min(axis=1)

    def alpha_93(self) -> pd.Series:
        """Alpha#93: (Ts_Rank(decay_linear(correlation(IndNeutralize(vwap, IndClass.industry), adv81,
        17.4193), 19.848), 7.54455) / rank(decay_linear(delta(((close * 0.524434) + (vwap * (1 -
        0.524434))), 2.77377), 16.2664)))"""
        left = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._ind_neutralize(self.data["vwap"]), self._adv(81), 17
                ),
                20,
            ),
            8,
        )
        right = self._rank(
            self._decay_linear(
                self._delta(
                    self.data["close"] * 0.524434 + self.data["vwap"] * (1 - 0.524434),
                    3,
                ),
                16,
            )
        )

        return left / (right + self.eps)

    def alpha_94(self) -> pd.Series:
        """Alpha#94: ((rank((vwap - ts_min(vwap, 11.5783)))^Ts_Rank(correlation(Ts_Rank(vwap,
        19.6462), Ts_Rank(adv60, 4.02992), 18.0926), 2.70756)) * -1)"""
        left = self._rank(self.data["vwap"] - self._ts_min(self.data["vwap"], 12))
        right = self._ts_rank(
            self._correlation(
                self._ts_rank(self.data["vwap"], 20),
                self._ts_rank(self._adv(60), 4),
                18,
            ),
            3,
        )

        return -(left**right)

    def alpha_95(self) -> None:
        """Alpha#95: (rank((open - ts_min(open, 12.4105))) < Ts_Rank((rank(correlation(sum(((high + low)
        / 2), 19.1351), sum(adv40, 19.1351), 12.8742))^5), 11.7584))"""
        left = self._rank(self.data["open"] - self._ts_min(self.data["open"], 12))
        right = self._ts_rank(
            self._rank(
                self._correlation(
                    self._sum((self.data["high"] + self.data["low"]) / 2, 19),
                    self._sum(self._adv(40), 19),
                    13,
                )
            ) ** 5,
            12,
        )
        alpha = (left < right).astype(float)
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_96(self) -> pd.Series:
        """Alpha#96: (max(Ts_Rank(decay_linear(correlation(rank(vwap), rank(volume), 3.83878),
        4.16783), 8.38151), Ts_Rank(decay_linear(Ts_ArgMax(correlation(Ts_Rank(close, 7.45404),
        Ts_Rank(adv60, 4.13242), 3.65459), 12.6556), 14.0365), 13.4143)) * -1)"""
        left = self._ts_rank(
            self._decay_linear(
                self._correlation(
                    self._rank(self.data["vwap"]), self._rank(self.data["volume"]), 4
                ),
                4,
            ),
            8,
        )
        right = self._ts_rank(
            self._decay_linear(
                self._ts_argmax(
                    self._correlation(
                        self._ts_rank(self.data["close"], 7),
                        self._ts_rank(self._adv(60), 4),
                        4,
                    ),
                    13,
                ),
                14,
            ),
            13,
        )

        return -pd.concat([left, right], axis=1).max(axis=1)

    def alpha_97(self) -> pd.Series:
        """Alpha#97: ((rank(decay_linear(delta(IndNeutralize(((low * 0.721001) + (vwap * (1 - 0.721001))),
        IndClass.industry), 3.3705), 20.4523)) - Ts_Rank(decay_linear(Ts_Rank(correlation(Ts_Rank(low,
        7.87871), Ts_Rank(adv60, 17.255), 4.97547), 18.5925), 15.7152), 6.71659)) * -1)"""
        left = self._rank(
            self._decay_linear(
                self._delta(
                    self._ind_neutralize(
                        self.data["low"] * 0.721001 + self.data["vwap"] * (1 - 0.721001)
                    ),
                    3,
                ),
                20,
            )
        )
        right = self._ts_rank(
            self._decay_linear(
                self._ts_rank(
                    self._correlation(
                        self._ts_rank(self.data["low"], 8),
                        self._ts_rank(self._adv(60), 17),
                        5,
                    ),
                    19,
                ),
                16,
            ),
            7,
        )

        return -(left - right)

    def alpha_98(self) -> pd.Series:
        """Alpha#98: (rank(decay_linear(correlation(vwap, sum(adv5, 26.4719), 4.58418), 7.18088)) -
        rank(decay_linear(Ts_Rank(Ts_ArgMin(correlation(rank(open), rank(adv15), 20.8187), 8.62571),
        6.95668), 8.07206)))"""
        left = self._rank(
            self._decay_linear(
                self._correlation(self.data["vwap"], self._sum(self._adv(5), 26), 5), 7
            )
        )
        right = self._rank(
            self._decay_linear(
                self._ts_rank(
                    self._ts_argmin(
                        self._correlation(
                            self._rank(self.data["open"]), self._rank(self._adv(15)), 21
                        ),
                        9,
                    ),
                    7,
                ),
                8,
            )
        )

        return left - right

    def alpha_99(self) -> None:
        """Alpha#99: ((rank(correlation(sum(((high + low) / 2), 19.8975), sum(adv60, 19.8975), 8.8136)) <
        rank(correlation(low, volume, 6.28259))) * -1)"""
        left = self._rank(
            self._correlation(
                self._sum((self.data["high"] + self.data["low"]) / 2, 20),
                self._sum(self._adv(60), 20),
                9,
            )
        )
        right = self._rank(self._correlation(self.data["low"], self.data["volume"], 6))
        alpha = -((left < right).astype(float))
        alpha.loc[left.isna() | right.isna()] = np.nan
        return alpha

    def alpha_100(self) -> pd.Series:
        """Alpha#100: (0 - (1 * (((1.5 * scale(indneutralize(indneutralize(rank(((((close - low) - (high -
        close)) / (high - low)) * volume)), IndClass.subindustry), IndClass.subindustry))) -
        scale(indneutralize((correlation(close, rank(adv20), 5) - rank(ts_argmin(close, 30))),
        IndClass.subindustry))) * (volume / adv20))))"""
        part_0 = 1.5 * self._scale(
            self._ind_neutralize(
                self._ind_neutralize(
                    self._rank(

                            (
                                (self.data["close"] - self.data["low"])
                                - (self.data["high"] - self.data["close"])
                            )
                            / (self.data["high"] - self.data["low"] + self.eps)
                            * self.data["volume"]

                    )
                )
            )
        )

        left = self._correlation(self.data["close"], self._rank(self._adv(20)), 5)
        right = self._rank(self._ts_argmin(self.data["close"], 30))
        part_1 = self._scale(self._ind_neutralize(left - right))

        part_2 = self.data["volume"] / (self._adv(20) + self.eps)

        return -(part_0 - part_1) * part_2

    def alpha_101(self) -> pd.Series:
        """Alpha#101: ((close - open) / ((high - low) + .001))"""
        return (self.data["close"] - self.data["open"]) / (
            self.data["high"] - self.data["low"] + 0.001
        )


def _sqlite_db_path() -> Path:
    return Path(settings.data_dir) / "gaoshou.db"


def _load_daily_basic_panel(symbols: Sequence[str], start_date: date, end_date: date) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, trade_date, circ_mv, total_mv
        FROM stock_daily_basic
        WHERE symbol IN ({placeholders})
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY symbol, trade_date
    """
    with sqlite3.connect(_sqlite_db_path()) as conn:
        return pd.read_sql_query(sql, conn, params=[*symbols, start_date.isoformat(), end_date.isoformat()])


def _load_industry_map(symbols: Sequence[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol", "industry"])
    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, COALESCE(industry3, industry2, industry, 'UNKNOWN') AS industry
        FROM stocks
        WHERE symbol IN ({placeholders})
    """
    with sqlite3.connect(_sqlite_db_path()) as conn:
        return pd.read_sql_query(sql, conn, params=list(symbols))


def build_alpha101_input_frame(
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    *,
    lookback_days: int = 370,
) -> pd.DataFrame:
    store = get_market_data_store()
    daily = store.load_daily(
        list(symbols),
        start_date - pd.Timedelta(days=lookback_days),
        end_date,
        columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
    )
    if daily.empty:
        return pd.DataFrame()
    frame = daily.reset_index() if "trade_date" not in daily.columns else daily.reset_index(drop=True)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["symbol", "trade_date", "open", "high", "low", "close"])
    volume = frame["volume"].where(frame["volume"].fillna(0.0) > 0)
    raw_vwap = frame["amount"] / volume
    raw_ratio = (raw_vwap / frame["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    median_raw_ratio = raw_ratio.dropna().median()
    # QMT/JQ daily volume is commonly stored in lots while amount is in CNY.
    # Infer the multiplier from amount / volume / close to avoid hardcoding a
    # data-source-specific unit when alternate daily feeds are imported later.
    volume_to_shares = 100.0 if pd.notna(median_raw_ratio) and float(median_raw_ratio) > 20.0 else 1.0
    frame["vwap"] = np.where(
        frame["volume"].fillna(0.0) > 0,
        frame["amount"] / (frame["volume"].replace(0, np.nan) * volume_to_shares),
        frame["close"],
    )
    frame["vwap"] = pd.to_numeric(frame["vwap"], errors="coerce").fillna(frame["close"])
    frame = frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    frame["return"] = frame.groupby("symbol")["close"].pct_change()

    daily_basic = _load_daily_basic_panel(symbols, start_date - pd.Timedelta(days=lookback_days), end_date)
    if not daily_basic.empty:
        daily_basic["trade_date"] = pd.to_datetime(daily_basic["trade_date"]).dt.date
        daily_basic["market_value"] = pd.to_numeric(daily_basic["circ_mv"], errors="coerce")
        daily_basic["market_value"] = daily_basic["market_value"].fillna(pd.to_numeric(daily_basic["total_mv"], errors="coerce"))
        frame = frame.merge(daily_basic[["symbol", "trade_date", "market_value"]], on=["symbol", "trade_date"], how="left")
    if "market_value" not in frame.columns:
        frame["market_value"] = np.nan
    frame["market_value"] = frame.groupby("symbol")["market_value"].ffill().bfill()

    industries = _load_industry_map(symbols)
    frame = frame.merge(industries, on="symbol", how="left")
    frame["industry"] = frame["industry"].fillna("UNKNOWN").astype(str)
    frame = frame.rename(columns={"trade_date": "date"})
    return frame.set_index(["symbol", "date"]).sort_index()


def compute_alpha101_factor_series(data: pd.DataFrame, factor_name: str) -> pd.Series:
    suffix = factor_name.split("_")[-1]
    method_name = f"alpha_{int(suffix)}"
    calculator = Alphas(data)
    method = getattr(calculator, method_name, None)
    if method is None:
        raise ValueError(f"Unsupported Alpha101 factor: {factor_name}")
    result = method()
    if result is None:
        raise ValueError(f"Alpha101 factor returned no result: {factor_name}")
    return pd.to_numeric(result, errors="coerce")


def precompute_alpha101_factors(
    *,
    factor_names: Sequence[str],
    start_date: date,
    end_date: date,
    symbols: Sequence[str],
    store: FactorValueStore | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    def report(progress: float, stage: str, **meta: Any) -> None:
        if progress_callback is not None:
            progress_callback(max(0.0, min(1.0, progress)), stage, meta)

    report(0.02, "parse")
    panel = build_alpha101_input_frame(symbols, start_date, end_date)
    if panel.empty:
        raise RuntimeError("No daily data available for Alpha101 precompute")
    created_at = datetime.now()
    empty_hash = factor_params_hash({})
    total = max(len(factor_names), 1)
    errors: dict[str, str] = {}
    factor_store = store or get_factor_value_store()
    writer = factor_store.batch_writer()
    from app.services.alpha101_wide_calculator import WideAlphas, is_alpha101_wide_supported

    # Wide matrices are faster for small universes, but they multiply the panel
    # into several dense DataFrames and can blow RAM on large pools. Fall back to
    # the long Series implementation when the input panel is already large.
    panel_rows = len(panel)
    policy = precompute_memory_policy()
    use_wide = any(is_alpha101_wide_supported(str(name)) for name in factor_names) and panel_rows <= policy.wide_panel_max_rows
    wide_calculator = WideAlphas(panel) if use_wide else None
    if not use_wide:
        logger.info("Alpha101 precompute using long-form calculator for {} rows and {} factors", panel_rows, len(factor_names))
    for index, factor_name in enumerate(factor_names, start=1):
        report(0.10 + 0.78 * (index / total), "alpha101", current=index, total=total, factor_name=factor_name)
        try:
            if wide_calculator is not None and is_alpha101_wide_supported(str(factor_name)):
                series = wide_calculator.compute_series(str(factor_name))
            else:
                series = compute_alpha101_factor_series(panel, factor_name)
        except Exception as exc:
            logger.exception("Alpha101 precompute failed for {}", factor_name)
            errors[str(factor_name)] = str(exc)
            continue
        if series.empty:
            continue
        factor_frame = series.rename("value").reset_index()
        factor_frame["date"] = pd.to_datetime(factor_frame["date"]).dt.date
        factor_frame = factor_frame[(factor_frame["date"] >= start_date) & (factor_frame["date"] <= end_date)]
        factor_frame["value"] = pd.to_numeric(factor_frame["value"], errors="coerce")
        factor_frame = factor_frame.dropna(subset=["value"])
        if factor_frame.empty:
            continue
        factor_frame = factor_frame.rename(columns={"date": "trade_date"})
        factor_frame["symbol"] = factor_frame["symbol"].astype(str)
        factor_frame["as_of_time"] = ""
        factor_frame["factor_name"] = factor_name
        factor_frame["params_hash"] = empty_hash
        factor_frame["source"] = "precompute.alpha101"
        factor_frame["created_at"] = created_at
        writer.write_frame(factor_frame[[
            "symbol",
            "trade_date",
            "as_of_time",
            "factor_name",
            "params_hash",
            "value",
            "source",
            "created_at",
        ]])
        report(0.10 + 0.78 * (index / total), "alpha101", current=index, total=total, factor_name=factor_name, rows_written=writer.written)
        del series, factor_frame
        release_precompute_memory()
    report(0.92, "write", rows_written=writer.written)
    report(1.0, "done", rows_written=writer.written)
    return {
        "symbols": len(symbols),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows": {str(name): writer.counts.get(str(name), 0) for name in factor_names},
        "rows_written": writer.written,
        "failed_factor_names": list(errors),
        "errors": errors,
    }
