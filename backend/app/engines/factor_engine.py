# backend/app/engines/factor_engine.py
"""因子计算引擎"""
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from clickhouse_driver import Client

from app.db.clickhouse import get_ch_client


@dataclass
class FactorConfig:
    """因子计算配置"""

    # 标准化窗口
    normalize_window: int = 5
    # 因子计算窗口
    factor_window: int = 20
    # IC计算的前瞻期（交易日）
    forward_period: int = 20


@dataclass
class FactorResult:
    """因子计算结果"""

    symbol: str
    trade_date: date
    factor_value: float
    details: dict[str, Any] | None = None


class FactorEngine:
    """因子计算引擎"""

    def __init__(self, ch_client: Client | None = None):
        self._ch_client = ch_client

    @property
    def ch_client(self) -> Client:
        """获取 ClickHouse 客户端"""
        if self._ch_client is None:
            self._ch_client = get_ch_client()
        return self._ch_client

    def get_klines(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """
        从 ClickHouse 获取 K 线数据

        Args:
            symbols: 股票代码列表，为空则获取所有
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with columns: symbol, trade_date, open, high, low, close, volume, amount
        """
        query = """
            SELECT symbol, trade_date, open, high, low, close, volume, amount
            FROM klines_daily
            WHERE 1=1
        """
        params = {}

        if symbols:
            query += " AND symbol IN %(symbols)s"
            params["symbols"] = symbols

        if start_date:
            query += " AND trade_date >= %(start_date)s"
            params["start_date"] = start_date

        if end_date:
            query += " AND trade_date <= %(end_date)s"
            params["end_date"] = end_date

        query += " ORDER BY symbol, trade_date"

        result = self.ch_client.execute(query, params)

        if not result:
            return pd.DataFrame()

        df = pd.DataFrame(
            result,
            columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
        )
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # 转换 Decimal 为 float
        for col in ["open", "high", "low", "close", "amount"]:
            df[col] = df[col].astype(float)
        df["volume"] = df["volume"].astype(int)
        return df

    def calc_upper_shadow_candle(self, df: pd.DataFrame) -> pd.Series:
        """
        计算蜡烛上影线

        公式: High - max(Open, Close)

        Args:
            df: K线数据，需包含 high, open, close 列

        Returns:
            上影线序列
        """
        return df["high"] - df[["open", "close"]].max(axis=1)

    def calc_lower_shadow_candle(self, df: pd.DataFrame) -> pd.Series:
        """
        计算蜡烛下影线

        公式: min(Open, Close) - Low

        Args:
            df: K线数据，需包含 low, open, close 列

        Returns:
            下影线序列
        """
        return df[["open", "close"]].min(axis=1) - df["low"]

    def calc_upper_shadow_williams(self, df: pd.DataFrame) -> pd.Series:
        """
        计算威廉上影线

        公式: High - Close

        Args:
            df: K线数据，需包含 high, close 列

        Returns:
            威廉上影线序列
        """
        return df["high"] - df["close"]

    def calc_lower_shadow_williams(self, df: pd.DataFrame) -> pd.Series:
        """
        计算威廉下影线

        公式: Close - Low

        Args:
            df: K线数据，需包含 close, low 列

        Returns:
            威廉下影线序列
        """
        return df["close"] - df["low"]

    def normalize_shadow(
        self,
        shadow: pd.Series,
        window: int = 5,
    ) -> pd.Series:
        """
        标准化影线

        公式: 当日影线 / 过去M日影线均值

        Args:
            shadow: 影线序列
            window: 滚动窗口（默认5日）

        Returns:
            标准化影线序列
        """
        # 使用过去 window 天的均值（不包括当天）
        rolling_mean = shadow.shift(1).rolling(window=window, min_periods=1).mean()
        return shadow / rolling_mean

    def calc_factor_mean(
        self,
        norm_shadow: pd.Series,
        window: int = 20,
    ) -> pd.Series:
        """
        计算均值因子

        公式: 过去N日标准化影线的均值

        Args:
            norm_shadow: 标准化影线序列
            window: 滚动窗口（默认20日）

        Returns:
            均值因子序列
        """
        return norm_shadow.rolling(window=window, min_periods=1).mean()

    def calc_factor_std(
        self,
        norm_shadow: pd.Series,
        window: int = 20,
    ) -> pd.Series:
        """
        计算标准差因子

        公式: 过去N日标准化影线的标准差

        Args:
            norm_shadow: 标准化影线序列
            window: 滚动窗口（默认20日）

        Returns:
            标准差因子序列
        """
        return norm_shadow.rolling(window=window, min_periods=1).std()

    def desize_factor(
        self,
        factor: pd.Series,
        log_mv: pd.Series,
    ) -> pd.Series:
        """
        市值中性化

        对因子值关于对数市值做线性回归，取残差

        Args:
            factor: 因子值序列
            log_mv: 对数市值序列

        Returns:
            市值中性化后的因子序列
        """
        # 移除 NaN
        valid_mask = ~(factor.isna() | log_mv.isna())
        if valid_mask.sum() < 2:
            return factor

        # 线性回归取残差
        x = log_mv[valid_mask].values.reshape(-1, 1)
        y = factor[valid_mask].values

        # 最小二乘拟合
        coef = np.linalg.lstsq(np.hstack([x, np.ones((len(x), 1))]), y, rcond=None)[0]
        predicted = x.flatten() * coef[0] + coef[1]

        # 计算残差
        residual = pd.Series(np.nan, index=factor.index)
        residual[valid_mask] = y - predicted

        return residual

    def zscore_cross_section(self, factor_df: pd.DataFrame, factor_col: str) -> pd.Series:
        """
        横截面标准化

        对每个交易日的因子值进行 z-score 标准化

        Args:
            factor_df: 包含 trade_date 和因子列的 DataFrame
            factor_col: 因子列名

        Returns:
            标准化后的因子序列（MultiIndex: symbol, trade_date）
        """
        return factor_df.groupby("trade_date")[factor_col].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
        )

    def calc_ubl_factor(
        self,
        df: pd.DataFrame,
        config: FactorConfig | None = None,
    ) -> pd.DataFrame:
        """
        计算 UBL 综合因子

        UBL = zscore(蜡烛上_std_desize) + zscore(威廉下_mean_desize)

        Args:
            df: K线数据，按 symbol 分组
            config: 因子计算配置

        Returns:
            DataFrame with columns: symbol, trade_date, upper_shadow_candle, lower_shadow_williams,
                                    upper_std, lower_mean, upper_std_desize, lower_mean_desize, ubl
        """
        if config is None:
            config = FactorConfig()

        result_dfs = []

        for symbol, group in df.groupby("symbol"):
            group = group.sort_values("trade_date").copy()

            # 1. 计算蜡烛上影线
            group["upper_shadow_candle"] = self.calc_upper_shadow_candle(group)

            # 2. 计算威廉下影线
            group["lower_shadow_williams"] = self.calc_lower_shadow_williams(group)

            # 3. 标准化
            norm_upper = self.normalize_shadow(
                group["upper_shadow_candle"], config.normalize_window
            )
            norm_lower = self.normalize_shadow(
                group["lower_shadow_williams"], config.normalize_window
            )

            # 4. 计算因子
            group["upper_std"] = self.calc_factor_std(norm_upper, config.factor_window)
            group["lower_mean"] = self.calc_factor_mean(norm_lower, config.factor_window)

            result_dfs.append(group)

        result = pd.concat(result_dfs, ignore_index=True)

        # 5. 计算对数市值（使用成交额作为市值代理）
        # 实际应用中应该使用真实市值数据
        result["log_mv"] = np.log(result["amount"].replace(0, np.nan) + 1)

        # 6. 按日期进行市值中性化
        result["upper_std_desize"] = result.groupby("trade_date").apply(
            lambda g: self.desize_factor(g["upper_std"], g["log_mv"])
        ).reset_index(level=0, drop=True)

        result["lower_mean_desize"] = result.groupby("trade_date").apply(
            lambda g: self.desize_factor(g["lower_mean"], g["log_mv"])
        ).reset_index(level=0, drop=True)

        # 7. 横截面标准化
        result["upper_std_zscore"] = self.zscore_cross_section(result, "upper_std_desize")
        result["lower_mean_zscore"] = self.zscore_cross_section(result, "lower_mean_desize")

        # 8. 合成 UBL 因子
        result["ubl"] = result["upper_std_zscore"] + result["lower_mean_zscore"]

        return result

    def calc_forward_return(
        self,
        df: pd.DataFrame,
        forward_period: int = 20,
    ) -> pd.DataFrame:
        """
        计算未来收益率

        Args:
            df: K线数据，需包含 close 列
            forward_period: 前瞻期

        Returns:
            DataFrame with forward_return column
        """
        result_dfs = []

        for symbol, group in df.groupby("symbol"):
            group = group.sort_values("trade_date").copy()
            # 计算未来收益率
            group["forward_return"] = group["close"].shift(-forward_period) / group["close"] - 1
            result_dfs.append(group)

        return pd.concat(result_dfs, ignore_index=True)

    def calc_ic(
        self,
        factor_df: pd.DataFrame,
        factor_col: str = "ubl",
        return_col: str = "forward_return",
    ) -> pd.DataFrame:
        """
        计算因子的 IC 值

        IC = corr(因子值, 未来收益率)

        Args:
            factor_df: 包含因子值和收益率的 DataFrame
            factor_col: 因子列名
            return_col: 收益率列名

        Returns:
            DataFrame with columns: trade_date, ic
        """
        ic_list = []

        for trade_date, group in factor_df.groupby("trade_date"):
            # 移除 NaN
            valid = group.dropna(subset=[factor_col, return_col])
            if len(valid) < 10:  # 样本太少跳过
                continue

            # 计算 Spearman 相关系数（秩相关）
            ic = valid[factor_col].corr(valid[return_col], method="spearman")
            ic_list.append({"trade_date": trade_date, "ic": ic})

        return pd.DataFrame(ic_list)

    def calc_group_returns(
        self,
        factor_df: pd.DataFrame,
        factor_col: str = "ubl",
        return_col: str = "forward_return",
        n_groups: int = 5,
    ) -> pd.DataFrame:
        """
        计算分组收益率

        将股票按因子值分成 n_groups 组，计算每组的平均收益率

        Args:
            factor_df: 包含因子值和收益率的 DataFrame
            factor_col: 因子列名
            return_col: 收益率列名
            n_groups: 分组数量

        Returns:
            DataFrame with columns: trade_date, group_1, group_2, ..., group_n, long_short
        """
        result_list = []

        for trade_date, group in factor_df.groupby("trade_date"):
            valid = group.dropna(subset=[factor_col, return_col])
            if len(valid) < n_groups * 2:  # 样本太少
                continue

            # 按因子值分组（因子值小 = 组别小 = 做多）
            valid["factor_group"] = pd.qcut(valid[factor_col], n_groups, labels=False, duplicates="drop")

            # 计算每组收益率
            group_returns = valid.groupby("factor_group")[return_col].mean()

            row = {"trade_date": trade_date}
            for g in range(n_groups):
                row[f"group_{g + 1}"] = group_returns.get(g, np.nan)

            # 多空收益：做多组1（因子值小），做空组5（因子值大）
            if 0 in group_returns.index and (n_groups - 1) in group_returns.index:
                row["long_short"] = group_returns[0] - group_returns[n_groups - 1]
            else:
                row["long_short"] = np.nan

            result_list.append(row)

        return pd.DataFrame(result_list)

    def run_factor_analysis(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        config: FactorConfig | None = None,
    ) -> dict[str, Any]:
        """
        运行完整的因子分析

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            config: 因子计算配置

        Returns:
            分析结果字典
        """
        if config is None:
            config = FactorConfig()

        # 1. 获取 K 线数据
        df = self.get_klines(symbols, start_date, end_date)
        if df.empty:
            return {"error": "No data available"}

        # 2. 计算 UBL 因子
        factor_df = self.calc_ubl_factor(df, config)

        # 3. 计算未来收益率
        factor_df = self.calc_forward_return(factor_df, config.forward_period)

        # 4. 计算 IC
        ic_df = self.calc_ic(factor_df)

        # 5. 计算分组收益
        group_df = self.calc_group_returns(factor_df)

        # 6. 汇总统计
        if ic_df.empty:
            return {
                "error": "Insufficient data for IC calculation",
                "total_stocks": factor_df["symbol"].nunique(),
                "total_dates": 0,
            }

        ic_mean = ic_df["ic"].mean()
        ic_std = ic_df["ic"].std()
        icir = ic_mean / ic_std if ic_std > 0 else 0

        # 年化 ICIR（假设月度数据）
        annual_icir = icir * np.sqrt(12)

        # 多空收益统计
        long_short_returns = group_df["long_short"].dropna()
        annual_return = long_short_returns.mean() * 12  # 年化
        annual_vol = long_short_returns.std() * np.sqrt(12)  # 年化波动
        information_ratio = annual_return / annual_vol if annual_vol > 0 else 0

        # 月度胜率
        win_rate = (long_short_returns > 0).sum() / len(long_short_returns) if len(long_short_returns) > 0 else 0

        # 最大回撤
        cum_returns = (1 + long_short_returns).cumprod()
        max_drawdown = (cum_returns / cum_returns.cummax() - 1).min()

        return {
            "ic_mean": ic_mean,
            "ic_std": ic_std,
            "icir": icir,
            "annual_icir": annual_icir,
            "annual_return": annual_return,
            "annual_vol": annual_vol,
            "information_ratio": information_ratio,
            "win_rate": win_rate,
            "max_drawdown": max_drawdown,
            "ic_series": ic_df.to_dict("records"),
            "group_returns": group_df.to_dict("records"),
            "total_stocks": factor_df["symbol"].nunique(),
            "total_dates": len(ic_df),
        }


# 全局单例
_factor_engine: FactorEngine | None = None


def get_factor_engine() -> FactorEngine:
    """获取因子引擎单例"""
    global _factor_engine
    if _factor_engine is None:
        _factor_engine = FactorEngine()
    return _factor_engine
