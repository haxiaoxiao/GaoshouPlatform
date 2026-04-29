"""事件驱动回测引擎 — 逐日/逐分钟信号触发"""
from typing import Any, Callable

import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig, BacktestResult


class EventDrivenBacktestEngine:
    """事件驱动回测引擎"""

    def run(
        self,
        ohlcv_data: dict[str, pd.DataFrame],
        signal_fn: Callable[[pd.DataFrame], pd.Series],
        config: BacktestConfig,
    ) -> BacktestResult:
        """
        Args:
            ohlcv_data: {symbol -> DataFrame} 日线或分钟线
            signal_fn: 信号函数，接受单只股票的 DataFrame，返回 1(买)/-1(卖)/0 信号序列
            config: 回测配置
        """
        daily_nav = []
        trades = []
        cash = config.initial_capital
        positions: dict[str, dict] = {}

        initial_capital = config.initial_capital
        commission = config.commission_rate
        slippage = config.slippage

        all_dates = set()
        for df in ohlcv_data.values():
            all_dates.update(df.index)
        all_dates = sorted(all_dates)

        for i, dt in enumerate(all_dates):
            total_value = cash

            # 更新持仓市值
            for sym, pos in list(positions.items()):
                df = ohlcv_data.get(sym)
                if df is None or dt not in df.index:
                    continue
                px = float(df.loc[dt, "close"])
                mv = pos["shares"] * px
                total_value += mv

                # 检查止损止盈
                if config.stop_loss and mv / pos["cost"] - 1 <= config.stop_loss:
                    cash += mv * (1 - commission)
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "sell", "reason": "stop_loss",
                        "price": px, "pnl": mv - pos["cost"],
                    })
                    del positions[sym]
                elif config.stop_profit and mv / pos["cost"] - 1 >= config.stop_profit:
                    cash += mv * (1 - commission)
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "sell", "reason": "stop_profit",
                        "price": px, "pnl": mv - pos["cost"],
                    })
                    del positions[sym]

            # 信号检查
            for sym, df in ohlcv_data.items():
                if dt not in df.index:
                    continue
                if config.max_positions and len(positions) >= config.max_positions:
                    break
                if sym in positions:
                    continue

                hist = df.loc[:dt]
                try:
                    signal = signal_fn(hist)
                    sig_val = signal.iloc[-1] if isinstance(signal, pd.Series) else signal
                except Exception:
                    continue

                px = float(df.loc[dt, "close"])
                if sig_val > 0 and cash > px * 100:
                    shares = int(cash * 0.2 / px / 100) * 100
                    if shares == 0:
                        continue
                    cost = shares * px * (1 + commission + slippage)
                    if cost > cash:
                        continue
                    cash -= cost
                    positions[sym] = {"shares": shares, "cost": cost}
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "buy",
                        "price": px, "shares": shares, "cost": cost,
                    })
                elif sig_val < 0 and sym in positions:
                    pos = positions.pop(sym)
                    mv = pos["shares"] * px * (1 - commission)
                    cash += mv
                    trades.append({
                        "date": str(dt.date() if hasattr(dt, 'date') else dt),
                        "symbol": sym, "action": "sell",
                        "price": px, "pnl": mv - pos["cost"],
                    })

            if i > 0:
                daily_nav.append({
                    "date": str(dt.date() if hasattr(dt, 'date') else dt),
                    "nav": total_value / initial_capital,
                })

        if not daily_nav:
            return BacktestResult(
                initial_capital=initial_capital, final_capital=initial_capital,
                trades=trades,
            )

        final_nav = daily_nav[-1]["nav"]
        total_return = final_nav - 1
        n_days = len(daily_nav)
        annual_return = (1 + total_return) ** (252 / max(n_days, 1)) - 1

        daily_rets = []
        for i in range(1, len(daily_nav)):
            r = daily_nav[i]["nav"] / daily_nav[i - 1]["nav"] - 1
            daily_rets.append({"date": daily_nav[i]["date"], "return": float(r)})

        rets = pd.Series([r["return"] for r in daily_rets])
        annual_vol = float(rets.std() * np.sqrt(252)) if len(rets) > 0 else 0
        sharpe = (annual_return - 0.02) / annual_vol if annual_vol > 0 else 0

        nav_vals = pd.Series([d["nav"] for d in daily_nav])
        dd = (nav_vals - nav_vals.cummax()) / nav_vals.cummax()
        max_dd = float(dd.min())

        wins = sum(1 for t in trades if t["action"] == "sell" and t.get("pnl", 0) > 0)
        sell_trades = [t for t in trades if t["action"] == "sell"]
        total_trades = len(sell_trades)
        win_rate = wins / total_trades if total_trades > 0 else 0

        return BacktestResult(
            total_return=total_return,
            annual_return=annual_return,
            annual_volatility=annual_vol,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            total_trades=total_trades,
            win_trades=wins,
            loss_trades=total_trades - wins,
            win_rate=win_rate,
            nav_series=daily_nav,
            daily_returns=daily_rets,
            trades=trades,
            start_date=daily_nav[0]["date"] if daily_nav else None,
            end_date=daily_nav[-1]["date"] if daily_nav else None,
            initial_capital=initial_capital,
            final_capital=initial_capital * final_nav,
            n_trading_days=n_days,
        )


# 全局单例
_event_driven_engine: EventDrivenBacktestEngine | None = None


def get_event_driven_engine() -> EventDrivenBacktestEngine:
    global _event_driven_engine
    if _event_driven_engine is None:
        _event_driven_engine = EventDrivenBacktestEngine()
    return _event_driven_engine
