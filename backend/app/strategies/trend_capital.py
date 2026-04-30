# backend/app/strategies/trend_capital.py
"""趋势资金日内交易行为事件驱动策略 — 研报十一

三路信号:
  Signal A — 小单资金净流出信号 (从分钟数据近似)
  Signal B — 趋势资金相对均价信号 (VWAP ratio)
  Signal C — 趋势资金净支撑量信号 (net support volume)

信号融合: 3个交易日窗口, 三个指标同时满足≥2日触发 → 综合入场信号
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np

from app.db.clickhouse import get_ch_client


# ═══════════════════════════════════════════════════════════════
# 配置参数
# ═══════════════════════════════════════════════════════════════

# 趋势资金识别
TREND_LOOKBACK_DAYS = 5      # k: 回看天数
TREND_VOL_PERCENTILE = 0.90  # m: 成交量分位数阈值

# 信号融合 — 研报原文: "三个指标同时满足3日中≥2日触发"
FUSION_WINDOW_DAYS = 3        # 多交易日融合窗口
FUSION_MIN_DAYS_PER_SIGNAL = 2  # 每个信号至少触发天数
FUSION_MIN_SIGNALS = 2        # 至少多少信号达标 (信号B、C 同时满足)

# 资金通道策略
NUM_CHANNELS = 5              # 最多持有股票数
CHANNEL_HOLD_DAYS = 20        # 每只股票持股天数


@dataclass
class DailySignal:
    """单只股票单日信号"""
    symbol: str
    trade_date: date

    signal_a_value: float | None = None  # 小单净流出量 (正=净流出)
    signal_b_value: float | None = None  # 趋势资金相对均价 (VWAP ratio - 1)
    signal_c_value: float | None = None  # 趋势资金净支撑量

    signal_a_triggered: bool = False
    signal_b_triggered: bool = False
    signal_c_triggered: bool = False

    trend_minute_count: int = 0
    total_minute_count: int = 0
    trend_vol_threshold: float = 0.0

    composite_triggered: bool = False


@dataclass
class ChannelPosition:
    """单个通道持仓"""
    channel_id: int
    symbol: str
    entry_date: date
    entry_price: float
    exit_date: date | None = None
    exit_price: float | None = None
    pnl_pct: float | None = None


class TrendCapitalStrategy:
    """趋势资金事件驱动策略"""

    def __init__(self, sample_space: str = "CSI800"):
        self.sample_space = sample_space
        self.ch_client = get_ch_client()

    # ═══════════════════════════════════════════════════════════
    # 趋势资金识别 & 日度信号
    # ═══════════════════════════════════════════════════════════

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        """获取交易日列表"""
        rows = self.ch_client.execute(
            "SELECT DISTINCT toDate(datetime) AS d "
            "FROM klines_minute "
            "WHERE toDate(datetime) >= %(start)s "
            "  AND toDate(datetime) <= %(end)s "
            "ORDER BY d",
            {"start": start, "end": end},
        )
        return [r[0] for r in rows]

    def compute_daily_signals(
        self, trade_date: date, symbols: list[str] | None = None
    ) -> dict[str, DailySignal]:
        """计算指定交易日所有股票的日度信号"""
        clickhouse = self.ch_client
        results: dict[str, DailySignal] = {}

        # Step 1: 获取过去k个交易日
        past_dates = clickhouse.execute(
            "SELECT DISTINCT toDate(datetime) AS d "
            "FROM klines_minute "
            "WHERE toDate(datetime) < %(td)s "
            "ORDER BY d DESC "
            "LIMIT %(k)s",
            {"td": trade_date, "k": TREND_LOOKBACK_DAYS},
        )
        if len(past_dates) < TREND_LOOKBACK_DAYS:
            return results

        past_date_list = [r[0] for r in past_dates]
        past_start = past_date_list[-1]
        past_end = past_date_list[0] + timedelta(days=1)

        # Step 2: 批量获取每只股票的成交量90%分位数阈值
        sym_filter = ""
        params: dict[str, Any] = {
            "past_start": past_start,
            "past_end": past_end,
            "td": trade_date,
        }
        if symbols:
            sym_filter = "AND symbol IN %(syms)s"
            params["syms"] = tuple(symbols)

        thresholds = clickhouse.execute(
            f"SELECT symbol, quantile(%(pct)s)(volume) AS th "
            f"FROM klines_minute "
            f"WHERE toDate(datetime) >= %(past_start)s "
            f"  AND toDate(datetime) < %(past_end)s "
            f"  {sym_filter} "
            f"GROUP BY symbol",
            {**params, "pct": TREND_VOL_PERCENTILE},
        )
        threshold_map: dict[str, float] = {r[0]: float(r[1]) for r in thresholds}
        if not threshold_map:
            return results

        # Step 3: 获取当日分钟数据
        sym_tuple = tuple(threshold_map.keys())
        minute_data = clickhouse.execute(
            "SELECT symbol, datetime, open, high, low, close, volume, amount "
            "FROM klines_minute "
            "WHERE toDate(datetime) = %(td)s "
            "  AND symbol IN %(syms)s "
            "ORDER BY symbol, datetime",
            {"td": trade_date, "syms": sym_tuple},
        )

        if not minute_data:
            return results

        # Step 4: 按股票分组计算
        current_symbol = None
        symbol_minutes: list[dict] = []

        def process_symbol(sym: str, minutes: list[dict]):
            thresh = threshold_map.get(sym, 0)
            if thresh <= 0 or len(minutes) < 10:
                return

            vols = np.array([m["volume"] for m in minutes], dtype=np.float64)
            closes = np.array([m["close"] for m in minutes], dtype=np.float64)
            opens_ = np.array([m["open"] for m in minutes], dtype=np.float64)
            amounts = np.array([m["amount"] for m in minutes], dtype=np.float64)

            is_trend = vols > thresh
            trend_count = is_trend.sum()
            total_count = len(minutes)

            signal = DailySignal(
                symbol=sym, trade_date=trade_date,
                trend_minute_count=int(trend_count),
                total_minute_count=total_count,
                trend_vol_threshold=float(thresh),
            )

            if trend_count < 3:
                results[sym] = signal
                return

            trend_vol = vols[is_trend]
            trend_close = closes[is_trend]
            trend_amount = amounts[is_trend]

            # --- Signal B: 趋势资金相对均价 ---
            trend_vwap = float(np.sum(trend_amount) / np.sum(trend_vol)) if trend_vol.sum() > 0 else 0.0
            all_vwap = float(np.sum(amounts) / np.sum(vols)) if vols.sum() > 0 else 0.0
            if all_vwap > 0:
                signal.signal_b_value = float(trend_vwap / all_vwap - 1)
                signal.signal_b_triggered = signal.signal_b_value < 0

            # --- Signal C: 趋势资金净支撑量 ---
            all_avg_close = float(np.mean(closes))
            support_vol = float(np.sum(trend_vol[trend_close < all_avg_close]))
            resist_vol = float(np.sum(trend_vol[trend_close > all_avg_close]))
            net_support = support_vol - resist_vol
            signal.signal_c_value = net_support
            signal.signal_c_triggered = net_support > 0

            results[sym] = signal

        for row in minute_data:
            sym = row[0]
            if sym != current_symbol:
                if current_symbol is not None and symbol_minutes:
                    process_symbol(current_symbol, symbol_minutes)
                current_symbol = sym
                symbol_minutes = []
            symbol_minutes.append({
                "volume": float(row[6]), "close": float(row[5]),
                "open": float(row[2]), "amount": float(row[7]),
            })

        if current_symbol is not None and symbol_minutes:
            process_symbol(current_symbol, symbol_minutes)

        return results

    # ═══════════════════════════════════════════════════════════
    # 信号融合
    # ═══════════════════════════════════════════════════════════

    def compute_composite_signals(
        self, start_date: date, end_date: date,
        symbols: list[str] | None = None,
    ) -> dict[date, dict[str, DailySignal]]:
        """计算区间内所有日度信号 + 综合信号融合"""
        trading_dates = self.get_trading_dates(start_date, end_date)
        all_signals: dict[date, dict[str, DailySignal]] = {}

        for td in trading_dates:
            all_signals[td] = self.compute_daily_signals(td, symbols)

        for i, td in enumerate(trading_dates):
            if i < FUSION_WINDOW_DAYS - 1:
                continue
            window_dates = trading_dates[i - FUSION_WINDOW_DAYS + 1 : i + 1]
            all_syms: set[str] = set()
            for wd in window_dates:
                all_syms.update(all_signals.get(wd, {}).keys())

            for sym in all_syms:
                sig = all_signals[td].get(sym)
                if sig is None:
                    continue
                # 每个信号独立检查: 3日中是否≥2日触发
                b_days = sum(1 for wd in window_dates
                           if all_signals.get(wd, {}).get(sym) and all_signals[wd][sym].signal_b_triggered)
                c_days = sum(1 for wd in window_dates
                           if all_signals.get(wd, {}).get(sym) and all_signals[wd][sym].signal_c_triggered)
                # 两个指标同时满足: 每个都 ≥2日
                signals_ok = (
                    (1 if b_days >= FUSION_MIN_DAYS_PER_SIGNAL else 0)
                    + (1 if c_days >= FUSION_MIN_DAYS_PER_SIGNAL else 0)
                )
                sig.composite_triggered = signals_ok >= FUSION_MIN_SIGNALS

        return all_signals

    # ═══════════════════════════════════════════════════════════
    # 组合策略 — 一个组合，评分选前5，同时买入同时卖出
    # ═══════════════════════════════════════════════════════════

    def run_basket_strategy(
        self, start_date: date, end_date: date,
        symbols: list[str] | None = None,
        precomputed_signals: dict[date, dict[str, DailySignal]] | None = None,
    ) -> dict[str, Any]:
        """运行组合策略回测：每周调仓，评分选前5，同时买入同时卖出"""
        if precomputed_signals is not None:
            all_signals = precomputed_signals
        else:
            all_signals = self.compute_composite_signals(start_date, end_date, symbols)
        trading_dates = sorted(all_signals.keys())

        if len(trading_dates) < 10:
            return {"error": "交易日不足", "trading_dates": len(trading_dates)}

        # 收集所有symbols, 分批获取日线价格
        sym_set: set[str] = set()
        for sigs in all_signals.values():
            sym_set.update(sigs.keys())
        sym_list = list(sym_set)
        if not sym_list:
            return {"error": "无股票", "symbols": 0}

        daily_prices: dict[str, dict[date, float]] = {}
        batch_size = 500
        for i in range(0, len(sym_list), batch_size):
            batch = sym_list[i : i + batch_size]
            rows = self.ch_client.execute(
                "SELECT symbol, trade_date, open "
                "FROM klines_daily "
                "WHERE symbol IN %(batch)s "
                "  AND trade_date >= %(start)s "
                "  AND trade_date <= %(end)s "
                "ORDER BY symbol, trade_date",
                {"batch": tuple(batch), "start": start_date, "end": end_date},
            )
            for r in rows:
                sym, td, price = r[0], r[1], float(r[2])
                daily_prices.setdefault(sym, {})[td] = price

        # ── 模拟组合 ──
        PORTFOLIO_SIZE = 5
        HOLD_DAYS = 20
        REBALANCE_EVERY = 5

        # 用列表存放篮子: 每个篮子 = {'entry_idx': int, 'stocks': [{symbol, price}]}
        baskets: list[dict[str, Any]] = []
        all_trades: list[dict] = []
        week_start_idx = 0

        for i in range(4, len(trading_dates)):
            td = trading_dates[i]

            # 检查是否有篮子到期 (按交易日数)
            remaining_baskets = []
            for basket in baskets:
                hold_trading_days = i - basket['entry_idx']
                if hold_trading_days >= HOLD_DAYS:
                    # 整个篮子到期，逐个卖出
                    for item in basket['stocks']:
                        exit_price = daily_prices.get(item['symbol'], {}).get(td)
                        if exit_price and exit_price > 0:
                            pnl_pct = round((exit_price / item['entry_price'] - 1) * 100, 2)
                            all_trades.append({
                                "symbol": item['symbol'],
                                "entry_date": trading_dates[basket['entry_idx']].isoformat(),
                                "entry_price": round(item['entry_price'], 2),
                                "exit_date": td.isoformat(),
                                "exit_price": round(exit_price, 2),
                                "pnl_pct": pnl_pct,
                            })
                else:
                    remaining_baskets.append(basket)
            baskets = remaining_baskets

            # 每周调仓: 如果当前没有持仓，建新篮子
            if (i - week_start_idx) < REBALANCE_EVERY:
                continue
            week_start_idx = i

            # 已有持仓就不重复建仓
            if baskets:
                continue

            # 回看过去5日的综合信号，评分排序
            candidate_scores: list[tuple[str, float]] = []
            for j in range(max(0, i - 5), i):
                wd = trading_dates[j]
                for sym, sig in all_signals.get(wd, {}).items():
                    if not sig.composite_triggered:
                        continue
                    b_score = -sig.signal_b_value * 100 if sig.signal_b_value else 0
                    c_score = sig.signal_c_value / sig.trend_vol_threshold if sig.signal_c_value and sig.trend_vol_threshold > 0 else 0
                    total_score = b_score + c_score
                    candidate_scores.append((sym, total_score))

            # 去重保留最高分
            best: dict[str, float] = {}
            for sym, sc in candidate_scores:
                if sc > best.get(sym, float('-inf')):
                    best[sym] = sc
            ranked = sorted(best.items(), key=lambda x: -x[1])

            if not ranked:
                continue

            # 建篮子：取前5名
            basket_stocks = []
            for sym, _ in ranked[:PORTFOLIO_SIZE]:
                price = daily_prices.get(sym, {}).get(td)
                if price and price > 0:
                    basket_stocks.append({"symbol": sym, "entry_price": price})

            if basket_stocks:
                baskets.append({
                    "entry_idx": i,
                    "stocks": basket_stocks,
                })

        # 强制平仓所有剩余持仓
        last_date = trading_dates[-1]
        for basket in baskets:
            for item in basket['stocks']:
                exit_price = daily_prices.get(item['symbol'], {}).get(last_date)
                if exit_price and exit_price > 0:
                    pnl_pct = round((exit_price / item['entry_price'] - 1) * 100, 2)
                    all_trades.append({
                        "symbol": item['symbol'],
                        "entry_date": trading_dates[basket['entry_idx']].isoformat(),
                        "entry_price": round(item['entry_price'], 2),
                        "exit_date": last_date.isoformat(),
                        "exit_price": round(exit_price, 2),
                        "pnl_pct": pnl_pct,
                    })

        n_trades = len(all_trades)
        pnls = [t['pnl_pct'] for t in all_trades if t['pnl_pct'] is not None]
        winning = [t for t in all_trades if t['pnl_pct'] is not None and t['pnl_pct'] > 0]

        # 按篮子分组统计
        basket_groups: dict[str, list[dict]] = {}
        for t in all_trades:
            basket_groups.setdefault(t['entry_date'], []).append(t)

        basket_returns = []
        for bd in sorted(basket_groups):
            stocks = basket_groups[bd]
            avg_ret = float(np.mean([s['pnl_pct'] for s in stocks]))
            basket_returns.append(avg_ret)

        return {
            "total_trades": n_trades,
            "win_count": len(winning),
            "win_rate": len(winning) / n_trades if n_trades > 0 else 0.0,
            "avg_return": float(np.mean(pnls)) if pnls else 0.0,
            "total_return": float(np.sum(pnls)) if pnls else 0.0,
            "best_trade": float(max(pnls)) if pnls else 0.0,
            "worst_trade": float(min(pnls)) if pnls else 0.0,
            "basket_avg_returns": [round(r, 2) for r in basket_returns],
            "total_basket_return": round(float(np.prod([1 + r/100 for r in basket_returns]) - 1) * 100, 2) if basket_returns else 0.0,
            "trades": sorted(all_trades, key=lambda x: x['entry_date'])[-50:],
        }


# 全局单例
trend_capital_strategy = TrendCapitalStrategy()
