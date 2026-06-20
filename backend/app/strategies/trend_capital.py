"""趋势资金日内交易行为事件驱动策略 - 研报十一。

这条策略专门识别“持续出现的活跃资金行为”，而不是单日脉冲：
信号 A 近似小单净流出，信号 B 看趋势资金相对 VWAP 的成交位置，
信号 C 看净支撑量。三个信号在 3 个交易日窗口内满足至少 2 日触发后，
才允许进入综合入场判断。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np

from app.data_stores import get_market_data_store

# ───────────────────────────────────────────────────────────────
# 配置参数
# ───────────────────────────────────────────────────────────────

# 趋势资金识别参数
TREND_LOOKBACK_DAYS = 5      # 回看多少个交易日来估计活跃成交阈值
TREND_VOL_PERCENTILE = 0.90  # 用成交量的 90% 分位数作为“活跃资金”门槛

# 信号融合参数：研报要求 3 日里至少 2 日触发
FUSION_WINDOW_DAYS = 3        # 参与统计的交易日窗口长度
FUSION_MIN_DAYS_PER_SIGNAL = 2  # 单个信号在窗口内至少触发几天
FUSION_MIN_SIGNALS = 2        # 至少有几个信号达标后才算通过

# 组合/通道参数
NUM_CHANNELS = 5              # 最多同时持有多少只股票
CHANNEL_HOLD_DAYS = 20        # 单票默认持有的交易日数


@dataclass
class DailySignal:
    """单只股票在单个交易日上的信号结果。

    字段含义:
        signal_a_value: 小单净流出近似值，正值表示净流出更强。
        signal_b_value: 趋势资金相对全日 VWAP 的偏离值，越低越偏向低位吸筹。
        signal_c_value: 趋势资金净支撑量，正值更像吸筹，负值更像派发。
        *_triggered: 对应信号是否达到阈值。
    """
    symbol: str
    trade_date: date

    signal_a_value: float | None = None  # 小单净流出量 (正=净流出)
    signal_b_value: float | None = None  # 趋势资金相对均价（VWAP ratio - 1）
    signal_c_value: float | None = None  # 趋势资金净支撑量（吸筹 - 派发）

    signal_a_triggered: bool = False
    signal_b_triggered: bool = False
    signal_c_triggered: bool = False

    trend_minute_count: int = 0
    total_minute_count: int = 0
    trend_vol_threshold: float = 0.0

    composite_triggered: bool = False


@dataclass
class ChannelPosition:
    """一个持仓通道对应的一笔交易篮子。

    它用于把同一轮信号买入的一组股票当成一个篮子管理，方便单独统计
    到期、盈亏和退出情况。
    """
    channel_id: int
    symbol: str
    entry_date: date
    entry_price: float
    exit_date: date | None = None
    exit_price: float | None = None
    pnl_pct: float | None = None


class TrendCapitalStrategy:
    """趋势资金事件驱动策略主类。

    作用:
        从分钟线中识别持续的趋势资金行为，按通道管理组合，并在信号
        满足持续性要求后择时进入。

    参数含义:
        TREND_LOOKBACK_DAYS: 用于估计活跃成交阈值的回看窗口。
        TREND_VOL_PERCENTILE: 成交量分位数门槛，越高越严格。
        FUSION_WINDOW_DAYS: 信号融合窗口长度。
        FUSION_MIN_DAYS_PER_SIGNAL: 单个信号至少要连续满足几天。
        FUSION_MIN_SIGNALS: 需要同时满足的信号数量。
        NUM_CHANNELS: 组合最多能同时开多少个持仓通道。
        CHANNEL_HOLD_DAYS: 单通道默认持有天数。
    """

    def __init__(self, sample_space: str = "CSI800"):
        # 策略不关心底层行情存储是 Parquet 还是其他实现，
        # 只依赖统一的数据接口。
        self.sample_space = sample_space
        self._store = get_market_data_store()

    def _resolve_symbols(self, symbols: list[str] | None, start: date, end: date) -> list[str]:
        if symbols:
            return symbols
        # 如果外部没有传入股票池，就退回到流动性较好的样本池，确保分钟
        # 信号有足够成交数据来估计每只股票自己的阈值。
        limit = 800 if self.sample_space.upper() == "CSI800" else 500
        return self._store.top_by_avg_amount(start, end, limit)

    # ═══════════════════════════════════════════════════════════
    # 趋势资金识别 & 日度信号
    # ═══════════════════════════════════════════════════════════

    def get_trading_dates(
        self, start: date, end: date, symbols: list[str] | None = None
    ) -> list[date]:
        """返回给定区间内的交易日列表。"""
        resolved_symbols = self._resolve_symbols(symbols, start, end)
        return self._store.load_trading_dates(resolved_symbols, start, end)

    def compute_daily_signals(
        self, trade_date: date, symbols: list[str] | None = None
    ) -> dict[str, DailySignal]:
        """计算指定交易日内所有股票的日度信号。"""
        results: dict[str, DailySignal] = {}
        resolved_symbols = self._resolve_symbols(symbols, trade_date - timedelta(days=30), trade_date)
        if not resolved_symbols:
            return results

        # 第 1 步：取过去 k 个交易日。
        # 这里的回看窗口刻意做得较短，因为阈值应该反映当前交易环境，
        # 而不是几周前的旧样本。
        past_dates = [
            d
            for d in self.get_trading_dates(
                trade_date - timedelta(days=30), trade_date, resolved_symbols
            )
            if d < trade_date
        ][-TREND_LOOKBACK_DAYS:]
        if len(past_dates) < TREND_LOOKBACK_DAYS:
            return results

        past_start = past_dates[0]
        past_end = past_dates[-1] + timedelta(days=1)

        # 第 2 步：批量计算每只股票自己的成交量 90% 分位数阈值。
        past_minutes = self._store.load_minute(
            resolved_symbols,
            datetime.combine(past_start, datetime.min.time()),
            datetime.combine(past_end, datetime.min.time()),
            columns=["symbol", "datetime", "volume"],
        )
        if past_minutes.empty:
            return results
        # 这里不用全市场统一阈值，而是让每只股票用自己的 90 分位成交量
        # 作为活跃门槛。
        thresholds = past_minutes.groupby("symbol")["volume"].quantile(TREND_VOL_PERCENTILE)
        threshold_map: dict[str, float] = {
            str(symbol): float(value)
            for symbol, value in thresholds.items()
            if value and value > 0
        }
        if not threshold_map:
            return results

        # 第 3 步：读取当日分钟数据。
        minute_data = self._store.load_minute(
            list(threshold_map.keys()),
            datetime.combine(trade_date, datetime.min.time()),
            datetime.combine(trade_date + timedelta(days=1), datetime.min.time()),
            columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"],
        )

        if minute_data.empty:
            return results

        # 第 4 步：按股票逐个计算信号，把分钟结构转换成日度信号摘要。
        def process_symbol(sym: str, minutes: list[dict]):
            thresh = threshold_map.get(sym, 0)
            if thresh <= 0 or len(minutes) < 10:
                return

            vols = np.array([m["volume"] for m in minutes], dtype=np.float64)
            closes = np.array([m["close"] for m in minutes], dtype=np.float64)
            np.array([m["open"] for m in minutes], dtype=np.float64)
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
                # 过滤稀薄冲击：只有持续出现的异常活跃才算数。
                results[sym] = signal
                return

            trend_vol = vols[is_trend]
            trend_close = closes[is_trend]
            trend_amount = amounts[is_trend]

            # --- 信号 B：趋势资金相对均价 ---
            # 如果趋势资金成交价格低于全天 VWAP，说明主动资金更容易在
            # 平均价下方成交，通常被视为偏建设性的行为。
            trend_vwap = float(np.sum(trend_amount) / np.sum(trend_vol)) if trend_vol.sum() > 0 else 0.0
            all_vwap = float(np.sum(amounts) / np.sum(vols)) if vols.sum() > 0 else 0.0
            if all_vwap > 0:
                signal.signal_b_value = float(trend_vwap / all_vwap - 1)
                signal.signal_b_triggered = signal.signal_b_value < 0

            # --- 信号 C：趋势资金净支撑量 ---
            # 比较活跃分钟在均价上方和下方的成交量，判断更像吸筹还是派发。
            all_avg_close = float(np.mean(closes))
            support_vol = float(np.sum(trend_vol[trend_close < all_avg_close]))
            resist_vol = float(np.sum(trend_vol[trend_close > all_avg_close]))
            net_support = support_vol - resist_vol
            signal.signal_c_value = net_support
            signal.signal_c_triggered = net_support > 0

            results[sym] = signal

        for sym, group in minute_data.groupby("symbol", sort=False):
            symbol_minutes = [
                {
                    "volume": float(row.volume),
                    "close": float(row.close),
                    "open": float(row.open),
                    "amount": float(row.amount),
                }
                for row in group.itertuples(index=False)
            ]
            process_symbol(str(sym), symbol_minutes)

        return results

    # ═══════════════════════════════════════════════════════════
    # 信号融合
    # ═══════════════════════════════════════════════════════════

    def compute_composite_signals(
        self, start_date: date, end_date: date,
        symbols: list[str] | None = None,
    ) -> dict[date, dict[str, DailySignal]]:
        """计算区间内的日度信号，并做多日融合。"""
        resolved_symbols = self._resolve_symbols(symbols, start_date, end_date)
        trading_dates = self.get_trading_dates(start_date, end_date, resolved_symbols)
        all_signals: dict[date, dict[str, DailySignal]] = {}

        for td in trading_dates:
            all_signals[td] = self.compute_daily_signals(td, resolved_symbols)

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
                # 每个信号单独检查：3 日里是否至少 2 日触发。
                # 这个持续性门槛会过滤掉只存在一天的脉冲信号。
                b_days = sum(1 for wd in window_dates
                           if all_signals.get(wd, {}).get(sym) and all_signals[wd][sym].signal_b_triggered)
                c_days = sum(1 for wd in window_dates
                           if all_signals.get(wd, {}).get(sym) and all_signals[wd][sym].signal_c_triggered)
                # 两个信号都达标才算整体通过。
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
        """运行组合策略回测：每周调仓，评分选前 5，同时建仓同时平仓。"""
        if precomputed_signals is not None:
            all_signals = precomputed_signals
        else:
            all_signals = self.compute_composite_signals(start_date, end_date, symbols)
        trading_dates = sorted(all_signals.keys())

        if len(trading_dates) < 10:
            return {"error": "交易日不足", "trading_dates": len(trading_dates)}

        # 收集所有标的，分批读取日线开盘价。
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
            daily_df = self._store.load_daily(
                batch,
                start_date,
                end_date,
                columns=["symbol", "trade_date", "open"],
            )
            if daily_df.empty:
                continue
            for row in daily_df.reset_index().itertuples(index=False):
                trade_date = row.trade_date.date() if hasattr(row.trade_date, "date") else row.trade_date
                daily_prices.setdefault(str(row.symbol), {})[trade_date] = float(row.open)

        # ── 模拟组合 ──
        # 这里用日开盘价作为可执行代理；当日收盘价不进入信号路径，避免
        # 未来函数。
        PORTFOLIO_SIZE = 5
        HOLD_DAYS = 20
        REBALANCE_EVERY = 5

        # 用列表存放交易篮子：每个篮子记录进入时点和其中的股票集合。
        baskets: list[dict[str, Any]] = []
        all_trades: list[dict] = []
        week_start_idx = 0

        for i in range(4, len(trading_dates)):
            td = trading_dates[i]

            # 检查是否有篮子到期（按交易日数）。
            remaining_baskets = []
            for basket in baskets:
                hold_trading_days = i - basket['entry_idx']
                if hold_trading_days >= HOLD_DAYS:
                    # 整个篮子到期后，按篮子内标的逐个卖出。
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

            # 每周调仓：如果当前没有持仓，就创建新篮子。
            if (i - week_start_idx) < REBALANCE_EVERY:
                continue
            week_start_idx = i

            # 已经有持仓时不重复建新篮子。
            if baskets:
                continue

            # 回看过去 5 日的综合信号并排序。
            # 这样能让持续信号排在只闪现一天的脉冲前面。
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

            # 去重后保留每只股票的最高分。
            best: dict[str, float] = {}
            for sym, sc in candidate_scores:
                if sc > best.get(sym, float('-inf')):
                    best[sym] = sc
            ranked = sorted(best.items(), key=lambda x: -x[1])

            if not ranked:
                continue

            # 建篮子：取前 5 名。
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

        # 强制平掉所有剩余持仓。
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

        # 按篮子分组统计回测结果。
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


# 全局单例，便于上层直接复用。
trend_capital_strategy = TrendCapitalStrategy()
