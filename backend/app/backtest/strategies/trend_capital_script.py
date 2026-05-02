"""趋势资金策略 — 用户脚本常量，作为回测预设策略"""

TREND_CAPITAL_CODE = r'''# 趋势资金日内交易行为事件驱动策略 — 研报十一
# 信号: 趋势资金相对均价(VWAP ratio) + 净支撑量
# 组合: 每周调仓，评分选前5，持股20日
# 数据: 分钟线 (需要在回测配置中设置 bar_type=minute)

import numpy as np

def init(context):
    # 趋势资金识别参数
    context.lookback = 5       # k: 回看天数
    context.vol_pct = 0.90     # m: 成交量分位数阈值
    context.min_trend_minutes = 3  # 最少趋势分钟数

    # 信号融合参数
    context.fusion_window = 3     # 融合窗口（交易日）
    context.fusion_min_days = 2   # 每个信号至少满足天数

    # 组合参数
    context.portfolio_size = 5    # 持仓数
    context.hold_days = 20        # 每只股票持股交易日数
    context.rebalance_every = 5   # 调仓频率（交易日）

    # 运行状态
    context.signal_history = {}   # {date: {symbol: {...信号数据...}}}
    context.baskets = []          # [{entry_idx, stocks: [{symbol, entry_price}]}]
    context.day_count = 0
    context.last_rebalance = -999

    log(f"趋势资金策略: 查{context.lookback}日 阈值{context.vol_pct} 持{context.portfolio_size}只 {context.hold_days}日")

# ── 信号计算辅助函数 ──
def compute_daily_signal(context, symbol, today):
    """计算单只股票单日信号"""
    # 获取过去 lookback 天的分钟数据，计算成交量阈值
    hist = context.get_intraday_history(symbol, context.lookback, today)
    if hist is None or hist.empty:
        return None

    # 过去N天成交量90%分位数
    past_vols = []
    for d in sorted(set(hist.index.date)):
        if d >= today:
            continue
        day_data = hist[hist.index.date == d]
        past_vols.extend(day_data["volume"].values.tolist())

    if len(past_vols) < context.lookback * 10:
        return None
    threshold = np.quantile(past_vols, context.vol_pct)

    # 获取当日分钟数据
    bars = context.get_intraday(symbol, today)
    if len(bars) < 10:
        return None

    vols = np.array([b.volume for b in bars], dtype=float)
    closes = np.array([b.close for b in bars], dtype=float)
    amounts = np.array([b.total_turnover for b in bars], dtype=float)

    is_trend = vols > threshold
    trend_count = int(is_trend.sum())
    if trend_count < context.min_trend_minutes:
        return {
            "sig_b": 0, "sig_b_ok": False,
            "sig_c": 0, "sig_c_ok": False,
            "trend_count": trend_count,
        }

    trend_vol = vols[is_trend]
    trend_close = closes[is_trend]
    trend_amount = amounts[is_trend]

    # Signal B: 趋势资金 VWAP vs 全部 VWAP
    trend_vwap = np.sum(trend_amount) / np.sum(trend_vol) if trend_vol.sum() > 0 else 0
    all_vwap = np.sum(amounts) / np.sum(vols) if vols.sum() > 0 else 0
    sig_b = trend_vwap / all_vwap - 1 if all_vwap > 0 else 0

    # Signal C: 净支撑量
    avg_close = np.mean(closes)
    support_vol = np.sum(trend_vol[trend_close < avg_close])
    resist_vol = np.sum(trend_vol[trend_close > avg_close])
    sig_c = support_vol - resist_vol

    return {
        "sig_b": float(sig_b), "sig_b_ok": sig_b < 0,
        "sig_c": float(sig_c), "sig_c_ok": sig_c > 0,
        "trend_count": trend_count,
    }

def before_trading(context):
    """盘前: 计算当日所有标的的信号"""
    today = context.now.date()

    signals = {}
    for symbol in list(context.universe):
        try:
            sig = compute_daily_signal(context, symbol, today)
            if sig is not None:
                signals[symbol] = sig
        except Exception:
            continue

    if signals:
        context.signal_history[today] = signals

    # 清理过期信号
    from datetime import timedelta
    cutoff = today - timedelta(days=10)
    for d in list(context.signal_history.keys()):
        if d < cutoff:
            del context.signal_history[d]

def handle_bar(context, bar_dict):
    """每日: 管理篮子组合"""
    context.day_count += 1
    now = context.now.date()

    # ── 1. 到期平仓 ──
    remaining = []
    for basket in context.baskets:
        held = context.day_count - basket["entry_idx"]
        if held >= context.hold_days:
            for item in basket["stocks"]:
                pos = context.portfolio.get_position(item["symbol"])
                if pos and pos.total_shares > 0:
                    order_shares(item["symbol"], -pos.total_shares)
        else:
            remaining.append(basket)
    context.baskets = remaining

    # ── 2. 调仓判断 ──
    if context.day_count - context.last_rebalance < context.rebalance_every:
        return
    if context.baskets:
        return
    context.last_rebalance = context.day_count

    dates = sorted(context.signal_history.keys())
    if len(dates) < context.fusion_window:
        return

    # ── 3. 信号融合评分 ──
    window_dates = dates[-context.fusion_window:]
    candidates = {}
    all_syms = set()
    for d in window_dates:
        all_syms.update(context.signal_history.get(d, {}).keys())

    for sym in all_syms:
        b_days = 0
        c_days = 0
        for d in window_dates:
            sig = context.signal_history.get(d, {}).get(sym, {})
            if sig.get("sig_b_ok"):
                b_days += 1
            if sig.get("sig_c_ok"):
                c_days += 1
        if b_days < context.fusion_min_days or c_days < context.fusion_min_days:
            continue

        latest = context.signal_history.get(window_dates[-1], {}).get(sym, {})
        b_score = -latest.get("sig_b", 0) * 100
        c_score = latest.get("sig_c", 0) / 1e6
        candidates[sym] = b_score + c_score

    if not candidates:
        return

    # ── 4. 选前N买入 ──
    ranked = sorted(candidates.items(), key=lambda x: -x[1])[:context.portfolio_size]

    total_value = context.portfolio.total_value
    per_stock = total_value * 0.9 / context.portfolio_size

    basket_stocks = []
    for sym, _ in ranked:
        bar = bar_dict[sym] if sym in bar_dict else None
        if bar is None or bar.suspended:
            continue
        context.order_value(sym, per_stock, bar.close)
        basket_stocks.append({"symbol": sym, "entry_price": bar.close})

    if basket_stocks:
        context.baskets.append({
            "entry_idx": context.day_count,
            "stocks": basket_stocks,
        })
        log(f"[{now}] 建仓 {len(basket_stocks)} 只")
'''
