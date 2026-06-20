"""趋势资金策略 - 用户脚本常量，作为回测预设策略。

这份脚本是事件驱动版“趋势资金”策略的可执行常量模板，适合直接放进
回测引擎做复现。它通过分钟线识别持续性的活跃资金行为，再用多日融合和
持仓篮子管理控制入场节奏与持有周期。
"""

TREND_CAPITAL_CODE = r'''# 趋势资金日内交易行为事件驱动策略 - 研报十一
# 核心思路：用分钟线识别持续出现的活跃资金，再结合成交位置和净支撑量判断是否值得入场。
# 交易节奏：每周调仓，评分选前 5，单篮子持股 20 个交易日。
# 数据要求：必须使用分钟线，回测配置需要设置 `bar_type=minute`。

import numpy as np

# 脚本保持自包含，回测引擎可以直接执行，不依赖额外的共享辅助模块。
def init(context):
    # 趋势资金识别参数：控制“什么算异常活跃”，以及至少要有多少分钟才算有效信号。
    context.lookback = 5       # 回看多少个交易日来估计成交量阈值
    context.vol_pct = 0.90     # 用历史成交量 90% 分位数作为阈值
    context.min_trend_minutes = 3  # 单日里至少有多少活跃分钟才继续算信号

    # 信号融合参数：要求信号在多个交易日内持续出现，而不是只闪现一天。
    context.fusion_window = 3     # 融合窗口长度，单位是交易日
    context.fusion_min_days = 2   # 单个信号至少满足几天

    # 组合参数：控制持仓规模、持有时长和调仓节奏。
    context.portfolio_size = 5    # 每次最多买入的股票数量
    context.hold_days = 20        # 每只股票的目标持有周期
    context.rebalance_every = 5   # 两次建仓之间至少间隔多少个交易日

    # 运行状态：保存多日信号和当前持仓篮子。
    context.signal_history = {}   # {date: {symbol: {...信号数据...}}}
    context.baskets = []          # [{entry_idx, stocks: [{symbol, entry_price}]}]
    context.day_count = 0
    context.last_rebalance = -999

    log(f"趋势资金策略: 查{context.lookback}日 阈值{context.vol_pct} 持{context.portfolio_size}只 {context.hold_days}日")


# 信号计算拆成单独函数，便于盘前批量构建日级信号缓存。
def compute_daily_signal(context, symbol, today):
    """计算单只股票在单日上的趋势资金信号。

    参数:
        context: 回测上下文，提供分钟线读取接口和策略参数。
        symbol: 需要计算信号的股票代码。
        today: 当前交易日。

    返回:
        当样本不足、分钟数据太少或信号不成立时返回 None；否则返回包含
        sig_b、sig_c、trend_count 的字典，供盘前融合和盘中调仓重复使用。
    """
    # 先取过去 lookback 天的分钟数据，用来估计活跃资金阈值。
    hist = context.get_intraday_history(symbol, context.lookback, today)
    if hist is None or hist.empty:
        return None

    # 过去 N 天成交量 90% 分位数。
    # 这个值是“个股自适应”的，不同标的会根据自己历史成交节奏得到不同门槛。
    past_vols = []
    for d in sorted(set(hist.index.date)):
        if d >= today:
            continue
        day_data = hist[hist.index.date == d]
        past_vols.extend(day_data["volume"].values.tolist())

    if len(past_vols) < context.lookback * 10:
        return None
    threshold = np.quantile(past_vols, context.vol_pct)

    # 读取当日分钟线，确保信号和真实交易日的盘中结构一致。
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

    # 信号 B：趋势资金 VWAP 与全日 VWAP 的偏离。
    # 越低表示活跃资金更倾向在低位成交，通常更像吸筹而不是追高。
    trend_vwap = np.sum(trend_amount) / np.sum(trend_vol) if trend_vol.sum() > 0 else 0
    all_vwap = np.sum(amounts) / np.sum(vols) if vols.sum() > 0 else 0
    sig_b = trend_vwap / all_vwap - 1 if all_vwap > 0 else 0

    # 信号 C：净支撑量。
    # 统计活跃分钟里，价格落在均价上下两侧的成交量差，近似判断是吸筹还是派发。
    avg_close = np.mean(closes)
    support_vol = np.sum(trend_vol[trend_close < avg_close])
    resist_vol = np.sum(trend_vol[trend_close > avg_close])
    sig_c = support_vol - resist_vol

    return {
        "sig_b": float(sig_b), "sig_b_ok": sig_b < 0,
        "sig_c": float(sig_c), "sig_c_ok": sig_c > 0,
        "trend_count": trend_count,
    }


# 盘前统一计算当日全市场信号，避免在盘中重复扫描历史窗口。
def before_trading(context):
    """盘前统一计算当日所有标的的信号，并写入日度缓存。"""
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

    # 清理过期信号，保持缓存窗口短而稳定，避免历史噪声越积越多。
    from datetime import timedelta
    cutoff = today - timedelta(days=10)
    for d in list(context.signal_history.keys()):
        if d < cutoff:
            del context.signal_history[d]


# 每日逻辑先处理到期篮子，再在满足冷却窗口时开新仓。
def handle_bar(context, bar_dict):
    """每日处理篮子组合的到期、调仓和新建仓。

    先处理到期篮子，再检查冷却窗口，最后按融合后的候选分数建立新篮子。
    """
    context.day_count += 1
    now = context.now.date()

    # 1. 到期平仓：持有达到上限后，按原仓位全部退出。
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

    # 2. 调仓判断：只有在冷却期结束且当前没有存量篮子时才允许新开仓。
    if context.day_count - context.last_rebalance < context.rebalance_every:
        return
    if context.baskets:
        return
    context.last_rebalance = context.day_count

    dates = sorted(context.signal_history.keys())
    if len(dates) < context.fusion_window:
        return

    # 3. 信号融合评分：要求多个交易日连续满足条件，过滤掉单日脉冲噪声。
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

    # 4. 选前 N 只买入：按融合分数排序后建立新的持仓篮子。
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
