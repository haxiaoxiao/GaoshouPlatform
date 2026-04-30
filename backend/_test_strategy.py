# backend/_test_strategy.py
"""趋势资金事件驱动策略 — 完整回测"""
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, ".")

import numpy as np
from app.strategies.trend_capital import TrendCapitalStrategy


def compute_benchmark(symbols: list[str], start: date, end: date) -> tuple[list[float], float, list[date]]:
    """计算等权基准日收益"""
    from app.db.clickhouse import get_ch_client
    ch = get_ch_client()

    daily_returns: dict[date, list[float]] = {}
    batch_size = 500

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        rows = ch.execute(
            "SELECT trade_date, close, symbol "
            "FROM klines_daily "
            "WHERE symbol IN %(batch)s "
            "  AND trade_date >= %(start)s "
            "  AND trade_date <= %(end)s "
            "ORDER BY symbol, trade_date",
            {"batch": tuple(batch), "start": start, "end": end},
        )
        prev_close: dict[str, float] = {}
        for r in rows:
            td, close, sym = r[0], float(r[1]), r[2]
            if sym in prev_close and prev_close[sym] > 0:
                ret = close / prev_close[sym] - 1
                daily_returns.setdefault(td, []).append(ret)
            prev_close[sym] = close

    dates = sorted(daily_returns.keys())
    eq_returns = [float(np.mean(daily_returns[d])) for d in dates]

    cumulative = 1.0
    eq_curve = [0.0]
    for r in eq_returns:
        cumulative *= (1 + r)
        eq_curve.append(cumulative - 1)

    n_years = (dates[-1] - dates[0]).days / 365.25 if len(dates) > 1 else 1.0
    total_ret = eq_curve[-1] if eq_curve else 0.0
    annual_ret = (1 + total_ret) ** (1 / max(n_years, 0.1)) - 1

    return eq_returns, annual_ret, dates


def compute_metrics(returns: list[float]) -> dict:
    """计算回测指标"""
    if not returns or len(returns) < 2:
        return {}

    rets = np.array(returns)
    n_years = len(rets) / 252
    total_ret = float(np.prod(1 + rets) - 1)
    annual_ret = float((1 + total_ret) ** (1 / max(n_years, 0.1)) - 1)
    annual_vol = float(np.std(rets) * np.sqrt(252))
    sharpe = (annual_ret - 0.02) / annual_vol if annual_vol > 0 else 0.0

    cumulative = np.cumprod(1 + rets)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = cumulative / running_max - 1
    max_dd = float(np.min(drawdowns))
    calmar = annual_ret / abs(max_dd) if max_dd != 0 else 0.0
    win_rate = float(np.sum(rets > 0) / len(rets))

    return {
        "n_days": len(rets),
        "annual_return": round(annual_ret * 100, 2),
        "annual_volatility": round(annual_vol * 100, 2),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown": round(max_dd * 100, 2),
        "calmar_ratio": round(calmar, 2),
        "win_rate": round(win_rate * 100, 2),
        "total_return": round(total_ret * 100, 2),
    }


def build_equity_curve(trades: list[dict]) -> tuple[list[float], list[date]]:
    """从交易记录构建资金曲线 (按篮子)"""
    if not trades:
        return [], []

    # 按入场日期分组（每个日期=一个篮子）
    basket_returns: dict[date, list[float]] = {}
    for t in trades:
        if t.get('pnl_pct') is not None:
            td = date.fromisoformat(t['entry_date'])
            basket_returns.setdefault(td, []).append(t['pnl_pct'])

    dates = sorted(basket_returns.keys())
    cumulative = 1.0
    curve = [0.0]
    for td in dates:
        avg_ret = float(np.mean(basket_returns[td])) / 100
        cumulative *= (1 + avg_ret)
        curve.append(round((cumulative - 1) * 100, 2))

    return curve, dates


def run_backtest(start_date: date, end_date: date):
    """运行完整回测"""
    print("=" * 70)
    print(f"趋势资金事件驱动策略回测")
    print(f"区间: {start_date} ~ {end_date}")
    print("=" * 70)

    strategy = TrendCapitalStrategy()
    t0 = time.time()

    # Step 1: 计算所有信号 (一次性)
    print("\n[1/2] 计算日度信号与信号融合...")
    all_signals = strategy.compute_composite_signals(start_date, end_date)
    trading_dates = sorted(all_signals.keys())
    print(f"  交易日: {len(trading_dates)} 天")
    print(f"  耗时: {time.time() - t0:.0f}s")

    total_comp = sum(
        sum(1 for s in sigs.values() if s.composite_triggered)
        for sigs in all_signals.values()
    )
    print(f"  综合信号总数: {total_comp}, 日均: {total_comp / max(1, len(trading_dates)):.0f} 只")

    # Step 2: 组合策略回测 (复用已计算的信号)
    print("\n[2/2] 运行组合策略...")
    bt_start = time.time()
    bt_result = strategy.run_basket_strategy(
        start_date, end_date,
        precomputed_signals=all_signals,
    )
    print(f"  耗时: {time.time() - bt_start:.0f}s")

    # Step 3: 计算指标
    equity, eq_dates = build_equity_curve(bt_result.get('trades', []))
    if equity and len(equity) > 2:
        daily_rets = np.diff(equity) / 100  # PnL% → decimal
        strategy_metrics = compute_metrics(list(daily_rets))
    else:
        strategy_metrics = {}

    # 基准
    sym_set = set()
    for sigs in all_signals.values():
        sym_set.update(sigs.keys())
    sym_list = list(sym_set)
    bench_ret, bench_ann, _ = compute_benchmark(sym_list[:300], start_date, end_date)
    bench_metrics = compute_metrics(bench_ret) if bench_ret else {}

    # ═══════════════════════════════════════
    # 输出
    # ═══════════════════════════════════════
    print("\n" + "=" * 70)
    print("回测结果")
    print("=" * 70)

    print(f"\n--- 交易统计 ---")
    print(f"  总交易: {bt_result.get('total_trades', 0)}")
    print(f"  胜率: {bt_result.get('win_rate', 0):.1%}")
    print(f"  平均收益: {bt_result.get('avg_return', 0):.2f}%")
    print(f"  累计收益: {bt_result.get('total_return', 0):.2f}%")
    print(f"  最佳单笔: {bt_result.get('best_trade', 0):.2f}%")
    print(f"  最差单笔: {bt_result.get('worst_trade', 0):.2f}%")

    if strategy_metrics:
        print(f"\n--- 策略组合指标 ---")
        for k, v in strategy_metrics.items():
            print(f"  {k}: {v}")

    if bench_metrics:
        print(f"\n--- 基准 (等权) ---")
        for k, v in bench_metrics.items():
            print(f"  {k}: {v}")

        if strategy_metrics and bench_metrics:
            excess = strategy_metrics.get('annual_return', 0) - bench_metrics.get('annual_return', 0)
            print(f"\n--- 超额收益 ---")
            print(f"  年化超额: {excess:.2f}%")

    # 分篮子统计
    trades = bt_result.get('trades', [])
    if trades:
        basket_groups: dict[str, list[dict]] = {}
        for t in trades:
            basket_groups.setdefault(t['entry_date'], []).append(t)

        print(f"\n--- 分篮子统计 ---")
        for bd in sorted(basket_groups):
            stocks = basket_groups[bd]
            pnls = [s['pnl_pct'] for s in stocks if s['pnl_pct'] is not None]
            wins = sum(1 for p in pnls if p > 0)
            syms = ', '.join(s['symbol'] for s in stocks)
            print(f"  {bd}: {len(stocks)}只, 胜率={wins}/{len(pnls)}, 均={np.mean(pnls):.2f}%, 累={np.sum(pnls):+.2f}%")
            print(f"     {syms}")

    total_time = time.time() - t0
    print(f"\n总耗时: {total_time:.0f}s ({total_time/60:.1f}min)")
    return bt_result


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-10-01")
    ap.add_argument("--end", default="2026-04-21")
    ap.add_argument("--quick", action="store_true", help="Fast: 1 month only")
    ap.add_argument("--full", action="store_true", help="Full period")
    args = ap.parse_args()

    if args.quick:
        start = date.fromisoformat(args.end) - timedelta(days=35)
        end = date.fromisoformat(args.end)
    elif args.full:
        # 从分钟数据最早可计算日开始
        start = date(2025, 5, 1)
        end = date.fromisoformat(args.end)
    else:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)

    run_backtest(start, end)
