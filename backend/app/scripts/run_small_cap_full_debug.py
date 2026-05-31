"""Run small-cap strategy over one continuous range and export debug artifacts."""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import sqlite3
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from pathlib import Path
from typing import Any

from loguru import logger

from app.backtest.config import BacktestConfig
from app.backtest.engine.akquant.engine import AkquantEngine
from app.backtest.engine.data_provider import StoreDataProvider
from app.services.index_components import load_index_symbols
from app.services.timer_minute_sync import find_earliest_timer_coverage_date, parse_timer_times

DEFAULT_TIMER_TIMES = ["09:15", "10:00", "10:30", "14:30", "14:50", "15:10"]
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _load_strategy_code(strategy_id: int) -> str:
    db_path = PROJECT_ROOT / "data/gaoshou.db"
    if not db_path.exists():
        db_path = PROJECT_ROOT / "backend/data/gaoshou.db"
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT code FROM strategies WHERE id=?", (strategy_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"strategy not found: {strategy_id}")
    return str(row[0])


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _order_time(order: dict[str, Any]) -> str:
    return str(order.get("updated_at") or order.get("created_at") or "")


def _rebuild_positions(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    positions: dict[str, float] = {}
    snapshots = []
    filled_orders = [o for o in orders if str(o.get("status", "")).lower() == "filled"]
    filled_orders.sort(key=_order_time)
    for order in filled_orders:
        symbol = str(order.get("symbol") or "")
        if not symbol:
            continue
        qty = float(order.get("filled_quantity") or order.get("quantity") or 0)
        side = str(order.get("side") or "").lower()
        if side == "buy":
            positions[symbol] = positions.get(symbol, 0.0) + qty
        elif side == "sell":
            positions[symbol] = max(0.0, positions.get(symbol, 0.0) - qty)
            if positions[symbol] <= 0:
                positions.pop(symbol, None)
        snapshots.append(
            {
                "time": _order_time(order),
                "symbol": symbol,
                "side": side,
                "quantity": qty,
                "positions": json.dumps(
                    {k: v for k, v in sorted(positions.items()) if v > 0},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }
        )
    return snapshots


async def _run(args: argparse.Namespace, start: date, end: date, out_dir: Path) -> dict[str, Any]:
    symbols = await load_index_symbols(args.index_symbol, start, end)
    code = _load_strategy_code(args.strategy_id)
    config = BacktestConfig(
        mode="event_driven",
        engine="akquant",
        strategy_id=args.strategy_id,
        strategy_code=code,
        symbols=symbols,
        start_date=start,
        end_date=end,
        initial_capital=args.initial_capital,
        bar_type="minute_timer",
        benchmark_symbol=args.index_symbol,
        strategy_params={
            "index_symbol": args.index_symbol,
            "universe_mode": "index",
            "timer_times": DEFAULT_TIMER_TIMES,
            "cash_buffer_rate": args.cash_buffer_rate,
            "filter_st": args.filter_st,
            "v4_indicator_mode": args.v4_indicator_mode,
            "industry_mode": args.industry_mode,
            "enable_indicator": args.enable_indicator,
            "enable_indicator_exit": args.enable_indicator_exit,
            "enable_industry_filter": args.enable_industry_filter,
            "HV_control": args.hv_control,
            "HV_duration": args.hv_duration,
            "HV_ratio": args.hv_ratio,
            "high_volume_mode": args.high_volume_mode,
            "enable_factor_value_cache": args.enable_factor_value_cache,
            "high_volume_factor_name": args.high_volume_factor_name,
            "daily_volume_to_share_multiplier": args.daily_volume_to_share_multiplier,
            "limit_price_source": args.limit_price_source,
            "timer_price_adjustment_mode": args.timer_price_adjustment_mode,
            "execution_plan_mode": args.execution_plan_mode,
            "debug_logging": args.debug_logging,
            "ak_history_depth": args.ak_history_depth,
            "enable_timer_snapshots": args.enable_timer_snapshots,
            "smart_timer_on_bar": args.smart_timer_on_bar,
            "smart_timer_full_universe_times": args.smart_timer_full_universe_times,
            "smart_timer_candidate_top_n": args.smart_timer_candidate_top_n,
            "run_stoploss": args.run_stoploss,
            "pass_april": args.pass_april,
            "stock_num": args.stock_num,
            "max_industry_weight": args.max_industry_weight,
            "stoploss_strategy": args.stoploss_strategy,
            "stoploss_limit": args.stoploss_limit,
            "stoploss_market": args.stoploss_market,
        },
        max_positions=args.stock_num,
        commission_rate=0.00025,
        stamp_tax_rate=0.001,
        transfer_fee_rate=0.00001,
        min_commission=5.0,
        slippage=0.001,
        volume_limit_pct=0.25,
        t_plus_one=True,
        lot_size=100,
        exit_on_last_bar=args.exit_on_last_bar,
    )

    log_buffer = io.StringIO()
    logging_root = logging.getLogger()
    stream_handler = logging.StreamHandler(log_buffer)
    stream_handler.setLevel(logging.INFO)
    logging_root.addHandler(stream_handler)
    loguru_id = logger.add(log_buffer, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
    started = time.time()
    try:
        with redirect_stdout(log_buffer), redirect_stderr(log_buffer):
            result = await AkquantEngine().run(config, StoreDataProvider())
    finally:
        logging_root.removeHandler(stream_handler)
        logger.remove(loguru_id)
    elapsed = time.time() - started

    data = result.to_dict()
    orders = data.get("orders") or []
    trades = data.get("trades") or []
    snapshots = _rebuild_positions(orders)
    summary = {
        "strategy_id": args.strategy_id,
        "index_symbol": args.index_symbol,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "symbol_count": len(symbols),
        "params": {
            "cash_buffer_rate": args.cash_buffer_rate,
            "filter_st": args.filter_st,
            "v4_indicator_mode": args.v4_indicator_mode,
            "industry_mode": args.industry_mode,
            "enable_indicator": args.enable_indicator,
            "enable_indicator_exit": args.enable_indicator_exit,
            "enable_industry_filter": args.enable_industry_filter,
            "HV_control": args.hv_control,
            "HV_duration": args.hv_duration,
            "HV_ratio": args.hv_ratio,
            "high_volume_mode": args.high_volume_mode,
            "enable_factor_value_cache": args.enable_factor_value_cache,
            "high_volume_factor_name": args.high_volume_factor_name,
            "daily_volume_to_share_multiplier": args.daily_volume_to_share_multiplier,
            "limit_price_source": args.limit_price_source,
            "timer_price_adjustment_mode": args.timer_price_adjustment_mode,
            "execution_plan_mode": args.execution_plan_mode,
            "smart_timer_on_bar": args.smart_timer_on_bar,
            "smart_timer_full_universe_times": args.smart_timer_full_universe_times,
            "smart_timer_candidate_top_n": args.smart_timer_candidate_top_n,
            "run_stoploss": args.run_stoploss,
            "pass_april": args.pass_april,
            "stock_num": args.stock_num,
            "max_industry_weight": args.max_industry_weight,
            "stoploss_strategy": args.stoploss_strategy,
            "stoploss_limit": args.stoploss_limit,
            "stoploss_market": args.stoploss_market,
            "exit_on_last_bar": args.exit_on_last_bar,
        },
        "metrics": {
            key: data.get(key)
            for key in [
                "total_return",
                "annual_return",
                "max_drawdown",
                "sharpe_ratio",
                "win_rate",
                "total_trades",
                "final_capital",
                "n_trading_days",
            ]
        },
        "order_count": len(orders),
        "trade_count": len(trades),
        "final_positions": json.loads(snapshots[-1]["positions"]) if snapshots else {},
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "strategy.log").write_text(log_buffer.getvalue(), encoding="utf-8")
    (out_dir / "summary.json").write_text(
        json.dumps(_json_safe(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(out_dir / "orders.csv", orders)
    _write_csv(out_dir / "trades.csv", trades)
    _write_csv(out_dir / "positions.csv", snapshots)
    _write_csv(out_dir / "nav.csv", data.get("nav_series") or [])
    return summary


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-id", type=int, default=43)
    parser.add_argument("--index-symbol", default="399101.SZ")
    parser.add_argument("--start", default="auto")
    parser.add_argument("--end", default="2026-05-08")
    parser.add_argument("--initial-capital", type=float, default=1_000_000)
    parser.add_argument("--out", default="backend/app/reports/small_cap_full_latest")
    parser.add_argument("--cash-buffer-rate", type=float, default=0.01)
    parser.add_argument("--filter-st", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--v4-indicator-mode", default="robust", choices=["robust", "jq_fallback", "fallback"])
    parser.add_argument("--industry-mode", default="local", choices=["local", "unknown", "jq_unknown"])
    parser.add_argument("--enable-indicator", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--enable-indicator-exit", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--enable-industry-filter", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--hv-control", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--hv-duration", type=int, default=120)
    parser.add_argument("--hv-ratio", type=float, default=0.9)
    parser.add_argument(
        "--high-volume-mode",
        default="daily_include_now",
        choices=["minute_to_now", "daily_include_now"],
    )
    parser.add_argument("--enable-factor-value-cache", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--high-volume-factor-name", default="high_volume_signal")
    parser.add_argument("--daily-volume-to-share-multiplier", type=float, default=100.0)
    parser.add_argument("--limit-price-source", default="table", choices=["table", "minute_prev_close", "daily_prev_close", "auto"])
    parser.add_argument(
        "--timer-price-adjustment-mode",
        default="previous_close_ratio",
        choices=["none", "previous_close_ratio"],
    )
    parser.add_argument("--execution-plan-mode", default="timer", choices=["timer", "pre_open"])
    parser.add_argument("--debug-logging", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ak-history-depth", type=int, default=0)
    parser.add_argument("--enable-timer-snapshots", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--smart-timer-on-bar", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--smart-timer-full-universe-times", default="09:15,10:30")
    parser.add_argument("--smart-timer-candidate-top-n", type=int, default=200)
    parser.add_argument("--run-stoploss", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--pass-april", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--stock-num", type=int, default=5)
    parser.add_argument("--max-industry-weight", type=float, default=0.3)
    parser.add_argument("--stoploss-strategy", type=int, default=3)
    parser.add_argument("--stoploss-limit", type=float, default=0.88)
    parser.add_argument("--stoploss-market", type=float, default=0.94)
    parser.add_argument("--exit-on-last-bar", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    end = date.fromisoformat(args.end)
    if str(args.start).lower() == "auto":
        coverage = find_earliest_timer_coverage_date(
            index_symbol=args.index_symbol,
            start=date(2020, 1, 1),
            end=end,
            timer_times=parse_timer_times(",".join(DEFAULT_TIMER_TIMES)),
        )
        if not coverage.get("earliest_date"):
            raise RuntimeError(f"no timer-minute coverage found for {args.index_symbol}: {coverage}")
        start = date.fromisoformat(str(coverage["earliest_date"]))
        print(
            f"auto start resolved: {start} "
            f"(symbols={coverage.get('symbols')}, times={coverage.get('timer_times')})",
            flush=True,
        )
    else:
        start = date.fromisoformat(args.start)
    out_dir = Path(args.out)
    if not out_dir.is_absolute():
        out_dir = PROJECT_ROOT / out_dir
    print(f"running full range: {start}..{end}", flush=True)
    summary = await _run(args, start, end, out_dir)
    print(json.dumps(_json_safe(summary), ensure_ascii=False, indent=2), flush=True)
    print(f"wrote {out_dir.resolve()}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
