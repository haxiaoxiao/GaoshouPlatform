"""Run small-cap strategy year by year and export debug artifacts."""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import sqlite3
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


def _year_segments(start: date, end: date) -> list[tuple[int, date, date]]:
    segments = []
    for year in range(start.year, end.year + 1):
        seg_start = max(start, date(year, 1, 1))
        seg_end = min(end, date(year, 12, 31))
        if seg_start <= seg_end:
            segments.append((year, seg_start, seg_end))
    return segments


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


async def _run_segment(args, year: int, start: date, end: date, out_dir: Path) -> dict[str, Any]:
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
            "execution_plan_mode": args.execution_plan_mode,
        },
        max_positions=5,
        commission_rate=0.00025,
        stamp_tax_rate=0.001,
        transfer_fee_rate=0.00001,
        min_commission=5.0,
        slippage=0.001,
        volume_limit_pct=0.25,
        t_plus_one=True,
        lot_size=100,
        exit_on_last_bar=True,
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

    year_dir = out_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    (year_dir / "strategy.log").write_text(log_buffer.getvalue(), encoding="utf-8")
    (year_dir / "summary.json").write_text(
        json.dumps(
            _json_safe(
                {
                    "year": year,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "elapsed_seconds": round(elapsed, 3),
                    "symbol_count": len(symbols),
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
            ),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_csv(year_dir / "orders.csv", orders)
    _write_csv(year_dir / "trades.csv", trades)
    _write_csv(year_dir / "positions.csv", snapshots)
    _write_csv(year_dir / "nav.csv", data.get("nav_series") or [])
    return {
        "year": year,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "symbol_count": len(symbols),
        "total_return": data.get("total_return"),
        "max_drawdown": data.get("max_drawdown"),
        "orders": len(orders),
        "trades": len(trades),
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-id", type=int, default=43)
    parser.add_argument("--index-symbol", default="399101.SZ")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default="2026-05-06")
    parser.add_argument("--initial-capital", type=float, default=1_000_000)
    parser.add_argument("--out", default="backend/app/reports/small_cap_yearly_debug")
    parser.add_argument("--cash-buffer-rate", type=float, default=0.002)
    parser.add_argument("--filter-st", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--v4-indicator-mode", default="robust", choices=["robust", "jq_fallback", "fallback"])
    parser.add_argument("--industry-mode", default="local", choices=["local", "unknown", "jq_unknown"])
    parser.add_argument("--execution-plan-mode", default="timer", choices=["timer", "pre_open"])
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    out_dir = Path(args.out)
    if not out_dir.is_absolute():
        out_dir = PROJECT_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for year, seg_start, seg_end in _year_segments(start, end):
        print(f"running {year}: {seg_start}..{seg_end}", flush=True)
        rows.append(await _run_segment(args, year, seg_start, seg_end, out_dir))
        print(rows[-1], flush=True)
    _write_csv(out_dir / "summary.csv", rows)
    (out_dir / "summary.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {out_dir.resolve()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
