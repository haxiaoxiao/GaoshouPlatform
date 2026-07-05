"""Run Strategy 66 AKQuant backtests on point-in-time index pools."""

from __future__ import annotations

import argparse
import asyncio
import faulthandler
import json
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.backtest.config import BacktestConfig
from app.backtest.engine.akquant.engine import AkquantEngine
from app.backtest.engine.data_provider import StoreDataProvider
from app.core.config import settings
from app.services.index_components import load_index_symbols, normalize_index_symbol


def _db_path() -> Path:
    return Path(settings.data_dir) / "gaoshou.db"


def _load_strategy(strategy_id: int) -> tuple[str, dict[str, Any]]:
    with sqlite3.connect(_db_path()) as conn:
        row = conn.execute(
            "SELECT code, parameters FROM strategies WHERE id = ?",
            (strategy_id,),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"Strategy {strategy_id} not found")
    code = str(row[0] or "")
    params = json.loads(row[1] or "{}")
    if not code.strip():
        raise RuntimeError(f"Strategy {strategy_id} has empty code")
    return code, params


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _summary(result: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "total_return",
        "annual_return",
        "annual_volatility",
        "sharpe",
        "sharpe_ratio",
        "sortino",
        "max_drawdown",
        "calmar",
        "total_trades",
        "win_rate",
        "final_capital",
        "n_trading_days",
        "benchmark_symbol",
    ]
    return {key: result.get(key) for key in keys if key in result}


async def _run_one(
    args: argparse.Namespace,
    index_symbol: str,
    code: str,
    stored_params: dict[str, Any],
    symbols: list[str],
) -> dict[str, Any]:
    params = {
        key: value
        for key, value in stored_params.items()
        if key not in {"backtest_settings", "risk_config"}
    }
    params.update(
        {
            "universe_mode": "index",
            "index_symbol": index_symbol,
            "exclude_smallest_market_cap_n": int(args.exclude_smallest_market_cap_n),
            "backtest_start_date": args.start,
            "backtest_end_date": args.end,
        }
    )
    if args.min_candidates is not None:
        params["min_candidates"] = int(args.min_candidates)
    if args.top_n is not None:
        params["top_n"] = int(args.top_n)
    if args.buy_rank_limit is not None:
        params["buy_rank_limit"] = int(args.buy_rank_limit)
    if args.hold_rank_buffer is not None:
        params["hold_rank_buffer"] = int(args.hold_rank_buffer)
    config = BacktestConfig(
        mode="event_driven",
        engine="akquant",
        strategy_id=args.strategy_id,
        strategy_code=code,
        strategy_params=params,
        symbols=symbols,
        universe_mode="index",
        index_symbol=index_symbol,
        start_date=date.fromisoformat(args.start),
        end_date=date.fromisoformat(args.end),
        initial_capital=float(args.capital),
        bar_type="daily",
        timer_times=list(stored_params.get("timer_times") or ["10:00", "10:30", "14:30"]),
        benchmark_symbol=args.benchmark_symbol or index_symbol,
        commission_rate=float(args.commission_rate),
        slippage=float(args.slippage),
        stamp_tax_rate=float(args.stamp_tax_rate),
        transfer_fee_rate=float(args.transfer_fee_rate),
        min_commission=float(args.min_commission),
        volume_limit_pct=float(args.volume_limit_pct) if args.volume_limit_pct is not None else None,
        lot_size=100,
        t_plus_one=True,
        exit_on_last_bar=True,
        max_positions=int(stored_params.get("top_n") or 30),
        risk_config=dict(stored_params.get("risk_config") or {}),
        warm_start={"mode": "auto", "chunk_days": int(args.warm_start_chunk_days), "keep_checkpoints": False},
    )
    safe_index = index_symbol.replace(".", "_")
    config._task_id = f"strategy66_{safe_index}_{int(time.time())}"

    last_emit = 0.0

    def on_progress(pct: float, live: dict | None) -> None:
        nonlocal last_emit
        now = time.time()
        if now - last_emit < args.progress_interval_seconds and pct < 1.0:
            return
        last_emit = now
        metadata = (live or {}).get("metadata") or {}
        current_date = (live or {}).get("current_date")
        print(
            json.dumps(
                {
                    "event": "progress",
                    "index_symbol": index_symbol,
                    "pct": round(float(pct or 0) * 100, 3),
                    "current_date": current_date,
                    "phase": metadata.get("phase"),
                    "message": metadata.get("progress_message") or metadata.get("message"),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    started = time.time()
    result = await AkquantEngine().run(config, StoreDataProvider(), progress_callback=on_progress)
    result_dict = result.to_dict()
    result_dict["run_meta"] = {
        "strategy_id": args.strategy_id,
        "index_symbol": index_symbol,
        "exclude_smallest_market_cap_n": int(args.exclude_smallest_market_cap_n),
        "min_candidates": int(args.min_candidates) if args.min_candidates is not None else None,
        "top_n": int(args.top_n) if args.top_n is not None else None,
        "start_date": args.start,
        "end_date": args.end,
        "symbol_count": len(symbols),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    return result_dict


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-id", type=int, default=66)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--indices", nargs="+", required=True)
    parser.add_argument("--exclude-smallest-market-cap-n", type=int, default=0)
    parser.add_argument("--min-candidates", type=int, default=None)
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--buy-rank-limit", type=int, default=None)
    parser.add_argument("--hold-rank-buffer", type=int, default=None)
    parser.add_argument("--capital", type=float, default=1_000_000)
    parser.add_argument("--benchmark-symbol", default=None)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage", type=float, default=0.001)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--transfer-fee-rate", type=float, default=0.00001)
    parser.add_argument("--min-commission", type=float, default=5.0)
    parser.add_argument("--volume-limit-pct", type=float, default=0.25)
    parser.add_argument("--warm-start-chunk-days", type=int, default=90)
    parser.add_argument("--progress-interval-seconds", type=int, default=120)
    parser.add_argument("--output-dir", default=str(Path(settings.data_dir) / "results" / "strategy66_index_pools"))
    parser.add_argument("--dump-traceback-seconds", type=int, default=0)
    args = parser.parse_args()

    if args.dump_traceback_seconds > 0:
        faulthandler.enable(file=sys.stderr, all_threads=True)
        faulthandler.dump_traceback_later(
            args.dump_traceback_seconds,
            repeat=True,
            file=sys.stderr,
            exit=False,
        )

    code, stored_params = _load_strategy(args.strategy_id)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, Any]] = []
    for raw_index in args.indices:
        index_symbol = normalize_index_symbol(raw_index) or raw_index
        symbols = await load_index_symbols(
            index_symbol,
            date.fromisoformat(args.start),
            date.fromisoformat(args.end),
        )
        if not symbols:
            raise RuntimeError(f"No symbols resolved for {index_symbol}")

        print(
            json.dumps(
                {
                    "event": "start",
                    "index_symbol": index_symbol,
                    "start": args.start,
                    "end": args.end,
                    "symbols": len(symbols),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        result = await _run_one(args, index_symbol, code, stored_params, symbols)
        safe_index = index_symbol.replace(".", "_")
        out = output_dir / f"strategy66_index_{safe_index}_{args.start}_{args.end}.json"
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        row = {"index_symbol": index_symbol, "path": str(out), **_summary(result), **result.get("run_meta", {})}
        manifest.append(row)
        print(json.dumps({"event": "done", **row}, ensure_ascii=False, default=_json_default), flush=True)

    manifest_path = output_dir / f"strategy66_index_{args.start}_{args.end}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    print(json.dumps({"event": "manifest", "path": str(manifest_path), "runs": manifest}, ensure_ascii=False, default=_json_default), flush=True)


if __name__ == "__main__":
    asyncio.run(_main())
