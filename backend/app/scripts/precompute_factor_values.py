"""Precompute factor research values into Parquet factor_values."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.factor_precompute import (
    precompute_high_volume_features,
    precompute_small_cap_core_features,
)


def _symbols(text: str | None) -> list[str] | None:
    if not text:
        return None
    return [item.strip().upper() for item in text.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Precompute generic factor values.")
    parser.add_argument("--factor", default="high_volume_signal")
    parser.add_argument("--group", choices=["small_cap_v4_core"])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbols")
    parser.add_argument("--index-symbol")
    parser.add_argument("--time", default="14:30")
    parser.add_argument("--window", type=int, default=120)
    parser.add_argument("--threshold", type=float, default=0.9)
    parser.add_argument("--daily-volume-to-share-multiplier", type=float, default=100.0)
    args = parser.parse_args()

    if args.group == "small_cap_v4_core":
        result = precompute_small_cap_core_features(
            start_date=args.start,
            end_date=args.end,
            symbols=_symbols(args.symbols),
            index_symbol=args.index_symbol,
            timer_time=args.time if args.time != "14:30" else "10:30",
            include_high_volume=True,
        )
        print(result)
        return

    supported = {
        "cum_volume_at_time",
        "rolling_max_volume",
        "high_volume_ratio",
        "high_volume_signal",
        "market_cap",
        "market_cap_rank",
        "is_st",
        "is_paused",
        "is_limit_up",
        "is_limit_down",
        "yesterday_limit_up",
    }
    if args.factor not in supported:
        raise SystemExit(f"Unsupported factor: {args.factor}. Supported: {sorted(supported)}")

    if args.factor in {"cum_volume_at_time", "rolling_max_volume", "high_volume_ratio", "high_volume_signal"}:
        result = precompute_high_volume_features(
            start_date=args.start,
            end_date=args.end,
            symbols=_symbols(args.symbols),
            index_symbol=args.index_symbol,
            as_of_time=args.time,
            window=args.window,
            threshold=args.threshold,
            daily_volume_to_share_multiplier=args.daily_volume_to_share_multiplier,
        )
    else:
        result = precompute_small_cap_core_features(
            start_date=args.start,
            end_date=args.end,
            symbols=_symbols(args.symbols),
            index_symbol=args.index_symbol,
            timer_time=args.time if args.time != "14:30" else "10:30",
            include_high_volume=False,
        )
    print(result)


if __name__ == "__main__":
    main()
