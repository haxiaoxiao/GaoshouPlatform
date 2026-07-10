"""Import full Tushare report_rc history into the local analyst forecast store."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.data_stores.parquet_store import ParquetMarketDataStore
from app.services.security_symbols import normalize_security_symbol

DEFAULT_SOURCE_DIR = Path("/Users/albert/Downloads/Tushare盈利预测历史数据2010-2026全量")
DEFAULT_DATASET = "analyst_report_forecasts"
DATE_COL = "report_date"
KEY_COLS = ["symbol", "report_date", "org_name", "author_name", "quarter", "title_hash"]
NUMERIC_COLS = ["op_rt", "op_pr", "tp", "np", "eps", "pe", "rd", "roe", "ev_ebitda", "max_price", "min_price"]
STRING_COLS = [
    "ts_code",
    "name",
    "report_title",
    "report_type",
    "classify",
    "org_name",
    "author_name",
    "quarter",
    "rating",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--years", nargs="*", type=int, help="Optional subset of years to import.")
    parser.add_argument("--dry-run", action="store_true", help="Read and validate without writing.")
    parser.add_argument("--report-json", type=Path, help="Optional path for an import summary JSON.")
    return parser.parse_args()


def _title_hash(title: Any) -> str:
    return hashlib.sha1(str(title).strip().encode("utf-8")).hexdigest()[:16]


def _normalize_symbol(symbol: Any) -> str:
    normalized = normalize_security_symbol(str(symbol) if symbol is not None else "")
    if normalized:
        return normalized
    value = str(symbol or "").strip().upper()
    if re.match(r"^\d{6}\.(SH|SZ|BJ)$", value):
        return value
    return ""


def _parse_report_date(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    fallback_mask = parsed.isna()
    if fallback_mask.any():
        parsed.loc[fallback_mask] = pd.to_datetime(text.loc[fallback_mask], errors="coerce")
    return parsed


def _clean_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    raw_rows = len(frame)
    frame = frame.copy()

    for col in STRING_COLS:
        if col not in frame.columns:
            frame[col] = ""
        frame[col] = frame[col].fillna("").astype(str).str.strip()

    for col in NUMERIC_COLS:
        if col not in frame.columns:
            frame[col] = pd.NA
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    if "report_date" not in frame.columns:
        raise KeyError("source data is missing report_date")
    frame["report_date"] = _parse_report_date(frame["report_date"])
    frame["symbol"] = frame["ts_code"].map(_normalize_symbol)
    frame["title_hash"] = frame["report_title"].map(_title_hash)
    frame["source"] = "tushare_report_rc_full_download"
    frame["source_api"] = "report_rc"
    frame["ingested_at"] = pd.Timestamp(datetime.now())

    invalid_date = int(frame["report_date"].isna().sum())
    invalid_symbol = int((frame["symbol"] == "").sum())
    empty_title = int((frame["report_title"] == "").sum())

    frame = frame[(frame["report_date"].notna()) & (frame["symbol"] != "") & (frame["report_title"] != "")].copy()
    frame = frame.drop_duplicates(subset=KEY_COLS, keep="last")
    frame = frame.sort_values(["symbol", "report_date", "org_name", "author_name", "quarter"])

    return frame, {
        "raw_rows": raw_rows,
        "clean_rows": len(frame),
        "dropped_invalid_date": invalid_date,
        "dropped_invalid_symbol": invalid_symbol,
        "dropped_empty_title": empty_title,
        "dropped_duplicate_keys": raw_rows - invalid_date - invalid_symbol - empty_title - len(frame),
    }


def _input_files(source_dir: Path, years: list[int] | None) -> list[Path]:
    files = sorted(source_dir.glob("report_rc_*.parquet"))
    if years:
        selected = {int(year) for year in years}
        files = [
            path
            for path in files
            if (match := re.search(r"report_rc_(\d{4})\.parquet$", path.name)) and int(match.group(1)) in selected
        ]
    return files


def main() -> None:
    args = parse_args()
    if not args.source_dir.exists():
        raise FileNotFoundError(args.source_dir)

    files = _input_files(args.source_dir, args.years)
    if not files:
        raise FileNotFoundError(f"No report_rc_*.parquet files found in {args.source_dir}")

    store = ParquetMarketDataStore()
    summary: dict[str, Any] = {
        "source_dir": str(args.source_dir),
        "dataset": args.dataset,
        "dry_run": bool(args.dry_run),
        "files": [],
        "total_raw_rows": 0,
        "total_clean_rows": 0,
        "total_written_rows": 0,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }

    for path in files:
        frame = pd.read_parquet(path)
        clean, stats = _clean_frame(frame)
        written = 0
        if not args.dry_run and not clean.empty:
            written = store.write_dataset(clean, dataset=args.dataset, date_col=DATE_COL)

        item = {"file": str(path), **stats, "written_rows": int(written)}
        summary["files"].append(item)
        summary["total_raw_rows"] += stats["raw_rows"]
        summary["total_clean_rows"] += stats["clean_rows"]
        summary["total_written_rows"] += int(written)
        print(json.dumps(item, ensure_ascii=False))

    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
