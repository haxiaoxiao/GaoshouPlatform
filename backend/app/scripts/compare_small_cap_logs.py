"""Compare JoinQuant and local AKQuant small-cap logs.

The JoinQuant export is usually GB18030 text and may be wrapped in a zip file.
The local AKQuant strategy log contains bare strategy lines such as
``[2020-02-04 10:30:00] 今日选股...``.  This script normalizes both formats and
emits a daily mismatch CSV plus optional asset and summary files.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import zipfile
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[3]

SYMBOL_RE = re.compile(r"\b\d{6}\.(?:XSHE|XSHG|SZ|SH)\b")
LINE_PATTERNS = [
    re.compile(
        r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) - "
        r"(?:INFO|WARNING|ERROR|DEBUG)\s+-\s+(.*)$"
    ),
    re.compile(
        r"^.*?\|\s*(?:INFO|WARNING|ERROR|DEBUG)\s*\|\s*"
        r"\[(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\]\s+(.*)$"
    ),
    re.compile(r"^\[(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\]\s+(.*)$"),
]

ASSET_RE = re.compile(r"总资产:\s*([0-9.]+)")
HOLD_COUNT_RE = re.compile(r"持仓数量:\s*(\d+)")
POSITION_RE = re.compile(
    r"(\d{6}\.(?:XSHE|XSHG|SZ|SH))\s*\|\s*"
    r"成本[:：]?\s*([0-9.]+)\s*\|\s*现价[:：]?\s*([0-9.]+)"
)
SELECT_RE = re.compile(r"今日选股\((\d+)只\):")
BUY_RE = re.compile(r"买入\[?(\d{6}\.(?:XSHE|XSHG|SZ|SH))\]?\s*([0-9.]+)?")
SELL_PATTERNS = [
    ("limitup_open", re.compile(r"\[(\d{6}\.(?:XSHE|XSHG|SZ|SH))\]涨停打开，卖出")),
    ("v4gv_dead_cross", re.compile(r"死叉信号.*?卖出\[?(\d{6}\.(?:XSHE|XSHG|SZ|SH))\]?")),
    ("stoploss", re.compile(r"动态止损,卖出\[?(\d{6}\.(?:XSHE|XSHG|SZ|SH))\]?")),
    ("high_volume", re.compile(r"\[(\d{6}\.(?:XSHE|XSHG|SZ|SH))\]放量.*?卖出")),
    ("special_month", re.compile(r"(?:清仓卖出|卖出)\[?(\d{6}\.(?:XSHE|XSHG|SZ|SH))\]?")),
]


def _resolve(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _decode(data: bytes, encoding: str | None = None) -> str:
    if encoding:
        return data.decode(encoding, errors="replace")
    for candidate in ("utf-8-sig", "gb18030", "gbk"):
        try:
            return data.decode(candidate)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _read_text(path: Path, encoding: str | None = None, zip_member: str | None = None) -> str:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            member = zip_member
            if member is None:
                names = [name for name in archive.namelist() if not name.endswith("/")]
                if not names:
                    return ""
                member = names[0]
            return _decode(archive.read(member), encoding)
    return _decode(path.read_bytes(), encoding)


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace(".XSHE", ".SZ").replace(".XSHG", ".SH")


def _match_line(line: str) -> tuple[str, str, str] | None:
    for pattern in LINE_PATTERNS:
        matched = pattern.match(line)
        if matched:
            return matched.groups()
    return None


def _empty_day() -> dict[str, Any]:
    return {
        "asset": None,
        "hold_count": None,
        "positions": {},
        "select": [],
        "buys": [],
        "sells": [],
        "sell_reasons": [],
    }


def parse_log(
    path: Path,
    encoding: str | None = None,
    start: str | None = None,
    end: str | None = None,
    zip_member: str | None = None,
) -> dict[str, dict[str, Any]]:
    daily: dict[str, dict[str, Any]] = {}
    text = _read_text(path, encoding=encoding, zip_member=zip_member)
    for line in text.splitlines():
        matched = _match_line(line)
        if not matched:
            continue
        day, _tm, message = matched
        if start and day < start:
            continue
        if end and day > end:
            continue
        day_events = daily.setdefault(day, _empty_day())

        if asset_match := ASSET_RE.search(message):
            day_events["asset"] = float(asset_match.group(1))
        if hold_match := HOLD_COUNT_RE.search(message):
            day_events["hold_count"] = int(hold_match.group(1))
        if position_match := POSITION_RE.search(message):
            symbol = _normalize_symbol(position_match.group(1))
            day_events["positions"][symbol] = (
                float(position_match.group(2)),
                float(position_match.group(3)),
            )
        if SELECT_RE.search(message):
            day_events["select"].extend(_normalize_symbol(symbol) for symbol in SYMBOL_RE.findall(message))
        if buy_match := BUY_RE.search(message):
            day_events["buys"].append(_normalize_symbol(buy_match.group(1)))

        for reason, pattern in SELL_PATTERNS:
            sell_match = pattern.search(message)
            if sell_match:
                symbol = _normalize_symbol(sell_match.group(1))
                day_events["sells"].append(symbol)
                day_events["sell_reasons"].append(f"{symbol}:{reason}")
                break
    return daily


def _position_symbols(value: Any) -> list[str]:
    if isinstance(value, dict):
        return sorted(value)
    return list(value or [])


def _join_values(value: Any) -> str:
    if isinstance(value, dict):
        return ",".join(sorted(value))
    return ",".join(str(item) for item in (value or []))


def _ordered_or_sorted(key: str, value: Any) -> Any:
    """Normalize comparison values while preserving order where it matters."""
    if key == "positions":
        return _position_symbols(value)
    if key in {"sells", "sell_reasons"}:
        return sorted(value or [])
    return value or []


def compare_daily(
    jq_daily: dict[str, dict[str, Any]],
    ak_daily: dict[str, dict[str, Any]],
    asset_tolerance: float = 1.0,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for day in sorted(set(jq_daily) | set(ak_daily)):
        jq_day = jq_daily.get(day, _empty_day())
        ak_day = ak_daily.get(day, _empty_day())
        mismatch = []
        for key in ("select", "buys", "sells", "sell_reasons", "positions", "hold_count"):
            jq_value = _ordered_or_sorted(key, jq_day.get(key))
            ak_value = _ordered_or_sorted(key, ak_day.get(key))
            if (jq_value or []) != (ak_value or []):
                mismatch.append(key)

        jq_asset = jq_day.get("asset")
        ak_asset = ak_day.get("asset")
        diff = diff_pct = None
        if jq_asset is not None and ak_asset is not None:
            diff = float(ak_asset) - float(jq_asset)
            diff_pct = diff / float(jq_asset) * 100 if jq_asset else None
            if abs(diff) > asset_tolerance:
                mismatch.append("asset")
        elif jq_asset is not None or ak_asset is not None:
            mismatch.append("asset_missing")

        if not mismatch:
            continue
        rows.append(
            {
                "date": day,
                "mismatch": ";".join(mismatch),
                "jq_asset": "" if jq_asset is None else f"{float(jq_asset):.2f}",
                "ak_asset": "" if ak_asset is None else f"{float(ak_asset):.2f}",
                "asset_diff": "" if diff is None else f"{diff:.2f}",
                "asset_diff_pct": "" if diff_pct is None else f"{diff_pct:.2f}",
                "jq_select": _join_values(jq_day.get("select")),
                "ak_select": _join_values(ak_day.get("select")),
                "jq_buys": _join_values(jq_day.get("buys")),
                "ak_buys": _join_values(ak_day.get("buys")),
                "jq_sells": _join_values(jq_day.get("sell_reasons")),
                "ak_sells": _join_values(ak_day.get("sell_reasons")),
                "jq_positions": _join_values(jq_day.get("positions")),
                "ak_positions": _join_values(ak_day.get("positions")),
            }
        )
    return rows


def asset_rows(
    jq_daily: dict[str, dict[str, Any]],
    ak_daily: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    rows = []
    for day in sorted(set(jq_daily) & set(ak_daily)):
        jq_asset = jq_daily[day].get("asset")
        ak_asset = ak_daily[day].get("asset")
        if jq_asset is None or ak_asset is None:
            continue
        diff = float(ak_asset) - float(jq_asset)
        diff_pct = diff / float(jq_asset) * 100 if jq_asset else 0.0
        rows.append(
            {
                "date": day,
                "jq_asset": f"{float(jq_asset):.2f}",
                "ak_asset": f"{float(ak_asset):.2f}",
                "diff": f"{diff:.2f}",
                "diff_pct": f"{diff_pct:.2f}",
            }
        )
    return rows


def build_summary(
    jq_daily: dict[str, dict[str, Any]],
    ak_daily: dict[str, dict[str, Any]],
    daily_rows: list[dict[str, str]],
    assets: list[dict[str, str]],
) -> dict[str, Any]:
    selection_mismatch = None
    for day in sorted(set(jq_daily) & set(ak_daily)):
        jq_select = jq_daily[day].get("select") or []
        ak_select = ak_daily[day].get("select") or []
        if jq_select or ak_select:
            if jq_select != ak_select:
                selection_mismatch = {"date": day, "jq": jq_select, "ak": ak_select}
                break

    max_abs_asset = None
    if assets:
        max_abs_asset = max(assets, key=lambda row: abs(float(row["diff"])))

    top_asset_diff_pct = sorted(
        assets,
        key=lambda row: abs(float(row["diff_pct"])),
        reverse=True,
    )[:10]

    return {
        "jq_days": len(jq_daily),
        "ak_days": len(ak_daily),
        "mismatch_days": len(daily_rows),
        "jq_select_dates": sum(1 for value in jq_daily.values() if value["select"]),
        "ak_select_dates": sum(1 for value in ak_daily.values() if value["select"]),
        "jq_buy_count": sum(len(value["buys"]) for value in jq_daily.values()),
        "ak_buy_count": sum(len(value["buys"]) for value in ak_daily.values()),
        "jq_sell_count": sum(len(value["sells"]) for value in jq_daily.values()),
        "ak_sell_count": sum(len(value["sells"]) for value in ak_daily.values()),
        "common_asset_points": len(assets),
        "first_mismatch": daily_rows[0] if daily_rows else None,
        "first_selection_mismatch": selection_mismatch,
        "last_common_asset": assets[-1] if assets else None,
        "max_abs_asset_diff": max_abs_asset,
        "top_asset_diff_pct": top_asset_diff_pct,
    }


def write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jq-log", default="backend/app/reports/jq_log/log.txt")
    parser.add_argument("--jq-encoding", default="gb18030")
    parser.add_argument("--jq-zip-member", default=None)
    parser.add_argument("--ak-log", required=True)
    parser.add_argument("--ak-encoding", default=None)
    parser.add_argument("--out", default="backend/app/reports/small_cap_yearly_debug/daily_event_compare.csv")
    parser.add_argument("--asset-out", default=None)
    parser.add_argument("--summary-out", default=None)
    parser.add_argument("--asset-tolerance", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    jq_log = _resolve(args.jq_log)
    ak_log = _resolve(args.ak_log)
    out_path = _resolve(args.out)
    asset_path = _resolve(args.asset_out) if args.asset_out else out_path.with_name("asset_compare.csv")
    summary_path = _resolve(args.summary_out) if args.summary_out else out_path.with_name("summary.json")

    jq_daily = parse_log(
        jq_log,
        encoding=args.jq_encoding,
        start=args.start,
        end=args.end,
        zip_member=args.jq_zip_member,
    )
    ak_daily = parse_log(ak_log, encoding=args.ak_encoding, start=args.start, end=args.end)
    daily_rows = compare_daily(jq_daily, ak_daily, asset_tolerance=args.asset_tolerance)
    assets = asset_rows(jq_daily, ak_daily)
    summary = build_summary(jq_daily, ak_daily, daily_rows, assets)

    daily_fields = [
        "date",
        "mismatch",
        "jq_asset",
        "ak_asset",
        "asset_diff",
        "asset_diff_pct",
        "jq_select",
        "ak_select",
        "jq_buys",
        "ak_buys",
        "jq_sells",
        "ak_sells",
        "jq_positions",
        "ak_positions",
    ]
    asset_fields = ["date", "jq_asset", "ak_asset", "diff", "diff_pct"]
    write_csv(out_path, daily_rows, daily_fields)
    write_csv(asset_path, assets, asset_fields)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for row in daily_rows[: args.limit]:
        print(row)
    print(f"wrote {out_path.resolve()} rows={len(daily_rows)}")
    print(f"wrote {asset_path.resolve()} rows={len(assets)}")
    print(f"wrote {summary_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
