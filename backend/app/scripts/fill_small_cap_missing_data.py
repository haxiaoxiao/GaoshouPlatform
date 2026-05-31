"""Fill missing daily data needed by the small-cap backtest.

The script is intentionally idempotent:
- existing valid ClickHouse daily bars are kept;
- only missing dates for each symbol are inserted;
- stock metadata is inserted or patched with better non-null values.

Data source priority:
1. miniQMT / xtquant via qmt_gateway
2. Tushare, when TUSHARE_TOKEN or TS_TOKEN is configured
3. AKShare Eastmoney daily history
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Any
from zipfile import ZipFile

import pandas as pd

from app.core.config import settings
from app.db.clickhouse import get_ch_client
from app.engines.qmt_gateway import qmt_gateway

JQ_SYMBOL_RE = re.compile(r"\b([036]\d{5})\.(XSHG|XSHE|SH|SZ)\b")
DEFAULT_JQ_SOURCE = Path(r"C:\Users\Albert\Downloads\jq源码.py")
DEFAULT_JQ_LOG_ZIP = Path(r"C:\Users\Albert\Downloads\log.zip")


@dataclass
class DailyBar:
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    turnover_rate: float | None = None


@dataclass
class StockMeta:
    symbol: str
    name: str
    exchange: str
    industry: str | None = None
    sector: str | None = None
    list_date: date | None = None
    is_st: int = 0
    is_delist: int = 0
    is_suspend: int = 0
    total_shares: float | None = None
    float_shares: float | None = None
    total_mv: float | None = None
    circ_mv: float | None = None
    security_type: str = "stock"
    product_class: str = "stock"
    raw_source: str | None = None


def ensure_reference_tables(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_name_changes (
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE,
                change_reason TEXT,
                source TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, name, start_date)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stock_name_changes_lookup
            ON stock_name_changes(symbol, start_date, end_date)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_daily_basic (
                symbol TEXT NOT NULL,
                trade_date DATE NOT NULL,
                total_share REAL,
                float_share REAL,
                total_mv REAL,
                circ_mv REAL,
                turnover_rate REAL,
                pe_ttm REAL,
                pb REAL,
                source TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stock_daily_basic_lookup
            ON stock_daily_basic(symbol, trade_date)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_limit_prices (
                symbol TEXT NOT NULL,
                trade_date DATE NOT NULL,
                up_limit REAL,
                down_limit REAL,
                source TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stock_limit_prices_lookup
            ON stock_limit_prices(symbol, trade_date)
            """
        )
        conn.commit()


def _parse_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if symbol.endswith(".XSHG"):
        return symbol.replace(".XSHG", ".SH")
    if symbol.endswith(".XSHE"):
        return symbol.replace(".XSHE", ".SZ")
    return symbol


def data_symbol(symbol: str) -> str:
    """Map platform aliases to actual ClickHouse symbols."""
    return "399101.XSHE" if symbol == "399101.SZ" else symbol


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8", "gb18030", "gbk"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def symbols_from_jq_artifacts(source_path: Path, log_zip_path: Path) -> set[str]:
    texts: list[str] = []
    if source_path.exists():
        texts.append(_read_text(source_path))
    if log_zip_path.exists():
        with ZipFile(log_zip_path) as zf:
            for name in zf.namelist():
                if name.endswith("/"):
                    continue
                raw = zf.read(name)
                for encoding in ("gb18030", "utf-8", "gbk"):
                    try:
                        texts.append(raw.decode(encoding))
                        break
                    except UnicodeDecodeError:
                        continue

    symbols: set[str] = set()
    for text in texts:
        for code, suffix in JQ_SYMBOL_RE.findall(text):
            if suffix in {"XSHG", "SH"}:
                symbols.add(f"{code}.SH")
            else:
                symbols.add(f"{code}.SZ")
    return symbols


def watchlist_symbols(db_path: Path, group_id: int | None) -> set[str]:
    if group_id is None:
        return set()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT symbol FROM watchlist_stocks WHERE group_id = ?",
            (group_id,),
        ).fetchall()
    return {str(r[0]).strip().upper() for r in rows if r[0]}


def index_component_symbols(db_path: Path, index_symbol: str | None, start: date, end: date) -> set[str]:
    if not index_symbol:
        return set()
    normalized = normalize_symbol(index_symbol)
    jq_symbol = normalized.replace(".SH", ".XSHG").replace(".SZ", ".XSHE")
    with sqlite3.connect(db_path) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='index_components'"
        ).fetchone()
        if not exists:
            return set()
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM index_components
            WHERE (index_symbol IN (?, ?) OR jq_index_symbol IN (?, ?))
              AND trade_date >= COALESCE((
                  SELECT MAX(trade_date)
                  FROM index_components
                  WHERE (index_symbol IN (?, ?) OR jq_index_symbol IN (?, ?))
                    AND trade_date <= ?
              ), ?)
              AND trade_date <= ?
            """,
            (
                normalized,
                jq_symbol,
                normalized,
                jq_symbol,
                normalized,
                jq_symbol,
                normalized,
                jq_symbol,
                start.isoformat(),
                start.isoformat(),
                end.isoformat(),
            ),
        ).fetchall()
    return {str(r[0]).strip().upper() for r in rows if r[0]}


def existing_stock_symbols(db_path: Path, symbols: set[str]) -> set[str]:
    if not symbols:
        return set()
    placeholders = ",".join("?" for _ in symbols)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT symbol FROM stocks WHERE symbol IN ({placeholders})",
            tuple(sorted(symbols)),
        ).fetchall()
    return {r[0] for r in rows}


def existing_valid_dates(symbol: str, start: date, end: date) -> set[date]:
    query_symbol = data_symbol(symbol)
    ch = get_ch_client()
    rows = ch.execute(
        """
        SELECT trade_date
        FROM klines_daily
        WHERE symbol = %(symbol)s
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
          AND open > 0
          AND high > 0
          AND low > 0
          AND close > 0
        """,
        {"symbol": query_symbol, "start": start, "end": end},
    )
    return {r[0] for r in rows}


def count_valid_bars(symbol: str, start: date, end: date) -> tuple[int, date | None, date | None]:
    ch = get_ch_client()
    rows = ch.execute(
        """
        SELECT count(), min(trade_date), max(trade_date)
        FROM klines_daily
        WHERE symbol = %(symbol)s
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
          AND open > 0
          AND high > 0
          AND low > 0
          AND close > 0
        """,
        {"symbol": data_symbol(symbol), "start": start, "end": end},
    )
    count, min_date, max_date = rows[0]
    return int(count), min_date if count else None, max_date if count else None


def _valid_bar(bar: DailyBar) -> bool:
    values = [bar.open, bar.high, bar.low, bar.close]
    return all(pd.notna(v) and float(v) > 0 for v in values)


async def fetch_qmt_bars(symbol: str, start: date, end: date) -> list[DailyBar]:
    rows = await qmt_gateway.get_kline_daily(data_symbol(symbol), start, end)
    bars: list[DailyBar] = []
    for row in rows:
        d = row.datetime if isinstance(row.datetime, date) else row.datetime.date()
        bar = DailyBar(
            symbol=data_symbol(symbol),
            trade_date=d,
            open=float(row.open or 0),
            high=float(row.high or 0),
            low=float(row.low or 0),
            close=float(row.close or 0),
            volume=float(row.volume or 0),
            amount=float(row.amount or 0),
            turnover_rate=float(row.turnover) if row.turnover is not None else None,
        )
        if _valid_bar(bar):
            bars.append(bar)
    return bars


def fetch_tushare_bars(symbol: str, start: date, end: date) -> list[DailyBar]:
    import tushare as ts

    token = os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or ts.get_token()
    if not token:
        return []

    ts.set_token(token)
    pro = ts.pro_api()
    api = pro.index_daily if symbol in {"000001.SH", "399101.SZ", "399101.XSHE"} else pro.daily
    ts_code = "399101.SZ" if symbol == "399101.XSHE" else symbol
    df = api(ts_code=ts_code, start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))
    if df is None or df.empty:
        return []
    bars: list[DailyBar] = []
    for _, row in df.iterrows():
        bar = DailyBar(
            symbol=data_symbol(symbol),
            trade_date=_parse_yyyymmdd(str(row["trade_date"])),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row.get("vol", 0) or 0),
            amount=float(row.get("amount", 0) or 0) * 1000,
        )
        if _valid_bar(bar):
            bars.append(bar)
    return sorted(bars, key=lambda b: b.trade_date)


def fetch_akshare_bars(symbol: str, start: date, end: date) -> list[DailyBar]:
    if symbol.startswith("399101"):
        return []
    if symbol == "000001.SH":
        return []

    import akshare as ak

    code = symbol.split(".")[0]
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
        adjust="",
    )
    if df is None or df.empty:
        return []

    bars: list[DailyBar] = []
    for _, row in df.iterrows():
        bar = DailyBar(
            symbol=symbol,
            trade_date=row["日期"] if isinstance(row["日期"], date) else pd.to_datetime(row["日期"]).date(),
            open=float(row["开盘"]),
            high=float(row["最高"]),
            low=float(row["最低"]),
            close=float(row["收盘"]),
            volume=float(row.get("成交量", 0) or 0),
            amount=float(row.get("成交额", 0) or 0),
            turnover_rate=float(row["换手率"]) if pd.notna(row.get("换手率")) else None,
        )
        if _valid_bar(bar):
            bars.append(bar)
    return bars


async def fetch_qmt_meta(symbol: str) -> StockMeta | None:
    info = await qmt_gateway.get_stock_full_info(data_symbol(symbol))
    if not info:
        return None
    name = info.name or symbol
    exchange = info.exchange or symbol.split(".")[-1]
    return StockMeta(
        symbol=data_symbol(symbol),
        name=name,
        exchange=exchange,
        industry=info.industry,
        sector=info.sector,
        list_date=info.list_date,
        is_st=int(info.is_st or 0),
        is_delist=int(info.is_delist or 0),
        is_suspend=int(info.is_suspend or 0),
        total_shares=float(info.total_shares) if info.total_shares else None,
        float_shares=float(info.float_shares) if info.float_shares else None,
        total_mv=float(info.total_mv) / 10000 if info.total_mv and info.total_mv > 1_000_000_000 else info.total_mv,
        circ_mv=float(info.circ_mv) / 10000 if info.circ_mv and info.circ_mv > 1_000_000_000 else info.circ_mv,
        security_type="index" if symbol.startswith(("000001.SH", "399101")) else (info.security_type or "stock"),
        product_class=info.product_class or ("index" if symbol.startswith(("000001.SH", "399101")) else "stock"),
        raw_source="qmt",
    )


def _num_or_none(value: Any) -> float | None:
    if value in {None, "", "-"}:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if pd.notna(v) else None


def fetch_tushare_meta(symbol: str) -> StockMeta | None:
    import tushare as ts

    token = os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or ts.get_token()
    if not token:
        return None
    ts.set_token(token)
    pro = ts.pro_api()
    frames = []
    for status in ("L", "D", "P"):
        try:
            df = pro.stock_basic(
                ts_code=symbol,
                list_status=status,
                fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs",
            )
        except Exception:
            continue
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    row = df.iloc[0]
    list_date = None
    if pd.notna(row.get("list_date")):
        list_text = str(row.get("list_date"))
        if len(list_text) >= 8:
            list_date = _parse_yyyymmdd(list_text[:8])
    name = str(row.get("name") or symbol).replace("(退)", "")
    market = str(row.get("market") or "")
    total_shares = float_shares = total_mv = circ_mv = None
    for trade_date in ("20200204", "20210104", datetime.now().strftime("%Y%m%d")):
        try:
            basic = pro.daily_basic(
                ts_code=symbol,
                trade_date=trade_date,
                fields="ts_code,trade_date,total_share,float_share,total_mv,circ_mv",
            )
        except Exception:
            basic = None
        if basic is not None and not basic.empty:
            b = basic.iloc[0]
            total_shares = _num_or_none(b.get("total_share"))
            float_shares = _num_or_none(b.get("float_share"))
            total_mv = _num_or_none(b.get("total_mv"))
            circ_mv = _num_or_none(b.get("circ_mv"))
            break
    return StockMeta(
        symbol=symbol,
        name=name[:-1] if name.endswith("退") and len(name) > 1 else name,
        exchange=symbol.split(".")[-1],
        industry=str(row.get("industry") or "") or None,
        sector=market or ("深市主板" if symbol.endswith(".SZ") else "沪市主板"),
        list_date=list_date,
        is_st=1 if "ST" in name.upper() else 0,
        is_delist=0,
        is_suspend=0,
        total_shares=total_shares,
        float_shares=float_shares,
        total_mv=total_mv,
        circ_mv=circ_mv,
        security_type="stock",
        product_class="stock",
        raw_source="tushare.stock_basic",
    )


def _tushare_client():
    import tushare as ts

    token = os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or ts.get_token()
    if not token:
        return None
    ts.set_token(token)
    return ts.pro_api()


def sync_tushare_name_changes(db_path: Path, symbols: set[str], pause: float = 0.35) -> int:
    pro = _tushare_client()
    if pro is None or not symbols:
        return 0
    ensure_reference_tables(db_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[tuple[Any, ...]] = []
    with sqlite3.connect(db_path) as conn:
        known = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT symbol FROM stock_name_changes WHERE symbol IN ({})".format(",".join("?" for _ in symbols)),
                tuple(sorted(symbols)),
            ).fetchall()
        } if symbols else set()
    for symbol in sorted(symbols - known):
        df = None
        for _attempt in range(3):
            try:
                df = pro.namechange(
                    ts_code=symbol,
                    fields="ts_code,name,start_date,end_date,change_reason",
                )
                break
            except Exception as exc:
                text = str(exc)
                print(f"[namechange] {symbol}: {text}", flush=True)
                if "频率超限" in text or "rate" in text.lower():
                    time.sleep(65)
                    continue
                break
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            start_date = str(row.get("start_date") or "")[:8]
            if len(start_date) != 8:
                continue
            end_value = row.get("end_date")
            end_date = str(end_value)[:8] if pd.notna(end_value) and str(end_value) != "None" else None
            rows.append(
                (
                    symbol,
                    str(row.get("name") or ""),
                    _parse_yyyymmdd(start_date).isoformat(),
                    _parse_yyyymmdd(end_date).isoformat() if end_date and len(end_date) == 8 else None,
                    str(row.get("change_reason") or ""),
                    "tushare.namechange",
                    now,
                    now,
                )
            )
        if pause > 0:
            time.sleep(pause)
    if not rows:
        return 0
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO stock_name_changes (
                symbol, name, start_date, end_date, change_reason,
                source, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, name, start_date) DO UPDATE SET
                end_date = excluded.end_date,
                change_reason = excluded.change_reason,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def sync_tushare_daily_basic(
    db_path: Path,
    symbols: set[str],
    start: date,
    end: date,
    pause: float = 0.12,
) -> int:
    pro = _tushare_client()
    if pro is None or not symbols:
        return 0
    ensure_reference_tables(db_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_inserted = 0
    current = start
    while current <= end:
        if current.weekday() >= 5:
            current = date.fromordinal(current.toordinal() + 1)
            continue
        with sqlite3.connect(db_path) as conn:
            cached = conn.execute(
                "SELECT count(*) FROM stock_daily_basic WHERE trade_date = ?",
                (current.isoformat(),),
            ).fetchone()[0]
        if cached >= max(1, int(len(symbols) * 0.6)):
            current = date.fromordinal(current.toordinal() + 1)
            continue
        trade_text = current.strftime("%Y%m%d")
        try:
            df = pro.daily_basic(
                trade_date=trade_text,
                fields=(
                    "ts_code,trade_date,total_share,float_share,total_mv,circ_mv,"
                    "turnover_rate,pe_ttm,pb"
                ),
            )
        except Exception as exc:
            print(f"[daily_basic] {trade_text}: {exc}")
            current = date.fromordinal(current.toordinal() + 1)
            continue
        rows: list[tuple[Any, ...]] = []
        if df is not None and not df.empty:
            df = df[df["ts_code"].isin(symbols)]
            for _, row in df.iterrows():
                rows.append(
                    (
                        str(row.get("ts_code")),
                        _parse_yyyymmdd(str(row.get("trade_date"))[:8]).isoformat(),
                        _num_or_none(row.get("total_share")),
                        _num_or_none(row.get("float_share")),
                        _num_or_none(row.get("total_mv")),
                        _num_or_none(row.get("circ_mv")),
                        _num_or_none(row.get("turnover_rate")),
                        _num_or_none(row.get("pe_ttm")),
                        _num_or_none(row.get("pb")),
                        "tushare.daily_basic",
                        now,
                        now,
                    )
                )
        if rows:
            with sqlite3.connect(db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO stock_daily_basic (
                        symbol, trade_date, total_share, float_share, total_mv, circ_mv,
                        turnover_rate, pe_ttm, pb, source, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, trade_date) DO UPDATE SET
                        total_share = COALESCE(excluded.total_share, stock_daily_basic.total_share),
                        float_share = COALESCE(excluded.float_share, stock_daily_basic.float_share),
                        total_mv = COALESCE(excluded.total_mv, stock_daily_basic.total_mv),
                        circ_mv = COALESCE(excluded.circ_mv, stock_daily_basic.circ_mv),
                        turnover_rate = COALESCE(excluded.turnover_rate, stock_daily_basic.turnover_rate),
                        pe_ttm = COALESCE(excluded.pe_ttm, stock_daily_basic.pe_ttm),
                        pb = COALESCE(excluded.pb, stock_daily_basic.pb),
                        source = excluded.source,
                        updated_at = excluded.updated_at
                    """,
                    rows,
                )
                conn.commit()
            total_inserted += len(rows)
        if rows or current.day == 1:
            print(f"[daily_basic] {trade_text} rows={len(rows)} total={total_inserted}", flush=True)
        if pause > 0:
            time.sleep(pause)
        current = date.fromordinal(current.toordinal() + 1)
    return total_inserted


def _shares_to_10k(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 10000 if value > 10_000_000 else value


def fetch_akshare_meta(symbol: str, bars: list[DailyBar]) -> StockMeta | None:
    if symbol == "000001.SH":
        return StockMeta(
            symbol=symbol,
            name="上证指数",
            exchange="SH",
            sector="指数",
            list_date=date(1990, 12, 19),
            security_type="index",
            product_class="index",
            raw_source="static_index",
        )
    if symbol.startswith("399101"):
        return StockMeta(
            symbol=data_symbol(symbol),
            name="中小综指",
            exchange="SZ",
            sector="指数",
            list_date=date(2005, 6, 8),
            security_type="index",
            product_class="index",
            raw_source="static_index",
        )

    import akshare as ak

    code = symbol.split(".")[0]
    info: dict[str, Any] = {}
    try:
        df = ak.stock_individual_info_em(symbol=code)
        if df is not None and not df.empty:
            info = dict(zip(df["item"], df["value"], strict=False))
    except Exception:
        info = {}

    raw_name = str(info.get("股票简称") or symbol)
    historical_name = raw_name[:-1] if raw_name.endswith("退") and len(raw_name) > 1 else raw_name
    total_shares = _shares_to_10k(_num_or_none(info.get("总股本")))
    float_shares = _shares_to_10k(_num_or_none(info.get("流通股")))

    if float_shares is None:
        estimates = []
        for bar in bars:
            if bar.turnover_rate and bar.turnover_rate > 0 and bar.volume > 0:
                traded_shares = bar.volume * 100
                estimates.append(traded_shares / (bar.turnover_rate / 100) / 10000)
        if estimates:
            float_shares = median(estimates)
    if total_shares is None:
        total_shares = float_shares

    last_close = next((b.close for b in reversed(bars) if b.close > 0), None)
    total_mv = total_shares * last_close if total_shares and last_close else None
    circ_mv = float_shares * last_close if float_shares and last_close else None
    list_date = min((b.trade_date for b in bars), default=None)

    return StockMeta(
        symbol=symbol,
        name=historical_name,
        exchange=symbol.split(".")[-1],
        industry=str(info.get("行业") or "") or None,
        sector="深市主板" if symbol.endswith(".SZ") and code.startswith("0") else "沪市主板",
        list_date=list_date,
        is_st=1 if "ST" in historical_name.upper() else 0,
        is_delist=0,
        is_suspend=0,
        total_shares=total_shares,
        float_shares=float_shares,
        total_mv=total_mv,
        circ_mv=circ_mv,
        security_type="stock",
        product_class="stock",
        raw_source=json.dumps({"source": "akshare", "current_name": raw_name}, ensure_ascii=False),
    )


def upsert_stock_meta(db_path: Path, meta: StockMeta) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO stocks (
                symbol, name, exchange, industry, sector, list_date,
                is_st, is_delist, is_suspend,
                total_shares, float_shares, total_mv, circ_mv,
                security_type, product_class, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = CASE
                    WHEN excluded.name IS NOT NULL AND excluded.name != excluded.symbol THEN excluded.name
                    WHEN stocks.name IS NULL OR stocks.name = stocks.symbol THEN excluded.name
                    ELSE stocks.name
                END,
                exchange = COALESCE(excluded.exchange, stocks.exchange),
                industry = COALESCE(NULLIF(excluded.industry, ''), stocks.industry),
                sector = COALESCE(excluded.sector, stocks.sector),
                list_date = COALESCE(excluded.list_date, stocks.list_date),
                is_st = COALESCE(excluded.is_st, stocks.is_st),
                is_delist = COALESCE(excluded.is_delist, stocks.is_delist),
                is_suspend = COALESCE(excluded.is_suspend, stocks.is_suspend),
                total_shares = COALESCE(excluded.total_shares, stocks.total_shares),
                float_shares = COALESCE(excluded.float_shares, stocks.float_shares),
                total_mv = COALESCE(excluded.total_mv, stocks.total_mv),
                circ_mv = COALESCE(excluded.circ_mv, stocks.circ_mv),
                security_type = COALESCE(excluded.security_type, stocks.security_type),
                product_class = COALESCE(excluded.product_class, stocks.product_class),
                updated_at = excluded.updated_at
            """,
            (
                meta.symbol,
                meta.name,
                meta.exchange,
                meta.industry,
                meta.sector,
                meta.list_date.isoformat() if meta.list_date else None,
                meta.is_st,
                meta.is_delist,
                meta.is_suspend,
                meta.total_shares,
                meta.float_shares,
                meta.total_mv,
                meta.circ_mv,
                meta.security_type,
                meta.product_class,
                now,
                now,
            ),
        )
        conn.commit()


def insert_missing_bars(symbol: str, bars: list[DailyBar], start: date, end: date) -> int:
    if not bars:
        return 0
    existing = existing_valid_dates(symbol, start, end)
    rows = [
        {
            "symbol": data_symbol(bar.symbol),
            "trade_date": bar.trade_date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "amount": bar.amount,
        }
        for bar in bars
        if start <= bar.trade_date <= end and bar.trade_date not in existing and _valid_bar(bar)
    ]
    if not rows:
        return 0
    get_ch_client().execute(
        """
        INSERT INTO klines_daily
        (symbol, trade_date, open, high, low, close, volume, amount)
        VALUES
        """,
        rows,
    )
    # Dual-write to Parquet when configured
    try:
        from app.core.config import settings
        if settings.market_data_backend == "parquet":
            from app.data_stores import get_market_data_store
            store = get_market_data_store()
            store.write_daily(pd.DataFrame(rows))
    except Exception:
        pass
    return len(rows)


async def fetch_bars(symbol: str, start: date, end: date) -> tuple[str, list[DailyBar]]:
    try:
        bars = await fetch_qmt_bars(symbol, start, end)
        if bars:
            return "qmt", bars
    except Exception as exc:
        print(f"  qmt failed: {exc}")

    try:
        bars = fetch_tushare_bars(symbol, start, end)
        if bars:
            return "tushare", bars
    except Exception as exc:
        print(f"  tushare failed: {exc}")

    last_error: Exception | None = None
    for attempt in range(4):
        try:
            bars = fetch_akshare_bars(symbol, start, end)
            if bars:
                return "akshare", bars
        except Exception as exc:
            last_error = exc
            time.sleep(1.5 + attempt)
    if last_error:
        print(f"  akshare failed: {last_error}")
    return "none", []


async def sync_symbol(db_path: Path, symbol: str, start: date, end: date) -> dict[str, Any]:
    before_count, before_min, before_max = count_valid_bars(symbol, start, end)
    source = "cached"
    bars: list[DailyBar] = []
    inserted = 0

    needs_fetch = (
        before_count == 0
        or before_min is None
        or before_max is None
        or before_min > start
        or before_max < end
    )
    if needs_fetch:
        source, bars = await fetch_bars(symbol, start, end)
    else:
        # Try to fetch anyway for sparse histories, but avoid extra network for complete-enough data.
        source, bars = "cached", []

    if bars:
        inserted = insert_missing_bars(symbol, bars, start, end)

    meta = await fetch_qmt_meta(symbol)
    if meta is None:
        meta = fetch_tushare_meta(symbol) if not symbol.startswith(("000001.SH", "399101")) else None
    if meta is None:
        meta = fetch_akshare_meta(symbol, bars)
    if meta is not None:
        upsert_stock_meta(db_path, meta)

    after_count, after_min, after_max = count_valid_bars(symbol, start, end)
    return {
        "symbol": symbol,
        "source": source,
        "before_count": before_count,
        "before_range": [str(before_min), str(before_max)],
        "inserted": inserted,
        "after_count": after_count,
        "after_range": [str(after_min), str(after_max)],
        "meta": bool(meta),
    }


def build_symbol_set(args: argparse.Namespace, db_path: Path, start: date, end: date) -> set[str]:
    symbols: set[str] = set()
    if args.symbols:
        symbols.update(normalize_symbol(s) for s in args.symbols.split(",") if s.strip())
    if args.watchlist_group:
        symbols.update(watchlist_symbols(db_path, args.watchlist_group))
    if args.index_symbol:
        symbols.update(index_component_symbols(db_path, args.index_symbol, start, end))
    if args.jq_artifacts:
        symbols.update(symbols_from_jq_artifacts(Path(args.jq_source), Path(args.jq_log_zip)))
    symbols.add("000001.SH")
    symbols.add("399101.SZ")
    return {s for s in symbols if s}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="20180101")
    parser.add_argument("--end", default="20260508")
    parser.add_argument("--watchlist-group", type=int, default=3)
    parser.add_argument("--index-symbol", default="")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--jq-artifacts", action="store_true", default=False)
    parser.add_argument("--jq-source", default=str(DEFAULT_JQ_SOURCE))
    parser.add_argument("--jq-log-zip", default=str(DEFAULT_JQ_LOG_ZIP))
    parser.add_argument("--only-missing-stock-meta", action="store_true")
    parser.add_argument("--sync-name-changes", action="store_true")
    parser.add_argument("--sync-daily-basic", action="store_true")
    parser.add_argument("--reference-only", action="store_true")
    args = parser.parse_args()

    db_path = Path(settings.data_dir) / "gaoshou.db"
    start = _parse_yyyymmdd(args.start)
    end = _parse_yyyymmdd(args.end)
    symbols = sorted(build_symbol_set(args, db_path, start, end))
    existing_meta = existing_stock_symbols(db_path, {data_symbol(s) for s in symbols} | set(symbols))

    if args.only_missing_stock_meta:
        symbols = [s for s in symbols if data_symbol(s) not in existing_meta and s not in existing_meta]

    print(f"db={db_path}")
    print(f"symbols={len(symbols)} range={start}..{end}")
    try:
        import tushare as ts

        saved_ts_token = ts.get_token()
    except Exception:
        saved_ts_token = None
    print("Tushare token:", "yes" if (os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or saved_ts_token) else "no")

    ensure_reference_tables(db_path)
    if args.sync_name_changes:
        inserted = sync_tushare_name_changes(db_path, set(symbols))
        print(f"stock_name_changes upserted={inserted}")
    if args.sync_daily_basic:
        inserted = sync_tushare_daily_basic(db_path, set(symbols), start, end)
        print(f"stock_daily_basic upserted={inserted}")
    if args.reference_only:
        return

    results = []
    for i, symbol in enumerate(symbols, start=1):
        print(f"[{i}/{len(symbols)}] {symbol}")
        result = await sync_symbol(db_path, symbol, start, end)
        print(
            f"  source={result['source']} inserted={result['inserted']} "
            f"count {result['before_count']} -> {result['after_count']} meta={result['meta']}"
        )
        results.append(result)
        if result["source"] == "akshare":
            time.sleep(0.8)

    missing = [r for r in results if r["after_count"] == 0]
    print("SUMMARY")
    print(json.dumps({"total": len(results), "missing_after": missing, "results": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
