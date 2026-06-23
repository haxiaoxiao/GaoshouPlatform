#!/usr/bin/env python
"""Validate the isolated dev sample database and cached datasets."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence

import duckdb

_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from app.core.config import settings

REQUIRED_SQLITE_TABLES = (
    "stocks",
    "financial_data",
    "stock_daily_basic",
    "stock_limit_prices",
    "index_components",
    "watchlist_groups",
    "watchlist_stocks",
    "strategies",
    "backtests",
    "factors",
    "factor_analysis",
    "sync_runs",
    "sync_logs",
    "sentiment_posts",
    "sentiment_threads",
)

JSON_COLUMNS = {
    "strategies": ("parameters",),
    "backtests": ("parameters", "result"),
    "sync_runs": ("request", "details"),
    "sync_logs": ("details",),
    "factor_research_runs": ("params", "summary", "detail"),
    "sentiment_posts": ("keywords_json", "raw_json"),
    "sentiment_threads": ("symbols_json", "keywords_json", "raw_json"),
    "factors": ("parameters",),
    "factor_analysis": ("details",),
}

DATE_COLUMNS = {
    "stock_daily_basic": "trade_date",
    "stock_limit_prices": "trade_date",
    "financial_data": "report_date",
    "index_components": "trade_date",
    "sentiment_posts": "published_at",
    "sentiment_threads": "last_reply_at",
    "sync_runs": "start_time",
    "sync_logs": "start_time",
    "factor_research_runs": "start_date",
}

PARQUET_DATASETS = {
    "klines_daily": "trade_date",
    "klines_minute": "datetime",
    "klines_minute_timer": "datetime",
    "klines_minute_cum_timer": "datetime",
    "factor_cache": "trade_date",
    "factor_values": "trade_date",
    "stock_indicators": "trade_date",
    "indicator_timeseries": "datetime",
    "adj_factors": "trade_date",
    "moneyflow": "trade_date",
    "block_moneyflow": "trade_date",
    "auction_replay": "datetime",
    "ths_index": "snapshot_date",
    "ths_member": "snapshot_date",
    "announcements": "ann_date",
    "research_reports": "report_date",
    "market_news": "publish_time",
    "analyst_report_forecasts": "report_date",
    "analyst_rank": "update_date",
    "analyst_detail": "latest_rating_date",
    "analyst_history": "in_date",
    "hsgt_moneyflow": "trade_date",
    "hsgt_holdings": "trade_date",
    "fund_portfolio_holdings": "end_date",
    "financial_income": "f_ann_date",
    "financial_balancesheet": "f_ann_date",
    "financial_cashflow": "f_ann_date",
}

INTERFACE_STORAGE_REQUIREMENTS = {
    "system_status": {
        "routes": ["/api/system/status"],
        "sqlite": [],
        "parquet": ["klines_daily", "klines_minute", "klines_minute_timer", "klines_minute_cum_timer"],
        "notes": "系统状态页用这些数据展示最新口径；分钟线可用完整分钟线兜底。",
    },
    "data_center": {
        "routes": ["/api/data/stocks", "/api/data/klines", "/api/data/watchlist/*", "/api/data/sync/*"],
        "sqlite": ["stocks", "watchlist_groups", "watchlist_stocks", "sync_runs", "sync_logs", "index_components"],
        "parquet": ["klines_daily", "klines_minute"],
    },
    "data_skill": {
        "routes": ["/api/skill/*"],
        "sqlite": ["stocks", "financial_data", "stock_daily_basic", "stock_limit_prices"],
        "parquet": ["klines_daily", "klines_minute", "stock_indicators", "indicator_timeseries"],
    },
    "data_explorer_core": {
        "routes": ["/api/explorer/*", "/api/explorer/parquet/*"],
        "sqlite": [],
        "parquet": [
            "klines_daily",
            "klines_minute",
            "klines_minute_timer",
            "klines_minute_cum_timer",
            "factor_values",
            "stock_indicators",
            "indicator_timeseries",
        ],
    },
    "data_explorer_auxiliary": {
        "routes": ["/api/explorer/*"],
        "sqlite": [],
        "parquet_optional": [
            "factor_cache",
            "adj_factors",
            "moneyflow",
            "block_moneyflow",
            "auction_replay",
            "ths_index",
            "ths_member",
            "announcements",
            "research_reports",
            "market_news",
            "analyst_report_forecasts",
            "analyst_rank",
            "analyst_detail",
            "analyst_history",
            "hsgt_moneyflow",
            "hsgt_holdings",
            "fund_portfolio_holdings",
            "financial_income",
            "financial_balancesheet",
            "financial_cashflow",
        ],
        "notes": "这些表由中继/舆情/公告数据源提供；源端不存在时只能标记为 not_available。",
    },
    "factor_definition": {
        "routes": ["/api/factor/*", "/api/factors/*"],
        "sqlite": ["factors", "factor_analysis"],
        "parquet": ["klines_daily"],
    },
    "factor_values": {
        "routes": ["/api/factor-values/*"],
        "sqlite": ["stocks", "index_components", "stock_daily_basic", "stock_limit_prices", "financial_data"],
        "parquet": ["factor_values", "klines_daily", "klines_minute", "klines_minute_cum_timer"],
        "parquet_optional": ["moneyflow", "auction_replay", "ths_member", "block_moneyflow", "hsgt_moneyflow", "hsgt_holdings", "fund_portfolio_holdings", "financial_income", "financial_balancesheet", "financial_cashflow"],
    },
    "factor_research": {
        "routes": ["/api/factor-research/*"],
        "sqlite": ["factor_research_runs", "factor_research_run_items", "index_components", "stock_daily_basic", "stock_limit_prices"],
        "parquet": ["factor_values", "klines_daily"],
    },
    "indicator": {
        "routes": ["/api/indicators/*"],
        "sqlite": ["stocks", "financial_data"],
        "parquet": ["stock_indicators", "indicator_timeseries"],
    },
    "backtest": {
        "routes": ["/api/backtest/*", "/api/v2/backtest/*"],
        "sqlite": ["strategies", "backtests", "stocks", "index_components", "stock_daily_basic", "stock_limit_prices"],
        "parquet": ["klines_daily", "klines_minute", "klines_minute_timer", "klines_minute_cum_timer", "factor_values"],
    },
    "sentiment": {
        "routes": ["/api/sentiment/*"],
        "sqlite": ["sentiment_posts", "sentiment_threads", "stocks"],
        "parquet_optional": ["market_news", "announcements", "research_reports", "analyst_report_forecasts", "analyst_rank", "analyst_detail", "analyst_history", "hsgt_moneyflow", "hsgt_holdings", "fund_portfolio_holdings", "financial_income", "financial_balancesheet", "financial_cashflow"],
    },
    "legacy_strategy_signals": {
        "routes": ["/api/strategy/signals/*", "/api/strategy/backtest"],
        "sqlite": ["watchlist_groups", "watchlist_stocks"],
        "parquet": ["klines_minute", "klines_daily"],
        "external": [],
        "notes": "这组接口通过 TrendCapitalStrategy 读取本地 Parquet 行情。",
    },
    "live_trading": {
        "routes": ["/api/live-trading/*"],
        "sqlite": ["live_strategy_profiles", "live_order_audits", "live_trade_records", "live_paper_accounts"],
        "parquet": [],
        "external": ["optional QMT account bridge"],
    },
}


def _connect_readonly(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _table_names(con: sqlite3.Connection) -> list[str]:
    return [
        str(row["name"])
        for row in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    ]


def _columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")').fetchall()]


def _count(con: sqlite3.Connection, table: str) -> int:
    return int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


def _relation_checks(con: sqlite3.Connection, tables: set[str]) -> list[dict[str, Any]]:
    checks = [
        (
            "watchlist_stocks.symbol -> stocks.symbol",
            "error",
            "SELECT COUNT(*) FROM watchlist_stocks ws LEFT JOIN stocks s ON s.symbol=ws.symbol WHERE s.symbol IS NULL",
        ),
        (
            "watchlist_stocks.group_id -> watchlist_groups.id",
            "error",
            "SELECT COUNT(*) FROM watchlist_stocks ws LEFT JOIN watchlist_groups wg ON wg.id=ws.group_id WHERE wg.id IS NULL",
        ),
        (
            "backtests.strategy_id -> strategies.id",
            "warning",
            "SELECT COUNT(*) FROM backtests b LEFT JOIN strategies s ON s.id=b.strategy_id WHERE s.id IS NULL",
        ),
        (
            "factor_analysis.factor_id -> factors.id",
            "error",
            "SELECT COUNT(*) FROM factor_analysis fa LEFT JOIN factors f ON f.id=fa.factor_id WHERE f.id IS NULL",
        ),
        (
            "stock_daily_basic.symbol -> stocks.symbol",
            "error",
            "SELECT COUNT(*) FROM stock_daily_basic d LEFT JOIN stocks s ON s.symbol=d.symbol WHERE s.symbol IS NULL",
        ),
        (
            "stock_limit_prices.symbol -> stocks.symbol",
            "error",
            "SELECT COUNT(*) FROM stock_limit_prices l LEFT JOIN stocks s ON s.symbol=l.symbol WHERE s.symbol IS NULL",
        ),
        (
            "financial_data.symbol -> stocks.symbol",
            "error",
            "SELECT COUNT(*) FROM financial_data f LEFT JOIN stocks s ON s.symbol=f.symbol WHERE s.symbol IS NULL",
        ),
        (
            "sentiment_posts.symbol -> stocks.symbol",
            "error",
            "SELECT COUNT(*) FROM sentiment_posts p LEFT JOIN stocks s ON s.symbol=p.symbol WHERE s.symbol IS NULL",
        ),
    ]
    results: list[dict[str, Any]] = []
    for name, severity, sql in checks:
        needed = {
            token.split(".")[0]
            for token in name.replace("->", " ").split()
            if "." in token
        }
        if not needed.issubset(tables):
            continue
        orphan_count = int(con.execute(sql).fetchone()[0])
        results.append({
            "name": name,
            "severity": severity,
            "orphan_count": orphan_count,
            "ok": orphan_count == 0,
        })
    return results


def _json_checks(con: sqlite3.Connection, tables: set[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for table, candidates in JSON_COLUMNS.items():
        if table not in tables:
            continue
        columns = set(_columns(con, table))
        json_columns = [column for column in candidates if column in columns]
        if not json_columns:
            continue
        bad: list[dict[str, Any]] = []
        non_null = 0
        select_cols = ", ".join(['rowid', *[f'"{column}"' for column in json_columns]])
        for row in con.execute(f'SELECT {select_cols} FROM "{table}"'):
            for column in json_columns:
                value = row[column]
                if value is None or value == "":
                    continue
                non_null += 1
                if not isinstance(value, str):
                    continue
                try:
                    json.loads(value)
                except Exception as exc:  # noqa: BLE001 - report validation details, do not hide them.
                    bad.append({
                        "rowid": row["rowid"],
                        "column": column,
                        "error": str(exc),
                        "preview": value[:120],
                    })
        results.append({
            "table": table,
            "columns": json_columns,
            "non_null_values": non_null,
            "invalid_count": len(bad),
            "invalid_examples": bad[:5],
            "ok": not bad,
        })
    return results


def _date_ranges(con: sqlite3.Connection, tables: set[str]) -> dict[str, dict[str, Any]]:
    ranges: dict[str, dict[str, Any]] = {}
    for table, column in DATE_COLUMNS.items():
        if table not in tables or column not in _columns(con, table):
            continue
        row = con.execute(
            f'SELECT MIN(date("{column}")) AS min_date, MAX(date("{column}")) AS max_date FROM "{table}"'
        ).fetchone()
        ranges[table] = {"column": column, "min": row["min_date"], "max": row["max_date"]}
    return ranges


def _absolute_path_refs(con: sqlite3.Connection, tables: set[str]) -> list[dict[str, Any]]:
    risky_patterns = (
        "E:\\Projects\\data",
        "E:/Projects/data",
        "GaoshouPlatform-prod",
    )
    refs: list[dict[str, Any]] = []
    for table in sorted(tables):
        if table == "dev_sample_manifest":
            continue
        columns = _columns(con, table)
        text_columns = [
            column
            for column in columns
            if any(token in column.lower() for token in ("path", "details", "request", "result", "raw", "json", "params", "url"))
        ]
        if not text_columns:
            continue
        select_expr = " || ".join([f'COALESCE(CAST("{column}" AS TEXT), "")' for column in text_columns])
        where = " OR ".join([f"({select_expr}) LIKE ?" for _ in risky_patterns])
        params = [f"%{pattern}%" for pattern in risky_patterns]
        count = int(con.execute(f'SELECT COUNT(*) FROM "{table}" WHERE {where}', params).fetchone()[0])
        if count:
            refs.append({"table": table, "count": count, "columns": text_columns})
    return refs


def _config_previews(con: sqlite3.Connection, tables: set[str]) -> dict[str, list[dict[str, Any]]]:
    preview_sql = {
        "strategies": "SELECT id, name, substr(COALESCE(parameters, ''), 1, 120) AS parameters FROM strategies ORDER BY id LIMIT 8",
        "backtests": "SELECT id, strategy_id, status, start_date, end_date, initial_capital FROM backtests ORDER BY id LIMIT 8",
        "factors": "SELECT id, name, category, source, substr(COALESCE(parameters, ''), 1, 120) AS parameters FROM factors ORDER BY id LIMIT 8",
        "watchlist_groups": "SELECT id, name, description FROM watchlist_groups ORDER BY id LIMIT 8",
        "sync_runs": "SELECT run_id, sync_type, status, total, current, success_count, failed_count, progress_percent FROM sync_runs ORDER BY COALESCE(start_time, created_at) DESC LIMIT 8",
    }
    previews: dict[str, list[dict[str, Any]]] = {}
    for table, sql in preview_sql.items():
        if table in tables:
            previews[table] = [dict(row) for row in con.execute(sql).fetchall()]
    return previews


def _parquet_summary(parquet_dir: Path) -> dict[str, dict[str, Any]]:
    con = duckdb.connect(":memory:")
    summary: dict[str, dict[str, Any]] = {}
    for dataset, date_column in PARQUET_DATASETS.items():
        root = parquet_dir / dataset
        files = [file for file in root.rglob("*.parquet")] if root.exists() else []
        if not files:
            summary[dataset] = {"files": 0, "rows": 0, "min": None, "max": None}
            continue
        pattern = str(root / "**" / "*.parquet").replace("\\", "/")
        row = con.execute(
            f"""
            SELECT COUNT(*) AS rows,
                   MIN(CAST({date_column} AS DATE)) AS min_date,
                   MAX(CAST({date_column} AS DATE)) AS max_date
            FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
            """,
            [pattern],
        ).fetchone()
        summary[dataset] = {
            "files": len(files),
            "rows": int(row[0]),
            "min": str(row[1]) if row[1] is not None else None,
            "max": str(row[2]) if row[2] is not None else None,
        }
    con.close()
    return summary


def _interface_coverage(
    table_counts: dict[str, int],
    parquet_summary: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = {}
    for name, spec in INTERFACE_STORAGE_REQUIREMENTS.items():
        missing_sqlite = [
            table for table in spec.get("sqlite", [])
            if table not in table_counts or table_counts.get(table, 0) <= 0
        ]
        missing_parquet = [
            dataset for dataset in spec.get("parquet", [])
            if int(parquet_summary.get(dataset, {}).get("rows") or 0) <= 0
        ]
        optional_missing = [
            dataset for dataset in spec.get("parquet_optional", [])
            if int(parquet_summary.get(dataset, {}).get("rows") or 0) <= 0
        ]
        status = "covered"
        if missing_sqlite or missing_parquet:
            status = "missing_required"
        elif spec.get("external"):
            status = "external_dependency"
        elif optional_missing:
            status = "covered_core_optional_missing"
        coverage[name] = {
            "status": status,
            "routes": spec.get("routes", []),
            "sqlite": {
                table: table_counts.get(table, 0)
                for table in spec.get("sqlite", [])
            },
            "parquet": {
                dataset: parquet_summary.get(dataset, {"rows": 0, "files": 0, "min": None, "max": None})
                for dataset in spec.get("parquet", [])
            },
            "optional_missing": optional_missing,
            "missing_required_sqlite": missing_sqlite,
            "missing_required_parquet": missing_parquet,
            "external": spec.get("external", []),
            "notes": spec.get("notes"),
        }
    return coverage


def validate_sample(db_path: Path, parquet_dir: Path, *, strict: bool = False) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if not db_path.exists():
        errors.append(f"SQLite database not found: {db_path}")
    if not parquet_dir.exists():
        errors.append(f"Parquet directory not found: {parquet_dir}")
    if errors:
        return {"ok": False, "errors": errors, "warnings": warnings}

    con = _connect_readonly(db_path)
    tables = set(_table_names(con))
    table_counts = {table: _count(con, table) for table in sorted(tables)}

    missing = [table for table in REQUIRED_SQLITE_TABLES if table not in tables]
    if missing:
        errors.append("Missing required SQLite tables: " + ", ".join(missing))

    empty_required = [
        table
        for table in REQUIRED_SQLITE_TABLES
        if table in table_counts and table_counts[table] == 0 and table not in {"sync_tasks"}
    ]
    for table in empty_required:
        warnings.append(f"Required UI table is empty: {table}")

    relation_results = _relation_checks(con, tables)
    for result in relation_results:
        if result["ok"]:
            continue
        message = f"{result['name']} has {result['orphan_count']} orphan rows"
        if result["severity"] == "error":
            errors.append(message)
        else:
            warnings.append(message)

    json_results = _json_checks(con, tables)
    for result in json_results:
        if not result["ok"]:
            errors.append(f"{result['table']} has {result['invalid_count']} invalid JSON values")

    path_refs = _absolute_path_refs(con, tables)
    for ref in path_refs:
        warnings.append(f"{ref['table']} still references source/prod path patterns ({ref['count']} rows)")

    parquet_datasets = _parquet_summary(parquet_dir)
    interface_coverage = _interface_coverage(table_counts, parquet_datasets)
    for name, item in interface_coverage.items():
        if item["status"] == "missing_required":
            errors.append(f"Interface group {name} is missing required dev storage")
        elif item["status"] == "external_dependency":
            warnings.append(f"Interface group {name} still depends on external services: {', '.join(item['external'])}")
        elif item["status"] == "covered_core_optional_missing":
            warnings.append(f"Interface group {name} has optional datasets missing: {', '.join(item['optional_missing'])}")

    report = {
        "ok": not errors and (not strict or not warnings),
        "strict": strict,
        "settings": {
            "database_url": settings.database_url,
            "parquet_data_dir": settings.parquet_data_dir,
            "market_data_backend": settings.market_data_backend,
            "enable_sync_scheduler": settings.enable_sync_scheduler,
        },
        "sqlite": {
            "db_path": str(db_path.resolve()),
            "table_counts": table_counts,
            "date_ranges": _date_ranges(con, tables),
            "relation_checks": relation_results,
            "json_checks": json_results,
            "source_or_prod_path_refs": path_refs,
            "config_previews": _config_previews(con, tables),
        },
        "parquet": {
            "parquet_dir": str(parquet_dir.resolve()),
            "datasets": parquet_datasets,
        },
        "interface_coverage": interface_coverage,
        "errors": errors,
        "warnings": warnings,
    }
    con.close()
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate isolated dev sample data.")
    parser.add_argument("--db-path", default=str(settings.sqlite_db_path), help="SQLite database path.")
    parser.add_argument("--parquet-dir", default=settings.parquet_data_dir, help="Parquet data directory.")
    parser.add_argument("--json-report", default=None, help="Optional path to write the full JSON report.")
    parser.add_argument("--strict", action="store_true", help="Return non-zero when warnings exist.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = validate_sample(Path(args.db_path), Path(args.parquet_dir), strict=args.strict)

    if args.json_report:
        Path(args.json_report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_report).write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    print(json.dumps({
        "ok": report.get("ok"),
        "errors": report.get("errors", []),
        "warnings": report.get("warnings", []),
        "sqlite_tables": report.get("sqlite", {}).get("table_counts", {}),
        "parquet_datasets": report.get("parquet", {}).get("datasets", {}),
    }, ensure_ascii=False, indent=2, default=str))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
