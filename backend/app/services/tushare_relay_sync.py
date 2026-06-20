from __future__ import annotations

import asyncio
import calendar
import copy
import hashlib
import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from loguru import logger
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.duckdb import get_duckdb
from app.db.models import Stock, StockConceptMembership
from app.services.security_symbols import normalize_security_symbol
from app.services.tushare_relay import TushareRelayClient, TushareRelayMeta
from app.services.tushare_relay_specs import (
    ANALYST_RELAY_DATASETS,
    FINANCIAL_STATEMENT_RELAY_DATASETS,
    INSTITUTION_RELAY_DATASETS,
    RELAY_DATASET_SPECS,
    STRUCTURED_RELAY_DATASETS,
    TEXT_RELAY_DATASETS,
    RelayDatasetSpec,
)

CATALOG_CACHE_TTL_SECONDS = 300
COVERAGE_CACHE_TTL_SECONDS = 300
_CATALOG_CACHE: dict[str, Any] = {"expires_at": 0.0, "value": None}
_COVERAGE_CACHE: dict[tuple[str, str, bool], tuple[float, dict[str, Any]]] = {}
_FAST_COVERAGE_DATASETS = {"klines_daily", "klines_minute", "factor_values"}




CORE_SYNC_CATALOG = [
    {
        "name": "stock_info",
        "display_name": "股票基础信息",
        "category": "core",
        "source": "QMT",
        "recommended_frequency": "daily",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "low",
        "description": "同步股票代码、名称、行业、状态等基础字段。",
        "default_enabled": True,
    },
    {
        "name": "stock_full",
        "display_name": "股票完整信息",
        "category": "core",
        "source": "QMT",
        "recommended_frequency": "weekly",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "同步市值、财务摘要和扩展字段，耗时比基础信息更长。",
        "default_enabled": False,
    },
    {
        "name": "financial_data",
        "display_name": "财务报表",
        "category": "core",
        "source": "QMT",
        "recommended_frequency": "weekly",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "同步季度财务数据。",
        "default_enabled": False,
    },
    {
        "name": "kline_daily",
        "display_name": "日 K 线",
        "category": "market",
        "source": "QMT",
        "storage_dataset": "klines_daily",
        "date_col": "trade_date",
        "recommended_frequency": "daily",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "同步 A 股日线行情，因子研究和回测的基础行情。",
        "default_enabled": True,
    },
    {
        "name": "index_daily",
        "display_name": "指数日线",
        "category": "market",
        "source": "Tushare",
        "storage_dataset": "klines_daily",
        "date_col": "trade_date",
        "recommended_frequency": "daily",
        "requires_qmt": False,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "同步指数 OHLCV，供基准和股票池研究使用。",
        "default_enabled": True,
    },
    {
        "name": "kline_minute",
        "display_name": "分钟 K 线",
        "category": "market",
        "source": "QMT",
        "storage_dataset": "klines_minute",
        "date_col": "datetime",
        "recommended_frequency": "on_demand",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "high",
        "description": "分钟数据量极大，建议只按明确股票池和日期段补齐。",
        "default_enabled": False,
    },
    {
        "name": "realtime_mv",
        "display_name": "实时市值",
        "category": "core",
        "source": "QMT",
        "recommended_frequency": "daily",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "low",
        "description": "刷新 SQLite 股票表中的实时市值字段。",
        "default_enabled": True,
    },
    {
        "name": "dividends",
        "display_name": "QMT 分红",
        "category": "core",
        "source": "QMT",
        "recommended_frequency": "weekly",
        "requires_qmt": True,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "平台已有 QMT 分红口径，Relay dividend 暂不接入以避免重复。",
        "default_enabled": False,
    },
    {
        "name": "factor_dependency",
        "display_name": "因子依赖数据",
        "category": "factor",
        "source": "Local",
        "recommended_frequency": "on_demand",
        "requires_qmt": False,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "按因子预计算计划补齐必要的行情和基础数据。",
        "default_enabled": False,
    },
    {
        "name": "ths_concept",
        "display_name": "同花顺概念分类",
        "category": "concept",
        "source": "Indevs Tushare Relay",
        "storage_dataset": "ths_member",
        "date_col": "snapshot_date",
        "recommended_frequency": "weekly",
        "requires_qmt": False,
        "requires_relay_key": True,
        "risk_level": "medium",
        "description": "同步同花顺概念字典和成分，保留原始 Parquet，并派生 SQLite 概念成员表与 stocks.concept 展示字段。",
        "default_enabled": False,
    },
    {
        "name": "sentiment_xueqiu",
        "display_name": "情绪 / 雪球个股讨论",
        "category": "core",
        "source": "Built-in",
        "recommended_frequency": "on_demand",
        "requires_qmt": False,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "按股票代码抓取雪球个股讨论帖，适合围绕重点标的补充情绪样本。",
        "default_enabled": False,
    },
    {
        "name": "sentiment_nga",
        "display_name": "情绪 / NGA 大时代",
        "category": "core",
        "source": "Built-in",
        "recommended_frequency": "on_demand",
        "requires_qmt": False,
        "requires_relay_key": False,
        "risk_level": "medium",
        "description": "按日期集中抓取 NGA 大时代版面（fid=706）帖子，并自动识别相关股票代码。",
        "default_enabled": False,
    },
]


def build_sync_catalog(*, refresh: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    if not refresh and _CATALOG_CACHE["value"] is not None and now < float(_CATALOG_CACHE["expires_at"]):
        return copy.deepcopy(_CATALOG_CACHE["value"])

    relay_items = [
        {
            "name": spec.name,
            "display_name": spec.display_name,
            "category": spec.category,
            "source": "Indevs Tushare Relay",
            "storage_dataset": spec.storage_dataset,
            "date_col": spec.date_col,
            "recommended_frequency": spec.recommended_frequency,
            "requires_qmt": spec.requires_qmt,
            "requires_relay_key": spec.requires_relay_key,
            "risk_level": spec.risk_level,
            "description": spec.description,
            "default_enabled": spec.default_enabled,
            "text_source": spec.text_source,
            "symbol_scoped": spec.symbol_scoped,
            "default_params": dict(spec.default_params),
            "coverage": dataset_coverage(spec.storage_dataset, spec.date_col),
        }
        for spec in RELAY_DATASET_SPECS.values()
    ]
    core_items = []
    for item in CORE_SYNC_CATALOG:
        entry = dict(item)
        dataset = entry.get("storage_dataset")
        date_col = entry.get("date_col")
        entry["coverage"] = dataset_coverage(str(dataset), str(date_col)) if dataset and date_col else None
        core_items.append(entry)
    payload = {
        "presets": [
            {
                "name": "daily",
                "display_name": "日常更新",
                "description": "股票基础信息、日线、指数、实时市值。",
                "sync_types": ["stock_info", "kline_daily", "index_daily", "realtime_mv"],
                "relay_datasets": [],
                "include_by_default": True,
            },
            {
                "name": "factor_research",
                "display_name": "因子研究准备",
                "description": "补齐日线、指数、因子依赖基础数据。",
                "sync_types": ["kline_daily", "index_daily", "factor_dependency"],
                "relay_datasets": [],
                "include_by_default": False,
            },
            {
                "name": "relay_structured",
                "display_name": "Relay 增强数据",
                "description": "复权、资金流、同花顺板块和集合竞价。",
                "sync_types": [],
                "relay_datasets": list(STRUCTURED_RELAY_DATASETS),
                "include_by_default": False,
            },
            {
                "name": "relay_analyst",
                "display_name": "分析师与研报",
                "description": "卖方盈利预测、分析师排名、跟踪股票和研报标题事件流。",
                "sync_types": [],
                "relay_datasets": [*ANALYST_RELAY_DATASETS, "stock_research_report_em"],
                "include_by_default": False,
            },
            {
                "name": "relay_text",
                "display_name": "新闻公告补充",
                "description": "高噪声文本源，仅限量采集，默认不进入一键同步。",
                "sync_types": [],
                "relay_datasets": list(TEXT_RELAY_DATASETS),
                "include_by_default": False,
            },
            {
                "name": "relay_institution",
                "display_name": "北向与基金持仓",
                "description": "沪深港通聚合资金、个股持股明细和公募基金股票持仓。",
                "sync_types": [],
                "relay_datasets": list(INSTITUTION_RELAY_DATASETS),
                "include_by_default": False,
            },
            {
                "name": "relay_financial_statement",
                "display_name": "Tushare 三表财务",
                "description": "利润表、资产负债表和现金流量表，按实际公告日落库。",
                "sync_types": [],
                "relay_datasets": list(FINANCIAL_STATEMENT_RELAY_DATASETS),
                "include_by_default": False,
            },
        ],
        "datasets": [*core_items, *relay_items],
        "relay": {
            "configured": bool(settings.indevs_tushare_api_key),
            "rps": settings.indevs_tushare_rps,
            "timeout_seconds": settings.indevs_tushare_timeout_seconds,
            "base_url_count": len([u for u in settings.indevs_tushare_base_urls.split(",") if u.strip()]),
        },
        "guardrails": {
            "news_default_days": 7,
            "news_default_daily_limit": 200,
            "structured_default_rps": 1,
            "dividend": "Relay dividend is intentionally excluded because QMT dividend sync already exists.",
        },
        "cache": {
            "generated_at": datetime.now().isoformat(),
            "ttl_seconds": CATALOG_CACHE_TTL_SECONDS,
        },
    }
    _CATALOG_CACHE["value"] = copy.deepcopy(payload)
    _CATALOG_CACHE["expires_at"] = now + CATALOG_CACHE_TTL_SECONDS
    return payload


def dataset_coverage(dataset: str, date_col: str, *, exact: bool = False) -> dict[str, Any]:
    cache_key = (dataset, date_col, exact)
    now = time.monotonic()
    cached = _COVERAGE_CACHE.get(cache_key)
    if cached and now < cached[0]:
        return copy.deepcopy(cached[1])

    store = ParquetMarketDataStore(settings.parquet_data_dir)
    if not dataset or not store._exists(dataset):
        return {"row_count": 0, "min_date": None, "max_date": None}
    fast = _fast_dataset_coverage(dataset, date_col)
    if not exact and (dataset in _FAST_COVERAGE_DATASETS or int(fast.get("partition_count") or 0) > 24):
        _COVERAGE_CACHE[cache_key] = (now + COVERAGE_CACHE_TTL_SECONDS, fast)
        return copy.deepcopy(fast)
    try:
        pattern = store._glob_pattern(dataset)
        date_expr = _quote_identifier(date_col)
        row = get_duckdb().execute(
            f"""
            SELECT count(*) AS row_count, min({date_expr}) AS min_date, max({date_expr}) AS max_date
            FROM read_parquet(?, hive_partitioning=true)
            """,
            [pattern],
        ).fetchone()
        result = {
            "row_count": int(row[0] or 0) if row else 0,
            "min_date": str(row[1]) if row and row[1] is not None else None,
            "max_date": str(row[2]) if row and row[2] is not None else None,
            "estimated": False,
        }
        _COVERAGE_CACHE[cache_key] = (now + COVERAGE_CACHE_TTL_SECONDS, result)
        return copy.deepcopy(result)
    except Exception as exc:
        logger.warning("Failed to read coverage for relay dataset {}: {}", dataset, exc)
        result = {**fast, "error": str(exc)}
        _COVERAGE_CACHE[cache_key] = (now + COVERAGE_CACHE_TTL_SECONDS, result)
        return copy.deepcopy(result)


def _quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _fast_dataset_coverage(dataset: str, date_col: str) -> dict[str, Any]:
    root = Path(settings.parquet_data_dir) / dataset
    partitions: list[tuple[int, int]] = []
    for year_dir in root.glob("year=*"):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        for month_dir in year_dir.glob("month=*"):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            partitions.append((year, month))
    if not partitions:
        return {"row_count": None, "min_date": None, "max_date": None, "estimated": True, "partition_count": 0}

    min_year, min_month = min(partitions)
    max_year, max_month = max(partitions)
    last_day = calendar.monthrange(max_year, max_month)[1]
    min_date = f"{min_year:04d}-{min_month:02d}-01"
    max_date = f"{max_year:04d}-{max_month:02d}-{last_day:02d}"
    if date_col == "datetime":
        min_date = f"{min_date} 00:00:00"
        max_date = f"{max_date} 23:59:59"
    return {
        "row_count": None,
        "min_date": min_date,
        "max_date": max_date,
        "estimated": True,
        "partition_count": len(partitions),
    }


async def run_tushare_relay_sync(
    session: AsyncSession,
    progress: Any,
    *,
    relay_datasets: Iterable[str] | None,
    symbols: list[str] | None,
    start_date: date | None,
    end_date: date | None,
    relay_options: dict[str, Any] | None,
    failure_strategy: str = "skip",
) -> Any:
    options = dict(relay_options or {})
    requested = [str(item).strip() for item in (relay_datasets or STRUCTURED_RELAY_DATASETS) if str(item).strip()]
    if not requested:
        requested = list(STRUCTURED_RELAY_DATASETS)
    unknown = [item for item in requested if item not in RELAY_DATASET_SPECS]
    if unknown:
        raise ValueError(f"Unsupported relay dataset(s): {unknown}")

    text_sources = [name for name in requested if RELAY_DATASET_SPECS[name].text_source]
    if text_sources and not bool(options.get("allow_text_sources")):
        raise ValueError("News/announcement relay datasets require relay_options.allow_text_sources=true")

    end = end_date or date.today()
    if text_sources and start_date is None:
        start = end - timedelta(days=6)
    else:
        start = start_date or end
    if start > end:
        raise ValueError("start_date must be <= end_date")

    explicit_symbols = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
    symbol_scoped = any(RELAY_DATASET_SPECS[name].symbol_scoped for name in requested)
    resolved_symbols = (
        await _resolve_symbols(session, symbols, allow_all=bool(options.get("allow_all_symbols")))
        if symbol_scoped
        else explicit_symbols
    )
    symbol_limit = _positive_int(options.get("symbol_limit"))
    if symbol_limit and resolved_symbols:
        resolved_symbols = resolved_symbols[:symbol_limit]

    client = TushareRelayClient(
        rps=float(options.get("rps") or settings.indevs_tushare_rps or 1),
        timeout_seconds=int(options.get("timeout_seconds") or settings.indevs_tushare_timeout_seconds or 30),
    )
    store = ParquetMarketDataStore(settings.parquet_data_dir)
    dates = list(_date_range(start, end))
    total = _estimate_total(requested, dates, resolved_symbols, options)
    progress.total = total
    progress.current = 0
    progress.status = "running"
    progress.details.update(
        {
            "relay_datasets": requested,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "symbol_count": len(resolved_symbols),
            "request_headers_logged": ["x-api-relay-cache", "x-tushare-cache-bucket", "x-request-id"],
            "dataset_total": len(requested),
            "dataset_completed": 0,
            "dataset_failed": 0,
            "dataset_remaining": len(requested),
            "datasets": {},
        }
    )

    ths_index_rows: list[dict[str, Any]] = []
    ths_member_rows: list[dict[str, Any]] = []
    analyst_rank_rows: list[dict[str, Any]] = []
    for dataset_index, dataset_name in enumerate(requested, start=1):
        spec = RELAY_DATASET_SPECS[dataset_name]
        dataset_rows: list[dict[str, Any]] = []
        dataset_meta: list[dict[str, Any]] = []
        for key in ("current_symbol", "current_date", "current_ths_code", "current_analyst_id", "current_indicator"):
            progress.details.pop(key, None)
        progress.details.update(
            {
                "current_dataset": dataset_name,
                "current_dataset_display_name": spec.display_name,
                "current_dataset_index": dataset_index,
                "current_dataset_unit_total": _estimate_total([dataset_name], dates, resolved_symbols, options),
                "dataset_remaining": max(0, len(requested) - dataset_index + 1),
            }
        )
        try:
            if dataset_name in {"adj_factor", "moneyflow", "stk_auction_replay"}:
                dataset_rows, dataset_meta = await _sync_symbol_date_dataset(
                    client,
                    spec,
                    symbols=resolved_symbols,
                    dates=dates,
                    options=options,
                    progress=progress,
                )
            elif dataset_name == "block_moneyflow":
                dataset_rows, dataset_meta = await _sync_block_moneyflow(client, spec, dates=dates, options=options, progress=progress)
            elif dataset_name == "moneyflow_hsgt":
                dataset_rows, dataset_meta = await _sync_moneyflow_hsgt(client, spec, dates=dates, options=options, progress=progress)
            elif dataset_name == "hk_hold":
                dataset_rows, dataset_meta = await _sync_hk_hold(
                    client,
                    spec,
                    dates=dates,
                    symbols=resolved_symbols,
                    options=options,
                    progress=progress,
                )
            elif dataset_name == "fund_portfolio":
                dataset_rows, dataset_meta = await _sync_fund_portfolio(
                    client,
                    spec,
                    dates=dates,
                    symbols=resolved_symbols,
                    options=options,
                    progress=progress,
                )
            elif dataset_name == "ths_index":
                dataset_rows, dataset_meta = await _sync_ths_index(client, spec, options=options, progress=progress)
                ths_index_rows = dataset_rows
            elif dataset_name == "ths_member":
                if not ths_index_rows:
                    ths_index_rows, extra_meta = await _sync_ths_index(client, RELAY_DATASET_SPECS["ths_index"], options=options, progress=progress)
                    dataset_meta.extend(extra_meta)
                dataset_rows, member_meta = await _sync_ths_member(client, spec, ths_index_rows=ths_index_rows, options=options, progress=progress)
                dataset_meta.extend(member_meta)
                ths_member_rows = dataset_rows
            elif dataset_name == "report_rc":
                dataset_rows, dataset_meta = await _sync_report_rc(
                    client,
                    spec,
                    symbols=resolved_symbols,
                    start=start,
                    end=end,
                    options=options,
                    progress=progress,
                )
            elif dataset_name == "analyst_rank":
                dataset_rows, dataset_meta = await _sync_analyst_rank(
                    client,
                    spec,
                    dates=dates,
                    options=options,
                    progress=progress,
                )
                analyst_rank_rows = dataset_rows
            elif dataset_name in {"analyst_detail", "analyst_history"}:
                if not analyst_rank_rows and not _analyst_ids_from_options(options):
                    analyst_rank_rows, rank_meta = await _sync_analyst_rank(
                        client,
                        RELAY_DATASET_SPECS["analyst_rank"],
                        dates=dates,
                        options=options,
                        progress=progress,
                    )
                    dataset_meta.extend(rank_meta)
                dataset_rows, analyst_meta = await _sync_analyst_followup(
                    client,
                    spec,
                    analyst_rank_rows=analyst_rank_rows,
                    options=options,
                    progress=progress,
                )
                dataset_meta.extend(analyst_meta)
            elif dataset_name in FINANCIAL_STATEMENT_RELAY_DATASETS:
                dataset_rows, dataset_meta = await _sync_financial_statement(
                    client,
                    spec,
                    symbols=resolved_symbols,
                    start=start,
                    end=end,
                    options=options,
                    progress=progress,
                )
            elif spec.text_source:
                dataset_rows, dataset_meta = await _sync_text_dataset(
                    client,
                    spec,
                    dates=dates,
                    symbols=resolved_symbols,
                    options=options,
                    progress=progress,
                )

            for row in dataset_rows:
                row.setdefault("source_api", spec.api_name)
            frame = _normalize_dataset_rows(dataset_name, dataset_rows, options)
            written = 0
            if not frame.empty:
                written = await asyncio.to_thread(
                    store.write_dataset,
                    frame,
                    dataset=spec.storage_dataset,
                    date_col=spec.date_col,
                )
            progress.success_count += 1
            progress.details["datasets"][dataset_name] = {
                "rows_fetched": len(dataset_rows),
                "rows_written": written,
                "storage_dataset": spec.storage_dataset,
                "date_col": spec.date_col,
                "calls": dataset_meta[-10:],
            }
            dataset_states = progress.details.get("datasets") if isinstance(progress.details.get("datasets"), dict) else {}
            progress.details["dataset_completed"] = len(dataset_states)
            progress.details["dataset_failed"] = sum(
                1 for value in dataset_states.values()
                if isinstance(value, dict) and value.get("error")
            )
            progress.details["dataset_remaining"] = max(0, len(requested) - len(dataset_states))
        except Exception as exc:
            progress.failed_count += 1
            progress.details["datasets"][dataset_name] = {
                "error": str(exc),
                "storage_dataset": spec.storage_dataset,
                "calls": dataset_meta[-10:],
            }
            dataset_states = progress.details.get("datasets") if isinstance(progress.details.get("datasets"), dict) else {}
            progress.details["dataset_completed"] = len(dataset_states)
            progress.details["dataset_failed"] = sum(
                1 for value in dataset_states.values()
                if isinstance(value, dict) and value.get("error")
            )
            progress.details["dataset_remaining"] = max(0, len(requested) - len(dataset_states))
            logger.opt(exception=True).warning("Tushare relay dataset {} sync failed: {}", dataset_name, exc)
            if failure_strategy == "stop":
                raise

    if options.get("derive_ths_concepts") and ths_member_rows:
        try:
            progress.details["ths_concept"] = await _derive_ths_concepts_to_sqlite(
                session,
                ths_index_rows=ths_index_rows,
                ths_member_rows=ths_member_rows,
            )
        except Exception as exc:
            progress.failed_count += 1
            progress.details["ths_concept"] = {"error": str(exc)}
            logger.opt(exception=True).warning("THS concept derivation failed: {}", exc)
            if options.get("derive_ths_concepts") or failure_strategy == "stop":
                raise

    progress.status = "failed" if progress.failed_count and progress.success_count == 0 else "completed"
    progress.end_time = datetime.now()
    return progress


async def _derive_ths_concepts_to_sqlite(
    session: AsyncSession,
    *,
    ths_index_rows: list[dict[str, Any]],
    ths_member_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    source = "indevs_tushare_relay"
    snapshot = date.today()
    concept_names: dict[str, str] = {}
    for row in ths_index_rows:
        code = _first_text(row, "ts_code", "ths_code", "code")
        if not code:
            continue
        concept_names[code] = _first_text(row, "name", "ths_name", "ts_name", "concept_name") or code

    memberships: list[StockConceptMembership] = []
    concepts_by_symbol: dict[str, list[str]] = {}
    seen: set[tuple[str, str]] = set()
    for row in ths_member_rows:
        concept_code = _first_text(row, "ts_code", "ths_code", "code")
        raw_symbol = _first_text(row, "con_code", "symbol", "stock_code")
        symbol = normalize_security_symbol(raw_symbol) or raw_symbol.upper()
        if not concept_code or not symbol:
            continue
        key = (concept_code, symbol)
        if key in seen:
            continue
        seen.add(key)
        concept_name = concept_names.get(concept_code) or _first_text(row, "concept_name", "ths_name") or concept_code
        concepts_by_symbol.setdefault(symbol, []).append(concept_name)
        memberships.append(
            StockConceptMembership(
                source=source,
                snapshot_date=snapshot,
                concept_code=concept_code,
                concept_name=concept_name,
                symbol=symbol,
                in_date=_parse_date_value(row.get("in_date")),
                out_date=_parse_date_value(row.get("out_date")),
                is_new=_parse_bool_value(row.get("is_new")),
            )
        )

    await session.execute(
        delete(StockConceptMembership).where(
            StockConceptMembership.source == source,
            StockConceptMembership.snapshot_date == snapshot,
        )
    )
    if memberships:
        session.add_all(memberships)

    updated_stocks = 0
    if concepts_by_symbol:
        rows = await session.execute(select(Stock).where(Stock.symbol.in_(list(concepts_by_symbol))))
        for stock in rows.scalars().all():
            names = sorted({name for name in concepts_by_symbol.get(stock.symbol, []) if name})
            stock.concept = ",".join(names)
            updated_stocks += 1
    await session.flush()
    return {
        "source": source,
        "snapshot_date": snapshot.isoformat(),
        "membership_rows": len(memberships),
        "stock_updates": updated_stocks,
        "concept_count": len({item.concept_code for item in memberships}),
    }


def _first_text(row: dict[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "nat"}:
            return text
    return ""


def _parse_date_value(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        parsed = pd.to_datetime(str(value), errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _parse_bool_value(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "new", "是"}:
        return True
    if text in {"0", "false", "no", "n", "否"}:
        return False
    return None


async def _resolve_symbols(session: AsyncSession, symbols: list[str] | None, *, allow_all: bool) -> list[str]:
    explicit = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
    if explicit:
        return explicit
    if not allow_all:
        raise ValueError("Symbol-scoped relay datasets require symbols, or relay_options.allow_all_symbols=true")
    result = await session.execute(
        select(Stock.symbol)
        .where(Stock.is_delist == 0)
        .order_by(Stock.symbol)
    )
    return [str(row[0]).upper() for row in result.fetchall()]


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _estimate_total(names: list[str], dates: list[date], symbols: list[str], options: dict[str, Any]) -> int:
    total = 0
    for name in names:
        spec = RELAY_DATASET_SPECS[name]
        if name in {"adj_factor", "moneyflow", "stk_auction_replay"}:
            total += max(1, len(symbols) * len(dates))
        elif name == "moneyflow_hsgt":
            total += max(1, len(dates))
        elif name == "hk_hold":
            exchanges = _hsgt_exchanges(options, spec)
            total += max(1, (len(symbols) or len(exchanges)) * len(dates))
        elif name == "fund_portfolio":
            periods = _fund_periods(dates, options, spec)
            selectors = _fund_codes(options) or symbols or ["period"]
            total += max(1, len(periods) * len(selectors))
        elif name in FINANCIAL_STATEMENT_RELAY_DATASETS:
            periods = _statement_periods(options)
            total += max(1, len(symbols) * max(len(periods), 1))
        elif name == "report_rc":
            total += max(1, len(symbols))
        elif name == "block_moneyflow":
            total += max(1, len(dates))
        elif name == "ths_member":
            total += _positive_int(options.get("ths_member_limit")) or int(spec.default_params.get("ths_member_limit", 50))
        elif name == "analyst_rank":
            total += max(1, len({item.year for item in dates}))
        elif name in {"analyst_detail", "analyst_history"}:
            total += _positive_int(options.get("analyst_limit")) or int(spec.default_params.get("analyst_limit", 50))
        elif spec.text_source:
            total += max(1, len(symbols) * len(dates)) if spec.symbol_scoped else max(1, len(dates))
        else:
            total += 1
    return max(total, 1)


async def _sync_symbol_date_dataset(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    symbols: list[str],
    dates: list[date],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    mode = str(options.get("mode") or spec.default_params.get("mode") or "summary")
    for symbol in symbols:
        progress.details["current_symbol"] = symbol
        for current_date in dates:
            progress.details["current_date"] = current_date.isoformat()
            params = {"ts_code": symbol, "trade_date": current_date.strftime("%Y%m%d")}
            if spec.name == "stk_auction_replay":
                params["mode"] = mode
            result = await asyncio.to_thread(client.request, spec.api_name, params)
            metas.append(_meta_dict(result.meta))
            for row in result.rows:
                item = dict(row)
                item.setdefault("ts_code", symbol)
                item.setdefault("trade_date", current_date.strftime("%Y%m%d"))
                if spec.name == "stk_auction_replay":
                    item["mode"] = mode
                rows.append(item)
            progress.current = min(progress.current + 1, progress.total)
            await asyncio.sleep(0)
    return rows, metas


async def _sync_block_moneyflow(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    limit = _positive_int(options.get("block_moneyflow_limit") or options.get("limit")) or int(spec.default_params.get("limit", 5))
    limit = min(max(limit, 1), 100)
    for current_date in dates:
        progress.details["current_date"] = current_date.isoformat()
        result = await asyncio.to_thread(
            client.request,
            spec.api_name,
            {"trade_date": current_date.strftime("%Y%m%d"), "limit": limit},
        )
        metas.append(_meta_dict(result.meta))
        rows.extend(result.rows)
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


async def _sync_moneyflow_hsgt(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    fields = str(options.get("moneyflow_hsgt_fields") or spec.default_params.get("fields") or "")
    for current_date in dates:
        progress.details["current_date"] = current_date.isoformat()
        params: dict[str, Any] = {"trade_date": current_date.strftime("%Y%m%d")}
        if fields:
            params["fields"] = fields
        result = await asyncio.to_thread(client.request, spec.api_name, params)
        metas.append(_meta_dict(result.meta))
        for row in result.rows:
            item = dict(row)
            item.setdefault("trade_date", current_date.strftime("%Y%m%d"))
            rows.append(item)
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


async def _sync_hk_hold(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    symbols: list[str],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    limit = _positive_int(options.get("hk_hold_limit") or options.get("hsgt_hold_limit") or options.get("limit")) or int(spec.default_params.get("limit", 3800))
    limit = min(max(limit, 1), 10000)
    fields = str(options.get("hk_hold_fields") or spec.default_params.get("fields") or "")
    stock_symbols = [_normalize_a_stock_symbol(symbol) for symbol in symbols]
    stock_symbols = [symbol for symbol in stock_symbols if symbol]
    exchanges = _hsgt_exchanges(options, spec)

    for current_date in dates:
        progress.details["current_date"] = current_date.isoformat()
        if stock_symbols:
            for symbol in stock_symbols:
                progress.details["current_symbol"] = symbol
                params: dict[str, Any] = {
                    "ts_code": symbol,
                    "trade_date": current_date.strftime("%Y%m%d"),
                    "limit": limit,
                }
                if fields:
                    params["fields"] = fields
                result = await asyncio.to_thread(client.request, spec.api_name, params)
                metas.append(_meta_dict(result.meta))
                for row in result.rows:
                    item = dict(row)
                    item.setdefault("ts_code", symbol)
                    item.setdefault("trade_date", current_date.strftime("%Y%m%d"))
                    rows.append(item)
                progress.current = min(progress.current + 1, progress.total)
                await asyncio.sleep(0)
            continue

        for exchange in exchanges:
            progress.details["current_exchange"] = exchange
            params = {
                "trade_date": current_date.strftime("%Y%m%d"),
                "exchange": exchange,
                "limit": limit,
            }
            if fields:
                params["fields"] = fields
            result = await asyncio.to_thread(client.request, spec.api_name, params)
            metas.append(_meta_dict(result.meta))
            for row in result.rows:
                item = dict(row)
                item.setdefault("trade_date", current_date.strftime("%Y%m%d"))
                item.setdefault("exchange", exchange)
                rows.append(item)
            progress.current = min(progress.current + 1, progress.total)
            await asyncio.sleep(0)
    return rows, metas


async def _sync_fund_portfolio(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    symbols: list[str],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    limit = _positive_int(options.get("fund_portfolio_limit") or options.get("fund_limit") or options.get("limit")) or int(spec.default_params.get("limit", 5000))
    limit = min(max(limit, 1), 10000)
    fields = str(options.get("fund_portfolio_fields") or spec.default_params.get("fields") or "")
    periods = _fund_periods(dates, options, spec)
    fund_codes = _fund_codes(options)
    stock_symbols = [_normalize_a_stock_symbol(symbol) for symbol in symbols]
    stock_symbols = [symbol for symbol in stock_symbols if symbol]
    allow_period_only = bool(options.get("allow_all_symbols") or options.get("allow_all_funds") or options.get("fund_allow_period_only"))

    if not fund_codes and not stock_symbols and not allow_period_only:
        raise ValueError("fund_portfolio requires symbols, relay_options.fund_codes, or allow_all_symbols=true")

    selectors: list[tuple[str, str | None]]
    if fund_codes:
        selectors = [("ts_code", code) for code in fund_codes]
    elif stock_symbols:
        selectors = [("symbol", symbol) for symbol in stock_symbols]
    else:
        selectors = [("period", None)]

    for period in periods:
        progress.details["current_period"] = period
        for key, value in selectors:
            if key == "ts_code" and value:
                progress.details["current_fund_code"] = value
            elif key == "symbol" and value:
                progress.details["current_symbol"] = value
            params: dict[str, Any] = {"period": period, "limit": limit}
            if value:
                params[key] = value
            if fields:
                params["fields"] = fields
            result = await asyncio.to_thread(client.request, spec.api_name, params)
            metas.append(_meta_dict(result.meta))
            for row in result.rows:
                item = dict(row)
                if key == "ts_code" and value:
                    item.setdefault("ts_code", value)
                elif key == "symbol" and value:
                    item.setdefault("symbol", value)
                item.setdefault("end_date", period)
                rows.append(item)
            progress.current = min(progress.current + 1, progress.total)
            await asyncio.sleep(0)
    return rows, metas


async def _sync_ths_index(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    limit = _positive_int(options.get("ths_index_limit") or options.get("limit")) or int(spec.default_params.get("limit", 1000))
    params = {
        "exchange": options.get("ths_exchange") or spec.default_params.get("exchange", "A"),
        "type": options.get("ths_type") or spec.default_params.get("type", "N"),
        "limit": min(max(limit, 1), 2000),
    }
    result = await asyncio.to_thread(client.request, spec.api_name, params)
    progress.current = min(progress.current + 1, progress.total)
    await asyncio.sleep(0)
    return result.rows, [_meta_dict(result.meta)]


async def _sync_ths_member(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    ths_index_rows: list[dict[str, Any]],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    codes = [str(code).strip() for code in options.get("ths_codes") or [] if str(code).strip()]
    if not codes:
        codes = [str(row.get("ts_code") or row.get("ths_code") or "").strip() for row in ths_index_rows]
    limit = _positive_int(options.get("ths_member_limit")) or int(spec.default_params.get("ths_member_limit", 50))
    codes = [code for code in codes if code][:limit]
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    for code in codes:
        progress.details["current_ths_code"] = code
        result = await asyncio.to_thread(
            client.request,
            spec.api_name,
            {"ts_code": code, "limit": _positive_int(options.get("limit")) or 1000},
        )
        metas.append(_meta_dict(result.meta))
        for row in result.rows:
            item = dict(row)
            item.setdefault("ts_code", code)
            rows.append(item)
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


async def _sync_report_rc(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    symbols: list[str],
    start: date,
    end: date,
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    limit = _positive_int(options.get("report_rc_limit") or options.get("limit")) or int(spec.default_params.get("limit", 100))
    limit = min(max(limit, 1), 500)
    for symbol in symbols:
        progress.details["current_symbol"] = symbol
        params = {
            "ts_code": symbol,
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
            "limit": limit,
        }
        result = await asyncio.to_thread(client.request, spec.api_name, params)
        metas.append(_meta_dict(result.meta))
        for row in result.rows:
            item = dict(row)
            item.setdefault("ts_code", symbol)
            rows.append(item)
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


async def _sync_financial_statement(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    symbols: list[str],
    start: date,
    end: date,
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    limit = _positive_int(options.get(f"{spec.name}_limit") or options.get("statement_limit") or options.get("limit")) or int(spec.default_params.get("limit", 5000))
    limit = min(max(limit, 1), 10000)
    fields = str(options.get(f"{spec.name}_fields") or options.get("statement_fields") or spec.default_params.get("fields") or "")
    report_type = options.get(f"{spec.name}_report_type") or options.get("report_type")
    comp_type = options.get(f"{spec.name}_comp_type") or options.get("comp_type")
    is_calc = options.get("is_calc")
    periods = _statement_periods(options)
    stock_symbols = [_normalize_a_stock_symbol(symbol) for symbol in symbols]
    stock_symbols = [symbol for symbol in stock_symbols if symbol]

    for symbol in stock_symbols:
        progress.details["current_symbol"] = symbol
        period_values = periods or [None]
        for period in period_values:
            if period:
                progress.details["current_period"] = period
            params: dict[str, Any] = {
                "ts_code": symbol,
                "limit": limit,
            }
            if period:
                params["period"] = period
            else:
                params["start_date"] = start.strftime("%Y%m%d")
                params["end_date"] = end.strftime("%Y%m%d")
            if fields:
                params["fields"] = fields
            if report_type not in (None, ""):
                params["report_type"] = report_type
            if comp_type not in (None, ""):
                params["comp_type"] = comp_type
            if is_calc not in (None, ""):
                params["is_calc"] = is_calc
            result = await asyncio.to_thread(client.request, spec.api_name, params)
            metas.append(_meta_dict(result.meta))
            for row in result.rows:
                item = dict(row)
                item.setdefault("ts_code", symbol)
                rows.append(item)
            progress.current = min(progress.current + 1, progress.total)
            await asyncio.sleep(0)
    return rows, metas


async def _sync_analyst_rank(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    years = _analyst_years(dates, options)
    limit = _positive_int(options.get("analyst_rank_limit") or options.get("analyst_limit") or options.get("limit")) or int(spec.default_params.get("limit", 50))
    limit = min(max(limit, 1), 500)
    for year in years:
        progress.details["current_date"] = str(year)
        result = await asyncio.to_thread(client.request, spec.api_name, {"year": str(year), "limit": limit})
        metas.append(_meta_dict(result.meta))
        for row in result.rows:
            item = dict(row)
            item.setdefault("\u5e74\u5ea6", str(year))
            rows.append(item)
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


async def _sync_analyst_followup(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    analyst_rank_rows: list[dict[str, Any]],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    analyst_ids = _analyst_ids_from_options(options) or _analyst_ids_from_rank_rows(analyst_rank_rows, options, spec)
    limit_key = "analyst_detail_limit" if spec.name == "analyst_detail" else "analyst_history_limit"
    limit = _positive_int(options.get(limit_key) or options.get("analyst_followup_limit") or options.get("limit")) or int(spec.default_params.get("limit", 50))
    limit = min(max(limit, 1), 500)
    indicators = _analyst_indicators(spec, options)
    for analyst_id in analyst_ids:
        progress.details["current_analyst_id"] = analyst_id
        for indicator in indicators:
            progress.details["current_indicator"] = indicator
            params = {"analyst_id": analyst_id, "indicator": indicator, "limit": limit}
            result = await asyncio.to_thread(client.request, spec.api_name, params)
            metas.append(_meta_dict(result.meta))
            for row in result.rows:
                item = dict(row)
                item.setdefault("analyst_id", analyst_id)
                item.setdefault("indicator", indicator)
                rows.append(item)
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


def _analyst_years(dates: list[date], options: dict[str, Any]) -> list[int]:
    explicit_years = _parse_int_list(options.get("analyst_years") or options.get("years"))
    if explicit_years:
        return sorted({year for year in explicit_years if 1900 <= year <= 2100})
    if dates:
        return sorted({item.year for item in dates})
    current_year = date.today().year
    return [current_year]


def _analyst_ids_from_options(options: dict[str, Any]) -> list[str]:
    raw = options.get("analyst_ids") or options.get("analyst_id") or []
    if isinstance(raw, str):
        raw_values = [part.strip() for part in raw.replace(";", ",").split(",")]
    elif isinstance(raw, Iterable):
        raw_values = [str(item).strip() for item in raw]
    else:
        raw_values = [str(raw).strip()] if raw else []
    return [value for value in raw_values if value]


def _analyst_ids_from_rank_rows(
    rows: list[dict[str, Any]],
    options: dict[str, Any],
    spec: RelayDatasetSpec,
) -> list[str]:
    limit = _positive_int(options.get("analyst_limit")) or int(spec.default_params.get("analyst_limit", 50))
    ids: list[str] = []
    seen: set[str] = set()
    for row in rows:
        analyst_id = _first_text(row, "analyst_id", "\u5206\u6790\u5e08ID")
        if not analyst_id or analyst_id in seen:
            continue
        seen.add(analyst_id)
        ids.append(analyst_id)
        if len(ids) >= limit:
            break
    return ids


def _analyst_indicators(spec: RelayDatasetSpec, options: dict[str, Any]) -> list[str]:
    if spec.name == "analyst_detail":
        return _string_list(options.get("analyst_detail_indicators")) or [
            str(options.get("analyst_detail_indicator") or spec.default_params.get("indicator") or "\u6700\u65b0\u8ddf\u8e2a\u6210\u5206\u80a1")
        ]
    if spec.name == "analyst_history":
        indicators = _string_list(options.get("analyst_history_indicators"))
        if indicators:
            return indicators
        if bool(options.get("analyst_history_include_index")):
            return ["\u5386\u53f2\u8ddf\u8e2a\u6210\u5206\u80a1", "\u5386\u53f2\u6307\u6570"]
        return [str(options.get("analyst_history_indicator") or spec.default_params.get("indicator") or "\u5386\u53f2\u8ddf\u8e2a\u6210\u5206\u80a1")]
    return [str(options.get("indicator") or spec.default_params.get("indicator") or "\u6700\u65b0\u8ddf\u8e2a\u6210\u5206\u80a1")]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace(";", ",").split(",")]
        return [part for part in parts if part]
    if isinstance(value, Iterable):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _parse_int_list(value: Any) -> list[int]:
    items = _string_list(value)
    years: list[int] = []
    for item in items:
        try:
            years.append(int(item))
        except (TypeError, ValueError):
            continue
    return years


def _hsgt_exchanges(options: dict[str, Any], spec: RelayDatasetSpec) -> list[str]:
    raw = (
        _string_list(options.get("hsgt_exchanges"))
        or _string_list(options.get("hk_hold_exchanges"))
        or _string_list(spec.default_params.get("exchanges"))
    )
    exchanges = [item.upper() for item in raw if item.upper() in {"SH", "SZ", "HK"}]
    return exchanges or ["SH", "SZ"]


def _fund_codes(options: dict[str, Any]) -> list[str]:
    raw = (
        _string_list(options.get("fund_codes"))
        or _string_list(options.get("fund_ts_codes"))
        or _string_list(options.get("fund_code"))
    )
    return [item.upper() for item in raw]


def _statement_periods(options: dict[str, Any]) -> list[str]:
    periods = (
        _string_list(options.get("statement_periods"))
        or _string_list(options.get("financial_periods"))
        or _string_list(options.get("periods"))
        or _string_list(options.get("period"))
    )
    return _valid_periods(periods)


def _fund_periods(dates: list[date], options: dict[str, Any], spec: RelayDatasetSpec) -> list[str]:
    explicit = (
        _string_list(options.get("fund_periods"))
        or _string_list(options.get("periods"))
        or _string_list(options.get("period"))
    )
    period_limit = _positive_int(options.get("fund_period_limit")) or int(spec.default_params.get("period_limit", 8))
    period_limit = min(max(period_limit, 1), 40)
    if explicit:
        return _valid_periods(explicit)[:period_limit]

    end = max(dates) if dates else date.today()
    start = min(dates) if dates else end - timedelta(days=365 * 2)
    periods: list[str] = []
    for year in range(start.year, end.year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            period_date = date(year, month, day)
            if start <= period_date <= end:
                periods.append(period_date.strftime("%Y%m%d"))
    if not periods:
        periods = _recent_quarter_periods(end, period_limit)
    return sorted(set(periods), reverse=True)[:period_limit]


def _recent_quarter_periods(end: date, limit: int) -> list[str]:
    year = end.year
    quarters = [(3, 31), (6, 30), (9, 30), (12, 31)]
    current = [
        date(year, month, day)
        for month, day in quarters
        if date(year, month, day) <= end
    ]
    if not current:
        year -= 1
        current = [date(year, month, day) for month, day in quarters]
    cursor = max(current)
    periods: list[str] = []
    while len(periods) < limit:
        periods.append(cursor.strftime("%Y%m%d"))
        month = cursor.month
        if month == 3:
            cursor = date(cursor.year - 1, 12, 31)
        elif month == 6:
            cursor = date(cursor.year, 3, 31)
        elif month == 9:
            cursor = date(cursor.year, 6, 30)
        else:
            cursor = date(cursor.year, 9, 30)
    return periods


def _valid_periods(values: list[str]) -> list[str]:
    periods: list[str] = []
    for value in values:
        text = str(value).replace("-", "").strip()
        if len(text) == 8 and text.isdigit():
            periods.append(text)
    return sorted(set(periods), reverse=True)


def _bare_stock_code(symbol: str | None) -> str | None:
    normalized = normalize_security_symbol(symbol)
    if not normalized:
        return None
    if "." not in normalized:
        return normalized
    code, _suffix = normalized.rsplit(".", 1)
    return code


async def _sync_text_dataset(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    symbols: list[str],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily_limit = _positive_int(options.get("daily_limit") or options.get("limit")) or int(spec.default_params.get("daily_limit", 200))
    daily_limit = min(max(daily_limit, 1), 500)
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    scoped_symbols = symbols if spec.symbol_scoped else [None]
    for symbol in scoped_symbols:
        if symbol:
            progress.details["current_symbol"] = symbol
        for current_date in dates:
            progress.details["current_date"] = current_date.isoformat()
            params = _text_params(spec.name, current_date, daily_limit, options, symbol=symbol)
            result = await asyncio.to_thread(client.request, spec.api_name, params)
            metas.append(_meta_dict(result.meta))
            for row in result.rows[:daily_limit]:
                item = dict(row)
                if symbol:
                    item.setdefault("symbol", symbol)
                rows.append(item)
            progress.current = min(progress.current + 1, progress.total)
            await asyncio.sleep(0)
    return rows, metas


def _text_params(name: str, current_date: date, limit: int, options: dict[str, Any], *, symbol: str | None = None) -> dict[str, Any]:
    ymd = current_date.strftime("%Y%m%d")
    iso = current_date.isoformat()
    if name == "anns_d":
        return {"ann_date": ymd, "limit": limit, "ts_code": symbol or options.get("symbol") or options.get("ts_code")}
    if name == "stock_zh_a_disclosure_report_cninfo":
        return {"date": iso, "limit": limit, "symbol": _bare_stock_code(symbol) or options.get("symbol")}
    if name == "stock_research_report_em":
        return {"date": iso, "limit": limit, "symbol": _bare_stock_code(symbol) or options.get("symbol")}
    if name == "news_cctv":
        return {"date": ymd, "limit": limit}
    if name == "news_economic_baidu":
        return {"date": ymd, "limit": limit}
    return {"start_date": ymd, "end_date": ymd, "limit": limit}


def _normalize_dataset_rows(name: str, rows: list[dict[str, Any]], options: dict[str, Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    if name == "adj_factor":
        return _normalize_common(rows, date_columns=["trade_date"], symbol_from="ts_code")
    if name == "moneyflow":
        frame = _normalize_common(rows, date_columns=["trade_date"], symbol_from="ts_code")
        _derive_moneyflow(frame)
        return frame
    if name == "moneyflow_hsgt":
        return _normalize_moneyflow_hsgt(rows)
    if name == "hk_hold":
        return _normalize_hk_hold(rows)
    if name == "fund_portfolio":
        return _normalize_fund_portfolio(rows)
    if name in FINANCIAL_STATEMENT_RELAY_DATASETS:
        return _normalize_financial_statement(name, rows)
    if name == "block_moneyflow":
        frame = _normalize_common(rows, date_columns=["trade_date"])
        if "ts_code" in frame.columns and "block_code" not in frame.columns:
            frame["block_code"] = frame["ts_code"].astype(str)
        return frame
    if name == "stk_auction_replay":
        frame = _normalize_common(rows, date_columns=["trade_date"], symbol_from="ts_code")
        if "vol" in frame.columns and "volume" not in frame.columns:
            frame["volume"] = pd.to_numeric(frame["vol"], errors="coerce")
        dt_col = "trade_time" if "trade_time" in frame.columns else "start_time"
        if dt_col in frame.columns:
            frame["datetime"] = pd.to_datetime(frame[dt_col], errors="coerce")
        elif "trade_date" in frame.columns:
            frame["datetime"] = pd.to_datetime(frame["trade_date"].astype(str) + " 09:25:00", errors="coerce")
        frame["data_level"] = frame.get("mode", options.get("mode") or "summary")
        frame["is_complete"] = frame["datetime"].notna()
        return frame[frame["datetime"].notna()].copy()
    if name == "ths_index":
        frame = _normalize_common(rows, date_columns=["list_date"])
        if "ts_code" in frame.columns and "ths_code" not in frame.columns:
            frame["ths_code"] = frame["ts_code"].astype(str)
        frame["snapshot_date"] = pd.Timestamp(date.today())
        return frame
    if name == "ths_member":
        frame = _normalize_common(rows, date_columns=["in_date", "out_date"])
        if "ts_code" in frame.columns and "ths_code" not in frame.columns:
            frame["ths_code"] = frame["ts_code"].astype(str)
        if "con_code" in frame.columns and "symbol" not in frame.columns:
            frame["symbol"] = frame["con_code"].astype(str)
        frame["snapshot_date"] = pd.Timestamp(date.today())
        return frame
    if name == "report_rc":
        return _normalize_report_rc(rows)
    if name == "analyst_rank":
        return _normalize_analyst_rank(rows)
    if name == "analyst_detail":
        return _normalize_analyst_detail(rows)
    if name == "analyst_history":
        return _normalize_analyst_history(rows)
    if name in {"anns_d", "stock_zh_a_disclosure_report_cninfo"}:
        return _normalize_text(rows, storage="announcements")
    if name == "stock_research_report_em":
        return _normalize_text(rows, storage="research_reports")
    if name in {"news_cctv", "news_economic_baidu", "major_news"}:
        return _normalize_text(rows, storage="market_news", source_api=name)
    return _normalize_common(rows)


def _normalize_common(
    rows: list[dict[str, Any]],
    *,
    date_columns: list[str] | None = None,
    symbol_from: str | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    if symbol_from and symbol_from in frame.columns and "symbol" not in frame.columns:
        frame["symbol"] = frame[symbol_from].astype(str)
    for col in date_columns or []:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col].astype(str), errors="coerce")
    frame["source"] = "indevs_tushare_relay"
    frame["source_api"] = frame.get("source_api", "")
    frame["ingested_at"] = pd.Timestamp(datetime.now())
    for col in frame.columns:
        if col not in {"symbol", "ts_code", "con_code", "ths_code", "block_code", "source", "source_api", "mode"}:
            frame[col] = _coerce_numeric_if_possible(frame[col])
    return frame


def _normalize_moneyflow_hsgt(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _normalize_common(rows, date_columns=["trade_date"])
    if frame.empty:
        return frame
    if "north_money" in frame.columns and "north_money_million" not in frame.columns:
        frame["north_money_million"] = pd.to_numeric(frame["north_money"], errors="coerce")
    if "south_money" in frame.columns and "south_money_million" not in frame.columns:
        frame["south_money_million"] = pd.to_numeric(frame["south_money"], errors="coerce")
    if "trade_date" in frame.columns:
        frame = frame[frame["trade_date"].notna()].copy()
    return frame


def _normalize_hk_hold(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _normalize_common(rows, date_columns=["trade_date"], symbol_from="ts_code")
    if frame.empty:
        return frame
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_a_stock_symbol)
    if "vol" in frame.columns and "holding_volume" not in frame.columns:
        frame["holding_volume"] = pd.to_numeric(frame["vol"], errors="coerce")
    if "ratio" in frame.columns and "holding_ratio" not in frame.columns:
        frame["holding_ratio"] = pd.to_numeric(frame["ratio"], errors="coerce")
    if "exchange" in frame.columns:
        frame["exchange"] = frame["exchange"].astype(str).str.upper()
    if "trade_date" in frame.columns:
        frame = frame[frame["trade_date"].notna()].copy()
    return frame


def _normalize_fund_portfolio(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _normalize_common(rows, date_columns=["ann_date", "end_date"])
    if frame.empty:
        return frame
    _copy_first(frame, "fund_code", ["fund_code", "ts_code"])
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_a_stock_symbol)
    if "end_date" not in frame.columns and "period" in frame.columns:
        frame["end_date"] = _parse_date_series(frame["period"])
    if "ann_date" not in frame.columns:
        frame["ann_date"] = pd.NaT
    if "end_date" in frame.columns:
        frame = frame[frame["end_date"].notna()].copy()
    return frame


def _normalize_financial_statement(name: str, rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _normalize_common(rows, date_columns=["ann_date", "f_ann_date", "end_date"], symbol_from="ts_code")
    if frame.empty:
        return frame
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_a_stock_symbol)
    if "f_ann_date" not in frame.columns:
        frame["f_ann_date"] = pd.NaT
    if "ann_date" in frame.columns:
        frame["f_ann_date"] = frame["f_ann_date"].fillna(frame["ann_date"])
    if "end_date" in frame.columns:
        frame["f_ann_date"] = frame["f_ann_date"].fillna(frame["end_date"])

    if name == "income":
        _copy_first(frame, "rd_expense", ["rd_expense", "rd_exp"])
        _copy_first(frame, "net_profit_attributable", ["net_profit_attributable", "n_income_attr_p"])
    elif name == "balancesheet":
        _copy_first(frame, "intangible_assets", ["intangible_assets", "intan_assets"])
        _copy_first(frame, "rd_capitalized", ["rd_capitalized", "r_and_d"])
        _copy_first(frame, "total_equity", ["total_equity", "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int"])
    elif name == "cashflow":
        _copy_first(frame, "net_operate_cash_flow", ["net_operate_cash_flow", "n_cashflow_act", "im_net_cashflow_oper_act"])
        _copy_first(frame, "capex", ["capex", "c_pay_acq_const_fiolta"])

    for col in (
        "rd_expense",
        "net_profit_attributable",
        "intangible_assets",
        "rd_capitalized",
        "total_equity",
        "net_operate_cash_flow",
        "capex",
    ):
        if col in frame.columns:
            frame[col] = _coerce_numeric_if_possible(frame[col])

    frame = frame[frame["f_ann_date"].notna()].copy()
    return frame


def _normalize_report_rc(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _normalize_common(rows, date_columns=["report_date", "create_time"], symbol_from="ts_code")
    if frame.empty:
        return frame
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_a_stock_symbol)
    title_col = _first_present(frame, ["report_title", "title"])
    frame["title_hash"] = frame[title_col].map(_hash_title) if title_col else ""
    if "report_date" in frame.columns:
        frame = frame[frame["report_date"].notna()].copy()
    return frame


def _normalize_analyst_rank(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    _copy_first(frame, "analyst_name", ["analyst_name", "\u5206\u6790\u5e08\u540d\u79f0"])
    _copy_first(frame, "org_name", ["org_name", "\u5206\u6790\u5e08\u5355\u4f4d"])
    _copy_first(frame, "annual_index", ["annual_index", "\u5e74\u5ea6\u6307\u6570"])
    _copy_first(frame, "return_12m", ["return_12m", "12\u4e2a\u6708\u6536\u76ca\u7387"])
    _copy_first(frame, "analyst_id", ["analyst_id", "\u5206\u6790\u5e08ID"])
    _copy_first(frame, "industry", ["industry", "\u884c\u4e1a"])
    _copy_first(frame, "update_date", ["update_date", "\u66f4\u65b0\u65e5\u671f"])
    _copy_first(frame, "year", ["year", "\u5e74\u5ea6"])
    frame["update_date"] = _parse_date_series(frame["update_date"]) if "update_date" in frame.columns else pd.Timestamp(date.today())
    return _finish_relay_frame(frame, date_columns=["update_date"])


def _normalize_analyst_detail(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    _copy_first(frame, "stock_code", ["stock_code", "\u80a1\u7968\u4ee3\u7801", "symbol"])
    _copy_first(frame, "stock_name", ["stock_name", "\u80a1\u7968\u540d\u79f0", "name"])
    _copy_first(frame, "in_date", ["in_date", "\u8c03\u5165\u65e5\u671f"])
    _copy_first(frame, "latest_rating_date", ["latest_rating_date", "\u6700\u65b0\u8bc4\u7ea7\u65e5\u671f"])
    _copy_first(frame, "rating", ["rating", "\u5f53\u524d\u8bc4\u7ea7\u540d\u79f0"])
    _copy_first(frame, "latest_price", ["latest_price", "\u6700\u65b0\u4ef7\u683c"])
    _copy_first(frame, "stage_return", ["stage_return", "\u9636\u6bb5\u6da8\u8dcc\u5e45"])
    frame["symbol"] = frame["stock_code"].map(_normalize_a_stock_symbol) if "stock_code" in frame.columns else ""
    frame["in_date"] = _parse_date_series(frame["in_date"]) if "in_date" in frame.columns else pd.NaT
    frame["latest_rating_date"] = _parse_date_series(frame["latest_rating_date"]) if "latest_rating_date" in frame.columns else frame["in_date"]
    return _finish_relay_frame(frame, date_columns=["in_date", "latest_rating_date"])


def _normalize_analyst_history(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    _copy_first(frame, "stock_code", ["stock_code", "\u80a1\u7968\u4ee3\u7801", "symbol"])
    _copy_first(frame, "stock_name", ["stock_name", "\u80a1\u7968\u540d\u79f0", "name"])
    _copy_first(frame, "in_date", ["in_date", "\u8c03\u5165\u65e5\u671f", "date"])
    _copy_first(frame, "out_date", ["out_date", "\u8c03\u51fa\u65e5\u671f"])
    _copy_first(frame, "rating_at_in", ["rating_at_in", "\u8c03\u5165\u65f6\u8bc4\u7ea7\u540d\u79f0", "rating"])
    _copy_first(frame, "out_reason", ["out_reason", "\u8c03\u51fa\u539f\u56e0"])
    _copy_first(frame, "cumulative_return", ["cumulative_return", "\u7d2f\u8ba1\u6da8\u8dcc\u5e45"])
    if "stock_code" in frame.columns:
        frame["symbol"] = frame["stock_code"].map(_normalize_a_stock_symbol)
    elif "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_a_stock_symbol)
    if "in_date" in frame.columns:
        frame["in_date"] = _parse_date_series(frame["in_date"])
    else:
        frame["in_date"] = pd.Timestamp(date.today())
    if "out_date" in frame.columns:
        frame["out_date"] = _parse_date_series(frame["out_date"])
    return _finish_relay_frame(frame, date_columns=["in_date", "out_date"])


def _copy_first(frame: pd.DataFrame, target: str, names: list[str]) -> None:
    source = _first_present(frame, names)
    if source:
        frame[target] = frame[source]


def _finish_relay_frame(frame: pd.DataFrame, *, date_columns: list[str]) -> pd.DataFrame:
    frame = frame.copy()
    frame["source"] = "indevs_tushare_relay"
    frame["source_api"] = frame.get("source_api", "")
    frame["ingested_at"] = pd.Timestamp(datetime.now())
    for col in date_columns:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")
    for col in frame.columns:
        if col not in {"symbol", "stock_code", "stock_name", "analyst_id", "analyst_name", "org_name", "industry", "indicator", "rating", "rating_at_in", "out_reason", "source", "source_api"}:
            frame[col] = _coerce_numeric_if_possible(frame[col])
    return frame


def _normalize_a_stock_symbol(symbol: Any) -> str:
    normalized = normalize_security_symbol(str(symbol) if symbol is not None else "")
    if not normalized:
        return ""
    if "." in normalized:
        return normalized
    code = normalized.strip()
    if len(code) == 6 and code.isdigit():
        if code.startswith(("6", "9")):
            return f"{code}.SH"
        if code.startswith(("0", "2", "3")):
            return f"{code}.SZ"
        if code.startswith(("4", "8")):
            return f"{code}.BJ"
    return normalized


def _derive_moneyflow(frame: pd.DataFrame) -> None:
    buy_amount_cols = [col for col in ("buy_sm_amount", "buy_md_amount", "buy_lg_amount", "buy_elg_amount") if col in frame.columns]
    sell_amount_cols = [col for col in ("sell_sm_amount", "sell_md_amount", "sell_lg_amount", "sell_elg_amount") if col in frame.columns]
    buy_vol_cols = [col for col in ("buy_sm_vol", "buy_md_vol", "buy_lg_vol", "buy_elg_vol") if col in frame.columns]
    sell_vol_cols = [col for col in ("sell_sm_vol", "sell_md_vol", "sell_lg_vol", "sell_elg_vol") if col in frame.columns]
    if "net_mf_amount" not in frame.columns and buy_amount_cols and sell_amount_cols:
        frame["net_mf_amount"] = frame[buy_amount_cols].sum(axis=1, skipna=True) - frame[sell_amount_cols].sum(axis=1, skipna=True)
    if "net_mf_vol" not in frame.columns and buy_vol_cols and sell_vol_cols:
        frame["net_mf_vol"] = frame[buy_vol_cols].sum(axis=1, skipna=True) - frame[sell_vol_cols].sum(axis=1, skipna=True)


def _normalize_text(rows: list[dict[str, Any]], *, storage: str, source_api: str | None = None) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    title_col = _first_present(frame, ["title", "\u516c\u544a\u6807\u9898", "\u62a5\u544a\u540d\u79f0", "\u62a5\u544a\u6807\u9898"])
    url_col = _first_present(frame, ["url", "\u516c\u544a\u94fe\u63a5", "\u62a5\u544aPDF\u94fe\u63a5", "link"])
    symbol_col = _first_present(frame, ["ts_code", "symbol", "\u4ee3\u7801", "\u80a1\u7968\u4ee3\u7801"])
    if title_col:
        frame["title"] = frame[title_col].astype(str).str.strip()
    else:
        frame["title"] = ""
    if url_col:
        frame["source_url"] = frame[url_col].astype(str).str.strip()
    else:
        frame["source_url"] = ""
    if symbol_col and "symbol" not in frame.columns:
        frame["symbol"] = frame[symbol_col].astype(str)
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].map(_normalize_a_stock_symbol)
    if storage == "announcements":
        date_col = _first_present(frame, ["ann_date", "\u516c\u544a\u65f6\u95f4", "\u516c\u544a\u65e5\u671f", "date"])
        frame["ann_date"] = _parse_date_series(frame[date_col]) if date_col else pd.Timestamp(date.today())
    elif storage == "research_reports":
        date_col = _first_present(frame, ["report_date", "\u65e5\u671f", "date"])
        frame["report_date"] = _parse_date_series(frame[date_col]) if date_col else pd.Timestamp(date.today())
    else:
        date_col = _first_present(frame, ["publish_time", "pub_time", "datetime", "date", "\u65e5\u671f"])
        frame["publish_time"] = pd.to_datetime(frame[date_col], errors="coerce") if date_col else pd.Timestamp(datetime.now())
    frame = frame[frame["title"].notna() & (frame["title"].str.len() > 0)].copy()
    if source_api:
        frame["source_api"] = source_api
    elif "source_api" not in frame.columns:
        frame["source_api"] = storage
    frame["source"] = "indevs_tushare_relay"
    frame["quality_flags"] = frame.apply(_quality_flags, axis=1)
    frame = frame[~frame["quality_flags"].str.contains("empty_title", na=False)].copy()
    frame["title_hash"] = frame["title"].map(_hash_title)
    frame["ingested_at"] = pd.Timestamp(datetime.now())
    return frame


def _first_present(frame: pd.DataFrame, names: list[str]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _parse_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str), errors="coerce")


def _quality_flags(row: pd.Series) -> str:
    flags: list[str] = []
    title = str(row.get("title") or "").strip()
    url = str(row.get("source_url") or "").strip().lower()
    if not title:
        flags.append("empty_title")
    if url and any(marker in url for marker in ("javascript:", "void(0)", "#")):
        flags.append("bad_url")
    if any(marker in title.lower() for marker in ("广告", "导航", "app下载")):
        flags.append("low_quality")
    return ",".join(flags)


def _hash_title(title: str) -> str:
    return hashlib.sha1(str(title).strip().encode("utf-8")).hexdigest()[:16]


def _coerce_numeric_if_possible(series: pd.Series) -> pd.Series:
    if series.dtype != object and not pd.api.types.is_string_dtype(series.dtype):
        return series
    converted = pd.to_numeric(series.replace("", math.nan), errors="coerce")
    non_empty = series.replace("", math.nan).notna()
    if non_empty.any() and converted.notna().sum() / max(int(non_empty.sum()), 1) >= 0.8:
        return converted
    return series


def _meta_dict(meta: TushareRelayMeta) -> dict[str, Any]:
    return {
        "api_name": meta.api_name,
        "base_url": meta.base_url,
        "status_code": meta.status_code,
        "elapsed_ms": meta.elapsed_ms,
        "cache": meta.cache,
        "cache_bucket": meta.cache_bucket,
        "request_id": meta.request_id,
        "attempt": meta.attempt,
    }


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
