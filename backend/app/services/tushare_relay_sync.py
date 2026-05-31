from __future__ import annotations

import asyncio
import hashlib
import calendar
import copy
import math
import time
from dataclasses import dataclass, field
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
from app.services.tushare_relay import TushareRelayClient, TushareRelayError, TushareRelayMeta


CATALOG_CACHE_TTL_SECONDS = 300
COVERAGE_CACHE_TTL_SECONDS = 300
_CATALOG_CACHE: dict[str, Any] = {"expires_at": 0.0, "value": None}
_COVERAGE_CACHE: dict[tuple[str, str, bool], tuple[float, dict[str, Any]]] = {}
_FAST_COVERAGE_DATASETS = {"klines_daily", "klines_minute", "factor_values"}


STRUCTURED_RELAY_DATASETS = (
    "adj_factor",
    "moneyflow",
    "ths_index",
    "ths_member",
    "block_moneyflow",
    "stk_auction_replay",
)

TEXT_RELAY_DATASETS = (
    "anns_d",
    "stock_zh_a_disclosure_report_cninfo",
    "stock_research_report_em",
    "news_cctv",
    "news_economic_baidu",
    "major_news",
)


@dataclass(frozen=True)
class RelayDatasetSpec:
    name: str
    display_name: str
    api_name: str
    storage_dataset: str
    date_col: str
    category: str
    description: str
    recommended_frequency: str
    risk_level: str
    requires_qmt: bool = False
    requires_relay_key: bool = True
    symbol_scoped: bool = False
    date_scoped: bool = True
    default_enabled: bool = False
    text_source: bool = False
    default_params: dict[str, Any] = field(default_factory=dict)


RELAY_DATASET_SPECS: dict[str, RelayDatasetSpec] = {
    "adj_factor": RelayDatasetSpec(
        name="adj_factor",
        display_name="复权因子",
        api_name="adj_factor",
        storage_dataset="adj_factors",
        date_col="trade_date",
        category="relay_structured",
        description="A 股复权因子，作为价格复权和收益校验的基础数据。",
        recommended_frequency="daily",
        risk_level="low",
        symbol_scoped=True,
        default_enabled=True,
    ),
    "moneyflow": RelayDatasetSpec(
        name="moneyflow",
        display_name="个股资金流",
        api_name="moneyflow",
        storage_dataset="moneyflow",
        date_col="trade_date",
        category="relay_structured",
        description="按大小单拆分的个股资金流，可派生净流入金额和净流入量。",
        recommended_frequency="daily",
        risk_level="medium",
        symbol_scoped=True,
        default_enabled=True,
    ),
    "ths_index": RelayDatasetSpec(
        name="ths_index",
        display_name="同花顺板块字典",
        api_name="ths_index",
        storage_dataset="ths_index",
        date_col="snapshot_date",
        category="relay_structured",
        description="同花顺行业、概念等板块字典，按快照日期留存。",
        recommended_frequency="weekly",
        risk_level="medium",
        date_scoped=False,
        default_enabled=True,
        default_params={"exchange": "A", "type": "N", "limit": 1000},
    ),
    "ths_member": RelayDatasetSpec(
        name="ths_member",
        display_name="同花顺板块成分",
        api_name="ths_member",
        storage_dataset="ths_member",
        date_col="snapshot_date",
        category="relay_structured",
        description="同花顺板块成分，用于板块暴露和主题池分析。",
        recommended_frequency="weekly",
        risk_level="medium",
        date_scoped=False,
        default_enabled=True,
        default_params={"limit": 1000, "ths_member_limit": 50},
    ),
    "block_moneyflow": RelayDatasetSpec(
        name="block_moneyflow",
        display_name="板块资金流",
        api_name="block_moneyflow",
        storage_dataset="block_moneyflow",
        date_col="trade_date",
        category="relay_structured",
        description="板块维度资金流，当前接口大 limit 不稳定，默认小批量采集。",
        recommended_frequency="daily",
        risk_level="medium",
        default_enabled=True,
        default_params={"limit": 5},
    ),
    "stk_auction_replay": RelayDatasetSpec(
        name="stk_auction_replay",
        display_name="集合竞价回放",
        api_name="stk_auction_replay",
        storage_dataset="auction_replay",
        date_col="datetime",
        category="relay_structured",
        description="09:15-09:30 集合竞价摘要或序列，独立于普通分钟 K 落库。",
        recommended_frequency="daily",
        risk_level="medium",
        symbol_scoped=True,
        default_enabled=True,
        default_params={"mode": "summary"},
    ),
    "anns_d": RelayDatasetSpec(
        name="anns_d",
        display_name="Tushare 公告",
        api_name="anns_d",
        storage_dataset="announcements",
        date_col="ann_date",
        category="relay_text",
        description="上市公司公告补充源，默认仅最近 7 天并限量采集。",
        recommended_frequency="daily",
        risk_level="high",
        symbol_scoped=False,
        default_enabled=False,
        text_source=True,
        default_params={"daily_limit": 200},
    ),
    "stock_zh_a_disclosure_report_cninfo": RelayDatasetSpec(
        name="stock_zh_a_disclosure_report_cninfo",
        display_name="巨潮公告",
        api_name="stock_zh_a_disclosure_report_cninfo",
        storage_dataset="announcements",
        date_col="ann_date",
        category="relay_text",
        description="巨潮公告补充源，必须带日期或标的过滤，默认不做全量回灌。",
        recommended_frequency="manual",
        risk_level="high",
        default_enabled=False,
        text_source=True,
        default_params={"daily_limit": 200},
    ),
    "stock_research_report_em": RelayDatasetSpec(
        name="stock_research_report_em",
        display_name="东方财富研报",
        api_name="stock_research_report_em",
        storage_dataset="research_reports",
        date_col="report_date",
        category="relay_text",
        description="研报标题和链接补充源，先用于查询回溯，不直接进入因子。",
        recommended_frequency="manual",
        risk_level="high",
        default_enabled=False,
        text_source=True,
        default_params={"daily_limit": 200},
    ),
    "news_cctv": RelayDatasetSpec(
        name="news_cctv",
        display_name="央视新闻",
        api_name="news_cctv",
        storage_dataset="market_news",
        date_col="publish_time",
        category="relay_text",
        description="宏观新闻补充源，限量采集并保留来源。",
        recommended_frequency="manual",
        risk_level="high",
        default_enabled=False,
        text_source=True,
        default_params={"daily_limit": 200},
    ),
    "news_economic_baidu": RelayDatasetSpec(
        name="news_economic_baidu",
        display_name="百度财经日历",
        api_name="news_economic_baidu",
        storage_dataset="market_news",
        date_col="publish_time",
        category="relay_text",
        description="经济日历补充源，适合宏观事件查询，不直接进入因子。",
        recommended_frequency="manual",
        risk_level="high",
        default_enabled=False,
        text_source=True,
        default_params={"daily_limit": 200},
    ),
    "major_news": RelayDatasetSpec(
        name="major_news",
        display_name="重大新闻",
        api_name="major_news",
        storage_dataset="market_news",
        date_col="publish_time",
        category="relay_text",
        description="市场重大新闻补充源，噪声较高，默认仅限量查询。",
        recommended_frequency="manual",
        risk_level="high",
        default_enabled=False,
        text_source=True,
        default_params={"daily_limit": 200},
    ),
}


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
                "name": "relay_text",
                "display_name": "新闻公告补充",
                "description": "高噪声文本源，仅限量采集，默认不进入一键同步。",
                "sync_types": [],
                "relay_datasets": list(TEXT_RELAY_DATASETS),
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
        row = get_duckdb().execute(
            f"""
            SELECT count(*) AS row_count, min({date_col}) AS min_date, max({date_col}) AS max_date
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

    symbol_scoped = any(RELAY_DATASET_SPECS[name].symbol_scoped for name in requested)
    resolved_symbols = await _resolve_symbols(session, symbols, allow_all=bool(options.get("allow_all_symbols"))) if symbol_scoped else []
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
            "datasets": {},
        }
    )

    ths_index_rows: list[dict[str, Any]] = []
    ths_member_rows: list[dict[str, Any]] = []
    for dataset_name in requested:
        spec = RELAY_DATASET_SPECS[dataset_name]
        dataset_rows: list[dict[str, Any]] = []
        dataset_meta: list[dict[str, Any]] = []
        progress.details["current_dataset"] = dataset_name
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
            elif spec.text_source:
                dataset_rows, dataset_meta = await _sync_text_dataset(client, spec, dates=dates, options=options, progress=progress)

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
        except Exception as exc:
            progress.failed_count += 1
            progress.details["datasets"][dataset_name] = {
                "error": str(exc),
                "storage_dataset": spec.storage_dataset,
                "calls": dataset_meta[-10:],
            }
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
        elif name == "block_moneyflow":
            total += max(1, len(dates))
        elif name == "ths_member":
            total += _positive_int(options.get("ths_member_limit")) or int(spec.default_params.get("ths_member_limit", 50))
        elif spec.text_source:
            total += max(1, len(dates))
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


async def _sync_text_dataset(
    client: TushareRelayClient,
    spec: RelayDatasetSpec,
    *,
    dates: list[date],
    options: dict[str, Any],
    progress: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily_limit = _positive_int(options.get("daily_limit") or options.get("limit")) or int(spec.default_params.get("daily_limit", 200))
    daily_limit = min(max(daily_limit, 1), 500)
    rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    for current_date in dates:
        progress.details["current_date"] = current_date.isoformat()
        params = _text_params(spec.name, current_date, daily_limit, options)
        result = await asyncio.to_thread(client.request, spec.api_name, params)
        metas.append(_meta_dict(result.meta))
        rows.extend(result.rows[:daily_limit])
        progress.current = min(progress.current + 1, progress.total)
        await asyncio.sleep(0)
    return rows, metas


def _text_params(name: str, current_date: date, limit: int, options: dict[str, Any]) -> dict[str, Any]:
    ymd = current_date.strftime("%Y%m%d")
    iso = current_date.isoformat()
    if name == "anns_d":
        return {"ann_date": ymd, "limit": limit, "ts_code": options.get("symbol") or options.get("ts_code")}
    if name == "stock_zh_a_disclosure_report_cninfo":
        return {"date": iso, "limit": limit, "symbol": options.get("symbol")}
    if name == "stock_research_report_em":
        return {"date": iso, "limit": limit, "symbol": options.get("symbol")}
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
    title_col = _first_present(frame, ["title", "公告标题", "报告名称", "报告标题"])
    url_col = _first_present(frame, ["url", "公告链接", "报告PDF链接", "link"])
    symbol_col = _first_present(frame, ["ts_code", "symbol", "代码", "股票代码"])
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
    if storage == "announcements":
        date_col = _first_present(frame, ["ann_date", "公告时间", "公告日期", "date"])
        frame["ann_date"] = _parse_date_series(frame[date_col]) if date_col else pd.Timestamp(date.today())
    elif storage == "research_reports":
        date_col = _first_present(frame, ["report_date", "日期", "date"])
        frame["report_date"] = _parse_date_series(frame[date_col]) if date_col else pd.Timestamp(date.today())
    else:
        date_col = _first_present(frame, ["publish_time", "pub_time", "datetime", "date", "日期"])
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
    if series.dtype != object:
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
