"""Factor definitions and Parquet-backed factor value cache."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.data_stores.parquet_store import _list_param, _sql_literal
from app.db.duckdb import get_duckdb
from app.services.factor_catalog import (
    get_catalog_definition,
    get_catalog_group,
    list_catalog_definitions,
    list_catalog_groups,
)
from app.services.factor_precompute_runtime import precompute_memory_policy

CUSTOM_FACTOR_CATEGORY = "custom"
CUSTOM_FACTOR_GROUP_NAME = "custom_factor_library"
DEFAULT_PRECOMPUTE_BATCH_ROWS = precompute_memory_policy().batch_rows


@dataclass(frozen=True)
class FactorDefinition:
    """Metadata shared by factor research UI, precompute, and strategy reuse."""

    name: str
    display_name: str
    factor_type: str
    category: str
    frequency: str
    description: str
    unit: str = ""
    as_of_time: str | None = None
    params_schema: dict[str, Any] = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    lookback_days: int = 0
    point_in_time_safe: bool = True
    source: str = "parquet"
    version: str = "v1"
    data_policy: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


FACTOR_DEFINITIONS: dict[str, FactorDefinition] = {
    "market_cap": FactorDefinition(
        name="market_cap",
        display_name="市值",
        factor_type="indicator",
        category="valuation",
        frequency="daily",
        unit="万元",
        description="Point-in-time market cap used by cross-sectional ranking.",
        dependencies=["stock_daily_basic.circ_mv", "stock_daily_basic.total_mv"],
        lookback_days=370,
        data_policy={"price_time": "previous_close_or_vendor_daily_basic"},
    ),
    "market_cap_rank": FactorDefinition(
        name="market_cap_rank",
        display_name="市值排名",
        factor_type="factor",
        category="valuation",
        frequency="daily",
        unit="rank",
        description="Ascending market-cap rank inside the selected universe.",
        dependencies=["market_cap", "index_components"],
        lookback_days=370,
    ),
    "is_st": FactorDefinition(
        name="is_st",
        display_name="ST 过滤",
        factor_type="state",
        category="status",
        frequency="daily",
        unit="bool",
        description="1 when a stock should be excluded by ST/delist/name-change rules.",
        dependencies=["stocks", "stock_name_changes"],
    ),
    "is_paused": FactorDefinition(
        name="is_paused",
        display_name="停牌过滤",
        factor_type="state",
        category="status",
        frequency="timer",
        as_of_time="10:30",
        unit="bool",
        description="1 when the stock has no usable timer bar.",
        params_schema={"time": {"type": "string", "default": "10:30"}},
        dependencies=["klines_minute_timer", "klines_minute"],
    ),
    "is_limit_up": FactorDefinition(
        name="is_limit_up",
        display_name="涨停过滤",
        factor_type="state",
        category="status",
        frequency="timer",
        as_of_time="10:30",
        unit="bool",
        description="1 when timer price is at or above adjusted up-limit.",
        params_schema={"time": {"type": "string", "default": "10:30"}},
        dependencies=["stock_limit_prices", "klines_minute_timer"],
    ),
    "is_limit_down": FactorDefinition(
        name="is_limit_down",
        display_name="跌停过滤",
        factor_type="state",
        category="status",
        frequency="timer",
        as_of_time="10:30",
        unit="bool",
        description="1 when timer price is at or below adjusted down-limit.",
        params_schema={"time": {"type": "string", "default": "10:30"}},
        dependencies=["stock_limit_prices", "klines_minute_timer"],
    ),
    "yesterday_limit_up": FactorDefinition(
        name="yesterday_limit_up",
        display_name="昨涨停",
        factor_type="state",
        category="status",
        frequency="daily",
        unit="bool",
        description="1 when the previous trading day's close was at the up-limit.",
        dependencies=["stock_limit_prices", "klines_daily"],
        lookback_days=5,
    ),
    "v4gv": FactorDefinition(
        name="v4gv",
        display_name="V4GV",
        factor_type="indicator",
        category="technical",
        frequency="daily",
        description="V4GV technical indicator calculated from daily OHLC windows.",
        dependencies=["klines_daily"],
        lookback_days=140,
    ),
    "v4gv_signal": FactorDefinition(
        name="v4gv_signal",
        display_name="V4GV 信号线",
        factor_type="indicator",
        category="technical",
        frequency="daily",
        description="V4GV signal line.",
        dependencies=["v4gv"],
        lookback_days=140,
    ),
    "macd_positive": FactorDefinition(
        name="macd_positive",
        display_name="MACD 正向",
        factor_type="signal",
        category="technical",
        frequency="daily",
        unit="bool",
        description="1 when MACD is above signal and positive.",
        dependencies=["klines_daily.close"],
        lookback_days=80,
    ),
    "indicator_buy_signal": FactorDefinition(
        name="indicator_buy_signal",
        display_name="指标买入信号",
        factor_type="signal",
        category="technical",
        frequency="daily",
        unit="bool",
        description="1 when V4GV > signal, V4GV > 0, and MACD is positive.",
        dependencies=["v4gv", "v4gv_signal", "macd_positive"],
        lookback_days=140,
    ),
    "v4gv_dead_cross": FactorDefinition(
        name="v4gv_dead_cross",
        display_name="V4GV 死叉",
        factor_type="signal",
        category="technical",
        frequency="daily",
        unit="bool",
        description="1 when V4GV < signal and MACD is not positive.",
        dependencies=["v4gv", "v4gv_signal", "macd_positive"],
        lookback_days=140,
    ),
    "cum_volume_at_time": FactorDefinition(
        name="cum_volume_at_time",
        display_name="指定时点累计成交量",
        factor_type="indicator",
        category="liquidity",
        frequency="timer",
        as_of_time="14:30",
        unit="share",
        description="Intraday cumulative volume up to a configured timer point.",
        params_schema={"time": {"type": "string", "default": "14:30"}},
        dependencies=["klines_minute"],
    ),
    "rolling_max_volume": FactorDefinition(
        name="rolling_max_volume",
        display_name="N日最大成交量",
        factor_type="indicator",
        category="liquidity",
        frequency="daily",
        unit="share",
        description="Rolling maximum daily volume, optionally combined with timer-day partial volume.",
        params_schema={
            "time": {"type": "string", "default": "14:30"},
            "window": {"type": "integer", "default": 120},
            "daily_volume_to_share_multiplier": {"type": "number", "default": 100.0},
        },
        dependencies=["klines_daily.volume", "cum_volume_at_time"],
        lookback_days=180,
    ),
    "high_volume_ratio": FactorDefinition(
        name="high_volume_ratio",
        display_name="放量比率",
        factor_type="factor",
        category="liquidity",
        frequency="timer",
        as_of_time="14:30",
        unit="x",
        description="Timer-day cumulative volume divided by rolling max volume.",
        params_schema={
            "time": {"type": "string", "default": "14:30"},
            "window": {"type": "integer", "default": 120},
            "daily_volume_to_share_multiplier": {"type": "number", "default": 100.0},
        },
        dependencies=["cum_volume_at_time", "rolling_max_volume"],
        lookback_days=180,
    ),
    "high_volume_signal": FactorDefinition(
        name="high_volume_signal",
        display_name="放量信号",
        factor_type="signal",
        category="liquidity",
        frequency="timer",
        as_of_time="14:30",
        unit="bool",
        description="1 when high_volume_ratio is above the configured threshold.",
        params_schema={
            "time": {"type": "string", "default": "14:30"},
            "window": {"type": "integer", "default": 120},
            "threshold": {"type": "number", "default": 0.9},
            "daily_volume_to_share_multiplier": {"type": "number", "default": 100.0},
        },
        dependencies=["high_volume_ratio"],
        lookback_days=180,
    ),
}


FACTOR_GROUPS: dict[str, dict[str, Any]] = {
    "small_cap_v4_core": {
        "name": "small_cap_v4_core",
        "display_name": "小市值 V4 核心因子",
        "description": "Small-cap strategy reusable factor set.",
        "factor_names": [
            "market_cap",
            "market_cap_rank",
            "is_st",
            "is_paused",
            "is_limit_up",
            "is_limit_down",
            "yesterday_limit_up",
            "v4gv",
            "v4gv_signal",
            "macd_positive",
            "indicator_buy_signal",
            "v4gv_dead_cross",
            "cum_volume_at_time",
            "rolling_max_volume",
            "high_volume_ratio",
            "high_volume_signal",
        ],
    }
}


def list_factor_definitions() -> list[dict[str, Any]]:
    builtins = [d.to_dict() for d in FACTOR_DEFINITIONS.values()]
    return builtins + list_catalog_definitions() + list_custom_factor_definitions()


def get_factor_definition(name: str) -> dict[str, Any] | None:
    definition = FACTOR_DEFINITIONS.get(name)
    if definition is not None:
        return definition.to_dict()
    catalog = get_catalog_definition(name)
    if catalog is not None:
        return catalog
    return get_custom_factor_definition(name)


def list_custom_factor_definitions() -> list[dict[str, Any]]:
    import sqlite3

    db_path = Path(settings.data_dir) / "gaoshou.db"
    if not db_path.exists():
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT name, category, source, code, parameters, description
                FROM factors
                WHERE COALESCE(source, '') = 'custom'
                   OR code IS NOT NULL
                ORDER BY name
                """
            ).fetchall()
    except sqlite3.Error:
        return []

    definitions: list[dict[str, Any]] = []
    for row in rows:
        params = _parse_json_params(row["parameters"])
        expression = str(row["code"] or params.get("expression") or "")
        if not expression:
            continue
        definitions.append(FactorDefinition(
            name=str(row["name"]),
            display_name=str(params.get("display_name") or row["name"]),
            factor_type=str(params.get("kind") or params.get("factor_type") or "factor"),
            category=CUSTOM_FACTOR_CATEGORY,
            frequency=str(params.get("frequency") or "daily"),
            description=str(row["description"] or params.get("description") or expression),
            unit=str(params.get("unit") or ""),
            as_of_time=params.get("as_of_time"),
            params_schema=dict(params.get("params_schema") or {}),
            dependencies=[str(item) for item in params.get("dependencies", [])],
            lookback_days=int(params.get("lookback_days") or 0),
            point_in_time_safe=bool(params.get("point_in_time_safe", True)),
            source="custom.factor",
            version=str(params.get("version") or "v1"),
            data_policy=dict(params.get("data_policy") or {}),
        ).to_dict())
    return definitions


def get_custom_factor_definition(name: str) -> dict[str, Any] | None:
    for item in list_custom_factor_definitions():
        if item["name"] == name:
            return item
    return None


def list_factor_groups() -> list[dict[str, Any]]:
    groups = list(FACTOR_GROUPS.values()) + list_catalog_groups()
    custom_names = [item["name"] for item in list_custom_factor_definitions()]
    if custom_names:
        groups.append({
            "name": CUSTOM_FACTOR_GROUP_NAME,
            "display_name": "自定义因子库",
            "description": "用户创建的 DSL / Python 自定义因子。",
            "factor_names": custom_names,
        })
    return groups


def get_factor_group(name: str) -> dict[str, Any] | None:
    if name == CUSTOM_FACTOR_GROUP_NAME:
        custom_names = [item["name"] for item in list_custom_factor_definitions()]
        if custom_names:
            return {
                "name": CUSTOM_FACTOR_GROUP_NAME,
                "display_name": "自定义因子库",
                "description": "用户创建的 DSL / Python 自定义因子。",
                "factor_names": custom_names,
            }
    return FACTOR_GROUPS.get(name) or get_catalog_group(name)


def _parse_json_params(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}


def normalize_factor_time(value: str | None) -> str:
    text = str(value or "14:30").strip()
    parts = text.split(":")
    if len(parts) < 2:
        raise ValueError(f"Invalid timer time: {value}")
    return f"{int(parts[0]):02d}:{int(parts[1]):02d}"


def factor_params_hash(params: dict[str, Any] | None) -> str:
    clean = {str(k): params[k] for k in sorted(params or {})}
    payload = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def default_factor_params(factor_name: str) -> dict[str, Any]:
    definition = get_factor_definition(factor_name) or {}
    schema = definition.get("params_schema") or {}
    if not isinstance(schema, dict):
        return {}
    params: dict[str, Any] = {}
    for name, meta in schema.items():
        if not isinstance(meta, dict):
            continue
        raw = meta.get("default")
        if raw is None and name in {"time", "as_of_time"}:
            raw = definition.get("as_of_time")
        if raw is None or raw == "":
            continue
        kind = str(meta.get("type") or "string")
        if kind == "integer":
            params[str(name)] = int(raw)
        elif kind == "number":
            params[str(name)] = float(raw)
        elif kind == "boolean":
            params[str(name)] = bool(raw)
        else:
            params[str(name)] = str(raw)
    return params


class FactorValueStore:
    """Parquet dataset for point-in-time factor values.

    Logical schema:
    symbol, trade_date, as_of_time, factor_name, params_hash, value, source, created_at
    """

    dataset = "factor_values"
    key_cols = ["symbol", "trade_date", "as_of_time", "factor_name", "params_hash"]

    def __init__(self, data_dir: str | None = None):
        self._data_dir = Path(data_dir or settings.parquet_data_dir)

    def _dataset_path(self) -> Path:
        return self._data_dir / self.dataset

    def _glob_pattern(self) -> str:
        return str(self._dataset_path() / "year=*" / "month=??" / "*.parquet").replace("\\", "/")

    def exists(self) -> bool:
        root = self._dataset_path()
        return root.exists() and any(".tmp-" not in str(path) for path in root.rglob("*.parquet"))

    def _schema_columns(self) -> set[str]:
        root = self._dataset_path()
        if not root.exists():
            return set()
        columns: set[str] = set()
        try:
            import pyarrow.parquet as pq

            for file in root.rglob("*.parquet"):
                if ".tmp-" in str(file):
                    continue
                columns.update(pq.read_schema(file).names)
        except Exception as exc:
            logger.warning("Failed to inspect factor_values schema: {}", exc)
        return columns

    def _name_expr(self) -> str:
        columns = self._schema_columns()
        has_factor = "factor_name" in columns
        has_feature = "feature_name" in columns
        if has_factor and has_feature:
            return "COALESCE(factor_name, feature_name)"
        if has_feature:
            return "feature_name"
        return "factor_name"

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        body = df.copy()
        if "feature_name" in body.columns and "factor_name" not in body.columns:
            body["factor_name"] = body["feature_name"]
        if "factor_name" in body.columns and "feature_name" not in body.columns:
            body["feature_name"] = body["factor_name"]
        return body

    def _has_year_month_partitions(self) -> bool:
        root = self._dataset_path()
        return root.exists() and any(root.glob("year=*/month=*"))

    def _year_month_filter(self, start_date: date, end_date: date) -> str:
        if not self._has_year_month_partitions():
            return ""
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        current = date(int(start_ts.year), int(start_ts.month), 1)
        last = date(int(end_ts.year), int(end_ts.month), 1)
        terms: list[str] = []
        while current <= last:
            terms.append(f"(year = {current.year} AND month = '{current.month:02d}')")
            current = date(current.year + 1, 1, 1) if current.month == 12 else date(current.year, current.month + 1, 1)
        return "\n              AND (" + " OR ".join(terms) + ")" if terms else ""

    def append(self, df: pd.DataFrame) -> int:
        """Append factor values without rewriting existing partitions.

        This is intended for large precompute batches where rewriting a whole
        month partition would require loading unrelated cached factors into
        memory. Read paths de-duplicate logical keys by latest created_at.
        """
        if df.empty:
            return 0
        body = self._normalize_columns(df)
        required = {"symbol", "trade_date", "factor_name", "value"}
        missing = required - set(body.columns)
        if missing:
            raise KeyError(f"Factor values missing columns: {sorted(missing)}")

        body["symbol"] = body["symbol"].astype(str)
        body["trade_date"] = pd.to_datetime(body["trade_date"]).dt.date
        body["as_of_time"] = body.get("as_of_time", "").fillna("").astype(str) if "as_of_time" in body.columns else ""
        body["factor_name"] = body["factor_name"].astype(str)
        if "params_hash" not in body.columns:
            body["params_hash"] = factor_params_hash({})
        body["params_hash"] = body["params_hash"].fillna(factor_params_hash({})).astype(str)
        if "source" not in body.columns:
            body["source"] = "precompute"
        body["source"] = body["source"].fillna("precompute").astype(str)
        if "created_at" not in body.columns:
            body["created_at"] = datetime.now()
        body["value"] = pd.to_numeric(body["value"], errors="coerce")
        body = body.dropna(subset=["value"])
        if body.empty:
            return 0

        dt = pd.to_datetime(body["trade_date"])
        body["year"] = dt.dt.year.astype(str)
        body["month"] = dt.dt.strftime("%m")
        root = self._dataset_path()
        root.mkdir(parents=True, exist_ok=True)

        import pyarrow as pa
        import pyarrow.parquet as pq

        total = 0
        for (_year, _month), part in body.groupby(["year", "month"], sort=False):
            partition_dir = root / f"year={_year}" / f"month={_month}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            part_body = self._normalize_columns(part.drop(columns=["year", "month"], errors="ignore"))
            part_body = part_body.drop_duplicates(subset=[c for c in self.key_cols if c in part_body.columns], keep="last")
            file_path = partition_dir / f"part-{uuid.uuid4().hex}.parquet"
            pq.write_table(pa.Table.from_pandas(part_body, preserve_index=False), file_path)
            total += len(part_body)
        return total

    def batch_writer(self, *, batch_size: int = DEFAULT_PRECOMPUTE_BATCH_ROWS) -> FactorValueBatchWriter:
        return FactorValueBatchWriter(self, batch_size=batch_size)

    def load(
        self,
        *,
        factor_names: Sequence[str],
        start_date: date,
        end_date: date,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params_hash: str | None = None,
    ) -> pd.DataFrame:
        if not factor_names or not self.exists():
            return pd.DataFrame()

        conditions = [
            f"factor_name IN {_list_param(factor_names)}",
            f"trade_date >= {_sql_literal(start_date)}",
            f"trade_date <= {_sql_literal(end_date)}",
        ]
        if symbols:
            conditions.append(f"symbol IN {_list_param(symbols)}")
        if as_of_time is not None:
            conditions.append(f"as_of_time = {_sql_literal(normalize_factor_time(as_of_time))}")
        if params_hash is not None:
            conditions.append(f"params_hash = {_sql_literal(params_hash)}")

        partition_filter = self._year_month_filter(start_date, end_date)
        name_expr = self._name_expr()
        sql = f"""
            WITH factor_values_normalized AS (
                SELECT
                    symbol,
                    trade_date,
                    as_of_time,
                    {name_expr} AS factor_name,
                    params_hash,
                    value,
                    source,
                    created_at,
                    year,
                    month
                FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true, union_by_name=true)
            ),
            factor_values_latest AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, trade_date, as_of_time, factor_name, params_hash
                            ORDER BY created_at DESC NULLS LAST
                        ) AS row_num
                    FROM factor_values_normalized
                    WHERE {' AND '.join(conditions)}
                      {partition_filter}
                )
                WHERE row_num = 1
            )
            SELECT symbol, trade_date, as_of_time, factor_name, params_hash, value, source, created_at
            FROM factor_values_latest
            ORDER BY trade_date, factor_name, symbol
        """
        df = get_duckdb().execute(sql).df()
        if df.empty:
            return df
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    def load_cross_section(
        self,
        factor_name: str,
        trade_date: date,
        *,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        params_hash = factor_params_hash(params) if params is not None else None
        df = self.load(
            factor_names=[factor_name],
            start_date=trade_date,
            end_date=trade_date,
            symbols=symbols,
            as_of_time=as_of_time,
            params_hash=params_hash,
        )
        if df.empty:
            return {}
        df = df.drop_duplicates(subset=["symbol"], keep="last")
        return {str(row.symbol): float(row.value) for row in df.itertuples(index=False)}

    def coverage(
        self,
        factor_name: str,
        *,
        start_date: date,
        end_date: date,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params: dict[str, Any] | None = None,
        params_hash: str | None = None,
        include_symbols_sample: bool = True,
    ) -> dict[str, Any]:
        if not self.exists():
            return self._empty_coverage(factor_name)

        effective_params_hash = params_hash if params_hash is not None else (factor_params_hash(params) if params is not None else None)
        conditions = [
            f"factor_name = {_sql_literal(factor_name)}",
            f"trade_date >= {_sql_literal(start_date)}",
            f"trade_date <= {_sql_literal(end_date)}",
        ]
        if symbols:
            conditions.append(f"symbol IN {_list_param(symbols)}")
        if as_of_time is not None:
            conditions.append(f"as_of_time = {_sql_literal(normalize_factor_time(as_of_time))}")
        if effective_params_hash is not None:
            conditions.append(f"params_hash = {_sql_literal(effective_params_hash)}")

        partition_filter = self._year_month_filter(start_date, end_date)
        where_sql = " AND ".join(conditions)
        name_expr = self._name_expr()
        stats_sql = f"""
            WITH factor_values_normalized AS (
                SELECT
                    symbol,
                    trade_date,
                    as_of_time,
                    {name_expr} AS factor_name,
                    params_hash,
                    value,
                    created_at,
                    year,
                    month
                FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true, union_by_name=true)
            ),
            factor_values_latest AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, trade_date, as_of_time, factor_name, params_hash
                            ORDER BY created_at DESC NULLS LAST
                        ) AS row_num
                    FROM factor_values_normalized
                    WHERE {where_sql}
                      {partition_filter}
                )
                WHERE row_num = 1
            )
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT symbol) AS symbol_count,
                COUNT(DISTINCT trade_date) AS date_count,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date
            FROM factor_values_latest
        """
        stats = get_duckdb().execute(stats_sql).fetchone()
        total_rows = int(stats[0] or 0) if stats else 0
        if total_rows == 0:
            return self._empty_coverage(factor_name)

        sample_symbols: list[str] = []
        if include_symbols_sample:
            sample_sql = f"""
                WITH factor_values_normalized AS (
                    SELECT
                        symbol,
                        trade_date,
                        as_of_time,
                        {name_expr} AS factor_name,
                        params_hash,
                        value,
                        created_at,
                        year,
                        month
                    FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true, union_by_name=true)
                ),
                factor_values_latest AS (
                    SELECT *
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY symbol, trade_date, as_of_time, factor_name, params_hash
                                ORDER BY created_at DESC NULLS LAST
                            ) AS row_num
                        FROM factor_values_normalized
                        WHERE {where_sql}
                          {partition_filter}
                    )
                    WHERE row_num = 1
                )
                SELECT DISTINCT symbol
                FROM factor_values_latest
                ORDER BY symbol
                LIMIT 20
            """
            sample_df = get_duckdb().execute(sample_sql).df()
            sample_symbols = sample_df["symbol"].astype(str).tolist() if not sample_df.empty else []
        return {
            "factor_name": factor_name,
            "total_rows": total_rows,
            "symbol_count": int(stats[1] or 0),
            "date_count": int(stats[2] or 0),
            "min_date": str(stats[3]) if stats[3] is not None else None,
            "max_date": str(stats[4]) if stats[4] is not None else None,
            "symbols_sample": sample_symbols,
        }

    def coverage_many(
        self,
        factor_names: Sequence[str],
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Return factor coverage in one DuckDB scan.

        When start_date/end_date are omitted, this reports the actual effective
        cached range. The factor board uses that mode so the displayed range is
        not clipped by the selected research window.
        """
        names = sorted({str(name) for name in factor_names if str(name)})
        if not names or not self.exists():
            return {name: self._empty_coverage(name) for name in names}

        params_hash = factor_params_hash(params) if params is not None else None
        conditions = [f"factor_name IN {_list_param(names)}"]
        partition_filter = ""
        if start_date is not None:
            conditions.append(f"trade_date >= {_sql_literal(start_date)}")
        if end_date is not None:
            conditions.append(f"trade_date <= {_sql_literal(end_date)}")
        if symbols:
            conditions.append(f"symbol IN {_list_param(symbols)}")
        if start_date is not None and end_date is not None:
            partition_filter = self._year_month_filter(start_date, end_date)
        if as_of_time is not None:
            conditions.append(f"as_of_time = {_sql_literal(normalize_factor_time(as_of_time))}")
        if params_hash is not None:
            conditions.append(f"params_hash = {_sql_literal(params_hash)}")

        where_sql = " AND ".join(conditions)
        name_expr = self._name_expr()
        sql = f"""
            WITH factor_values_normalized AS (
                SELECT
                    symbol,
                    trade_date,
                    as_of_time,
                    {name_expr} AS factor_name,
                    params_hash,
                    value,
                    created_at,
                    year,
                    month
                FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true, union_by_name=true)
            ),
            factor_values_latest AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, trade_date, as_of_time, factor_name, params_hash
                            ORDER BY created_at DESC NULLS LAST
                        ) AS row_num
                    FROM factor_values_normalized
                    WHERE {where_sql}
                      {partition_filter}
                )
                WHERE row_num = 1
            )
            SELECT
                factor_name,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT symbol) AS symbol_count,
                COUNT(DISTINCT trade_date) AS date_count,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date
            FROM factor_values_latest
            GROUP BY factor_name
        """
        rows = get_duckdb().execute(sql).fetchall()
        result = {name: self._empty_coverage(name) for name in names}
        for row in rows:
            factor_name = str(row[0])
            result[factor_name] = {
                "factor_name": factor_name,
                "total_rows": int(row[1] or 0),
                "symbol_count": int(row[2] or 0),
                "date_count": int(row[3] or 0),
                "min_date": str(row[4]) if row[4] is not None else None,
                "max_date": str(row[5]) if row[5] is not None else None,
                "symbols_sample": [],
            }
        return result

    def list_param_hashes(
        self,
        factor_names: Sequence[str],
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        symbols: Sequence[str] | None = None,
        limit_per_factor: int = 12,
    ) -> list[dict[str, Any]]:
        names = sorted({str(name) for name in factor_names if str(name)})
        if not names or not self.exists():
            return []

        conditions = [f"factor_name IN {_list_param(names)}"]
        partition_filter = ""
        if start_date is not None:
            conditions.append(f"trade_date >= {_sql_literal(start_date)}")
        if end_date is not None:
            conditions.append(f"trade_date <= {_sql_literal(end_date)}")
        if symbols:
            conditions.append(f"symbol IN {_list_param(symbols)}")
        if start_date is not None and end_date is not None:
            partition_filter = self._year_month_filter(start_date, end_date)

        where_sql = " AND ".join(conditions)
        name_expr = self._name_expr()
        sql = f"""
            WITH factor_values_normalized AS (
                SELECT
                    symbol,
                    trade_date,
                    as_of_time,
                    {name_expr} AS factor_name,
                    params_hash,
                    source,
                    created_at,
                    year,
                    month
                FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true, union_by_name=true)
            ),
            factor_values_latest AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, trade_date, as_of_time, factor_name, params_hash
                            ORDER BY created_at DESC NULLS LAST
                        ) AS row_num
                    FROM factor_values_normalized
                    WHERE {where_sql}
                      {partition_filter}
                )
                WHERE row_num = 1
            )
            SELECT
                factor_name,
                params_hash,
                as_of_time,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT symbol) AS symbol_count,
                COUNT(DISTINCT trade_date) AS date_count,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date,
                MAX(created_at) AS latest_created_at,
                ANY_VALUE(source) AS source
            FROM factor_values_latest
            GROUP BY factor_name, params_hash, as_of_time
        """
        rows = get_duckdb().execute(sql).fetchall()
        default_hash_by_factor = {
            name: factor_params_hash(default_factor_params(name))
            for name in names
        }
        items: list[dict[str, Any]] = []
        for row in rows:
            factor_name = str(row[0])
            params_hash_value = str(row[1] or "")
            items.append({
                "factor_name": factor_name,
                "params_hash": params_hash_value,
                "as_of_time": str(row[2] or ""),
                "total_rows": int(row[3] or 0),
                "symbol_count": int(row[4] or 0),
                "date_count": int(row[5] or 0),
                "min_date": str(row[6]) if row[6] is not None else None,
                "max_date": str(row[7]) if row[7] is not None else None,
                "latest_created_at": str(row[8]) if row[8] is not None else None,
                "source": str(row[9] or ""),
                "is_default": params_hash_value == default_hash_by_factor.get(factor_name),
            })

        items.sort(
            key=lambda item: (
                str(item["factor_name"]),
                bool(item["is_default"]),
                str(item["max_date"] or ""),
                int(item["total_rows"] or 0),
            ),
            reverse=True,
        )
        limited: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for item in items:
            factor_name = str(item["factor_name"])
            counts[factor_name] = counts.get(factor_name, 0) + 1
            if counts[factor_name] <= max(1, int(limit_per_factor or 1)):
                limited.append(item)
        return limited

    def preview(
        self,
        factor_name: str,
        trade_date: date,
        *,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params: dict[str, Any] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        values = self.load_cross_section(
            factor_name,
            trade_date,
            symbols=symbols,
            as_of_time=as_of_time,
            params=params,
        )
        items = sorted(values.items(), key=lambda item: (item[1], item[0]))[:limit]
        return [{"symbol": symbol, "value": value} for symbol, value in items]

    @staticmethod
    def _empty_coverage(factor_name: str) -> dict[str, Any]:
        return {
            "factor_name": factor_name,
            "total_rows": 0,
            "symbol_count": 0,
            "date_count": 0,
            "min_date": None,
            "max_date": None,
            "symbols_sample": [],
        }


class FactorValueBatchWriter:
    """Buffered append writer for large factor precompute jobs.

    The precompute path should append small parquet parts and rely on the read
    path's latest-row de-duplication. Rewriting a whole month partition can load
    unrelated cached factors into memory and spike RAM during batch jobs.
    """

    def __init__(self, store: FactorValueStore, *, batch_size: int = DEFAULT_PRECOMPUTE_BATCH_ROWS) -> None:
        self.store = store
        self.batch_size = max(1, int(batch_size))
        self._rows: list[dict[str, Any]] = []
        self.counts: dict[str, int] = {}
        self.written = 0

    @property
    def rows_buffered(self) -> int:
        return len(self._rows)

    def add(self, row: dict[str, Any]) -> None:
        self._rows.append(row)
        factor_name = str(row.get("factor_name") or "")
        if factor_name:
            self.counts[factor_name] = self.counts.get(factor_name, 0) + 1
        if len(self._rows) >= self.batch_size:
            self.flush()

    def extend(self, rows: Sequence[dict[str, Any]]) -> None:
        for row in rows:
            self.add(row)

    def write_frame(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        if "factor_name" in frame.columns:
            for factor_name, count in frame["factor_name"].astype(str).value_counts().items():
                self.counts[str(factor_name)] = self.counts.get(str(factor_name), 0) + int(count)
        if len(frame) <= self.batch_size:
            written = self.store.append(frame)
            self.written += written
            return written

        written = 0
        for start in range(0, len(frame), self.batch_size):
            chunk = frame.iloc[start:start + self.batch_size].copy()
            written += self.store.append(chunk)
        self.written += written
        return written

    def flush(self) -> int:
        if not self._rows:
            return 0
        frame = pd.DataFrame(self._rows)
        self._rows = []
        written = self.store.append(frame)
        self.written += written
        return written


def get_factor_value_store() -> FactorValueStore:
    return FactorValueStore()


def get_factor_store() -> FactorValueStore:
    return get_factor_value_store()
