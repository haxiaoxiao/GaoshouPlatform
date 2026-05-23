"""Factor definitions and Parquet-backed factor value cache."""

from __future__ import annotations

import hashlib
import json
import shutil
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
    return [d.to_dict() for d in FACTOR_DEFINITIONS.values()] + list_custom_factor_definitions()


def get_factor_definition(name: str) -> dict[str, Any] | None:
    definition = FACTOR_DEFINITIONS.get(name)
    if definition is not None:
        return definition.to_dict()
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
            category=str(row["category"] or params.get("category") or "custom"),
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
    return list(FACTOR_GROUPS.values())


def get_factor_group(name: str) -> dict[str, Any] | None:
    return FACTOR_GROUPS.get(name)


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
        return str(self._dataset_path() / "**" / "*.parquet").replace("\\", "/")

    def exists(self) -> bool:
        root = self._dataset_path()
        return root.exists() and any(root.rglob("*.parquet"))

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

    def write(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        body = df.copy()
        # 向后兼容：旧 code 可能传 feature_name 列
        if "feature_name" in body.columns and "factor_name" not in body.columns:
            body["factor_name"] = body["feature_name"]
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

        for (_year, _month), part in body.groupby(["year", "month"], sort=False):
            partition_dir = root / f"year={_year}" / f"month={_month}"
            existing_frames = []
            if partition_dir.exists():
                for file in partition_dir.glob("*.parquet"):
                    try:
                        existing_frames.append(pd.read_parquet(file))
                    except Exception as exc:
                        logger.warning("Failed to read existing factor parquet {}: {}", file, exc)
            part_body = part.drop(columns=["year", "month"], errors="ignore")
            if existing_frames:
                part_body = pd.concat(existing_frames + [part_body], ignore_index=True)
            present_keys = [c for c in self.key_cols if c in part_body.columns]
            part_body = part_body.drop_duplicates(subset=present_keys, keep="last")
            part_body = part_body.sort_values(["factor_name", "symbol", "trade_date", "as_of_time"])

            tmp_dir = partition_dir.with_name(f"{partition_dir.name}.tmp-{uuid.uuid4().hex}")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pandas(part_body, preserve_index=False), tmp_dir / "part-0.parquet")
            if partition_dir.exists():
                shutil.rmtree(partition_dir)
            tmp_dir.replace(partition_dir)
        return len(body)

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
        sql = f"""
            SELECT symbol, trade_date, as_of_time, factor_name, params_hash, value, source, created_at
            FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true)
            WHERE {' AND '.join(conditions)}
              {partition_filter}
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
    ) -> dict[str, Any]:
        if not self.exists():
            return self._empty_coverage(factor_name)

        params_hash = factor_params_hash(params) if params is not None else None
        conditions = [
            f"factor_name = {_sql_literal(factor_name)}",
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
        where_sql = " AND ".join(conditions)
        stats_sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT symbol) AS symbol_count,
                COUNT(DISTINCT trade_date) AS date_count,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date
            FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true)
            WHERE {where_sql}
              {partition_filter}
        """
        stats = get_duckdb().execute(stats_sql).fetchone()
        total_rows = int(stats[0] or 0) if stats else 0
        if total_rows == 0:
            return self._empty_coverage(factor_name)

        sample_sql = f"""
            SELECT DISTINCT symbol
            FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true)
            WHERE {where_sql}
              {partition_filter}
            ORDER BY symbol
            LIMIT 20
        """
        sample_df = get_duckdb().execute(sample_sql).df()
        return {
            "factor_name": factor_name,
            "total_rows": total_rows,
            "symbol_count": int(stats[1] or 0),
            "date_count": int(stats[2] or 0),
            "min_date": str(stats[3]) if stats[3] is not None else None,
            "max_date": str(stats[4]) if stats[4] is not None else None,
            "symbols_sample": sample_df["symbol"].astype(str).tolist() if not sample_df.empty else [],
        }

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


def get_factor_value_store() -> FactorValueStore:
    return FactorValueStore()


def get_factor_store() -> FactorValueStore:
    return get_factor_value_store()
