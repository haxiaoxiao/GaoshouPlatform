"""Generic feature definitions and Parquet-backed feature value store."""

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
class FeatureDefinition:
    """Metadata that lets UI, strategy, and precompute share one feature catalog."""

    name: str
    display_name: str
    feature_type: str
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


FEATURE_DEFINITIONS: dict[str, FeatureDefinition] = {
    "smallcap_market_cap": FeatureDefinition(
        name="smallcap_market_cap",
        display_name="Small-Cap Market Cap",
        feature_type="indicator",
        category="valuation",
        frequency="daily",
        unit="10k CNY",
        description="Point-in-time market cap used by ID=43 small-cap ranking.",
        dependencies=["stock_daily_basic.circ_mv", "stock_daily_basic.total_mv"],
        lookback_days=370,
    ),
    "smallcap_market_cap_rank": FeatureDefinition(
        name="smallcap_market_cap_rank",
        display_name="Small-Cap Market Cap Rank",
        feature_type="factor",
        category="valuation",
        frequency="daily",
        unit="rank",
        description="Ascending market-cap rank inside the selected universe on each trade date.",
        dependencies=["smallcap_market_cap", "index_components"],
        lookback_days=370,
    ),
    "smallcap_is_st": FeatureDefinition(
        name="smallcap_is_st",
        display_name="ST Filter Flag",
        feature_type="state",
        category="status",
        frequency="daily",
        unit="bool",
        description="1 when a stock should be excluded by ST/delist/name-change rules.",
        dependencies=["stocks", "stock_name_changes"],
        lookback_days=0,
    ),
    "smallcap_is_paused": FeatureDefinition(
        name="smallcap_is_paused",
        display_name="Paused Filter Flag",
        feature_type="state",
        category="status",
        frequency="timer",
        as_of_time="10:30",
        unit="bool",
        description="1 when the stock has no usable 10:30 timer bar.",
        params_schema={"time": {"type": "string", "default": "10:30"}},
        dependencies=["klines_minute_timer", "klines_minute"],
        lookback_days=0,
    ),
    "smallcap_is_limit_up": FeatureDefinition(
        name="smallcap_is_limit_up",
        display_name="Limit-Up Filter Flag",
        feature_type="state",
        category="status",
        frequency="timer",
        as_of_time="10:30",
        unit="bool",
        description="1 when timer price is at or above adjusted up-limit.",
        params_schema={"time": {"type": "string", "default": "10:30"}},
        dependencies=["stock_limit_prices", "klines_minute_timer"],
        lookback_days=0,
    ),
    "smallcap_is_limit_down": FeatureDefinition(
        name="smallcap_is_limit_down",
        display_name="Limit-Down Filter Flag",
        feature_type="state",
        category="status",
        frequency="timer",
        as_of_time="10:30",
        unit="bool",
        description="1 when timer price is at or below adjusted down-limit.",
        params_schema={"time": {"type": "string", "default": "10:30"}},
        dependencies=["stock_limit_prices", "klines_minute_timer"],
        lookback_days=0,
    ),
    "smallcap_yesterday_limit_up": FeatureDefinition(
        name="smallcap_yesterday_limit_up",
        display_name="Yesterday Limit-Up",
        feature_type="state",
        category="status",
        frequency="daily",
        unit="bool",
        description="1 when the previous trading day's close was at the up-limit.",
        dependencies=["stock_limit_prices", "klines_daily"],
        lookback_days=5,
    ),
    "smallcap_v4gv": FeatureDefinition(
        name="smallcap_v4gv",
        display_name="V4GV",
        feature_type="indicator",
        category="technical",
        frequency="daily",
        unit="",
        description="ID=43 V4GV indicator value calculated from daily OHLC windows.",
        dependencies=["klines_daily"],
        lookback_days=140,
    ),
    "smallcap_v4gv21": FeatureDefinition(
        name="smallcap_v4gv21",
        display_name="V4GV21",
        feature_type="indicator",
        category="technical",
        frequency="daily",
        unit="",
        description="ID=43 V4GV21 signal line.",
        dependencies=["smallcap_v4gv"],
        lookback_days=140,
    ),
    "smallcap_macd_signal": FeatureDefinition(
        name="smallcap_macd_signal",
        display_name="MACD Positive Signal",
        feature_type="signal",
        category="technical",
        frequency="daily",
        unit="bool",
        description="1 when MACD is above signal and positive for ID=43 filtering.",
        dependencies=["klines_daily.close"],
        lookback_days=80,
    ),
    "smallcap_indicator_buy_signal": FeatureDefinition(
        name="smallcap_indicator_buy_signal",
        display_name="ID=43 Indicator Buy Signal",
        feature_type="signal",
        category="technical",
        frequency="daily",
        unit="bool",
        description="1 when V4GV > V4GV21, V4GV > 0, and MACD signal is positive.",
        dependencies=["smallcap_v4gv", "smallcap_v4gv21", "smallcap_macd_signal"],
        lookback_days=140,
    ),
    "smallcap_v4gv_dead_cross": FeatureDefinition(
        name="smallcap_v4gv_dead_cross",
        display_name="ID=43 V4GV Dead Cross",
        feature_type="signal",
        category="technical",
        frequency="daily",
        unit="bool",
        description="1 when V4GV < V4GV21 and MACD is not positive.",
        dependencies=["smallcap_v4gv", "smallcap_v4gv21", "smallcap_macd_signal"],
        lookback_days=140,
    ),
    "cum_volume_at_time": FeatureDefinition(
        name="cum_volume_at_time",
        display_name="Cumulative Volume At Time",
        feature_type="indicator",
        category="liquidity",
        frequency="timer",
        as_of_time="14:30",
        unit="share",
        description="Intraday cumulative volume up to a configured timer point.",
        params_schema={"time": {"type": "string", "default": "14:30"}},
        dependencies=["klines_minute"],
        lookback_days=0,
    ),
    "max_volume_nd": FeatureDefinition(
        name="max_volume_nd",
        display_name="N-Day Max Volume",
        feature_type="indicator",
        category="liquidity",
        frequency="daily",
        unit="share",
        description="Rolling maximum volume using historical daily volume plus the timer-day partial volume.",
        params_schema={
            "time": {"type": "string", "default": "14:30"},
            "window": {"type": "integer", "default": 120},
            "daily_volume_to_share_multiplier": {"type": "number", "default": 100.0},
        },
        dependencies=["klines_daily.volume", "cum_volume_at_time"],
        lookback_days=180,
    ),
    "high_volume_ratio": FeatureDefinition(
        name="high_volume_ratio",
        display_name="High Volume Ratio",
        feature_type="factor",
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
        dependencies=["cum_volume_at_time", "max_volume_nd"],
        lookback_days=180,
    ),
    "high_volume_signal": FeatureDefinition(
        name="high_volume_signal",
        display_name="High Volume Sell Signal",
        feature_type="signal",
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

FEATURE_GROUPS: dict[str, dict[str, Any]] = {
    "small_cap_v4_core": {
        "name": "small_cap_v4_core",
        "display_name": "ID=43 Small-Cap V4 Core",
        "description": "Core feature set used to inspect and accelerate ID=43 small-cap strategy paths.",
        "feature_names": [
            "smallcap_market_cap",
            "smallcap_market_cap_rank",
            "smallcap_is_st",
            "smallcap_is_paused",
            "smallcap_is_limit_up",
            "smallcap_is_limit_down",
            "smallcap_yesterday_limit_up",
            "smallcap_v4gv",
            "smallcap_v4gv21",
            "smallcap_macd_signal",
            "smallcap_indicator_buy_signal",
            "smallcap_v4gv_dead_cross",
            "cum_volume_at_time",
            "max_volume_nd",
            "high_volume_ratio",
            "high_volume_signal",
        ],
    }
}


def list_feature_definitions() -> list[dict[str, Any]]:
    return [definition.to_dict() for definition in FEATURE_DEFINITIONS.values()] + list_custom_feature_definitions()


def get_feature_definition(name: str) -> FeatureDefinition | None:
    return FEATURE_DEFINITIONS.get(name) or get_custom_feature_definition(name)


def list_custom_feature_definitions() -> list[dict[str, Any]]:
    """Expose saved expression factors/indicators as Feature Store definitions."""
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
        feature_type = str(params.get("kind") or params.get("feature_type") or "factor")
        definitions.append(FeatureDefinition(
            name=str(row["name"]),
            display_name=str(params.get("display_name") or row["name"]),
            feature_type=feature_type,
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
        ).to_dict())
    return definitions


def get_custom_feature_definition(name: str) -> FeatureDefinition | None:
    for item in list_custom_feature_definitions():
        if item["name"] == name:
            return FeatureDefinition(**item)
    return None


def _parse_json_params(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}


def list_feature_groups() -> list[dict[str, Any]]:
    return list(FEATURE_GROUPS.values())


def get_feature_group(name: str) -> dict[str, Any] | None:
    return FEATURE_GROUPS.get(name)


def normalize_feature_time(value: str | None) -> str:
    text = str(value or "14:30").strip()
    parts = text.split(":")
    if len(parts) < 2:
        raise ValueError(f"Invalid timer time: {value}")
    return f"{int(parts[0]):02d}:{int(parts[1]):02d}"


def feature_params_hash(params: dict[str, Any] | None) -> str:
    clean = {str(k): params[k] for k in sorted(params or {})}
    payload = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


class FeatureValueStore:
    """Parquet dataset for point-in-time feature values.

    Schema:
    symbol, trade_date, as_of_time, feature_name, params_hash, value, source, created_at
    """

    dataset = "feature_values"
    key_cols = ["symbol", "trade_date", "as_of_time", "feature_name", "params_hash"]

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
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        return "\n              AND (" + " OR ".join(terms) + ")" if terms else ""

    def write(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        body = df.copy()
        required = {"symbol", "trade_date", "feature_name", "value"}
        missing = required - set(body.columns)
        if missing:
            raise KeyError(f"Feature values missing columns: {sorted(missing)}")

        body["symbol"] = body["symbol"].astype(str)
        body["trade_date"] = pd.to_datetime(body["trade_date"]).dt.date
        if "as_of_time" not in body.columns:
            body["as_of_time"] = ""
        body["as_of_time"] = body["as_of_time"].fillna("").astype(str)
        body["feature_name"] = body["feature_name"].astype(str)
        if "params_hash" not in body.columns:
            body["params_hash"] = feature_params_hash({})
        body["params_hash"] = body["params_hash"].fillna(feature_params_hash({})).astype(str)
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

        for (year, month), part in body.groupby(["year", "month"], sort=False):
            partition_dir = root / f"year={year}" / f"month={month}"
            existing_frames = []
            if partition_dir.exists():
                for file in partition_dir.glob("*.parquet"):
                    try:
                        existing_frames.append(pd.read_parquet(file))
                    except Exception as exc:
                        logger.warning("Failed to read existing feature parquet {}: {}", file, exc)
            part_body = part.drop(columns=["year", "month"], errors="ignore")
            if existing_frames:
                part_body = pd.concat(existing_frames + [part_body], ignore_index=True)
            present_keys = [c for c in self.key_cols if c in part_body.columns]
            part_body = part_body.drop_duplicates(subset=present_keys, keep="last")
            part_body = part_body.sort_values(["feature_name", "symbol", "trade_date", "as_of_time"])

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
        feature_names: Sequence[str],
        start_date: date,
        end_date: date,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params_hash: str | None = None,
    ) -> pd.DataFrame:
        if not feature_names or not self.exists():
            return pd.DataFrame()

        conditions = [
            f"feature_name IN {_list_param(feature_names)}",
            f"trade_date >= {_sql_literal(start_date)}",
            f"trade_date <= {_sql_literal(end_date)}",
        ]
        if symbols:
            conditions.append(f"symbol IN {_list_param(symbols)}")
        if as_of_time is not None:
            conditions.append(f"as_of_time = {_sql_literal(normalize_feature_time(as_of_time))}")
        if params_hash is not None:
            conditions.append(f"params_hash = {_sql_literal(params_hash)}")

        partition_filter = self._year_month_filter(start_date, end_date)
        sql = f"""
            SELECT symbol, trade_date, as_of_time, feature_name, params_hash, value, source, created_at
            FROM read_parquet('{self._glob_pattern()}', hive_partitioning=true)
            WHERE {' AND '.join(conditions)}
              {partition_filter}
            ORDER BY trade_date, feature_name, symbol
        """
        df = get_duckdb().execute(sql).df()
        if df.empty:
            return df
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    def load_cross_section(
        self,
        feature_name: str,
        trade_date: date,
        *,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        params_hash = feature_params_hash(params) if params is not None else None
        df = self.load(
            feature_names=[feature_name],
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
        feature_name: str,
        *,
        start_date: date,
        end_date: date,
        symbols: Sequence[str] | None = None,
        as_of_time: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.exists():
            return {
                "feature_name": feature_name,
                "total_rows": 0,
                "symbol_count": 0,
                "date_count": 0,
                "min_date": None,
                "max_date": None,
                "symbols_sample": [],
            }

        params_hash = feature_params_hash(params) if params is not None else None
        df = self.load(
            feature_names=[feature_name],
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            as_of_time=as_of_time,
            params_hash=params_hash,
        )
        if df.empty:
            return {
                "feature_name": feature_name,
                "total_rows": 0,
                "symbol_count": 0,
                "date_count": 0,
                "min_date": None,
                "max_date": None,
                "symbols_sample": [],
            }
        symbols_sorted = sorted(df["symbol"].astype(str).unique().tolist())
        return {
            "feature_name": feature_name,
            "total_rows": int(len(df)),
            "symbol_count": int(df["symbol"].nunique()),
            "date_count": int(df["trade_date"].nunique()),
            "min_date": str(min(df["trade_date"])),
            "max_date": str(max(df["trade_date"])),
            "symbols_sample": symbols_sorted[:20],
        }


def get_feature_store() -> FeatureValueStore:
    return FeatureValueStore()
