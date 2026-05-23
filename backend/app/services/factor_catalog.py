"""Catalog metadata for built-in factor families."""

from __future__ import annotations

from typing import Any


def _definition(
    name: str,
    display_name: str,
    *,
    factor_type: str = "factor",
    category: str,
    frequency: str = "daily",
    description: str,
    unit: str = "",
    dependencies: list[str] | None = None,
    lookback_days: int = 0,
    source: str = "catalog",
    data_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "display_name": display_name,
        "factor_type": factor_type,
        "category": category,
        "frequency": frequency,
        "description": description,
        "unit": unit,
        "as_of_time": None,
        "params_schema": {},
        "dependencies": dependencies or ["klines_daily"],
        "lookback_days": lookback_days,
        "point_in_time_safe": True,
        "source": source,
        "version": "v1",
        "data_policy": data_policy or {},
    }


TA_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "ta_sma_20": {"label": "SMA 20", "category": "ta_trend", "lookback": 40},
    "ta_ema_20": {"label": "EMA 20", "category": "ta_trend", "lookback": 60},
    "ta_rsi_14": {"label": "RSI 14", "category": "ta_momentum", "lookback": 40},
    "ta_macd_dif_12_26_9": {"label": "MACD DIF", "category": "ta_momentum", "lookback": 80},
    "ta_macd_dea_12_26_9": {"label": "MACD DEA", "category": "ta_momentum", "lookback": 80},
    "ta_macd_hist_12_26_9": {"label": "MACD HIST", "category": "ta_momentum", "lookback": 80},
    "ta_bbands_upper_20": {"label": "BBANDS Upper 20", "category": "ta_volatility", "lookback": 60},
    "ta_bbands_middle_20": {"label": "BBANDS Middle 20", "category": "ta_volatility", "lookback": 60},
    "ta_bbands_lower_20": {"label": "BBANDS Lower 20", "category": "ta_volatility", "lookback": 60},
    "ta_atr_14": {"label": "ATR 14", "category": "ta_volatility", "lookback": 40},
    "ta_natr_14": {"label": "NATR 14", "category": "ta_volatility", "lookback": 40},
    "ta_obv": {"label": "OBV", "category": "ta_volume", "lookback": 10},
    "ta_ad": {"label": "Accumulation/Distribution", "category": "ta_volume", "lookback": 10},
    "ta_mfi_14": {"label": "MFI 14", "category": "ta_volume", "lookback": 40},
    "ta_cci_14": {"label": "CCI 14", "category": "ta_momentum", "lookback": 40},
    "ta_willr_14": {"label": "WILLR 14", "category": "ta_momentum", "lookback": 40},
    "ta_roc_10": {"label": "ROC 10", "category": "ta_momentum", "lookback": 30},
    "ta_adx_14": {"label": "ADX 14", "category": "ta_trend", "lookback": 50},
    "ta_aroonosc_14": {"label": "AROONOSC 14", "category": "ta_trend", "lookback": 50},
    "ta_typprice": {"label": "Typical Price", "category": "ta_price", "lookback": 1},
}

RESEARCH_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "research_gross_profitability": {
        "label": "Gross Profitability",
        "category": "research_quality",
        "dependencies": ["stocks.gross_margin", "stocks.revenue"],
        "description": (
            "Gross profitability proxy inspired by Novy-Marx quality research. "
            "Source: https://ssrn.com/abstract=1598056"
        ),
    },
    "research_asset_growth": {
        "label": "Asset Growth",
        "category": "research_investment",
        "dependencies": ["financial_data.total_assets"],
        "description": (
            "Asset growth / investment proxy. "
            "Source: https://ssrn.com/abstract=760967"
        ),
    },
    "research_accruals": {
        "label": "Accruals",
        "category": "research_quality",
        "dependencies": ["financial_data.net_profit", "financial_data.total_assets"],
        "description": (
            "Accounting accrual quality proxy using available local fields. "
            "Source: https://ssrn.com/abstract=2598"
        ),
    },
    "research_low_beta": {
        "label": "Low Beta",
        "category": "research_risk",
        "dependencies": ["klines_daily.close"],
        "description": (
            "Negative market beta proxy inspired by betting-against-beta research. "
            "Source: https://ssrn.com/abstract=1723048"
        ),
        "lookback": 252,
    },
    "research_idiosyncratic_volatility": {
        "label": "Idiosyncratic Volatility",
        "category": "research_risk",
        "dependencies": ["klines_daily.close"],
        "description": (
            "Negative residual volatility proxy versus equal-weight market return. "
            "Source: https://ssrn.com/abstract=1947020"
        ),
        "lookback": 252,
    },
    "research_residual_momentum": {
        "label": "Residual Momentum",
        "category": "research_momentum",
        "dependencies": ["klines_daily.close"],
        "description": (
            "12-1 month momentum residualized by market return. "
            "Source: https://repub.eur.nl/pub/22252/ResidualMomentum-2011.pdf"
        ),
        "lookback": 252,
    },
    "research_short_reversal": {
        "label": "Short Reversal",
        "category": "research_reversal",
        "dependencies": ["klines_daily.close"],
        "description": (
            "Negative 5-day return short-term reversal. "
            "Source: https://www.nber.org/papers/w2533"
        ),
        "lookback": 10,
    },
    "research_turnover_liquidity": {
        "label": "Turnover Liquidity",
        "category": "research_liquidity",
        "dependencies": ["klines_daily.volume", "klines_daily.amount"],
        "description": (
            "Liquidity proxy using amount and volume. "
            "Source: https://ideas.repec.org/a/eee/finmar/v5y2002i1p31-56.html"
        ),
        "lookback": 20,
    },
}


CATALOG_GROUPS: dict[str, dict[str, Any]] = {
    "ta_lib_core": {
        "name": "ta_lib_core",
        "display_name": "TA-Lib 核心因子",
        "description": "AKQuant TA-Lib backed technical factors.",
        "factor_names": list(TA_FACTOR_SPECS),
    },
    "alpha101": {
        "name": "alpha101",
        "display_name": "Alpha101",
        "description": "WorldQuant 101 Formulaic Alphas adapted to local OHLCV data.",
        "factor_names": [f"alpha101_{i:03d}" for i in range(1, 102)],
    },
    "research_factor_ideas": {
        "name": "research_factor_ideas",
        "display_name": "海外研究因子",
        "description": "Research-inspired quality, investment, risk, momentum and liquidity factors.",
        "factor_names": list(RESEARCH_FACTOR_SPECS),
    },
}


def list_catalog_definitions() -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    for name, spec in TA_FACTOR_SPECS.items():
        definitions.append(_definition(
            name,
            str(spec["label"]),
            category=str(spec["category"]),
            description=f"TA-Lib technical factor: {spec['label']}.",
            lookback_days=int(spec.get("lookback") or 0),
            source="catalog.ta_lib",
        ))
    for i in range(1, 102):
        definitions.append(_definition(
            f"alpha101_{i:03d}",
            f"Alpha101 #{i:03d}",
            category="alpha101",
            description="WorldQuant 101 Formulaic Alphas local-compatible implementation.",
            lookback_days=260,
            source="catalog.alpha101",
            data_policy={"requires_panel": True, "industry_neutralization": "stocks.industry when available"},
        ))
    for name, spec in RESEARCH_FACTOR_SPECS.items():
        definitions.append(_definition(
            name,
            str(spec["label"]),
            category=str(spec["category"]),
            description=str(spec["description"]),
            dependencies=list(spec.get("dependencies") or []),
            lookback_days=int(spec.get("lookback") or 0),
            source="catalog.research",
        ))
    return definitions


def get_catalog_definition(name: str) -> dict[str, Any] | None:
    return next((item for item in list_catalog_definitions() if item["name"] == name), None)


def list_catalog_groups() -> list[dict[str, Any]]:
    return list(CATALOG_GROUPS.values())


def get_catalog_group(name: str) -> dict[str, Any] | None:
    return CATALOG_GROUPS.get(name)


def is_catalog_factor(name: str) -> bool:
    return (
        name in TA_FACTOR_SPECS
        or name in RESEARCH_FACTOR_SPECS
        or (name.startswith("alpha101_") and name[-3:].isdigit() and 1 <= int(name[-3:]) <= 101)
    )
