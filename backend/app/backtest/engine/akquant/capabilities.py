"""Runtime capability discovery for optional AKQuant features."""
from __future__ import annotations

import importlib.util
from typing import Any

from app.backtest.engine.akquant import AKQUANT_AVAILABLE


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def get_akquant_capabilities() -> dict[str, Any]:
    """Return feature flags and public API availability for the installed AKQuant."""
    if not AKQUANT_AVAILABLE:
        return {
            "available": False,
            "version": None,
            "features": {},
            "missing": ["akquant"],
        }

    import akquant as aq

    talib_names = []
    try:
        import akquant.talib as aq_talib

        talib_names = [
            name for name in getattr(aq_talib, "__all__", [])
            if not str(name).startswith("_")
        ]
    except Exception:
        talib_names = []

    feature_checks = {
        "backtest": hasattr(aq, "run_backtest"),
        "grid_search": hasattr(aq, "run_grid_search"),
        "walk_forward": hasattr(aq, "run_walk_forward"),
        "polars_factor_engine": _module_available("polars")
        and _module_available("akquant.factor.engine"),
        "talib_compat": bool(talib_names),
        "strategy_params": all(
            hasattr(aq, name)
            for name in ("ParamModel", "get_strategy_param_schema", "validate_strategy_params")
        ),
        "risk_config": hasattr(aq, "RiskConfig"),
        "instrument_config": hasattr(aq, "InstrumentConfig"),
        "live_runner": hasattr(aq, "live") or _module_available("akquant.live"),
        "ml_adapters": _module_available("akquant.ml.model"),
    }

    optional_modules = {
        "polars": _module_available("polars"),
        "sklearn": _module_available("sklearn"),
        "torch": _module_available("torch"),
        "talib": _module_available("talib"),
        "joblib": _module_available("joblib"),
    }

    return {
        "available": True,
        "version": getattr(aq, "__version__", "unknown"),
        "features": feature_checks,
        "optional_modules": optional_modules,
        "talib_function_count": len(talib_names),
        "talib_functions": talib_names,
    }
