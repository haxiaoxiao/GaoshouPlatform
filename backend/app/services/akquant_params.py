"""AKQuant strategy parameter schema helpers."""
from __future__ import annotations

from typing import Any, Mapping

from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.akquant.engine import _load_strategy_class


class AkquantParamsUnavailableError(RuntimeError):
    """Raised when AKQuant parameter helpers cannot run."""


def get_strategy_param_schema(strategy_code: str) -> dict[str, Any]:
    """Return AKQuant ParamModel schema for a strategy class."""
    if not AKQUANT_AVAILABLE:
        raise AkquantParamsUnavailableError("akquant is not installed")
    if not strategy_code or not strategy_code.strip():
        raise ValueError("strategy_code is required")

    import akquant as aq

    strategy_cls = _load_strategy_class(strategy_code)
    if not hasattr(aq, "get_strategy_param_schema"):
        raise AkquantParamsUnavailableError("akquant.get_strategy_param_schema is unavailable")
    return aq.get_strategy_param_schema(strategy_cls)


def validate_strategy_params(
    strategy_code: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a strategy parameter payload with AKQuant."""
    if not AKQUANT_AVAILABLE:
        raise AkquantParamsUnavailableError("akquant is not installed")
    if not strategy_code or not strategy_code.strip():
        raise ValueError("strategy_code is required")

    import akquant as aq

    strategy_cls = _load_strategy_class(strategy_code)
    if not hasattr(aq, "validate_strategy_params"):
        raise AkquantParamsUnavailableError("akquant.validate_strategy_params is unavailable")
    return aq.validate_strategy_params(strategy_cls, payload)
