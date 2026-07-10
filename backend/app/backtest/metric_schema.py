from __future__ import annotations

from typing import Any


def normalize_metrics_v2(payload: dict[str, Any], *, costs: dict[str, Any] | None = None) -> dict[str, Any]:
    annualized = payload.get("annualized_return", payload.get("annual_return", 0.0))
    sharpe = payload.get("sharpe_ratio", payload.get("sharpe", 0.0))
    benchmark_symbol = payload.get("benchmark_symbol")
    return {
        "result_schema_version": 2,
        "total_return": float(payload.get("total_return") or 0.0),
        "annualized_return": float(annualized or 0.0),
        "annualized_volatility": float(payload.get("annual_volatility") or 0.0),
        "max_drawdown": abs(float(payload.get("max_drawdown") or 0.0)),
        "sharpe_ratio": float(sharpe or 0.0),
        "sortino_ratio": float(payload.get("sortino") or 0.0),
        "turnover_rate": float(payload.get("turnover_rate") or 0.0),
        "benchmark": {"symbol": str(benchmark_symbol)} if benchmark_symbol else {},
        "costs": dict(costs or {}),
        "warnings": list(payload.get("warnings") or []),
    }
