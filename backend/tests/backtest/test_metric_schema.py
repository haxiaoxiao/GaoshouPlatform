from app.backtest.metric_schema import normalize_metrics_v2


def test_metric_schema_v2_normalizes_drawdown_and_names():
    result = normalize_metrics_v2(
        {
            "total_return": 0.25,
            "annual_return": 0.12,
            "max_drawdown": -0.18,
            "sharpe": 1.3,
            "turnover_rate": 2.1,
            "benchmark_symbol": "000300.SH",
            "warnings": ["sample"],
        },
        costs={"commission_rate": 0.0003, "slippage": 0.001},
    )

    assert result["result_schema_version"] == 2
    assert result["annualized_return"] == 0.12
    assert result["max_drawdown"] == 0.18
    assert result["sharpe_ratio"] == 1.3
    assert result["benchmark"] == {"symbol": "000300.SH"}
    assert result["costs"]["slippage"] == 0.001
    assert result["warnings"] == ["sample"]
