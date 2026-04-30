"""因子评估 API 测试"""
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

from app.backtest.config import BacktestResult
from app.main import app


@pytest.fixture
def mock_service():
    """Mock FactorEvaluationService 所有方法"""
    with patch("app.api.evaluation.FactorEvaluationService") as mock_cls:
        instance = MagicMock()

        # run_ic_analysis
        instance.run_ic_analysis = AsyncMock(return_value={
            "ic_series": [{"date": "2024-01-05", "ic": 0.15}],
            "ic_stats": {"mean": 0.12, "std": 0.05, "icir": 2.4, "positive_rate": 0.65},
            "ic_decay": [
                {"lag": 1, "ic_mean": 0.12},
                {"lag": 3, "ic_mean": 0.10},
                {"lag": 5, "ic_mean": 0.08},
                {"lag": 10, "ic_mean": 0.05},
                {"lag": 20, "ic_mean": 0.02},
            ],
        })

        # run_quantile_backtest
        instance.run_quantile_backtest = AsyncMock(return_value={
            "total_return": 0.12,
            "annual_return": 0.08,
            "sharpe_ratio": 1.5,
            "max_drawdown": -0.05,
            "calmar_ratio": 1.6,
            "nav_series": [{"date": "2024-01-05", "nav": 1.0}],
            "group_navs": [
                {"group": "Q1", "dates": ["2024-01-05"], "navs": [1.0]},
                {"group": "Q5", "dates": ["2024-01-05"], "navs": [1.0]},
            ],
            "n_trading_days": 100,
        })

        # run_full_report
        instance.run_full_report = AsyncMock(return_value={
            "expression": "Mean($close, 5)",
            "parameters": {
                "symbols": ["000001.SZ"],
                "start_date": "2024-01-02",
                "end_date": "2024-04-30",
                "n_groups": 5,
                "rebalance_freq": "monthly",
            },
            "ic_analysis": {"ic_stats": {"mean": 0.12}},
            "quantile_backtest": {"sharpe_ratio": 1.5},
            "summary": {
                "ic_mean": 0.12,
                "icir": 2.4,
                "long_short_annual_return": 0.08,
                "long_short_sharpe": 1.5,
                "max_drawdown": -0.05,
            },
        })

        mock_cls.return_value = instance
        yield instance


@pytest.mark.asyncio
async def test_ic_analysis_success(mock_service):
    """IC 分析端点返回正确结构"""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.post(
            "/api/v2/evaluation/ic-analysis",
            json={
                "expression": "Mean($close, 5)",
                "symbols": ["000001.SZ", "000002.SZ"],
                "start_date": "2024-01-02",
                "end_date": "2024-04-30",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["message"] == "success"
    assert body["data"] is not None
    assert "ic_series" in body["data"]
    assert "ic_stats" in body["data"]
    assert "ic_decay" in body["data"]


@pytest.mark.asyncio
async def test_quantile_backtest_success(mock_service):
    """分层回测端点返回正确结构"""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.post(
            "/api/v2/evaluation/quantile-backtest",
            json={
                "expression": "Mean($close, 5)",
                "symbols": ["000001.SZ", "000002.SZ"],
                "start_date": "2024-01-02",
                "end_date": "2024-04-30",
                "n_groups": 5,
                "rebalance_freq": "monthly",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["message"] == "success"
    assert body["data"] is not None
    assert "total_return" in body["data"]
    assert "sharpe_ratio" in body["data"]


@pytest.mark.asyncio
async def test_full_report_success(mock_service):
    """完整报告端点返回正确结构"""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.post(
            "/api/v2/evaluation/full-report",
            json={
                "expression": "Mean($close, 5)",
                "symbols": ["000001.SZ", "000002.SZ"],
                "start_date": "2024-01-02",
                "end_date": "2024-04-30",
                "n_groups": 5,
                "rebalance_freq": "monthly",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["message"] == "success"
    assert body["data"] is not None
    assert "expression" in body["data"]
    assert "parameters" in body["data"]
    assert "ic_analysis" in body["data"]
    assert "quantile_backtest" in body["data"]
    assert "summary" in body["data"]


@pytest.mark.asyncio
async def test_ic_analysis_error_handling(mock_service):
    """异常时返回错误格式"""
    mock_service.run_ic_analysis.side_effect = ValueError("Test error")

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.post(
            "/api/v2/evaluation/ic-analysis",
            json={
                "expression": "InvalidExpr(",
                "symbols": ["000001.SZ"],
                "start_date": "2024-01-02",
                "end_date": "2024-04-30",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 1
    assert "Test error" in body["message"]
    assert body["data"] is None


@pytest.mark.asyncio
async def test_missing_symbols_returns_422():
    """缺失必填字段返回 422"""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.post(
            "/api/v2/evaluation/ic-analysis",
            json={
                "expression": "Mean($close, 5)",
                "start_date": "2024-01-02",
                "end_date": "2024-04-30",
            },
        )

    assert resp.status_code == 422
