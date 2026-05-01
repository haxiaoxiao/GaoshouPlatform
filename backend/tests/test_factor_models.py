"""Tests for factor Pydantic models."""
from datetime import date

import pytest
from pydantic import ValidationError

from app.models.factor import (
    FactorConfig,
    EvalConfig,
    BtConfig,
    FactorReport,
    BacktestReport,
    StockPool,
    FactorDirection,
    BoardQuery,
    ValidateRequest,
    ValidateResponse,
)


class TestFactorConfig:
    def test_minimal_config(self):
        cfg = FactorConfig(
            expression="(1/PE_TTM) + ROE",
            stock_pool=StockPool.HS300,
            start_date=date(2020, 1, 1),
            end_date=date(2025, 12, 31),
        )
        assert cfg.benchmark == "000300.SH"
        assert cfg.direction == FactorDirection.DESC

    def test_defaults(self):
        cfg = FactorConfig(
            expression="$close",
            stock_pool="hs300",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        assert cfg.benchmark == "000300.SH"
        assert cfg.direction == FactorDirection.DESC

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            FactorConfig(expression="$close")  # missing stock_pool, dates

    def test_invalid_stock_pool(self):
        with pytest.raises(ValidationError):
            FactorConfig(
                expression="$close",
                stock_pool="invalid_pool",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )


class TestEvalConfig:
    def test_defaults(self):
        cfg = EvalConfig()
        assert cfg.ic_method == "spearman"
        assert cfg.group_count == 5
        assert cfg.outlier_handling == "winsorize"
        assert cfg.industry_neutralization is False

    def test_group_count_bounds(self):
        with pytest.raises(ValidationError):
            EvalConfig(group_count=1)
        with pytest.raises(ValidationError):
            EvalConfig(group_count=21)


class TestBtConfig:
    def test_defaults(self):
        cfg = BtConfig()
        assert cfg.rebalance_period == "monthly"
        assert cfg.fee_rate == 0.001
        assert cfg.portfolio_type == "long_only"

    def test_fee_rate_bounds(self):
        with pytest.raises(ValidationError):
            BtConfig(fee_rate=-0.01)
        with pytest.raises(ValidationError):
            BtConfig(fee_rate=0.06)


class TestBoardQuery:
    def test_defaults(self):
        q = BoardQuery()
        assert q.stock_pool == StockPool.ZZ500
        assert q.page == 1
        assert q.page_size == 20

    def test_page_size_bounds(self):
        with pytest.raises(ValidationError):
            BoardQuery(page_size=0)
        with pytest.raises(ValidationError):
            BoardQuery(page_size=101)


class TestValidateResponse:
    def test_valid_response(self):
        r = ValidateResponse(valid=True, preview_rows=[{"symbol": "000001.XSHE", "value": 1.23}])
        assert r.valid is True
        assert r.error is None
        assert len(r.preview_rows) == 1

    def test_invalid_response(self):
        r = ValidateResponse(valid=False, error="Unknown variable: $foo")
        assert r.valid is False
        assert r.error == "Unknown variable: $foo"
        assert r.preview_rows is None


class TestFactorReport:
    def test_empty_report(self):
        r = FactorReport(update_date=date(2026, 5, 1))
        assert r.ic_series == []
        assert r.top20 == []
        assert r.bottom20 == []


class TestBacktestReport:
    def test_minimal(self):
        r = BacktestReport(logs=["Backtest completed in 2.3s"])
        assert r.nav_series == []
        assert r.metrics is None
        assert len(r.logs) == 1
