"""Tests for factor backtest service."""
from datetime import date

from app.models.factor import FactorConfig, BtConfig, StockPool, PortfolioType
from app.services.factor_backtest import FactorBacktestService


class TestFactorBacktestService:
    def setup_method(self):
        self.service = FactorBacktestService()

    def test_validate_config_long_only(self):
        config = FactorConfig(
            expression="$close",
            stock_pool=StockPool.HS300,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )
        bt_config = BtConfig(portfolio_type=PortfolioType.LONG_ONLY)
        errors = self.service.validate_config(config, bt_config)
        assert errors == []

    def test_validate_config_empty_expression(self):
        config = FactorConfig(
            expression="",
            stock_pool=StockPool.HS300,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )
        bt_config = BtConfig()
        errors = self.service.validate_config(config, bt_config)
        assert len(errors) > 0

    def test_validate_config_invalid_dates(self):
        config = FactorConfig(
            expression="$close",
            stock_pool=StockPool.HS300,
            start_date=date(2024, 12, 31),
            end_date=date(2024, 1, 1),
        )
        errors = self.service.validate_config(config, BtConfig())
        assert len(errors) > 0

    def test_default_bt_config(self):
        bt = BtConfig()
        assert bt.rebalance_period == "monthly"
        assert bt.fee_rate == 0.001
        assert bt.slippage == 0.001
        assert bt.filter_limit_up is True
