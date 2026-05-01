"""Factor backtest service -- quantile-based layered backtest."""

from datetime import date

from loguru import logger

from app.models.factor import (
    BacktestMetrics,
    BacktestReport,
    BtConfig,
    FactorConfig,
    NAVPoint,
)


class FactorBacktestService:
    """Runs quantile-based factor backtests."""

    def validate_config(self, config: FactorConfig, bt_config: BtConfig) -> list[str]:
        """Validate config before running backtest. Returns list of error messages."""
        errors = []
        if not config.expression or not config.expression.strip():
            errors.append("Factor expression is empty")
        if config.start_date >= config.end_date:
            errors.append("start_date must be before end_date")
        return errors

    async def run(
        self, config: FactorConfig, bt_config: BtConfig | None = None
    ) -> BacktestReport:
        """Run factor backtest and return BacktestReport."""
        bt_config = bt_config or BtConfig()

        errors = self.validate_config(config, bt_config)
        if errors:
            return BacktestReport(logs=[f"Validation error: {e}" for e in errors])

        try:
            from app.backtest.runner import get_backtest_runner
            from app.backtest.config import BacktestConfig as V2BacktestConfig

            runner = get_backtest_runner()

            # Resolve stock pool to symbols
            from app.services.compute_service import compute_service

            symbols = await compute_service._resolve_stock_pool(config.stock_pool)

            # Map RebalancePeriod enum value to string for v2 config
            rebalance_val = (
                bt_config.rebalance_period.value
                if hasattr(bt_config.rebalance_period, "value")
                else bt_config.rebalance_period
            )

            v2_config = V2BacktestConfig(
                mode="vectorized",
                symbols=symbols,
                start_date=config.start_date,
                end_date=config.end_date,
                initial_capital=1_000_000,
                factor_expression=config.expression,
                rebalance_freq=str(rebalance_val),
                n_groups=5,
                commission_rate=bt_config.fee_rate,
                slippage=bt_config.slippage,
                bar_type="daily",
            )

            result = await runner.run(v2_config)
            result_dict = result.to_dict()

            # Convert nav_series from [{"date": str, "nav": float}] to NAVPoint list
            nav_points = [
                NAVPoint(date=date.fromisoformat(d["date"]), value=d["nav"])
                for d in result_dict.get("nav_series", [])
            ]

            return BacktestReport(
                nav_series=nav_points,
                benchmark_series=[],
                metrics=BacktestMetrics(
                    total_return=result_dict.get("total_return", 0),
                    annual_return=result_dict.get("annual_return", 0),
                    sharpe=result_dict.get("sharpe_ratio", 0),
                    max_drawdown=result_dict.get("max_drawdown", 0),
                    alpha=0.0,
                    beta=0.0,
                    ir=0.0,
                ),
                logs=[
                    f"Backtest completed. Total return: {result_dict.get('total_return', 0):.2%}"
                ],
            )
        except Exception as e:
            logger.opt(exception=True).error(f"Factor backtest failed: {e}")
            return BacktestReport(logs=[f"Backtest failed: {str(e)}"])


factor_backtest_service = FactorBacktestService()
