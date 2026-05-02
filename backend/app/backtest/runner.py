"""回测统一入口"""
import uuid
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.vectorized import get_vectorized_engine


class BacktestRunner:
    """回测运行器 — 统一入口"""

    async def run(self, config: BacktestConfig, task_store: dict | None = None) -> BacktestResult:
        """运行回测"""
        if config.mode == "vectorized":
            return await self._run_vectorized(config)
        elif config.mode == "event_driven":
            return await self._run_event_driven(config, task_store=task_store)
        else:
            raise ValueError(f"Unknown backtest mode: {config.mode}")

    async def _run_vectorized(self, config: BacktestConfig) -> BacktestResult:
        if config.factor_expression is None:
            raise ValueError("factor_expression is required for vectorized backtest")

        factor_matrix = await self._load_factor_matrix(
            config.factor_expression, config.symbols,
            config.start_date, config.end_date,
        )

        return_matrix = await self._load_return_matrix(
            config.symbols, config.start_date, config.end_date,
        )

        engine = get_vectorized_engine()
        result = engine.run(factor_matrix, return_matrix, config)
        return result

    async def _run_event_driven(self, config: BacktestConfig, task_store: dict | None = None) -> BacktestResult:
        """事件驱动回测 — 使用 EventBus + Portfolio + Risk + Analysis 完整管线"""
        from app.backtest.event import (
            Event, EventBus, EventType, BarEventSource,
            TradingCalendar, EventDrivenExecutor,
        )
        from app.backtest.portfolio import (
            Account, PositionManager, Portfolio,
            CashValidator, PriceValidator, PositionLimitValidator,
        )
        from app.backtest.analysis import TradeCollector, OrderCollector
        from app.backtest.analysis.metrics import compute_metrics
        from app.backtest.strategy_loader import ExpressionSignalStrategy, StrategyContext

        symbols = config.symbols
        start = config.start_date or date(2020, 1, 1)
        end = config.end_date or date(2025, 12, 31)

        # ── 1. 建立事件总线 ──
        event_bus = EventBus()
        logger.info("EventBus initialized")

        # ── 2. 加载交易日历 ──
        calendar = await TradingCalendar.from_clickhouse(symbols, start, end)
        if len(calendar) == 0:
            logger.warning("No trading dates found, returning empty result")
            return BacktestResult(initial_capital=config.initial_capital)

        # ── 3. 加载 Bar 数据 ──
        try:
            event_source = await BarEventSource.from_clickhouse(symbols, start, end, calendar)
        except ValueError as e:
            logger.error("BarEventSource load failed: {}", e)
            return BacktestResult(initial_capital=config.initial_capital)

        # ── 4. 构建投资组合组件 ──
        account = Account(cash=config.initial_capital)
        position_manager = PositionManager()
        portfolio = Portfolio(account=account, position_manager=position_manager)

        # ── 5. 注册系统监听器 ──
        # Portfolio 快照录制
        portfolio.register_listeners(event_bus)

        # 风控校验链
        CashValidator(account).register(event_bus)
        PriceValidator(0.1).register(event_bus)
        if config.max_positions:
            PositionLimitValidator(position_manager, config.max_positions).register(event_bus)

        # 数据收集器
        trade_collector = TradeCollector()
        trade_collector.register(event_bus)

        # ── Live data recording for polling API ──
        events_buffer: list[dict] = []
        _day_count = 0

        def record_event(event_type: str, data: dict):
            if task_store is not None:
                data_copy = {k: v for k, v in data.items() if k != "ctx"}
                events_buffer.append({
                    "type": event_type,
                    "timestamp": datetime.now().isoformat(),
                    **data_copy,
                })

        def on_after_trading_snapshot(event: Event):
            nonlocal _day_count
            if task_store is None:
                return
            _day_count += 1
            dt = event.data.get("date")
            task_store["progress"] = _day_count / len(calendar) if len(calendar) > 0 else 0
            task_store["live"] = {
                "current_date": dt.isoformat() if hasattr(dt, 'isoformat') else str(dt) if dt else None,
                "events": events_buffer[-200:],
                "positions": {
                    sym: {
                        "shares": pos.total_shares,
                        "avg_cost": round(pos.avg_cost, 4),
                        "market_value": round(pos.market_value, 2),
                        "unrealized_pnl": round(pos.unrealized_pnl, 2),
                    }
                    for sym, pos in position_manager.positions.items()
                    if pos.total_shares > 0
                },
                "metrics_snapshot": {
                    "total_return": round(portfolio.total_value / config.initial_capital - 1, 4),
                    "cash": round(account.cash, 2),
                    "total_value": round(portfolio.total_value, 2),
                    "n_trades": len(trade_collector.trades),
                },
            }

        event_bus.add_listener(EventType.AFTER_TRADING, on_after_trading_snapshot, system=True)

        order_collector = OrderCollector()
        order_collector.register(event_bus)

        # ── 6. 策略上下文 + 信号策略 ──
        ctx = StrategyContext(
            account=account,
            position_manager=position_manager,
            event_source=event_source,
        )

        # 使用 factor_expression 作为信号表达式（事件驱动模式下）
        expression = config.factor_expression or config.buy_condition
        if expression:
            strategy = ExpressionSignalStrategy(
                expression=expression,
                symbols=symbols,
                buy_threshold=0.0,
                sell_threshold=0.0,
                position_pct=0.2,
            )
            strategy.register(event_bus, ctx)

        # ── 7. BAR → 订单 → 成交管线 ──
        def on_order_creation_pass(event: Event) -> None:
            """订单风控通过 → 撮合成交"""
            order = event.data.get("order", {})
            bar = event.data.get("bar")
            date_val = event.data.get("date")

            if bar is None:
                return

            order_id = order.get("order_id", "")
            symbol = order.get("symbol", "")
            direction = order.get("direction", "buy")
            price = order.get("price", bar.close)
            quantity = order.get("quantity", 0)

            # 模拟撮合 — 按当日收盘价成交
            trade_price = bar.close
            commission = trade_price * quantity * config.commission_rate
            slippage_cost = trade_price * quantity * config.slippage

            if direction == "buy":
                cost = trade_price * quantity + commission + slippage_cost
                if cost > account.available_cash:
                    event.stop_propagation()
                    return
                account.commit_buy(cost)
                try:
                    position_manager.get_or_create(symbol).buy(quantity, trade_price, date_val)
                except Exception:
                    return
            else:
                proceeds = trade_price * quantity - commission - slippage_cost
                try:
                    _, realized_pnl = position_manager.get_or_create(symbol).sell(
                        quantity, trade_price, date_val
                    )
                except ValueError:
                    event.stop_propagation()
                    return
                account.commit_sell(proceeds)

            trade_id = str(uuid.uuid4())[:12]
            event_bus.publish_event(Event(
                EventType.TRADE,
                data={
                    "trade_id": trade_id,
                    "order_id": order_id,
                    "symbol": symbol,
                    "direction": direction,
                    "price": trade_price,
                    "quantity": quantity,
                    "commission": commission,
                    "date": date_val,
                    "pnl": None,
                },
            ))
            record_event("TRADE", {
                "symbol": symbol, "direction": direction,
                "quantity": quantity, "price": round(trade_price, 4),
                "commission": round(commission, 4),
            })

        event_bus.add_listener(EventType.ORDER_CREATION_PASS, on_order_creation_pass, system=True)

        # ── 8. 策略上下文中转订单到事件总线 ──
        def on_bar_dispatch_orders(event: Event) -> None:
            """BAR 处理后，将策略上下文的待发订单推送到 EventBus 做风控"""
            ctx.current_date = event.data.get("date")
            ctx.current_bar = event.data.get("bar")
            if ctx.current_bar:
                bar = ctx.current_bar
                record_event("BAR", {
                    "symbol": bar.symbol,
                    "close": round(bar.close, 4),
                })
                orders = ctx.pending_orders.copy()
                ctx.clear_orders()

                for order in orders:
                    # 补充 ref_price 用于价格校验
                    order["ref_price"] = bar.close
                    # 风控校验 → 通过则撮合
                    order_event = Event(
                        EventType.ORDER_PENDING_NEW,
                        data={"order": order, "bar": bar, "date": ctx.current_date},
                    )
                    if event_bus.publish_event(order_event):
                        event_bus.publish_event(Event(
                            EventType.ORDER_CREATION_PASS,
                            data=order_event.data,
                        ))
                        record_event("ORDER_PASS", {
                            "order_id": order.get("order_id", ""),
                            "symbol": order.get("symbol", ""),
                            "direction": order.get("direction", "buy"),
                            "quantity": order.get("quantity", 0),
                            "price": order.get("price", 0),
                        })
                    else:
                        record_event("ORDER_REJECT", {
                            "order_id": order.get("order_id", ""),
                            "symbol": order.get("symbol", ""),
                            "reason": "风控拒绝",
                        })

        event_bus.add_listener(EventType.POST_BAR, on_bar_dispatch_orders, system=True)

        # ── 9. 执行主循环 ──
        executor = EventDrivenExecutor(event_bus, calendar)
        executor.run(event_source)

        # ── 10. 计算绩效 ──
        navs = portfolio.nav_series
        d_returns = portfolio.daily_returns

        if not navs:
            return BacktestResult(initial_capital=config.initial_capital,
                                  final_capital=config.initial_capital)

        metrics = compute_metrics(
            nav_series=navs,
            daily_returns=d_returns,
            trades=trade_collector.to_dicts(),
            risk_free=0.02,
            freq=config.bar_type,
        )

        final_nav = navs[-1]["nav"] if navs else 1.0
        n_days = len(navs)

        return BacktestResult(
            total_return=metrics.total_return,
            annual_return=metrics.annual_return,
            annual_volatility=metrics.annual_volatility,
            sharpe_ratio=metrics.sharpe_ratio,
            sortino_ratio=metrics.sortino_ratio,
            max_drawdown=metrics.max_drawdown,
            calmar_ratio=metrics.calmar_ratio,
            alpha=metrics.alpha,
            beta=metrics.beta,
            information_ratio=metrics.information_ratio,
            total_trades=metrics.total_trades,
            win_trades=metrics.win_trades,
            loss_trades=metrics.loss_trades,
            win_rate=metrics.win_rate,
            avg_return=metrics.avg_return,
            nav_series=navs,
            daily_returns=d_returns,
            trades=trade_collector.to_dicts(),
            start_date=start,
            end_date=end,
            initial_capital=config.initial_capital,
            final_capital=config.initial_capital * final_nav,
            n_trading_days=n_days,
        )

    async def _load_factor_matrix(
        self,
        expression: str,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载并计算因子矩阵"""
        from app.db.clickhouse import get_ch_client
        from app.compute.expression import evaluate_expression

        ch = get_ch_client()

        query = """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
        """
        params: dict = {"syms": symbols}
        if start_date:
            query += " AND trade_date >= %(start)s"
            params["start"] = start_date
        if end_date:
            query += " AND trade_date <= %(end)s"
            params["end"] = end_date
        query += " ORDER BY symbol, trade_date"

        rows = ch.execute(query, params)
        if not rows:
            raise ValueError("No data found")

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close",
                     "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        result = evaluate_expression(expression, data)

        if isinstance(result, dict):
            factor_dfs = []
            for sym, series in result.items():
                if isinstance(series, pd.Series):
                    s = series.rename(sym)
                    factor_dfs.append(s)
            if factor_dfs:
                return pd.concat(factor_dfs, axis=1)

        raise ValueError("Failed to compute factor matrix")

    async def _load_return_matrix(
        self,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载收益率矩阵 — 下一日收益率"""
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()

        rows = ch.execute(
            """
            SELECT symbol, trade_date, close
            FROM klines_daily
            WHERE symbol IN %(syms)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols},
        )

        df = pd.DataFrame(rows, columns=["symbol", "trade_date", "close"])
        df["close"] = df["close"].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        return_matrix = {}
        for sym, grp in df.groupby("symbol"):
            grp = grp.sort_values("trade_date")
            ret = grp["close"].pct_change().shift(-1)
            ret.index = grp["trade_date"]
            return_matrix[sym] = ret

        if return_matrix:
            return pd.DataFrame(return_matrix)
        return pd.DataFrame()


# 全局单例
_runner: BacktestRunner | None = None


def get_backtest_runner() -> BacktestRunner:
    global _runner
    if _runner is None:
        _runner = BacktestRunner()
    return _runner
