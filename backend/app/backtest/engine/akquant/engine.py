"""AKQuant engine adapter for the platform backtest interface."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
import gc
import hashlib
import os
import shutil
import textwrap
from datetime import date, timedelta
from typing import Any, Callable

from loguru import logger

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.engine import EngineRegistry
from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.interface import IBacktestEngine, IDataProvider

if AKQUANT_AVAILABLE:
    import akquant as aq
    from akquant.config import BacktestConfig as AQBacktestConfig
    from akquant.config import InstrumentConfig
    from akquant.config import RiskConfig
    from akquant.config import StrategyConfig

from app.backtest.engine.akquant.adapter import ClickHouseFeedAdapter
from app.backtest.engine.akquant.normalizer import normalize_result
from app.backtest.engine.akquant.reporter import REPORTS_DIR, generate_report
from app.services.timer_minute_sync import timer_times_from_params, timer_times_to_strings

_DYNAMIC_STRATEGY_REGISTRY: dict[str, type] = {}
_DROP_STATE_VALUE = object()
_TRANSIENT_STATE_KEYS = {
    "ctx",
    "current_bar",
    "current_tick",
    "_engine",
    "_analyzer_manager",
    "_current_bars",
    "_current_day_volume",
}
_TRANSIENT_EXTENSION_TYPES = {
    ("builtins", "Bar"),
    ("builtins", "Tick"),
    ("builtins", "StrategyContext"),
}


def _timer_strings_from_strategy_param(value: Any) -> tuple[str, ...]:
    if value in (None, "", []):
        return ()
    return timer_times_to_strings(timer_times_from_params({"timer_times": value}))


def _smart_keep_index_symbols(config: BacktestConfig) -> tuple[str, ...]:
    params = config.strategy_params or {}
    symbol = str(params.get("index_symbol") or config.benchmark_symbol or "")
    values = {symbol} if symbol else set()
    if symbol == "399101.SZ":
        values.add("399101.XSHE")
    elif symbol == "399101.XSHE":
        values.add("399101.SZ")
    return tuple(v for v in values if v)


@EngineRegistry.register
class AkquantEngine(IBacktestEngine):
    """AKQuant backtest engine wrapper."""

    name = "akquant"
    label = "AKQuant"
    supported_modes = ["event_driven"]

    async def run(
        self,
        config: BacktestConfig,
        data_provider: IDataProvider,
        progress_callback: Callable[[float, dict | None], None] | None = None,
    ) -> BacktestResult:
        if not AKQUANT_AVAILABLE:
            raise RuntimeError("akquant is not installed. Run: pip install akquant")

        start_date = config.start_date or date(2020, 1, 1)
        end_date = config.end_date or date(2025, 12, 31)

        timer_times = (
            timer_times_to_strings(timer_times_from_params(config.strategy_params))
            if config.bar_type == "minute_timer"
            else None
        )
        adapter = ClickHouseFeedAdapter(
            data_provider,
            config.symbols,
            start_date,
            end_date,
            config.bar_type,
            timer_times=timer_times,
            smart_candidate_top_n=int((config.strategy_params or {}).get("smart_timer_candidate_top_n", 0) or 0),
            smart_full_universe_times=_timer_strings_from_strategy_param(
                (config.strategy_params or {}).get("smart_timer_full_universe_times")
            ),
            smart_keep_index_symbols=_smart_keep_index_symbols(config),
            timer_price_adjustment_mode=str(
                (config.strategy_params or {}).get("timer_price_adjustment_mode", "none")
            ),
        )
        await adapter.preload()

        if not adapter.has_any_data:
            logger.warning("AkquantEngine: no data loaded, returning empty result")
            return BacktestResult(initial_capital=config.initial_capital)

        strategy_code = config.strategy_code or config.factor_expression or ""
        strategy = _build_strategy(strategy_code, config)
        _apply_strategy_params(strategy, config)

        benchmark_returns = None
        if config.benchmark_symbol:
            benchmark_returns = await data_provider.load_benchmark(
                config.benchmark_symbol, start_date, end_date
            )

        history_depth = self._history_depth(config)
        if self._should_run_chunked(config, start_date, end_date):
            raw_result = await self._run_chunked(
                config=config,
                data_provider=data_provider,
                strategy=strategy,
                start_date=start_date,
                end_date=end_date,
                history_depth=history_depth,
                timer_times=timer_times,
                progress_callback=progress_callback,
            )
        else:
            aq_config = self._build_aq_config(config, start_date, end_date, history_depth)

            def _run_sync():
                return aq.run_backtest(
                    data=adapter,
                    strategy=strategy,
                    symbols=config.symbols,
                    config=aq_config,
                    start_time=str(start_date),
                    end_time=str(end_date),
                    t_plus_one=config.t_plus_one,
                    lot_size=config.lot_size,
                    fill_policy=_build_fill_policy(config),
                )

            loop = asyncio.get_running_loop()
            raw_result = await loop.run_in_executor(None, _run_sync)

        result = normalize_result(
            raw_result,
            start_date=start_date,
            end_date=end_date,
            initial_capital=config.initial_capital,
        )

        try:
            task_id = getattr(config, "_task_id", None)
            if task_id:
                asyncio.create_task(
                    asyncio.to_thread(
                        generate_report, task_id, raw_result, benchmark_returns
                    )
                )
        except Exception:
            pass

        if progress_callback:
            live_data = {
                "current_date": str(end_date),
                "events": [
                    {
                        "type": "backtest_done",
                        "timestamp": str(end_date),
                        "message": "AKQuant backtest completed",
                    }
                ],
                "positions": {},
                "metrics_snapshot": {
                    "total_return": result.total_return,
                    "max_drawdown": result.max_drawdown,
                    "sharpe": result.sharpe_ratio,
                    "cash": result.final_capital,
                    "total_value": result.final_capital,
                    "n_trades": result.total_trades,
                },
                "metadata": {
                    "phase": "completed",
                    "bar_type": config.bar_type,
                    "symbol_count": len(config.symbols),
                },
            }
            progress_callback(1.0, live_data)

        return result

    def _build_aq_config(
        self,
        config: BacktestConfig,
        start_date: date,
        end_date: date,
        history_depth: int,
    ) -> Any:
        risk_kwargs = dict(config.risk_config or {})
        if config.max_positions and "max_long_positions" not in risk_kwargs:
            # max_long_positions belongs to StrategyConfig, not RiskConfig.
            pass
        if config.stop_loss is not None and "stop_loss_threshold" not in risk_kwargs:
            risk_kwargs["stop_loss_threshold"] = config.stop_loss
        risk = RiskConfig(**risk_kwargs) if risk_kwargs else None
        instruments_config = _build_instruments_config(config.instruments_config)
        return AQBacktestConfig(
            strategy_config=StrategyConfig(
                initial_cash=config.initial_capital,
                commission_rate=config.commission_rate,
                stamp_tax_rate=config.stamp_tax_rate,
                transfer_fee_rate=config.transfer_fee_rate,
                min_commission=config.min_commission,
                slippage=_build_slippage_policy(config.slippage),
                volume_limit_pct=(
                    config.volume_limit_pct
                    if config.volume_limit_pct is not None
                    else 0.25
                ),
                max_long_positions=config.max_positions,
                exit_on_last_bar=config.exit_on_last_bar,
                indicator_mode=config.indicator_mode,
                risk=risk,
            ),
            start_time=str(start_date),
            end_time=str(end_date),
            instruments=config.symbols,
            instruments_config=instruments_config,
            benchmark=config.benchmark_symbol,
            show_progress=False,
            history_depth=history_depth,
            bootstrap_samples=config.bootstrap_samples,
            analysis_config=config.analysis_config,
        )

    def _history_depth(self, config: BacktestConfig) -> int:
        params = config.strategy_params or {}
        if "ak_history_depth" in params:
            try:
                return max(0, int(params.get("ak_history_depth") or 0))
            except Exception:
                return 0
        if config.bar_type == "minute_timer":
            return 0
        if config.bar_type == "minute":
            return 1200
        return 600

    def _should_run_chunked(
        self,
        config: BacktestConfig,
        start_date: date,
        end_date: date,
    ) -> bool:
        if config.bar_type not in {"minute", "minute_timer"}:
            return False
        if config.bar_type == "minute_timer":
            return False
        if os.getenv("AKQUANT_DISABLE_CHUNKED", "").lower() in {"1", "true", "yes"}:
            return False
        days = (end_date - start_date).days + 1
        threshold_days = int(os.getenv("AKQUANT_CHUNK_THRESHOLD_DAYS", "45"))
        threshold_symbols = int(os.getenv("AKQUANT_CHUNK_THRESHOLD_SYMBOLS", "100"))
        return days > threshold_days or len(config.symbols) > threshold_symbols

    def _chunk_dates(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        chunk_days = max(1, int(os.getenv("AKQUANT_CHUNK_DAYS", "30")))
        chunks: list[tuple[date, date]] = []
        current = start_date
        while current <= end_date:
            chunk_end = min(end_date, current + timedelta(days=chunk_days - 1))
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
        return chunks

    async def _run_chunked(
        self,
        config: BacktestConfig,
        data_provider: IDataProvider,
        strategy: Any,
        start_date: date,
        end_date: date,
        history_depth: int,
        timer_times: tuple[str, ...] | None = None,
        progress_callback: Callable[[float, dict | None], None] | None = None,
    ) -> Any:
        chunks = self._chunk_dates(start_date, end_date)
        task_id = getattr(config, "_task_id", "manual")
        checkpoint_dir = REPORTS_DIR / "checkpoints" / str(task_id)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = checkpoint_dir / "akquant.chkpt"
        raw_result = None

        logger.info(
            "AkquantEngine: chunked minute run, symbols={}, days={}, chunks={}, chunk_days={}",
            len(config.symbols),
            (end_date - start_date).days + 1,
            len(chunks),
            os.getenv("AKQUANT_CHUNK_DAYS", "30"),
        )

        loop = asyncio.get_running_loop()
        try:
            for idx, (chunk_start, chunk_end) in enumerate(chunks, start=1):
                segment_adapter = ClickHouseFeedAdapter(
                    data_provider,
                    config.symbols,
                    chunk_start,
                    chunk_end,
                    config.bar_type,
                    timer_times=timer_times,
                    smart_candidate_top_n=int((config.strategy_params or {}).get("smart_timer_candidate_top_n", 0) or 0),
                    smart_full_universe_times=_timer_strings_from_strategy_param(
                        (config.strategy_params or {}).get("smart_timer_full_universe_times")
                    ),
                    smart_keep_index_symbols=_smart_keep_index_symbols(config),
                    timer_price_adjustment_mode=str(
                        (config.strategy_params or {}).get("timer_price_adjustment_mode", "none")
                    ),
                )
                await segment_adapter.preload()
                if not segment_adapter.has_any_data:
                    logger.info(
                        "AkquantEngine: skip empty chunk {}/{} {}..{}",
                        idx,
                        len(chunks),
                        chunk_start,
                        chunk_end,
                    )
                    if progress_callback:
                        progress_callback(
                            idx / len(chunks),
                            {
                                "current_date": str(chunk_end),
                                "events": [
                                    {
                                        "type": "chunk_skip",
                                        "timestamp": f"{chunk_end}T00:00:00",
                                        "message": (
                                            f"AKQuant chunk {idx}/{len(chunks)} skipped: no data"
                                        ),
                                    }
                                ],
                                "positions": {},
                                "metrics_snapshot": {
                                    "chunk_index": idx,
                                    "chunk_total": len(chunks),
                                    "symbol_count": len(config.symbols),
                                },
                                "metadata": {
                                    "phase": "chunk_skip",
                                    "chunk_index": idx,
                                    "chunk_total": len(chunks),
                                    "chunk_start": str(chunk_start),
                                    "chunk_end": str(chunk_end),
                                    "symbol_count": len(config.symbols),
                                },
                            },
                        )
                    segment_adapter.clear_cache()
                    del segment_adapter
                    gc.collect()
                    continue

                aq_config = self._build_aq_config(
                    config, chunk_start, chunk_end, history_depth
                )
                has_checkpoint = checkpoint_path.exists()

                if progress_callback:
                    progress_callback(
                        (idx - 1) / len(chunks),
                        {
                            "current_date": str(chunk_start),
                            "events": [
                                {
                                    "type": "chunk_start",
                                    "timestamp": f"{chunk_start}T00:00:00",
                                    "message": f"AKQuant chunk {idx}/{len(chunks)}",
                                }
                            ],
                            "positions": {},
                            "metrics_snapshot": {
                                "chunk_index": idx,
                                "chunk_total": len(chunks),
                                "symbol_count": len(config.symbols),
                            },
                            "metadata": {
                                "phase": "chunk_start",
                                "chunk_index": idx,
                                "chunk_total": len(chunks),
                                "chunk_start": str(chunk_start),
                                "chunk_end": str(chunk_end),
                                "symbol_count": len(config.symbols),
                            },
                        },
                    )

                def _run_segment():
                    if not has_checkpoint:
                        result = aq.run_backtest(
                            data=segment_adapter,
                            strategy=strategy,
                            symbols=config.symbols,
                            config=aq_config,
                            start_time=str(chunk_start),
                            end_time=str(chunk_end),
                            t_plus_one=config.t_plus_one,
                            lot_size=config.lot_size,
                            fill_policy=_build_fill_policy(config),
                        )
                    else:
                        result = aq.run_warm_start(
                            str(checkpoint_path),
                            data=segment_adapter,
                            symbols=config.symbols,
                            config=aq_config,
                            show_progress=False,
                            start_time=str(chunk_start),
                            end_time=str(chunk_end),
                        )
                    aq.save_snapshot(result.engine, result.strategy, str(checkpoint_path))
                    return result

                raw_result = await loop.run_in_executor(None, _run_segment)
                segment_adapter.clear_cache()
                del segment_adapter
                gc.collect()

                if progress_callback:
                    progress_callback(
                        idx / len(chunks),
                        {
                            "current_date": str(chunk_end),
                            "events": [
                                {
                                    "type": "chunk_done",
                                    "timestamp": f"{chunk_end}T00:00:00",
                                    "message": (
                                        f"AKQuant chunk {idx}/{len(chunks)} completed"
                                    ),
                                }
                            ],
                            "positions": {},
                            "metrics_snapshot": {
                                "chunk_index": idx,
                                "chunk_total": len(chunks),
                                "symbol_count": len(config.symbols),
                            },
                            "metadata": {
                                "phase": "chunk_done",
                                "chunk_index": idx,
                                "chunk_total": len(chunks),
                                "chunk_start": str(chunk_start),
                                "chunk_end": str(chunk_end),
                                "symbol_count": len(config.symbols),
                            },
                        },
                    )

            if raw_result is None:
                raise RuntimeError("No AKQuant chunks were executed")
            return raw_result
        finally:
            keep = os.getenv("AKQUANT_KEEP_CHECKPOINTS", "").lower()
            if keep not in {"1", "true", "yes"}:
                shutil.rmtree(checkpoint_dir, ignore_errors=True)

    def validate_config(self, config: BacktestConfig) -> list[str]:
        errors: list[str] = []
        if not config.symbols:
            errors.append("symbols cannot be empty")
        if config.start_date is None or config.end_date is None:
            errors.append("start_date and end_date are required")
        if not config.strategy_code and not config.factor_expression:
            errors.append("strategy_code or factor_expression is required")
        return errors


def _build_strategy(code: str, config: BacktestConfig) -> Any:
    """Build an AKQuant strategy from user code or a factor expression."""
    if not code or not code.strip():
        raise ValueError(
            "Strategy code is empty. Define an akquant Strategy subclass, e.g.\n"
            "class MyStrategy(aq.Strategy):\n"
            "    def on_bar(self, bar):\n"
            "        if bar.close > bar.open:\n"
            "            self.buy(bar.symbol, 100)"
        )

    code = code.strip()

    if "def handle_bar" in code or "def init(context)" in code:
        raise ValueError(
            "RQAlpha syntax was detected. AKQuant uses Strategy class syntax:\n"
            "class MyStrategy(aq.Strategy):\n"
            "    def on_bar(self, bar): ..."
        )

    if ("class " in code and "aq.Strategy" in code) or (
        "Strategy" in code and "def on_bar" in code
    ):
        return _load_strategy_class(code)

    return _build_expression_strategy(code, config)


def _build_instruments_config(raw: Any) -> Any:
    """Convert API dictionaries into AKQuant InstrumentConfig objects."""
    if not raw:
        return None

    def _one(item: dict[str, Any]) -> Any:
        allowed = {
            "symbol",
            "asset_type",
            "multiplier",
            "margin_ratio",
            "tick_size",
            "lot_size",
            "commission_rate",
            "min_commission",
            "stamp_tax_rate",
            "transfer_fee_rate",
            "slippage",
            "option_type",
            "strike_price",
            "expiry_date",
            "underlying_symbol",
            "option_margin_model",
            "implied_volatility",
            "reference_volatility",
            "settlement_type",
            "settlement_price",
            "static_attrs",
        }
        return InstrumentConfig(**{k: v for k, v in item.items() if k in allowed})

    if isinstance(raw, list):
        return [_one(item) for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        return {
            symbol: _one({"symbol": symbol, **item})
            for symbol, item in raw.items()
            if isinstance(item, dict)
        }
    return None


def _build_fill_policy(config: BacktestConfig) -> Any:
    """Use AKQuant fill policy defaults when available."""
    params = dict(config.strategy_params or {})
    if config.bar_type == "daily":
        temporal = params.get("fill_temporal", "next_event")
        bar_offset = int(params.get("fill_bar_offset", 1))
    elif config.bar_type == "minute_timer":
        temporal = params.get("fill_temporal", "same_cycle")
        bar_offset = int(params.get("fill_bar_offset", 0))
        params.setdefault("fill_price_basis", "close")
    else:
        temporal = params.get("fill_temporal", "same_cycle")
        bar_offset = int(params.get("fill_bar_offset", 1))
    return {
        "price_basis": params.get("fill_price_basis", "open"),
        "bar_offset": bar_offset,
        "temporal": temporal,
    }


def _build_slippage_policy(slippage: Any) -> Any:
    """Normalize legacy numeric slippage to AKQuant's explicit policy format."""
    if isinstance(slippage, (int, float)):
        return {"type": "percent", "value": float(slippage)}
    return slippage


def _apply_strategy_params(strategy: Any, config: BacktestConfig) -> None:
    params = dict(config.strategy_params or {})
    if config.index_symbol:
        params.setdefault("index_symbol", config.index_symbol)
        params.setdefault("universe_mode", config.universe_mode)
    if config.start_date:
        params.setdefault("backtest_start_date", config.start_date)
    if config.end_date:
        params.setdefault("backtest_end_date", config.end_date)
    for key, value in params.items():
        if key.startswith("_"):
            continue
        try:
            setattr(strategy, key, value)
        except Exception:
            logger.debug("Failed to apply strategy param {}={}", key, value)


def _load_strategy_class(code: str) -> Any:
    """Dynamically load an akquant Strategy subclass."""
    import akquant as aq
    import numpy as np
    import pandas as pd

    namespace = {"aq": aq, "np": np, "pd": pd}
    exec(code, namespace)

    candidates = []
    for obj in namespace.values():
        if (
            isinstance(obj, type)
            and issubclass(obj, aq.Strategy)
            and obj is not aq.Strategy
        ):
            candidates.append(obj)

    if not candidates:
        raise ValueError(
            "No akquant.Strategy subclass found in code. "
            "Define: class MyStrategy(aq.Strategy): def on_bar(self, bar): ..."
        )

    strategy_class = candidates[0]
    class_alias = _register_dynamic_strategy(strategy_class, code)
    logger.info("Loaded strategy class: {} as {}", strategy_class.__name__, class_alias)
    return strategy_class


def _register_dynamic_strategy(strategy_class: type, code: str) -> str:
    """Expose dynamic strategy classes through this module so they can be pickled."""
    _install_dynamic_strategy_pickle_hooks(strategy_class)
    digest = hashlib.sha1(code.encode("utf-8")).hexdigest()[:12]
    alias = f"DynamicStrategy_{digest}"
    strategy_class.__name__ = alias
    strategy_class.__qualname__ = alias
    strategy_class.__module__ = __name__
    globals()[alias] = strategy_class
    _DYNAMIC_STRATEGY_REGISTRY[alias] = strategy_class
    return alias


def _install_dynamic_strategy_pickle_hooks(strategy_class: type) -> None:
    """Patch dynamic strategies so checkpoint snapshots exclude transient Rust objects."""
    if getattr(strategy_class, "_codex_pickle_patched", False):
        return

    import akquant as aq

    original_getstate = strategy_class.__dict__.get("__getstate__")
    original_setstate = strategy_class.__dict__.get("__setstate__")
    original_log = strategy_class.__dict__.get("log")

    def __getstate__(self):
        if original_getstate is not None:
            state = original_getstate(self)
        else:
            state = aq.Strategy.__getstate__(self)
        if not isinstance(state, dict):
            state = dict(state)
        for key in _TRANSIENT_STATE_KEYS:
            state.pop(key, None)
        return _sanitize_strategy_state(state)

    def __setstate__(self, state):
        if original_setstate is not None:
            original_setstate(self, state)
        else:
            aq.Strategy.__setstate__(self, state)
        if not hasattr(self, "_current_bars"):
            self._current_bars = {}
        if not hasattr(self, "_current_day_volume"):
            self._current_day_volume = {}
        if not hasattr(self, "_last_trade_date"):
            self._last_trade_date = None
        if not hasattr(self, "_stock_meta") and hasattr(self, "_load_stock_meta"):
            try:
                self._stock_meta = self._load_stock_meta()
            except Exception:
                pass

    strategy_class.__getstate__ = __getstate__
    strategy_class.__setstate__ = __setstate__
    strategy_class.log = _build_strategy_log_wrapper(aq, original_log)
    strategy_class._codex_pickle_patched = True


def _build_strategy_log_wrapper(aq: Any, original_log: Any):
    """Accept both AKQuant self.log(msg, level) and loguru-style self.log(msg, *args)."""
    import logging

    def log(self, msg: str, *args: Any, level: int = logging.INFO, **kwargs: Any) -> None:
        if args:
            if len(args) == 1 and isinstance(args[0], int) and not kwargs:
                level = args[0]
            else:
                try:
                    msg = str(msg).format(*args, **kwargs)
                except Exception:
                    msg = " ".join([str(msg), *[str(arg) for arg in args]])
        if original_log is not None:
            try:
                return original_log(self, msg, level)
            except TypeError:
                pass
        return aq.Strategy.log(self, msg, level)

    return log


def _sanitize_strategy_state(value: Any) -> Any:
    """Drop known transient extension objects from strategy state before pickling."""
    value_type = type(value)
    if (value_type.__module__, value_type.__name__) in _TRANSIENT_EXTENSION_TYPES:
        return _DROP_STATE_VALUE

    if isinstance(value, defaultdict):
        sanitized = defaultdict(value.default_factory)
        for key, item in value.items():
            item_sanitized = _sanitize_strategy_state(item)
            if item_sanitized is not _DROP_STATE_VALUE:
                sanitized[key] = item_sanitized
        return sanitized

    if isinstance(value, dict):
        sanitized_dict = {}
        for key, item in value.items():
            item_sanitized = _sanitize_strategy_state(item)
            if item_sanitized is not _DROP_STATE_VALUE:
                sanitized_dict[key] = item_sanitized
        return sanitized_dict

    if isinstance(value, list):
        return [
            item_sanitized
            for item in value
            if (item_sanitized := _sanitize_strategy_state(item)) is not _DROP_STATE_VALUE
        ]

    if isinstance(value, tuple):
        return tuple(
            item_sanitized
            for item in value
            if (item_sanitized := _sanitize_strategy_state(item)) is not _DROP_STATE_VALUE
        )

    if isinstance(value, set):
        return {
            item_sanitized
            for item in value
            if (item_sanitized := _sanitize_strategy_state(item)) is not _DROP_STATE_VALUE
        }

    if isinstance(value, deque):
        return deque(
            (
                item_sanitized
                for item in value
                if (item_sanitized := _sanitize_strategy_state(item)) is not _DROP_STATE_VALUE
            ),
            maxlen=value.maxlen,
        )

    return value


def _build_expression_strategy(expression: str, config: BacktestConfig) -> Any:
    """Convert a simple expression into an AKQuant FunctionalStrategy."""
    from akquant.backtest.engine import FunctionalStrategy

    def on_bar_fn(strategy, bar):
        pos = strategy.get_position(bar.symbol)
        ctx = {
            "close": float(bar.close),
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "volume": float(bar.volume),
        }

        try:
            signal = float(eval(expression, {"__builtins__": {}}, ctx))
        except Exception:
            return

        if signal > 0 and pos == 0:
            strategy.buy(bar.symbol, 100)
        elif signal < 0 and pos > 0:
            strategy.close_position(bar.symbol)

    return FunctionalStrategy(lambda s: None, on_bar=on_bar_fn)
