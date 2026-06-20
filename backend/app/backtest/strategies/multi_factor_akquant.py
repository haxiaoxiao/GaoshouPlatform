"""AKQuant 通用多因子策略模板。

这份模板负责“选股 + 调仓 + 风控”的执行骨架，不把具体研究结论写死在
流程里。因子列表、权重、过滤条件、主题约束、持仓上限、止损止盈和隔夜
风险都通过顶部常量或前端预设传入，方便同一套执行引擎承载不同策略版本。
"""

MULTI_FACTOR_STRATEGY_CODE = r'''
"""AKQuant 通用多因子执行模板。

这是一份偏执行层的模板代码：它负责读取平台传入的因子配置、过滤条件、
主题约束和风控参数，然后把这些研究配置落成可回测、可实盘对齐的调仓动作。
默认保留单边多因子、长仓、目标权重调仓和分层风控，方便不同研究版本在
同一套骨架上替换因子而不改交易流程。
"""

from datetime import date, datetime
from typing import Any

import akquant as aq

from app.services.factor_pipeline import FactorPipeline, LinearFactorScorer
from app.services.us_market import (
    apply_entry_filter_to_target_weights,
    us_overnight_entry_filter_state,
)


FACTOR_CONFIGS = [
    # 核心市值因子：默认模板先用市值暴露把风格骨架立住，再由前端预设或
    # 研究版本去调整其它因子的权重组合。
    {
        "name": "market_cap",
        "weight": 0.45,
        "direction": "lower_better",
        "transform": "zscore",
        "neutralize_market_cap": False,
        "industry_zscore": True,
    },
    {
        "name": "market_cap_rank",
        "weight": 0.35,
        "direction": "lower_better",
        "transform": "rank_pct",
    },
    {
        "name": "v4gv",
        "weight": 0.20,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
]

FILTER_FACTORS = [
    # 交易性硬过滤：先把 ST、停牌、涨跌停等明显无法有效成交的标的排除，
    # 再进入因子打分，避免把评分预算浪费在不可交易样本上。
    {"name": "is_st", "operator": ">=", "value": 0.5},
    {"name": "is_paused", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_up", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_down", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
]

# 可选静态股票池：留空时使用平台传入的标的，或者从行情事件中自动发现
# 的标的集合。只有在研究或回放需要固定池子时才显式填写。
DEFAULT_UNIVERSE_SYMBOLS = []

# 调仓和仓位默认值刻意设得保守，目的是让新预设即使漏填某个字段，也能以
# 相对安全的方式运行，而不是因为默认值过激导致大幅偏离预期。
TOP_N = 10
MIN_CANDIDATES = 5
MIN_FACTOR_COVERAGE = 0.60
REBALANCE_EVERY_N_DAYS = 1
REBALANCE_TIME = "10:30"
CASH_BUFFER_PCT = 0.05
LOT_SIZE = 100
MAX_POSITION_PCT = 0.12
REBALANCE_TOLERANCE_PCT = 0.01
SCORER_TYPE = "linear"
THEME_INCLUDE_INDUSTRIES = []
THEME_INCLUDE_KEYWORDS = []
THEME_EXCLUDE_INDUSTRIES = []
THEME_EXCLUDE_KEYWORDS = []
THEME_MIN_CANDIDATES = 0
STRICT_THEME_FILTER = False
STOP_LOSS_PCT = 0.0
TRAILING_STOP_PCT = 0.0
TAKE_PROFIT_PCT = 0.0
PORTFOLIO_DRAWDOWN_STOP_PCT = 0.0
HIGH_VOLUME_RISK_FACTOR = ""
HIGH_VOLUME_RISK_AS_OF_TIME = "14:30"
HIGH_VOLUME_RISK_MAX = 0.0
HIGH_VOLUME_RISK_PARAMS = {}
RISK_CHECK_TIMES = []
BLOCK_REBUY_AFTER_STOP = True
HOLD_RANK_BUFFER = 0
USE_TARGET_WEIGHT_REBALANCE = True
CANCEL_OPEN_ORDERS_ON_REBALANCE = True
US_OVERNIGHT_ENTRY_FILTER = "none"
US_OVERNIGHT_DATA_PATH = ""
US_OVERNIGHT_MAX_LAG_DAYS = 5
US_OVERNIGHT_CAUTION_EXPOSURE = 0.85
US_OVERNIGHT_DEFENSIVE_EXPOSURE = 0.70
US_OVERNIGHT_QQQ_CAUTION_RET = -0.01
US_OVERNIGHT_QQQ_DEFENSIVE_RET = -0.02
US_OVERNIGHT_SEMI_CAUTION_RET = -0.02
US_OVERNIGHT_SEMI_DEFENSIVE_RET = -0.03
US_OVERNIGHT_NVDA_CAUTION_RET = -0.03
US_OVERNIGHT_NVDA_DEFENSIVE_RET = -0.04

# 风控和费用统一交给平台回测配置控制：risk_config、max_positions、
# volume_limit_pct、commission_rate、stamp_tax_rate、transfer_fee_rate、
# min_commission 和 slippage 都应由外层面板决定，而不是在策略里硬编码。


class ModelScorer:
    """模型打分器协议占位符。

    参数:
        frame: 已完成预处理的因子截面，每一行对应一个股票样本。
        factor_specs: 因子定义列表，包含名字、方向、变换、权重和行业中性设置。

    作用:
        预留给未来的机器学习排序器。当前模板默认使用线性因子打分，但如果
        后续接入 sklearn、PyTorch 或 LightGBM，只要实现这个接口即可复用整
        套调仓控制流，而不必重写选股和调仓骨架。
    """

    def score(self, frame, factor_specs):
        raise NotImplementedError


class CustomModelScorer(ModelScorer):
    """为 sklearn / PyTorch / LightGBM 预留的模型推理扩展点。

    当前实现故意抛出异常，避免策略在没有真正接入模型时悄悄退化成“看起来
    能跑、其实没按预期打分”的状态。

    一旦后续接入真实模型，这里就应返回与线性打分器同样形状的分数序列。
    """

    def score(self, frame, factor_specs):
        raise NotImplementedError("Set SCORER_TYPE='linear' or implement model inference here.")


class MultiFactorStrategy(aq.Strategy):
    """通用的单边多因子选股策略执行器。

    这个类只负责执行层面的事情：何时调仓、如何筛选候选、如何按权重下单、
    如何做止损止盈和组合回撤控制。因子读取、预处理和最终排序交给
    FactorPipeline，因此预设只需要声明“用哪些因子、怎么配权重、要不要做
    主题约束”即可。

    常用参数含义:
        TOP_N: 最终持仓数量上限，决定组合有多集中。
        MIN_CANDIDATES: 候选池最低样本数，样本太少时直接跳过。
        MIN_FACTOR_COVERAGE: 因子覆盖率下限，越高越强调数据完整性。
        REBALANCE_EVERY_N_DAYS: 调仓频率，越大越慢、越小越灵敏。
        CASH_BUFFER_PCT: 预留现金比例，防止调仓后现金过紧。
        MAX_POSITION_PCT: 单票最大仓位上限。
        SCORER_TYPE: 打分器类型，当前默认 linear。
    """

    symbols = DEFAULT_UNIVERSE_SYMBOLS

    def __init__(self, **kwargs: Any):
        super().__init__()
        self.factor_configs = list(FACTOR_CONFIGS)
        self.filter_factors = list(FILTER_FACTORS)
        self.top_n = int(TOP_N)
        self.min_candidates = int(MIN_CANDIDATES)
        self.min_factor_coverage = float(MIN_FACTOR_COVERAGE)
        self.rebalance_every_n_days = max(1, int(REBALANCE_EVERY_N_DAYS))
        self.rebalance_time = str(REBALANCE_TIME)
        self.cash_buffer_pct = float(CASH_BUFFER_PCT)
        self.lot_size = int(LOT_SIZE)
        self.max_position_pct = float(MAX_POSITION_PCT)
        self.rebalance_tolerance_pct = float(REBALANCE_TOLERANCE_PCT)
        self.scorer_type = str(SCORER_TYPE)
        self.theme_include_industries = list(THEME_INCLUDE_INDUSTRIES)
        self.theme_include_keywords = list(THEME_INCLUDE_KEYWORDS)
        self.theme_exclude_industries = list(THEME_EXCLUDE_INDUSTRIES)
        self.theme_exclude_keywords = list(THEME_EXCLUDE_KEYWORDS)
        self.theme_min_candidates = int(THEME_MIN_CANDIDATES)
        self.strict_theme_filter = bool(STRICT_THEME_FILTER)
        self.stop_loss_pct = float(STOP_LOSS_PCT)
        self.trailing_stop_pct = float(TRAILING_STOP_PCT)
        self.take_profit_pct = float(TAKE_PROFIT_PCT)
        self.portfolio_drawdown_stop_pct = float(PORTFOLIO_DRAWDOWN_STOP_PCT)
        self.high_volume_risk_factor = str(HIGH_VOLUME_RISK_FACTOR)
        self.high_volume_risk_as_of_time = str(HIGH_VOLUME_RISK_AS_OF_TIME)
        self.high_volume_risk_max = float(HIGH_VOLUME_RISK_MAX)
        self.high_volume_risk_params = dict(HIGH_VOLUME_RISK_PARAMS)
        self.risk_check_times = list(RISK_CHECK_TIMES)
        self.block_rebuy_after_stop = bool(BLOCK_REBUY_AFTER_STOP)
        self.hold_rank_buffer = int(HOLD_RANK_BUFFER)
        self.use_target_weight_rebalance = bool(USE_TARGET_WEIGHT_REBALANCE)
        self.cancel_open_orders_on_rebalance = bool(CANCEL_OPEN_ORDERS_ON_REBALANCE)
        self.us_overnight_entry_filter = str(US_OVERNIGHT_ENTRY_FILTER)
        self.us_overnight_data_path = str(US_OVERNIGHT_DATA_PATH)
        self.us_overnight_max_lag_days = int(US_OVERNIGHT_MAX_LAG_DAYS)
        self.us_overnight_caution_exposure = float(US_OVERNIGHT_CAUTION_EXPOSURE)
        self.us_overnight_defensive_exposure = float(US_OVERNIGHT_DEFENSIVE_EXPOSURE)
        self.us_overnight_qqq_caution_ret = float(US_OVERNIGHT_QQQ_CAUTION_RET)
        self.us_overnight_qqq_defensive_ret = float(US_OVERNIGHT_QQQ_DEFENSIVE_RET)
        self.us_overnight_semi_caution_ret = float(US_OVERNIGHT_SEMI_CAUTION_RET)
        self.us_overnight_semi_defensive_ret = float(US_OVERNIGHT_SEMI_DEFENSIVE_RET)
        self.us_overnight_nvda_caution_ret = float(US_OVERNIGHT_NVDA_CAUTION_RET)
        self.us_overnight_nvda_defensive_ret = float(US_OVERNIGHT_NVDA_DEFENSIVE_RET)
        self._pipeline = None
        self._seen_symbols = set(DEFAULT_UNIVERSE_SYMBOLS)
        self._latest_price = {}
        self._prices_by_day = {}
        self._day_open_price = {}
        self._cost_basis = {}
        self._peak_price = {}
        self._portfolio_peak_value = 0.0
        self._risk_exit_dates = set()
        self._last_rebalance_date = None
        self._rebalance_count = 0
        self._daily_rebalanced = set()
        self._pending_sell_symbols = set()
        self._last_entry_filter_state = {}

    def on_start(self):
        self.set_history_depth(0)
        self._pipeline = FactorPipeline()
        # Timer 是可选能力：有些 AKQuant 数据源只提供 bar，不提供定时事件；
        # 当 timer 可用时，调仓和风控就能在指定时点精确触发一次。
        try:
            self.add_daily_timer(self.rebalance_time, "multi_factor_rebalance")
        except Exception:
            pass
        for risk_time in self._as_list(self.risk_check_times):
            try:
                self.add_daily_timer(str(risk_time), "multi_factor_risk_check")
            except Exception:
                pass

    def on_bar(self, bar):
        symbol = str(getattr(bar, "symbol", "") or "")
        if not symbol:
            return
        price = self._bar_price(bar)
        if price <= 0:
            return
        self._seen_symbols.add(symbol)
        self._latest_price[symbol] = price
        open_price = float(getattr(bar, "open", 0.0) or 0.0)
        if open_price > 0:
            self._day_open_price[symbol] = open_price
        trade_date = self._bar_date(bar)
        if trade_date is None:
            return
        day_prices = self._prices_by_day.setdefault(trade_date, {})
        day_prices[symbol] = price
        self._risk_check_symbol(symbol, price, trade_date, bar_time=self._bar_time(bar))

        # 有些日线数据源不会显式发出盘中 timer，因此这里做一次日内兜底：
        # 当当天的截面已经足够完整时，就允许策略在 bar 驱动下完成一次调仓。
        universe = self._universe()
        if trade_date not in self._daily_rebalanced and len(day_prices) >= max(1, int(len(universe) * 0.8)):
            self._rebalance(trade_date, price_map=day_prices)
            self._daily_rebalanced.add(trade_date)

    def on_timer(self, payload):
        if str(payload) == "multi_factor_risk_check":
            trade_date = self._current_trade_date()
            if trade_date is not None:
                self._risk_check_all(trade_date, price_map=dict(self._latest_price))
            return
        if str(payload) != "multi_factor_rebalance":
            return
        trade_date = self._current_trade_date()
        if trade_date is None:
            return
        self._rebalance(trade_date, price_map=dict(self._latest_price))

    def _rebalance(self, trade_date, price_map):
        if not self._should_rebalance(trade_date):
            return
        # 只取“当前股票池 ∩ 已有价格”的交集，避免个别标的缺少盘中 bar 时
        # 把整个调仓流程卡住。
        universe = [symbol for symbol in self._universe() if symbol in price_map and price_map[symbol] > 0]
        if len(universe) < max(1, self.min_candidates):
            self.log(f"MF skip {trade_date}: candidate universe too small ({len(universe)})")
            return

        pipeline = self._pipeline or FactorPipeline()
        result = pipeline.build_cross_section(
            factor_specs=self.factor_configs,
            trade_date=trade_date,
            symbols=universe,
            filters=self.filter_factors,
            min_factor_coverage=self.min_factor_coverage,
            scorer=self._scorer(),
        )
        frame = result.frame
        if frame.empty or len(frame) < max(1, self.min_candidates):
            self.log(f"MF skip {trade_date}: no usable factor scores")
            return
        # 主题过滤在行业型预设里可以非常严格，但模板仍然保留显式兜底路径，
        # 这样候选过少时的行为是可见、可解释的。
        frame = self._apply_theme_filter(frame)
        if frame.empty or len(frame) < max(1, self.min_candidates):
            self.log(f"MF skip {trade_date}: theme-filtered candidates too small ({len(frame)})")
            return

        target_symbols = self._rank_buffer_targets(frame)
        if not target_symbols:
            return
        target_weight = min(
            self.max_position_pct,
            (1.0 - self.cash_buffer_pct) / float(len(target_symbols)),
        )
        entry_filter_state = self._us_entry_filter_state(trade_date)
        self._last_entry_filter_state = dict(entry_filter_state)
        self._submit_targets(target_symbols, target_weight, price_map, entry_filter_state=entry_filter_state)
        self._last_rebalance_date = trade_date
        self._rebalance_count += 1
        self.log(
            f"MF rebalance {trade_date}: selected={target_symbols[:5]} "
            f"excluded={len(result.excluded_symbols)} entry_filter={entry_filter_state.get('us_overnight_reason', 'off')}"
        )

    def _rank_buffer_targets(self, frame):
        top_n = max(1, int(getattr(self, "top_n", TOP_N) or TOP_N))
        buffer_n = max(0, int(getattr(self, "hold_rank_buffer", 0) or 0))
        ranked = list(frame.index.astype(str))
        if buffer_n <= top_n:
            return ranked[:top_n]
        rank_buffer = set(ranked[:buffer_n])
        current = {symbol for symbol, qty in self._positions().items() if float(qty or 0.0) > 0}
        keep = [symbol for symbol in ranked if symbol in current and symbol in rank_buffer]
        additions = [symbol for symbol in ranked if symbol not in keep]
        return (keep + additions)[:top_n]

    def _us_entry_filter_state(self, trade_date):
        return us_overnight_entry_filter_state(
            trade_date,
            mode=str(getattr(self, "us_overnight_entry_filter", "none") or "none"),
            data_path=str(getattr(self, "us_overnight_data_path", "") or ""),
            max_lag_days=int(getattr(self, "us_overnight_max_lag_days", 5) or 5),
            caution_exposure=float(getattr(self, "us_overnight_caution_exposure", 0.85) or 0.85),
            defensive_exposure=float(getattr(self, "us_overnight_defensive_exposure", 0.70) or 0.70),
            qqq_caution_ret=float(getattr(self, "us_overnight_qqq_caution_ret", -0.01) or -0.01),
            qqq_defensive_ret=float(getattr(self, "us_overnight_qqq_defensive_ret", -0.02) or -0.02),
            semi_caution_ret=float(getattr(self, "us_overnight_semi_caution_ret", -0.02) or -0.02),
            semi_defensive_ret=float(getattr(self, "us_overnight_semi_defensive_ret", -0.03) or -0.03),
            nvda_caution_ret=float(getattr(self, "us_overnight_nvda_caution_ret", -0.03) or -0.03),
            nvda_defensive_ret=float(getattr(self, "us_overnight_nvda_defensive_ret", -0.04) or -0.04),
        )

    def _submit_targets(self, target_symbols, target_weight, price_map, entry_filter_state=None):
        portfolio_value = self._portfolio_value(price_map)
        current_positions = self._positions()
        target_weights = {
            str(symbol): float(target_weight)
            for symbol in target_symbols
            if str(symbol).strip() and float(target_weight) > 0
        }
        target_weights, entry_filter_state = apply_entry_filter_to_target_weights(
            target_weights,
            current_positions=current_positions,
            price_map=price_map,
            portfolio_value=portfolio_value,
            entry_filter_state=dict(entry_filter_state or {}),
        )
        self._last_entry_filter_state = dict(entry_filter_state or {})

        # 优先使用引擎原生的目标权重调仓接口；只有适配器不提供高阶下单助手
        # 时，才退回到手工按手数计算的兼容路径。
        if bool(getattr(self, "use_target_weight_rebalance", False)) and hasattr(self, "order_target_weights"):
            if bool(getattr(self, "cancel_open_orders_on_rebalance", True)):
                try:
                    self.cancel_all_orders()
                    self._pending_sell_symbols.clear()
                except Exception:
                    pass
            try:
                self.order_target_weights(
                    target_weights=target_weights,
                    price_map=None,
                    liquidate_unmentioned=True,
                    allow_leverage=False,
                    rebalance_tolerance=float(self.rebalance_tolerance_pct),
                )
                return
            except Exception as exc:
                self.log(f"MF target-weight rebalance fallback: {exc}")

        target_set = set(target_weights)
        target_positions = {}
        for symbol, weight in target_weights.items():
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0:
                continue
            target_value = portfolio_value * float(weight)
            target_positions[symbol] = self._round_lot(target_value / price)

        for symbol, qty in current_positions.items():
            if qty > 0 and symbol not in target_set and symbol not in self._pending_sell_symbols:
                sell_qty = self._round_lot(qty)
                if sell_qty > 0:
                    self._pending_sell_symbols.add(symbol)
                    self.sell(symbol=symbol, quantity=sell_qty, price=None)

        for symbol, target_qty in target_positions.items():
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0:
                continue
            current_qty = float(current_positions.get(symbol, 0.0) or 0.0)
            delta = target_qty - current_qty
            if abs(delta) * price < portfolio_value * self.rebalance_tolerance_pct:
                continue
            if delta > 0:
                buy_qty = self._round_lot(delta)
                if buy_qty > 0 and self._can_buy(buy_qty, price):
                    self._note_submitted_buy(symbol, buy_qty, price, current_qty)
                    self.buy(symbol=symbol, quantity=buy_qty, price=None)
            elif delta < 0:
                sell_qty = self._round_lot(-delta)
                if sell_qty > 0 and symbol not in self._pending_sell_symbols:
                    self._pending_sell_symbols.add(symbol)
                    self.sell(symbol=symbol, quantity=sell_qty, price=None)

    def _should_rebalance(self, trade_date):
        # 同一调仓周期内只做一次调仓；如果当天已经触发过风控退出，就不再
        # 继续补仓，避免“刚风控、又立刻回补”的抖动。
        if trade_date in self._risk_exit_dates:
            return False
        if self._last_rebalance_date == trade_date:
            return False
        if self._last_rebalance_date is None:
            return True
        try:
            return (trade_date - self._last_rebalance_date).days >= self.rebalance_every_n_days
        except Exception:
            return True

    def _scorer(self):
        # 默认保持确定性排序；模型打分是可选扩展，不影响标准模板的复现性。
        if self.scorer_type.lower() == "linear":
            return LinearFactorScorer()
        return CustomModelScorer()

    def on_trade(self, trade):
        symbol = str(getattr(trade, "symbol", "") or "")
        if not symbol:
            return
        side = str(getattr(trade, "side", "") or "").lower()
        quantity = abs(float(getattr(trade, "quantity", 0.0) or 0.0))
        price = float(getattr(trade, "price", 0.0) or 0.0)
        if quantity <= 0 or price <= 0:
            return
        current_qty = abs(float(self.get_position(symbol) or 0.0))
        if "buy" in side:
            previous_qty = max(0.0, current_qty - quantity)
            previous_cost = float(self._cost_basis.get(symbol, price) or price)
            if current_qty > 0:
                self._cost_basis[symbol] = (
                    previous_cost * previous_qty + price * quantity
                ) / max(current_qty, quantity)
                self._peak_price[symbol] = max(price, float(self._peak_price.get(symbol, price) or price))
            return
        if "sell" in side or "close" in side:
            self._pending_sell_symbols.discard(symbol)
            if current_qty <= 0:
                self._cost_basis.pop(symbol, None)
                self._peak_price.pop(symbol, None)

    def _apply_theme_filter(self, frame):
        include_terms = self._as_list(self.theme_include_industries) + self._as_list(self.theme_include_keywords)
        exclude_terms = self._as_list(self.theme_exclude_industries) + self._as_list(self.theme_exclude_keywords)
        if frame.empty or (not include_terms and not exclude_terms):
            return frame

        filtered = frame
        if exclude_terms:
            # 先执行排除项，这样即便包含列表很宽，也能先把不想要的行业或
            # 主题剔除掉。
            keep_mask = [
                not self._row_matches_terms(row, exclude_terms)
                for _, row in filtered.iterrows()
            ]
            filtered = filtered.loc[keep_mask]
        if include_terms:
            # 严格主题路径是故意做成“硬约束”的：要么主题候选足够多，要么
            # 就明确回落到仅排除项后的更宽股票池，避免悄悄混入无关标的。
            themed_mask = [
                self._row_matches_terms(row, include_terms)
                for _, row in filtered.iterrows()
            ]
            themed = filtered.loc[themed_mask]
            min_candidates = max(0, int(getattr(self, "theme_min_candidates", 0) or 0))
            if self.strict_theme_filter or len(themed) >= min_candidates:
                filtered = themed
            else:
                self.log(
                    f"MF theme fallback: themed={len(themed)} below min={min_candidates}; "
                    f"using exclude-only candidates={len(filtered)}"
                )
        return filtered

    def _row_matches_terms(self, row, terms):
        text = " ".join(
            self._normalize_text(row.get(column, ""))
            for column in ("industry", "industry2", "industry3", "sector", "concept")
        )
        if not text:
            return False
        return any(self._normalize_text(term) in text for term in terms if self._normalize_text(term))

    def _risk_check_all(self, trade_date, price_map):
        if not price_map:
            return
        # 组合回撤优先于单票检查：一旦触发整体降风险事件，就先整体清仓，
        # 再去处理单票级风控。
        if self._portfolio_drawdown_hit(trade_date, price_map):
            return
        for symbol, qty in self._positions().items():
            if qty <= 0:
                continue
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price > 0:
                self._risk_check_symbol(symbol, price, trade_date)

    def _risk_check_symbol(self, symbol, price, trade_date, bar_time=None):
        qty = float(self._positions().get(symbol, 0.0) or 0.0)
        if qty <= 0 or price <= 0:
            return False
        if self._portfolio_drawdown_hit(trade_date, dict(self._latest_price)):
            return True

        # 把开仓价和峰值价保存在本地状态里，止损/移动止盈就不需要额外的
        # 状态对象，逻辑更直接也更容易排查。
        cost = float(self._cost_basis.get(symbol, 0.0) or 0.0)
        if cost <= 0:
            cost = price
            self._cost_basis[symbol] = cost
        peak = max(price, float(self._peak_price.get(symbol, price) or price))
        self._peak_price[symbol] = peak

        stop_loss = float(getattr(self, "stop_loss_pct", 0.0) or 0.0)
        if stop_loss > 0 and price <= cost * (1.0 - stop_loss):
            return self._risk_exit(symbol, qty, price, trade_date, f"stop_loss {price:.4f}/{cost:.4f}")

        trailing = float(getattr(self, "trailing_stop_pct", 0.0) or 0.0)
        if trailing > 0 and peak > cost and price <= peak * (1.0 - trailing):
            return self._risk_exit(symbol, qty, price, trade_date, f"trailing_stop {price:.4f}/{peak:.4f}")

        take_profit = float(getattr(self, "take_profit_pct", 0.0) or 0.0)
        if take_profit > 0 and price >= cost * (1.0 + take_profit):
            return self._risk_exit(symbol, qty, price, trade_date, f"take_profit {price:.4f}/{cost:.4f}")

        if self._high_volume_risk_hit(symbol, trade_date, bar_time):
            return self._risk_exit(symbol, qty, price, trade_date, "high_volume_risk")
        return False

    def _portfolio_drawdown_hit(self, trade_date, price_map):
        stop = float(getattr(self, "portfolio_drawdown_stop_pct", 0.0) or 0.0)
        if stop <= 0:
            return False
        value = self._portfolio_value(price_map)
        if value <= 0:
            return False
        # 峰值只会单向上移，不会被盘中噪声拉低，因此回撤计算在多次 timer
        # 触发时仍然稳定。
        self._portfolio_peak_value = max(float(self._portfolio_peak_value or 0.0), value)
        if self._portfolio_peak_value <= 0 or value > self._portfolio_peak_value * (1.0 - stop):
            return False
        positions = self._positions()
        for symbol, qty in positions.items():
            if qty <= 0:
                continue
            price = float(price_map.get(symbol, self._latest_price.get(symbol, 0.0)) or 0.0)
            self._risk_exit(symbol, qty, price, trade_date, "portfolio_drawdown_stop", mark_date=False)
        if self.block_rebuy_after_stop:
            self._risk_exit_dates.add(trade_date)
        self.log(f"MF portfolio drawdown stop {trade_date}: value={value:.2f}")
        return True

    def _high_volume_risk_hit(self, symbol, trade_date, bar_time):
        factor_name = str(getattr(self, "high_volume_risk_factor", "") or "")
        threshold = float(getattr(self, "high_volume_risk_max", 0.0) or 0.0)
        if not factor_name or threshold <= 0:
            return False
        risk_time = str(getattr(self, "high_volume_risk_as_of_time", "") or "")
        if risk_time and bar_time and not self._time_at_or_after(bar_time, risk_time):
            return False
        try:
            store = (self._pipeline or FactorPipeline()).store
            values = store.load_cross_section(
                factor_name,
                trade_date,
                symbols=[symbol],
                as_of_time=risk_time or None,
                params=dict(getattr(self, "high_volume_risk_params", {}) or {}),
            )
            value = float(values.get(symbol, 0.0) or 0.0)
        except Exception:
            return False
        return value >= threshold

    def _risk_exit(self, symbol, qty, price, trade_date, reason, mark_date=True):
        sell_qty = self._round_lot(qty)
        if sell_qty <= 0:
            return False
        self.sell(symbol=symbol, quantity=sell_qty, price=price if price > 0 else None)
        if mark_date and self.block_rebuy_after_stop:
            self._risk_exit_dates.add(trade_date)
        self._cost_basis.pop(symbol, None)
        self._peak_price.pop(symbol, None)
        self.log(f"MF risk exit {trade_date}: {symbol} qty={sell_qty} reason={reason}")
        return True

    def _note_submitted_buy(self, symbol, qty, price, current_qty):
        previous_qty = max(0.0, float(current_qty or 0.0))
        previous_cost = float(self._cost_basis.get(symbol, price) or price)
        new_qty = previous_qty + float(qty or 0.0)
        if new_qty <= 0 or price <= 0:
            return
        self._cost_basis[symbol] = (previous_cost * previous_qty + float(price) * float(qty)) / new_qty
        self._peak_price[symbol] = max(float(price), float(self._peak_price.get(symbol, price) or price))

    def _as_list(self, value):
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        try:
            return [str(item).strip() for item in value if str(item).strip()]
        except Exception:
            return [str(value).strip()] if str(value).strip() else []

    def _normalize_text(self, value):
        return str(value or "").strip().lower()

    def _bar_time(self, bar):
        timestamp_text = str(getattr(bar, "timestamp_str", "") or "")
        if len(timestamp_text) >= 16 and ":" in timestamp_text:
            return timestamp_text[-8:-3]
        ts = getattr(bar, "timestamp", None)
        try:
            raw = int(ts)
            if raw > 10_000_000_000:
                raw = raw // 1_000_000_000
            return datetime.utcfromtimestamp(raw).strftime("%H:%M")
        except Exception:
            return None

    def _time_at_or_after(self, left, right):
        try:
            left_parts = [int(part) for part in str(left)[:5].split(":")]
            right_parts = [int(part) for part in str(right)[:5].split(":")]
            return tuple(left_parts[:2]) >= tuple(right_parts[:2])
        except Exception:
            return True

    def _universe(self):
        configured = list(getattr(self, "symbols", None) or [])
        if configured:
            return [str(symbol) for symbol in configured]
        return sorted(self._seen_symbols)

    def _positions(self):
        try:
            return {str(k): float(v or 0.0) for k, v in self.get_positions().items()}
        except Exception:
            return {symbol: float(self.get_position(symbol) or 0.0) for symbol in self._universe()}

    def _portfolio_value(self, price_map):
        try:
            return float(self.get_portfolio_value())
        except Exception:
            value = self._cash()
            for symbol, qty in self._positions().items():
                value += float(qty or 0.0) * float(price_map.get(symbol, 0.0) or 0.0)
            return value

    def _cash(self):
        try:
            return float(self.get_cash())
        except Exception:
            return 0.0

    def _can_buy(self, qty, price):
        return self._cash() * (1.0 - self.cash_buffer_pct) >= float(qty) * float(price)

    def _round_lot(self, qty):
        lot = max(1, int(self.lot_size))
        return int(float(qty or 0.0) // lot * lot)

    def _bar_price(self, bar):
        for field in ("close", "open", "price"):
            value = float(getattr(bar, field, 0.0) or 0.0)
            if value > 0:
                return value
        return 0.0

    def _bar_date(self, bar):
        ts = getattr(bar, "timestamp", None)
        return self._date_from_timestamp(ts)

    def _current_trade_date(self):
        current_bar = getattr(self, "current_bar", None)
        if current_bar is not None:
            return self._bar_date(current_bar)
        ctx = getattr(self, "ctx", None)
        current_time = getattr(ctx, "current_time", None) if ctx is not None else None
        return self._date_from_timestamp(current_time)

    def _date_from_timestamp(self, value):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        try:
            ts = int(value)
            if ts > 10_000_000_000:
                ts = ts // 1_000_000_000
            return datetime.utcfromtimestamp(ts).date()
        except Exception:
            try:
                return datetime.fromisoformat(str(value)).date()
            except Exception:
                return None
'''


DEFAULT_MULTI_FACTOR_PARAMS = {
    "top_n": 10,
    "min_factor_coverage": 0.60,
    "rebalance_time": "10:30",
    "cash_buffer_pct": 0.05,
    "lot_size": 100,
    "max_position_pct": 0.12,
    "scorer_type": "linear",
    "hold_rank_buffer": 0,
    "use_target_weight_rebalance": True,
    "us_overnight_entry_filter": "none",
    "us_overnight_max_lag_days": 5,
}


DEFAULT_MULTI_FACTOR_RISK_CONFIG = {
    "max_position_pct": 0.12,
}
