"""Catalog metadata for built-in factor families."""

from __future__ import annotations

import inspect
import re
from typing import Any


def _definition(
    name: str,
    display_name: str,
    *,
    factor_type: str = "factor",
    category: str,
    frequency: str = "daily",
    description: str,
    unit: str = "",
    dependencies: list[str] | None = None,
    lookback_days: int = 0,
    source: str = "catalog",
    data_policy: dict[str, Any] | None = None,
    formula: str | None = None,
    human_description: str | None = None,
) -> dict[str, Any]:
    payload = {
        "name": name,
        "display_name": display_name,
        "factor_type": factor_type,
        "category": category,
        "frequency": frequency,
        "description": description,
        "unit": unit,
        "as_of_time": None,
        "params_schema": {},
        "dependencies": dependencies or ["klines_daily"],
        "lookback_days": lookback_days,
        "point_in_time_safe": True,
        "source": source,
        "version": "v1",
        "data_policy": data_policy or {},
    }
    if formula:
        payload["formula"] = formula
    if human_description:
        payload["human_description"] = human_description
    return payload


def _alpha101_formula(index: int) -> str:
    from app.services.alpha101_calculator import Alphas

    method = getattr(Alphas, f"alpha_{index}", None)
    doc = inspect.getdoc(method) if method else None
    if not doc:
        return f"Alpha#{index}: formula is implemented in Alphas.alpha_{index}."
    return re.sub(r"\s+", " ", doc).strip()


def _alpha101_description(index: int, formula: str) -> str:
    return _alpha101_human_description(index, formula)


_ALPHA101_FIELD_LABELS: tuple[tuple[str, str], ...] = (
    (r"\bopen\b", "开盘价"),
    (r"\bhigh\b", "最高价"),
    (r"\blow\b", "最低价"),
    (r"\bclose\b", "收盘价"),
    (r"\bvwap\b", "成交均价 VWAP"),
    (r"\bvolume\b", "成交量"),
    (r"\badv\d+\b", "平均成交量"),
    (r"\breturns?\b", "收益率"),
    (r"\bcap\b", "市值"),
)

_ALPHA101_OPERATOR_LABELS: tuple[tuple[str, str], ...] = (
    (r"\bcorrelation\b", "滚动相关性"),
    (r"\bcovariance\b", "滚动协方差"),
    (r"\bTs_Rank\b|\bts_rank\b", "时序排名"),
    (r"\brank\b", "横截面排名"),
    (r"\bdelta\b", "变化量"),
    (r"\bdelay\b", "滞后值"),
    (r"\bdecay_linear\b", "线性衰减加权"),
    (r"\bIndNeutralize\b", "行业中性化"),
    (r"\bscale\b", "截面缩放"),
    (r"\bsignedpower\b", "带符号幂变换"),
    (r"\bstddev\b", "滚动波动"),
    (r"\bsum\b", "滚动求和"),
    (r"\bproduct\b", "滚动连乘"),
    (r"\bts_min\b", "滚动低点"),
    (r"\bts_max\b", "滚动高点"),
    (r"\bts_argmin\b", "低点出现位置"),
    (r"\bts_argmax\b", "高点出现位置"),
    (r"\bmin\b|\bmax\b", "条件取极值"),
)


def _alpha101_formula_body(formula: str) -> str:
    return re.sub(r"^Alpha#\d+:\s*", "", formula).strip()


def _detect_alpha101_labels(formula: str, specs: tuple[tuple[str, str], ...]) -> list[str]:
    labels: list[str] = []
    for pattern, label in specs:
        if re.search(pattern, formula, flags=re.IGNORECASE) and label not in labels:
            labels.append(label)
    return labels


def _join_cn(items: list[str], *, fallback: str) -> str:
    if not items:
        return fallback
    if len(items) == 1:
        return items[0]
    return "、".join(items)


def _alpha101_signal_family(formula: str, fields: list[str], operators: list[str]) -> str:
    lowered = formula.lower()
    has_price = any(label in fields for label in ("开盘价", "最高价", "最低价", "收盘价", "成交均价 VWAP"))
    has_volume = any(label in fields for label in ("成交量", "平均成交量"))
    if "indneutralize" in lowered:
        return "行业中性化后的相对强弱信号"
    if "correlation" in lowered and has_volume and has_price:
        return "价量关系或价量背离信号"
    if "covariance" in lowered and has_volume and has_price:
        return "价量协方差信号"
    if "returns" in lowered and ("stddev" in lowered or "ts_rank" in lowered):
        return "收益率动量/波动信号"
    if "delta" in lowered and has_price:
        return "短周期价格变化信号"
    if has_volume and not has_price:
        return "成交活跃度变化信号"
    if "rank" in lowered:
        return "横截面相对排序信号"
    if operators:
        return f"{operators[0]}衍生信号"
    return "公式化截面因子"


def _alpha101_direction_hint(formula: str) -> str:
    stripped = _alpha101_formula_body(formula).lstrip()
    if stripped.startswith("(-1") or stripped.startswith("-1"):
        return "公式外层带负号，数值方向相对原始关系取反；实际多空方向建议用 IC 和分组收益确认。"
    if "?" in stripped or "if" in stripped.lower():
        return "公式包含条件分支，数值方向会随市场状态切换；使用前重点看分组收益是否单调。"
    return "数值越高表示该组合特征越强，是否代表正向超额收益需要用历史 IC/分组回测校准。"


def _alpha101_human_description(index: int, formula: str) -> str:
    if index == 2:
        return (
            "Alpha101 #002 衡量“成交量变化”和“日内价格强弱”之间的 6 日滚动相关性并取负。"
            "先计算 log(volume) 的 2 日变化并做当日横截面排名，再计算 (close - open) / open 表示日内收益并做横截面排名；"
            "最后对每只股票滚动计算两者 6 日相关系数并乘以 -1。"
            "它属于短周期价量背离/反转类信号，数值越高通常表示量能变化与日内强弱越负相关。"
        )
    body = _alpha101_formula_body(formula)
    fields = _detect_alpha101_labels(body, _ALPHA101_FIELD_LABELS)
    operators = _detect_alpha101_labels(body, _ALPHA101_OPERATOR_LABELS)
    family = _alpha101_signal_family(body, fields, operators)
    return (
        f"Alpha101 #{index:03d} 是一个{family}。"
        f"它主要使用{_join_cn(fields, fallback='OHLCV 面板数据')}，"
        f"通过{_join_cn(operators, fallback='公式中的嵌套时序/截面算子')}把单只股票的历史行为和当日股票池内的相对位置组合成截面分值。"
        f"{_alpha101_direction_hint(formula)}"
        "阅读时可以从公式最内层开始：先构造价格、成交量或收益率序列，再看 rolling/delta/correlation 等时序窗口，最后看 rank/scale/neutralize 等截面处理。"
    )


TA_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "ta_sma_20": {"label": "SMA 20", "category": "ta_trend", "lookback": 40},
    "ta_ema_20": {"label": "EMA 20", "category": "ta_trend", "lookback": 60},
    "ta_rsi_14": {"label": "RSI 14", "category": "ta_momentum", "lookback": 40},
    "ta_macd_dif_12_26_9": {"label": "MACD DIF", "category": "ta_momentum", "lookback": 80},
    "ta_macd_dea_12_26_9": {"label": "MACD DEA", "category": "ta_momentum", "lookback": 80},
    "ta_macd_hist_12_26_9": {"label": "MACD HIST", "category": "ta_momentum", "lookback": 80},
    "ta_bbands_upper_20": {"label": "BBANDS Upper 20", "category": "ta_volatility", "lookback": 60},
    "ta_bbands_middle_20": {"label": "BBANDS Middle 20", "category": "ta_volatility", "lookback": 60},
    "ta_bbands_lower_20": {"label": "BBANDS Lower 20", "category": "ta_volatility", "lookback": 60},
    "ta_atr_14": {"label": "ATR 14", "category": "ta_volatility", "lookback": 40},
    "ta_natr_14": {"label": "NATR 14", "category": "ta_volatility", "lookback": 40},
    "ta_obv": {"label": "OBV", "category": "ta_volume", "lookback": 10},
    "ta_ad": {"label": "Accumulation/Distribution", "category": "ta_volume", "lookback": 10},
    "ta_mfi_14": {"label": "MFI 14", "category": "ta_volume", "lookback": 40},
    "ta_cci_14": {"label": "CCI 14", "category": "ta_momentum", "lookback": 40},
    "ta_willr_14": {"label": "WILLR 14", "category": "ta_momentum", "lookback": 40},
    "ta_roc_10": {"label": "ROC 10", "category": "ta_momentum", "lookback": 30},
    "ta_adx_14": {"label": "ADX 14", "category": "ta_trend", "lookback": 50},
    "ta_aroonosc_14": {"label": "AROONOSC 14", "category": "ta_trend", "lookback": 50},
    "ta_typprice": {"label": "Typical Price", "category": "ta_price", "lookback": 1},
}

RESEARCH_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "research_gross_profitability": {
        "label": "Gross Profitability",
        "category": "research_quality",
        "dependencies": ["stocks.gross_margin", "stocks.revenue"],
        "description": (
            "Gross profitability proxy inspired by Novy-Marx quality research. "
            "Source: https://ssrn.com/abstract=1598056"
        ),
    },
    "research_asset_growth": {
        "label": "Asset Growth",
        "category": "research_investment",
        "dependencies": ["financial_data.total_assets"],
        "description": (
            "Asset growth / investment proxy. "
            "Source: https://ssrn.com/abstract=760967"
        ),
    },
    "research_accruals": {
        "label": "Accruals",
        "category": "research_quality",
        "dependencies": ["financial_data.net_profit", "financial_data.total_assets"],
        "description": (
            "Accounting accrual quality proxy using available local fields. "
            "Source: https://ssrn.com/abstract=2598"
        ),
    },
    "research_low_beta": {
        "label": "Low Beta",
        "category": "research_risk",
        "dependencies": ["klines_daily.close"],
        "description": (
            "Negative market beta proxy inspired by betting-against-beta research. "
            "Source: https://ssrn.com/abstract=1723048"
        ),
        "lookback": 252,
    },
    "research_idiosyncratic_volatility": {
        "label": "Idiosyncratic Volatility",
        "category": "research_risk",
        "dependencies": ["klines_daily.close"],
        "description": (
            "Negative residual volatility proxy versus equal-weight market return. "
            "Source: https://ssrn.com/abstract=1947020"
        ),
        "lookback": 252,
    },
    "research_residual_momentum": {
        "label": "Residual Momentum",
        "category": "research_momentum",
        "dependencies": ["klines_daily.close"],
        "description": (
            "12-1 month momentum residualized by market return. "
            "Source: https://repub.eur.nl/pub/22252/ResidualMomentum-2011.pdf"
        ),
        "lookback": 252,
    },
    "research_short_reversal": {
        "label": "Short Reversal",
        "category": "research_reversal",
        "dependencies": ["klines_daily.close"],
        "description": (
            "Negative 5-day return short-term reversal. "
            "Source: https://www.nber.org/papers/w2533"
        ),
        "lookback": 10,
    },
    "research_turnover_liquidity": {
        "label": "Turnover Liquidity",
        "category": "research_liquidity",
        "dependencies": ["klines_daily.volume", "klines_daily.amount"],
        "description": (
            "Liquidity proxy using amount and volume. "
            "Source: https://ideas.repec.org/a/eee/finmar/v5y2002i1p31-56.html"
        ),
        "lookback": 20,
    },
}


RELAY_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "moneyflow_net_mf_amount": {
        "label": "Moneyflow Net Amount",
        "category": "relay_moneyflow",
        "dependencies": ["moneyflow.net_mf_amount"],
        "description": "Net active money inflow amount from Indevs Tushare Relay moneyflow data.",
        "formula": "buy_sm_amount + buy_md_amount + buy_lg_amount + buy_elg_amount - sell_sm_amount - sell_md_amount - sell_lg_amount - sell_elg_amount",
        "human_description": "衡量个股当日主动资金净流入金额。正值表示买入侧金额大于卖出侧金额，适合先做横截面排序和行业内比较。",
    },
    "moneyflow_net_mf_vol": {
        "label": "Moneyflow Net Volume",
        "category": "relay_moneyflow",
        "dependencies": ["moneyflow.net_mf_vol"],
        "description": "Net active money inflow volume from Indevs Tushare Relay moneyflow data.",
        "formula": "buy_sm_vol + buy_md_vol + buy_lg_vol + buy_elg_vol - sell_sm_vol - sell_md_vol - sell_lg_vol - sell_elg_vol",
        "human_description": "衡量个股当日主动买卖量差。它更偏交易行为，不直接等同收益预测，需要结合覆盖率和分组收益验证。",
    },
    "block_moneyflow_net_amount": {
        "label": "Block Moneyflow Net Amount",
        "category": "relay_block",
        "dependencies": ["block_moneyflow.net_amount", "ths_member"],
        "description": "Block-level net moneyflow projected to member stocks when block membership is available.",
        "formula": "block_moneyflow.net_amount joined by ths_member membership",
        "human_description": "衡量所属板块的资金净流入强弱。它是板块暴露信号，不是单只股票自身资金流，使用时要注意同板块股票会共享一部分取值。",
    },
    "auction_amount": {
        "label": "Auction Amount",
        "category": "relay_auction",
        "dependencies": ["auction_replay.amount"],
        "description": "Opening auction turnover amount from Relay stk_auction_replay summary data.",
        "formula": "auction_replay.amount",
        "human_description": "衡量开盘集合竞价成交额。数值越高表示开盘前撮合活跃度越高，通常要结合流通市值或历史成交额做标准化。",
    },
    "auction_gap_pct": {
        "label": "Auction Gap Percent",
        "category": "relay_auction",
        "dependencies": ["auction_replay.price", "auction_replay.open"],
        "description": "Opening auction price gap proxy from auction replay price and open fields.",
        "formula": "(price - open) / open",
        "human_description": "衡量集合竞价价格相对竞价开盘参考价的偏离。它反映开盘情绪，但应避免把未复权或异常竞价数据直接当成方向信号。",
    },
    "auction_vwap": {
        "label": "Auction VWAP",
        "category": "relay_auction",
        "dependencies": ["auction_replay.vwap"],
        "description": "Opening auction VWAP from Relay stk_auction_replay data.",
        "formula": "auction_replay.vwap",
        "human_description": "集合竞价阶段的成交均价，可用于开盘成交质量、竞价冲击和高低开研究。",
    },
}


CN_PAPER_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "paper_pb_roe_residual": {
        "label": "PB-ROE Residual",
        "category": "paper_fundamental",
        "dependencies": ["stock_daily_basic.pb", "financial_data.roe"],
        "description": "PB-ROE residual value factor from domestic fundamental quant reports.",
        "lookback": 370,
        "formula": "BP - OLS(BP ~ ROE) residual by trade_date",
        "human_description": "用 BP=1/PB 剔除 ROE 解释后的截面残差来刻画同等盈利质量下的低估程度，数值越高通常表示相对更便宜。",
        "paper_ids": [22, 27],
        "frequency": "monthly",
    },
    "paper_composite_value": {
        "label": "Composite Value",
        "category": "paper_fundamental",
        "dependencies": ["stock_daily_basic.pb", "stock_daily_basic.pe_ttm"],
        "description": "Composite value score from BP and earnings yield ranks.",
        "lookback": 370,
        "formula": "rank_pct(1 / PB) + rank_pct(1 / PE_TTM)",
        "human_description": "综合市净率倒数和市盈率倒数的截面排名，避免单一估值口径失真；数值越高表示估值越低。",
        "paper_ids": [22, 27],
        "frequency": "monthly",
    },
    "paper_growth_quality_score": {
        "label": "Growth Quality Score",
        "category": "paper_fundamental",
        "dependencies": ["financial_data.revenue_yoy", "financial_data.profit_yoy", "financial_data.roe", "financial_data.gross_margin"],
        "description": "Growth-factor improvement score using growth, profitability and margin fields.",
        "lookback": 370,
        "formula": "zscore(revenue_yoy) + zscore(profit_yoy) + zscore(roe) + zscore(gross_margin)",
        "human_description": "融合收入增速、利润增速、ROE 和毛利率，强调成长持续性与质量，而不是只追单一高增速。",
        "paper_ids": [21, 31, 44],
        "frequency": "monthly",
    },
    "paper_financial_health_score": {
        "label": "Financial Health Score",
        "category": "paper_fundamental",
        "dependencies": ["financial_data.total_assets", "financial_data.total_liability", "financial_data.net_profit", "financial_data.revenue"],
        "description": "Financial health / distress avoidance score using local financial statement fields.",
        "lookback": 370,
        "formula": "profitability rank - leverage rank - accrual pressure rank",
        "human_description": "用盈利能力、杠杆压力和应计压力构造避雷分值；数值越高表示财务风险相对更低。",
        "paper_ids": [24, 31, 44],
        "frequency": "quarterly",
    },
    "tsmf_recent_effective_score": {
        "label": "TSMF Recent Effective Score",
        "category": "tsmf_composite",
        "dependencies": [
            "stock_daily_basic.pb",
            "stock_daily_basic.pe_ttm",
            "financial_data.revenue_yoy",
            "financial_data.profit_yoy",
            "financial_data.roe",
            "financial_data.gross_margin",
            "financial_data.total_assets",
            "financial_data.total_liability",
            "financial_data.net_profit",
        ],
        "description": "TSMF composite factor from the recent-factor audit: growth quality + composite value + financial health.",
        "lookback": 370,
        "formula": "0.40 * rank(growth_quality) + 0.35 * rank(composite_value) + 0.25 * rank(financial_health)",
        "human_description": "TSMF 近期有效复合因子。保留成长质量、综合价值、财务健康三类近期 RankIC 较稳的信号，不把 V4GV/技术买点作为正向加分。",
        "paper_ids": [19, 21, 22, 24, 31, 44],
        "frequency": "monthly",
    },
    "paper_overnight_turnover_corr": {
        "label": "Overnight-Turnover Corr",
        "category": "paper_price_volume",
        "dependencies": ["klines_daily.open", "klines_daily.close", "stock_daily_basic.turnover_rate"],
        "description": "20-day correlation between absolute overnight return and turnover.",
        "lookback": 60,
        "formula": "corr(abs(open_t / close_t-1 - 1), turnover_rate, 20)",
        "human_description": "衡量隔夜信息冲击与换手活跃度的联动，近似刻画知情交易者信息优势。",
        "paper_ids": [6],
        "frequency": "monthly",
    },
    "paper_rsi_reversal_score": {
        "label": "RSI Reversal Score",
        "category": "paper_price_volume",
        "dependencies": ["klines_daily.close", "klines_daily.volume"],
        "description": "Cross-sectional RSI reversal score with volume confirmation.",
        "lookback": 40,
        "formula": "-RSI(close, 14) adjusted by 20-day volume rank",
        "human_description": "把 RSI 从时序择时扩展到截面选股，偏向识别成交活跃但技术过热后可能反转的股票。",
        "paper_ids": [8],
        "frequency": "monthly",
    },
    "paper_new_high_anchor": {
        "label": "New High Anchor Event",
        "category": "paper_event",
        "dependencies": ["klines_daily.close"],
        "description": "Anchoring-effect new-high event signal based on 240-day highs.",
        "lookback": 260,
        "formula": "1 if close >= rolling_max(close, 240) else 0",
        "human_description": "标记接近或突破 240 日新高的股票，用于验证创新高后的锚定效应 Alpha。",
        "paper_ids": [37],
        "frequency": "daily",
    },
    "paper_high_low_volume_event": {
        "label": "High/Low Volume Event",
        "category": "paper_event",
        "dependencies": ["klines_daily.close", "klines_daily.volume"],
        "description": "Positive low-position high-volume event minus negative high-position high-volume event.",
        "lookback": 180,
        "formula": "high_volume_ratio * (1 - 2 * price_position_120d)",
        "human_description": "低位放量给正向分，高位放量给负向分，作为高低位放量事件簇的日频近似版。",
        "paper_ids": [40],
        "frequency": "weekly",
    },
    "paper_reversal_20d": {
        "label": "20D Reversal",
        "category": "paper_price_volume",
        "dependencies": ["klines_daily.close"],
        "description": "Daily-data momentum/reversal baseline from domestic price-volume reports.",
        "lookback": 40,
        "formula": "-pct_change(close, 20)",
        "human_description": "用过去 20 日收益率取反构造反转基线，后续可叠加交易者结构或资金流数据。",
        "paper_ids": [38],
        "frequency": "monthly",
    },
    "paper_size_rotation_score": {
        "label": "Size Rotation Score",
        "category": "paper_style_rotation",
        "dependencies": ["klines_daily.close", "stock_daily_basic.circ_mv", "stock_daily_basic.total_mv"],
        "description": "A-share large/small-cap rotation proxy based on trailing style-leg momentum.",
        "lookback": 80,
        "formula": "prefer small-cap rank when small-cap 20D return beats large-cap 20D return, otherwise prefer large-cap rank",
        "human_description": "用当前股票池内小市值组与大市值组的过去 20 日相对表现判断大小盘风格，再把该风格映射为个股截面分数。",
        "paper_ids": [28, 29, 43],
        "frequency": "monthly",
    },
    "paper_value_growth_rotation_score": {
        "label": "Value/Growth Rotation Score",
        "category": "paper_style_rotation",
        "dependencies": [
            "stock_daily_basic.pb",
            "stock_daily_basic.pe_ttm",
            "financial_data.revenue_yoy",
            "financial_data.profit_yoy",
            "financial_data.roe",
            "financial_data.gross_margin",
            "klines_daily.close",
        ],
        "description": "Value/growth style rotation proxy using trailing performance of value and growth factor legs.",
        "lookback": 370,
        "formula": "use growth_quality score when growth leg 20D momentum beats value leg, otherwise use composite_value",
        "human_description": "先分别构造价值组和成长组，再用两类风格腿过去 20 日表现决定当前更偏价值还是成长。",
        "paper_ids": [30, 31, 44],
        "frequency": "monthly",
    },
    "paper_industry_momentum_20d": {
        "label": "Industry Momentum 20D",
        "category": "paper_style_rotation",
        "dependencies": ["klines_daily.close", "stocks.industry"],
        "description": "Industry rotation factor from trailing 20-day industry mean return.",
        "lookback": 60,
        "formula": "rank_pct(mean(pct_change(close, 20)) by industry per trade_date)",
        "human_description": "把个股 20 日收益聚合到申万一级行业，再把行业动量分数映射回行业内股票，用于行业轮动腿。",
        "paper_ids": [18, 29, 43],
        "frequency": "monthly",
    },
    "paper_defensive_quality_lowvol": {
        "label": "Defensive Quality LowVol",
        "category": "paper_asset_allocation",
        "dependencies": ["klines_daily.close", "financial_data.total_assets", "financial_data.total_liability", "financial_data.net_profit"],
        "description": "Defensive equity sleeve proxy combining financial health and low realized volatility.",
        "lookback": 370,
        "formula": "zscore(financial_health_score) - zscore(60D volatility)",
        "human_description": "在缺少债券、商品和宏观配置数据时，先落成股票侧的防御资产代理：财务质量高、波动低的标的得分更高。",
        "paper_ids": [15, 18],
        "frequency": "monthly",
    },
    "paper_asset_allocation_proxy": {
        "label": "Asset Allocation Proxy",
        "category": "paper_asset_allocation",
        "dependencies": ["klines_daily.close"],
        "description": "Equity-only all-weather proxy using momentum, volatility and drawdown from daily bars.",
        "lookback": 120,
        "formula": "zscore(60D return) - zscore(60D volatility) + zscore(60D drawdown)",
        "human_description": "用日线能取得的收益、波动和回撤构造权益资产代理分数；宏观增长/通胀和多资产子信号仍留在待数据源清单。",
        "paper_ids": [15, 18],
        "frequency": "monthly",
    },
    "paper_trend_fund_vwap_ratio": {
        "label": "Trend Fund VWAP Ratio",
        "category": "paper_minute",
        "dependencies": ["klines_minute.close", "klines_minute.volume"],
        "description": "Minute-bar proxy for trend-fund relative average price.",
        "lookback": 10,
        "formula": "(VWAP(trend_minutes) - VWAP(day)) / VWAP(day)",
        "human_description": "用过去 5 日分钟成交量 90% 分位识别趋势资金分钟，再比较趋势资金均价与全天 VWAP。",
        "paper_ids": [10, 11, 12],
        "frequency": "weekly",
    },
    "paper_trend_fund_support": {
        "label": "Trend Fund Support",
        "category": "paper_minute",
        "dependencies": ["klines_minute.open", "klines_minute.close", "klines_minute.volume"],
        "description": "Minute-bar proxy for trend-fund net support volume.",
        "lookback": 10,
        "formula": "sum(uptrend_minute_volume - downtrend_minute_volume) / sum(trend_minute_volume)",
        "human_description": "在趋势资金分钟内统计上涨分钟与下跌分钟的成交量差，近似趋势资金净支撑量。",
        "paper_ids": [10, 11, 12],
        "frequency": "weekly",
    },
}


def _landing_grade(status: str, factor_names: list[str]) -> str:
    normalized = status.strip().lower()
    if normalized == "implemented" or normalized.startswith("implemented"):
        return "A"
    if factor_names or normalized.startswith("partial"):
        return "B"
    if normalized in {"pending_data", "backlog_data"}:
        return "C"
    if normalized in {"backlog_tick", "out_of_scope"}:
        return "D"
    return "C"


def _paper_entry(
    paper_id: int,
    title: str,
    strategy_type: str,
    data_frequency: str,
    landing_status: str,
    platform_mapping: str,
    factor_names: list[str] | None = None,
    notes: str = "",
    data_dependencies: list[str] | None = None,
    factor_rules: str | None = None,
    rebalance_frequency: str | None = None,
    validation_metrics: list[str] | None = None,
) -> dict[str, Any]:
    names = factor_names or []
    deps = data_dependencies
    if deps is None:
        dep_set: set[str] = set()
        for name in names:
            dep_set.update(str(dep).split(".", 1)[0] for dep in CN_PAPER_FACTOR_SPECS.get(name, {}).get("dependencies") or [])
        deps = sorted(dep_set)
    rules = factor_rules
    if rules is None and names:
        rules = "; ".join(
            str(CN_PAPER_FACTOR_SPECS.get(name, {}).get("formula") or name)
            for name in names
        )
    metrics = validation_metrics
    if metrics is None:
        metrics = ["coverage", "IC", "quantile_return", "turnover"] if names else ["data_availability", "spec_completeness"]
    return {
        "paper_id": paper_id,
        "title": title,
        "strategy_type": strategy_type,
        "data_frequency": data_frequency,
        "data_dependencies": deps,
        "factor_rules": rules or notes,
        "rebalance_frequency": rebalance_frequency or ("monthly" if data_frequency in {"low", "mixed", "monthly"} else data_frequency),
        "landing_status": landing_status,
        "landing_grade": _landing_grade(landing_status, names),
        "platform_mapping": platform_mapping,
        "validation_metrics": metrics,
        "factor_names": names,
        "notes": notes,
    }


PAPER_IMPLEMENTATION_MANIFEST: list[dict[str, Any]] = [
    _paper_entry(1, "2024-2025年度中国量化投资白皮书", "行业调研", "mixed", "method_only", "docs/backlog", notes="趋势判断，不直接生成交易因子。"),
    _paper_entry(2, "ETF投资宝典之一：热点概念相关ETF的自动匹配与对比", "工具研究", "text", "backlog_data", "llm/offline", notes="需要 ETF/概念映射和文本数据。"),
    _paper_entry(3, "ETF投资宝典之三：泛科技ETF优选与细分赛道ETF轮动", "ETF轮动", "weekly", "backlog_data", "strategy_template", notes="需要 ETF 历史行情和赛道标签。"),
    _paper_entry(4, "ETF投资宝典之二：自由现金流全方位解析", "Smart Beta", "semiannual", "pending_data", "factor_catalog", notes="缺自由现金流字段，先不造代理值。"),
    _paper_entry(5, "LLM赋能资产配置：AI宏观因子构建与应用", "AI宏观因子", "weekly_text", "backlog_data", "llm/offline", notes="需要新闻文本和宏观资产价格。"),
    _paper_entry(6, "量价淘金(一)：隔夜涨跌选股因子", "选股因子", "daily", "implemented", "factor_values", ["paper_overnight_turnover_corr"]),
    _paper_entry(7, "量价淘金(七)：羊群效应因子", "选股因子", "minute", "partial_minute_proxy", "factor_values", ["paper_trend_fund_support"], "逐笔版本不做，分钟版先做代理。"),
    _paper_entry(8, "量价淘金(三)：RSI技术指标选股因子", "选股因子", "daily", "implemented", "factor_values", ["paper_rsi_reversal_score"]),
    _paper_entry(9, "量价淘金(九)：Memory Map因子生产加速", "因子方法论", "minute_tick", "method_only", "factor_pipeline", notes="作为批量生产/性能方法，不单独生成 Alpha。"),
    _paper_entry(10, "量价淘金(五)：趋势资金日内交易行为因子", "选股因子", "minute", "implemented", "factor_values", ["paper_trend_fund_vwap_ratio", "paper_trend_fund_support"]),
    _paper_entry(11, "量价淘金(十一)：趋势资金事件驱动策略", "事件驱动", "minute_weekly", "partial_factor_first", "akquant_strategy", ["paper_trend_fund_vwap_ratio", "paper_trend_fund_support"]),
    _paper_entry(12, "量价淘金(十三)：事件簇规模化生产", "事件驱动", "minute_weekly", "partial_factor_first", "factor_pipeline", ["paper_trend_fund_vwap_ratio", "paper_trend_fund_support"]),
    _paper_entry(13, "主题量化投资之一：科技主题投资攻略", "主题选股", "monthly", "pending_data", "strategy_template", notes="需要研发、专利、主题标签数据。"),
    _paper_entry(14, "主题量化投资之二：反内卷量化策略", "主题选股", "mixed", "pending_spec", "docs/backlog", notes="md 未给出可执行规则。"),
    _paper_entry(15, "从资产配置走向因子配置：中国版全天候增强", "资产配置", "monthly", "partial_equity_proxy", "factor_values", ["paper_defensive_quality_lowvol", "paper_asset_allocation_proxy"], "宏观增长/通胀和多资产子信号待数据源，先落权益代理。"),
    _paper_entry(16, "固收量化入门指南", "固收量化", "low", "out_of_scope", "docs/backlog", notes="当前平台主线是 A 股股票/ETF。"),
    _paper_entry(17, "量化2025年度复盘：选股策略回顾", "年度复盘", "monthly", "method_only", "factor_pipeline", notes="作为组合加权方式参考。"),
    _paper_entry(18, "基于宏观数据的资产配置与风格行业轮动体系", "资产配置", "monthly", "partial_style_rotation", "akquant_strategy", ["paper_size_rotation_score", "paper_value_growth_rotation_score", "paper_industry_momentum_20d", "paper_asset_allocation_proxy"], "宏观子信号待接入，A 股风格/行业腿已落地。"),
    _paper_entry(19, "基本面量化的当下和未来：因子篇", "方法论", "low", "partial_implemented", "factor_values", ["paper_composite_value", "paper_growth_quality_score", "paper_financial_health_score"]),
    _paper_entry(20, "基本面量化的当下和未来：策略篇", "方法论", "low", "partial_template", "akquant_strategy", notes="落为多因子组合模板。"),
    _paper_entry(21, "基本面量化之五：成长因子改造手册", "选股因子", "monthly", "implemented", "factor_values", ["paper_growth_quality_score"]),
    _paper_entry(22, "基本面量化之一：估值因子的内涵与逻辑", "选股因子", "monthly", "implemented", "factor_values", ["paper_pb_roe_residual", "paper_composite_value"]),
    _paper_entry(23, "基本面量化之三：自由现金流投资全解析", "选股策略", "monthly", "pending_data", "factor_catalog", notes="缺自由现金流字段。"),
    _paper_entry(24, "基本面量化之二：财务健康指标体系", "风险识别", "quarterly", "implemented", "factor_values", ["paper_financial_health_score"]),
    _paper_entry(25, "广发量化基本面之四：宏观触底与微观领涨", "择时+选股", "signal", "backlog_data", "strategy_template", notes="需要宏观触底信号定义和行业映射。"),
    _paper_entry(26, "金工深度研究：分析师预期类因子初探", "选股因子", "monthly", "pending_data", "factor_catalog", notes="缺分析师覆盖、评级和盈利预测数据。"),
    _paper_entry(27, "金工深度研究：如何使价值因子更具价值", "选股因子", "monthly", "implemented", "factor_values", ["paper_pb_roe_residual", "paper_composite_value"]),
    _paper_entry(28, "风格轮动策略(一)：大小盘的趋势与周期", "风格轮动", "20d", "implemented_template", "akquant_strategy", ["paper_size_rotation_score", "paper_reversal_20d"]),
    _paper_entry(29, "风格轮动策略(二)：风格轮动框架构建", "方法论", "monthly", "implemented_template", "akquant_strategy", ["paper_size_rotation_score", "paper_value_growth_rotation_score", "paper_industry_momentum_20d"]),
    _paper_entry(30, "风格轮动策略(三)：成长价值轮动的宏观信号", "风格轮动", "monthly", "partial_data", "factor_values", ["paper_value_growth_rotation_score"], "宏观信号待接入，先用基本面风格腿表现做代理。"),
    _paper_entry(31, "风格轮动策略(四)：成长价值轮动的基本面信号", "风格轮动", "monthly", "implemented_factor_leg", "factor_values", ["paper_growth_quality_score", "paper_composite_value", "paper_financial_health_score", "paper_value_growth_rotation_score"]),
    _paper_entry(32, "深度学习揭秘之一：量价与基本面结合", "深度学习", "5d_20d", "backlog_experiment", "offline_ml", notes="先不进入主线策略库。"),
    _paper_entry(33, "深度学习揭秘之二：多模型结合神经网络", "深度学习", "mixed", "backlog_experiment", "offline_ml"),
    _paper_entry(34, "深度学习揭秘之三：用DeepSeek优化价量因子", "AI因子工程", "weekly", "backlog_experiment", "offline_ml"),
    _paper_entry(35, "深度学习揭秘之四：DeepSeek大模型助力投研", "工具研究", "text", "method_only", "docs/backlog"),
    _paper_entry(36, "深度学习揭秘之五：AI能否终结人工因子挖掘", "AI因子挖掘", "mixed", "backlog_experiment", "offline_ml"),
    _paper_entry(37, "量价淘金(六)：创新高股票中的Alpha", "事件驱动", "daily", "implemented", "factor_values", ["paper_new_high_anchor"]),
    _paper_entry(38, "量价淘金(二)：不同交易者结构下的动量与反转", "选股因子", "daily", "partial_daily_proxy", "factor_values", ["paper_reversal_20d"], "大小单数据缺失，先做日频反转基线。"),
    _paper_entry(39, "量价淘金(八)：条件成交不平衡因子", "选股因子", "tick", "backlog_tick", "docs/backlog", notes="Tick-only，按要求不实现。"),
    _paper_entry(40, "量价淘金(十五)：高低位放量事件簇", "事件驱动", "daily", "implemented", "factor_values", ["paper_high_low_volume_event"]),
    _paper_entry(41, "金工专题报告：多模型结合神经网络", "深度学习", "mixed", "backlog_experiment", "offline_ml", notes="与报告 33 重复。"),
    _paper_entry(42, "量价淘金(十二)：高频数据+离散化构建方式", "选股因子", "minute_tick", "partial_minute_proxy", "factor_values", ["paper_trend_fund_support"]),
    _paper_entry(43, "风格轮动策略(二)：风格轮动框架构建", "方法论", "monthly", "implemented_template", "akquant_strategy", ["paper_size_rotation_score", "paper_value_growth_rotation_score", "paper_industry_momentum_20d"], "与报告 29 重复。"),
    _paper_entry(44, "风格轮动策略(四)：成长价值轮动的基本面信号", "风格轮动", "monthly", "implemented_factor_leg", "factor_values", ["paper_growth_quality_score", "paper_composite_value", "paper_financial_health_score", "paper_value_growth_rotation_score"]),
]


TSMF_FULL_FACTOR_POOL: list[str] = list(dict.fromkeys([
    "market_cap",
    "market_cap_rank",
    "is_st",
    "is_paused",
    "is_limit_up",
    "is_limit_down",
    "yesterday_limit_up",
    "v4gv",
    "v4gv_signal",
    "macd_positive",
    "indicator_buy_signal",
    "tsmf_overheat_penalty",
    "v4gv_dead_cross",
    "cum_volume_at_time",
    "rolling_max_volume",
    "high_volume_ratio",
    "avoid_high_volume_ratio",
    "high_volume_signal",
    "paper_composite_value",
    "paper_pb_roe_residual",
    "paper_growth_quality_score",
    "paper_financial_health_score",
    "tsmf_recent_effective_score",
    "paper_defensive_quality_lowvol",
    "paper_industry_momentum_20d",
    "paper_value_growth_rotation_score",
    "paper_size_rotation_score",
    "paper_high_low_volume_event",
    "paper_overnight_turnover_corr",
    "paper_rsi_reversal_score",
    "paper_new_high_anchor",
    "paper_reversal_20d",
    "paper_asset_allocation_proxy",
    "paper_trend_fund_vwap_ratio",
    "paper_trend_fund_support",
]))

TSMF_PREFERRED_FACTOR_POOL_BUCKETS: dict[str, list[str]] = {
    "size_core": ["market_cap_rank", "paper_size_rotation_score"],
    "value": ["paper_composite_value", "paper_pb_roe_residual"],
    "growth_quality": ["paper_growth_quality_score", "paper_financial_health_score", "tsmf_recent_effective_score"],
    "risk_quality": ["paper_defensive_quality_lowvol", "avoid_high_volume_ratio", "tsmf_overheat_penalty"],
    "rotation_momentum": ["paper_industry_momentum_20d", "paper_value_growth_rotation_score"],
    "minute_flow": ["paper_trend_fund_support", "paper_trend_fund_vwap_ratio"],
}
TSMF_PREFERRED_FACTOR_POOL: list[str] = list(dict.fromkeys(
    name for names in TSMF_PREFERRED_FACTOR_POOL_BUCKETS.values() for name in names
))

TSMF_STRATEGY_SIGNAL_NOTES: list[dict[str, str]] = [
    {
        "name": "rank40_buy_hold_spread",
        "status": "strategy_rule",
        "description": "R3 buy/hold spread: buy top20, hold until rank buffer around 40-50 before selling.",
    },
    {
        "name": "risk_score_rebound_reentry",
        "status": "strategy_state",
        "description": "Use low risk_score followed by price or breadth repair as a rebound/re-entry signal instead of only cutting exposure.",
    },
    {
        "name": "rebound_price_or_breadth",
        "status": "strategy_state",
        "description": "Mild rebound confirmation used by R3 and later tests: price repair or market breadth repair can release cooldown/re-entry.",
    },
    {
        "name": "risk_score",
        "status": "portfolio_state",
        "description": "Portfolio-level market state score from the TSMF V2 strategy; useful for factor rotation templates but not a per-stock cross-section.",
    },
    {
        "name": "us_entry_filter_combined_downside",
        "status": "portfolio_timing",
        "description": "QQQ/SMH/SOXX/NVDA overnight downside filter; blocks new buys/adds rather than forcing old holdings down.",
    },
    {
        "name": "largecap_lowvol_sleeve",
        "status": "researched_not_default",
        "description": "CSI300/large-cap low-vol defensive sleeve. Tested but not selected as default because cash was cleaner.",
    },
    {
        "name": "dividend_lowvol_etf_sleeve",
        "status": "researched_not_default",
        "description": "Dividend low-vol ETF/proxy sleeve using 512890.SH when available and 930955.SH as local proxy.",
    },
]


CATALOG_GROUPS: dict[str, dict[str, Any]] = {
    "ta_lib_core": {
        "name": "ta_lib_core",
        "display_name": "TA-Lib 核心因子",
        "description": "AKQuant TA-Lib backed technical factors.",
        "factor_names": list(TA_FACTOR_SPECS),
    },
    "alpha101": {
        "name": "alpha101",
        "display_name": "Alpha101",
        "description": "WorldQuant 101 Formulaic Alphas adapted to local OHLCV data.",
        "factor_names": [f"alpha101_{i:03d}" for i in range(1, 102)],
    },
    "research_factor_ideas": {
        "name": "research_factor_ideas",
        "display_name": "海外研究因子",
        "description": "Research-inspired quality, investment, risk, momentum and liquidity factors.",
        "factor_names": list(RESEARCH_FACTOR_SPECS),
    },
    "relay_structured_factors": {
        "name": "relay_structured_factors",
        "display_name": "Relay 结构化因子",
        "description": "Lightweight factors derived from Relay moneyflow, block moneyflow and auction replay datasets.",
        "factor_names": list(RELAY_FACTOR_SPECS),
    },
    "tsmf_research_factor_library": {
        "name": "tsmf_research_factor_library",
        "display_name": "TSMF Research Factor Library",
        "description": (
            "Factors and reusable state signals surfaced while developing the "
            "TSMF technology small-cap strategy, including core filters, value/"
            "quality/growth legs, overheat penalties, high-volume risk, style "
            "rotation, and minute trend-fund proxies."
        ),
        "factor_names": TSMF_FULL_FACTOR_POOL,
        "strategy_signals": TSMF_STRATEGY_SIGNAL_NOTES,
    },
    "tsmf_preferred_rotation_pool": {
        "name": "tsmf_preferred_rotation_pool",
        "display_name": "TSMF Preferred Rotation Pool",
        "description": (
            "Curated 2-3 factors per type for later factor-rotation strategies. "
            "Selected from the current database/catalog based on the recent TSMF "
            "audit: value, growth/quality, risk quality, style rotation, and "
            "minute flow."
        ),
        "factor_names": TSMF_PREFERRED_FACTOR_POOL,
        "selection_buckets": TSMF_PREFERRED_FACTOR_POOL_BUCKETS,
        "strategy_signals": [
            item for item in TSMF_STRATEGY_SIGNAL_NOTES
            if item["name"] in {"risk_score_rebound_reentry", "rebound_price_or_breadth", "us_entry_filter_combined_downside"}
        ],
    },
    "cn_paper_fundamental": {
        "name": "cn_paper_fundamental",
        "display_name": "研报基本面因子",
        "description": "Domestic research-report factors that use financial statements and daily basic data.",
        "factor_names": [
            name for name, spec in CN_PAPER_FACTOR_SPECS.items()
            if str(spec.get("category")) == "paper_fundamental"
        ],
    },
    "cn_paper_daily_events": {
        "name": "cn_paper_daily_events",
        "display_name": "研报日频量价/事件因子",
        "description": "Domestic research-report factors built from daily OHLCV and daily basic data.",
        "factor_names": [
            name for name, spec in CN_PAPER_FACTOR_SPECS.items()
            if str(spec.get("category")) in {"paper_price_volume", "paper_event"}
        ],
    },
    "cn_paper_minute": {
        "name": "cn_paper_minute",
        "display_name": "研报分钟线因子",
        "description": "Minute-bar proxies for non-Tick domestic research-report signals.",
        "factor_names": [
            name for name, spec in CN_PAPER_FACTOR_SPECS.items()
            if str(spec.get("category")) == "paper_minute"
        ],
    },
    "cn_paper_style_rotation": {
        "name": "cn_paper_style_rotation",
        "display_name": "研报风格轮动/配置因子",
        "description": "A-share style rotation, industry rotation and equity-only asset allocation proxy factors.",
        "factor_names": [
            name for name, spec in CN_PAPER_FACTOR_SPECS.items()
            if str(spec.get("category")) in {"paper_style_rotation", "paper_asset_allocation"}
        ],
    },
    "cn_paper_implemented": {
        "name": "cn_paper_implemented",
        "display_name": "研报已落地因子",
        "description": "All currently implemented non-Tick paper factors.",
        "factor_names": list(CN_PAPER_FACTOR_SPECS),
    },
}


def list_catalog_definitions() -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    for name, spec in TA_FACTOR_SPECS.items():
        definitions.append(_definition(
            name,
            str(spec["label"]),
            category=str(spec["category"]),
            description=f"TA-Lib technical factor: {spec['label']}.",
            lookback_days=int(spec.get("lookback") or 0),
            source="catalog.ta_lib",
        ))
    for i in range(1, 102):
        formula = _alpha101_formula(i)
        human_description = _alpha101_description(i, formula)
        definitions.append(_definition(
            f"alpha101_{i:03d}",
            f"Alpha101 #{i:03d}",
            category="alpha101",
            description=human_description,
            lookback_days=370,
            source="catalog.alpha101",
            dependencies=[
                "klines_daily.open",
                "klines_daily.high",
                "klines_daily.low",
                "klines_daily.close",
                "klines_daily.volume",
                "klines_daily.amount",
                "stocks.total_mv",
                "stocks.industry",
            ],
            data_policy={
                "requires_panel": True,
                "lookback_days": 370,
                "rank_scope": "selected universe per trade_date",
                "industry_neutralization": "stocks.industry when formula requires IndNeutralize",
            },
            formula=formula,
            human_description=human_description,
        ))
    for name, spec in RESEARCH_FACTOR_SPECS.items():
        definitions.append(_definition(
            name,
            str(spec["label"]),
            category=str(spec["category"]),
            description=str(spec["description"]),
            dependencies=list(spec.get("dependencies") or []),
            lookback_days=int(spec.get("lookback") or 0),
            source="catalog.research",
        ))
    for name, spec in RELAY_FACTOR_SPECS.items():
        definitions.append(_definition(
            name,
            str(spec["label"]),
            category=str(spec["category"]),
            description=str(spec["description"]),
            dependencies=list(spec.get("dependencies") or []),
            lookback_days=5,
            source="catalog.relay",
            formula=str(spec.get("formula") or ""),
            human_description=str(spec.get("human_description") or spec["description"]),
            data_policy={
                "requires_relay_dataset": True,
                "precompute_source": "Parquet Relay datasets; run sync_type=tushare_relay first",
            },
        ))
    for name, spec in CN_PAPER_FACTOR_SPECS.items():
        definitions.append(_definition(
            name,
            str(spec["label"]),
            category=str(spec["category"]),
            frequency=str(spec.get("frequency") or "monthly"),
            description=str(spec["description"]),
            dependencies=list(spec.get("dependencies") or []),
            lookback_days=int(spec.get("lookback") or 0),
            source="catalog.cn_paper",
            formula=str(spec.get("formula") or ""),
            human_description=str(spec.get("human_description") or spec["description"]),
            data_policy={
                "report_paper_ids": list(spec.get("paper_ids") or []),
                "tick_policy": "implemented factors never require Tick/order-book data",
                "missing_source_policy": "papers with unavailable fields stay in PAPER_IMPLEMENTATION_MANIFEST backlog",
            },
        ))
    return definitions


def get_catalog_definition(name: str) -> dict[str, Any] | None:
    return next((item for item in list_catalog_definitions() if item["name"] == name), None)


def list_catalog_groups() -> list[dict[str, Any]]:
    return list(CATALOG_GROUPS.values())


def list_paper_implementation_manifest() -> list[dict[str, Any]]:
    return [dict(item) for item in PAPER_IMPLEMENTATION_MANIFEST]


def get_catalog_group(name: str) -> dict[str, Any] | None:
    return CATALOG_GROUPS.get(name)


def is_catalog_factor(name: str) -> bool:
    return (
        name in TA_FACTOR_SPECS
        or name in RESEARCH_FACTOR_SPECS
        or name in RELAY_FACTOR_SPECS
        or name in CN_PAPER_FACTOR_SPECS
        or (name.startswith("alpha101_") and name[-3:].isdigit() and 1 <= int(name[-3:]) <= 101)
    )
