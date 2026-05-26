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
    return definitions


def get_catalog_definition(name: str) -> dict[str, Any] | None:
    return next((item for item in list_catalog_definitions() if item["name"] == name), None)


def list_catalog_groups() -> list[dict[str, Any]]:
    return list(CATALOG_GROUPS.values())


def get_catalog_group(name: str) -> dict[str, Any] | None:
    return CATALOG_GROUPS.get(name)


def is_catalog_factor(name: str) -> bool:
    return (
        name in TA_FACTOR_SPECS
        or name in RESEARCH_FACTOR_SPECS
        or name in RELAY_FACTOR_SPECS
        or (name.startswith("alpha101_") and name[-3:].isdigit() and 1 <= int(name[-3:]) <= 101)
    )
