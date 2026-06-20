"""Shared catalog for local Parquet datasets."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ParquetDatasetSpec:
    name: str
    label: str
    category: str
    date_column: str | None = None
    symbol_column: str | None = "symbol"
    description: str = ""
    factor_ready: bool = False
    exact_summary: bool = False


_SPECS: tuple[ParquetDatasetSpec, ...] = (
    ParquetDatasetSpec("klines_daily", "A 股日线", "market", "trade_date", description="A 股 OHLCV 日频行情。"),
    ParquetDatasetSpec("klines_minute", "A 股 1 分钟线", "market", "datetime", description="A 股完整 1 分钟 OHLCV 行情。"),
    ParquetDatasetSpec("klines_minute_timer", "A 股定时分钟线", "market", "datetime", description="固定时点抽样的分钟线。"),
    ParquetDatasetSpec("klines_minute_cum_timer", "A 股累计定时分钟线", "market", "datetime", description="固定时点累计成交量分钟特征。"),
    ParquetDatasetSpec("factor_cache", "表达式因子缓存", "factor", "trade_date", description="Compute Engine 表达式因子缓存。"),
    ParquetDatasetSpec("factor_values", "因子值缓存", "factor", "trade_date", description="策略和因子评估共用的预计算因子值。"),
    ParquetDatasetSpec("stock_indicators", "指标缓存", "factor", "trade_date", description="Indicator 体系截面指标缓存。"),
    ParquetDatasetSpec("indicator_timeseries", "时序指标缓存", "factor", "datetime", description="Indicator 体系时序指标缓存。"),
    ParquetDatasetSpec("adj_factors", "复权因子", "market", "trade_date", description="个股复权因子。"),
    ParquetDatasetSpec("moneyflow", "Tushare 个股资金流", "moneyflow", "trade_date", description="Tushare/Relay 个股资金流。", factor_ready=True),
    ParquetDatasetSpec("block_moneyflow", "板块资金流", "moneyflow", "trade_date", symbol_column="block_code", description="板块级资金流。", factor_ready=True),
    ParquetDatasetSpec("auction_replay", "集合竞价回放", "market_microstructure", "datetime", description="集合竞价阶段摘要。", factor_ready=True),
    ParquetDatasetSpec("ths_index", "同花顺概念指数", "theme", "snapshot_date", symbol_column="ths_code", description="同花顺概念指数快照。"),
    ParquetDatasetSpec("ths_member", "同花顺概念成分", "theme", "snapshot_date", description="同花顺概念成员快照。"),
    ParquetDatasetSpec("announcements", "公告", "text", "ann_date", description="上市公司公告。"),
    ParquetDatasetSpec("research_reports", "研报", "text", "report_date", description="卖方研报元数据。"),
    ParquetDatasetSpec("market_news", "市场新闻", "text", "publish_time", symbol_column=None, description="市场新闻。"),
    ParquetDatasetSpec("analyst_report_forecasts", "分析师预测", "analyst", "report_date", description="分析师盈利预测。"),
    ParquetDatasetSpec("analyst_rank", "分析师排名", "analyst", "update_date", symbol_column="analyst_id", description="分析师排名。"),
    ParquetDatasetSpec("analyst_detail", "分析师覆盖明细", "analyst", "latest_rating_date", symbol_column="analyst_id", description="分析师覆盖明细。"),
    ParquetDatasetSpec("analyst_history", "分析师历史覆盖", "analyst", "in_date", symbol_column="analyst_id", description="分析师历史覆盖。"),
    ParquetDatasetSpec("hsgt_moneyflow", "沪深港通资金流", "moneyflow", "trade_date", symbol_column=None, description="沪深港通资金流。"),
    ParquetDatasetSpec("hsgt_holdings", "沪深港通持股", "moneyflow", "trade_date", description="沪深港通持股。"),
    ParquetDatasetSpec("fund_portfolio_holdings", "基金持仓", "fund", "end_date", symbol_column="fund_code", description="基金组合持仓。"),
    ParquetDatasetSpec("financial_income", "Tushare 利润表", "financial", "f_ann_date", description="Tushare 利润表。"),
    ParquetDatasetSpec("financial_balancesheet", "Tushare 资产负债表", "financial", "f_ann_date", description="Tushare 资产负债表。"),
    ParquetDatasetSpec("financial_cashflow", "Tushare 现金流量表", "financial", "f_ann_date", description="Tushare 现金流量表。"),
    ParquetDatasetSpec("jq_etf_daily_bars", "JQ ETF 日线", "joinquant", "trade_date", description="JoinQuant ETF 日线行情。"),
    ParquetDatasetSpec("jq_index_daily_bars", "JQ 指数日线", "joinquant", "trade_date", description="JoinQuant 指数日线行情。"),
    ParquetDatasetSpec("jq_index_minute_bars", "JQ 指数分钟线", "joinquant", "datetime", description="JoinQuant 指数分钟行情。"),
    ParquetDatasetSpec("jq_money_flow_daily", "JQ 个股资金流", "joinquant", "trade_date_1", description="JoinQuant 个股资金流；trade_date 为空，统一使用 trade_date_1。", factor_ready=True, exact_summary=True),
    ParquetDatasetSpec("jq_financial_income", "JQ 利润表", "joinquant", "available_date", description="JoinQuant 利润表，available_date 为可用日期。"),
    ParquetDatasetSpec("jq_financial_balance", "JQ 资产负债表", "joinquant", "available_date", description="JoinQuant 资产负债表，available_date 为可用日期。"),
    ParquetDatasetSpec("jq_financial_cash_flow", "JQ 现金流量表", "joinquant", "available_date", description="JoinQuant 现金流量表，available_date 为可用日期。"),
    ParquetDatasetSpec("jq_stock_static_snapshot", "JQ 股票静态快照", "joinquant", "snapshot_date", description="JoinQuant 股票静态信息快照。"),
    ParquetDatasetSpec("tushare_moneyflow_daily_partial", "Tushare 个股资金流增量", "moneyflow", "trade_date", description="Tushare 个股资金流增量样本。", factor_ready=True),
    ParquetDatasetSpec("tushare_margin", "融资融券汇总", "margin", "trade_date", symbol_column="exchange_id", description="融资融券市场汇总。"),
    ParquetDatasetSpec("tushare_margin_detail", "融资融券明细", "margin", "trade_date", description="融资融券个股明细。"),
    ParquetDatasetSpec("tushare_limit_list_d", "涨跌停明细", "limit", "trade_date", description="涨跌停明细。"),
    ParquetDatasetSpec("tushare_limit_step", "连板梯队", "limit", "trade_date", description="连板梯队。"),
    ParquetDatasetSpec("tushare_limit_cpt_list", "涨停概念", "limit", "trade_date", description="涨停概念列表。"),
    ParquetDatasetSpec("tushare_stk_limit", "个股涨跌停价", "limit", "trade_date", description="个股涨跌停价格。"),
    ParquetDatasetSpec("tushare_top_list", "龙虎榜", "market_microstructure", "trade_date", description="龙虎榜个股明细。"),
    ParquetDatasetSpec("tushare_top_inst", "龙虎榜机构席位", "market_microstructure", "trade_date", description="龙虎榜机构席位。"),
    ParquetDatasetSpec("tushare_kpl_list", "开盘啦题材", "theme", "trade_date", description="开盘啦题材列表。"),
    ParquetDatasetSpec("tushare_hm_list", "游资名录", "market_microstructure", "trade_date", symbol_column=None, description="游资/席位名录。"),
    ParquetDatasetSpec("tushare_report_rc", "券商研报", "analyst", "report_date", description="券商研报。"),
    ParquetDatasetSpec("tushare_fund_basic", "基金基础信息", "fund", "found_date", symbol_column="ts_code", description="基金基础信息。"),
    ParquetDatasetSpec("tushare_fund_company", "基金公司", "fund", "setup_date", symbol_column=None, description="基金公司信息。"),
    ParquetDatasetSpec("tushare_fund_daily", "基金日线", "fund", "trade_date", symbol_column="ts_code", description="基金日线行情。"),
    ParquetDatasetSpec("tushare_fund_adj", "基金复权因子", "fund", "trade_date", symbol_column="ts_code", description="基金复权因子。"),
    ParquetDatasetSpec("tushare_fund_div", "基金分红", "fund", "ann_date", symbol_column="ts_code", description="基金分红。"),
    ParquetDatasetSpec("tushare_hk_basic", "港股基础信息", "hk", "list_date", symbol_column="ts_code", description="港股基础信息。"),
    ParquetDatasetSpec("tushare_hk_income", "港股利润表", "hk", "ann_date", symbol_column="ts_code", description="港股利润表。"),
    ParquetDatasetSpec("tushare_hk_balancesheet", "港股资产负债表", "hk", "ann_date", symbol_column="ts_code", description="港股资产负债表。"),
    ParquetDatasetSpec("tushare_hk_cashflow", "港股现金流量表", "hk", "ann_date", symbol_column="ts_code", description="港股现金流量表。"),
    ParquetDatasetSpec("tushare_hk_fina_indicator", "港股财务指标", "hk", "ann_date", symbol_column="ts_code", description="港股财务指标。"),
    ParquetDatasetSpec("tushare_us_basic", "美股基础信息", "us", "list_date", symbol_column="ts_code", description="美股基础信息。"),
    ParquetDatasetSpec("tushare_us_income", "美股利润表", "us", "ann_date", symbol_column="ts_code", description="美股利润表。"),
    ParquetDatasetSpec("tushare_us_balancesheet", "美股资产负债表", "us", "ann_date", symbol_column="ts_code", description="美股资产负债表。"),
    ParquetDatasetSpec("tushare_us_cashflow", "美股现金流量表", "us", "ann_date", symbol_column="ts_code", description="美股现金流量表。"),
    ParquetDatasetSpec("tushare_us_fina_indicator", "美股财务指标", "us", "ann_date", symbol_column="ts_code", description="美股财务指标。"),
)

PARQUET_DATASET_SPECS: dict[str, ParquetDatasetSpec] = {spec.name: spec for spec in _SPECS}
PARQUET_DATE_COLUMNS: dict[str, str] = {
    spec.name: spec.date_column for spec in _SPECS if spec.date_column
}


def get_parquet_dataset_spec(name: str) -> ParquetDatasetSpec | None:
    return PARQUET_DATASET_SPECS.get(name)


def get_parquet_date_column(name: str) -> str | None:
    spec = get_parquet_dataset_spec(name)
    return spec.date_column if spec else None


def iter_parquet_dataset_specs(root: str | Path | None = None) -> Iterable[ParquetDatasetSpec]:
    if root is None:
        yield from _SPECS
        return

    root_path = Path(root)
    known = set(PARQUET_DATASET_SPECS)
    for spec in _SPECS:
        yield spec
    if root_path.exists():
        for child in sorted(path for path in root_path.iterdir() if path.is_dir()):
            if child.name in known or child.name in {"temp", "import_state"}:
                continue
            yield ParquetDatasetSpec(
                name=child.name,
                label=child.name,
                category="external",
                date_column=None,
                description="未登记的本地 Parquet 数据集。",
            )


def build_dataset_summary(spec: ParquetDatasetSpec, **extra: object) -> dict[str, object]:
    return {
        "name": spec.name,
        "label": spec.label,
        "category": spec.category,
        "date_column": spec.date_column,
        "symbol_column": spec.symbol_column,
        "description": spec.description,
        "factor_ready": spec.factor_ready,
        "exact_summary": spec.exact_summary,
        **extra,
    }
