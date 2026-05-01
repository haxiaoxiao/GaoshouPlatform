"""Unified Pydantic models for factor construction, evaluation, and backtest.

FactorConfig is the single entry-point config used across compute, eval,
and backtest layers. Each layer adds its own config (EvalConfig, BtConfig)
and produces its own result type (FactorMatrix, FactorReport, BacktestReport).
"""

from __future__ import annotations

import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────────

class StockPool(str, Enum):
    HS300 = "hs300"
    ZZ500 = "zz500"
    ZZ800 = "zz800"
    ZZ1000 = "zz1000"
    ZZ_QUANZHI = "zz_quanzhi"


class FactorDirection(str, Enum):
    ASC = "asc"    # smaller factor value is better
    DESC = "desc"  # larger factor value is better


class ICMethod(str, Enum):
    PEARSON = "pearson"
    SPEARMAN = "spearman"


class OutlierHandling(str, Enum):
    NONE = "none"
    WINSORIZE = "winsorize"
    STANDARDIZE = "standardize"


class RebalancePeriod(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class PortfolioType(str, Enum):
    LONG_ONLY = "long_only"
    LONG_SHORT_I = "long_short_i"
    LONG_SHORT_II = "long_short_ii"


# ── Unified Factor Config ─────────────────────────────────────────────────────

class FactorConfig(BaseModel):
    """Single config reused across compute, evaluation, and backtest layers."""
    expression: str = Field(..., description="Factor expression, e.g. '(1/PE_TTM) + ROE'")
    stock_pool: StockPool = Field(..., description="Stock universe")
    start_date: datetime.date
    end_date: datetime.date
    benchmark: str = Field("000300.SH", description="Benchmark symbol")
    direction: FactorDirection = Field(FactorDirection.DESC, description="Factor direction")


# ── Evaluation Config ────────────────────────────────────────────────────────

class EvalConfig(BaseModel):
    """Parameters specific to factor evaluation / IC analysis."""
    ic_method: ICMethod = Field(ICMethod.SPEARMAN)
    group_count: int = Field(5, ge=2, le=20)
    outlier_handling: OutlierHandling = Field(OutlierHandling.WINSORIZE)
    industry_neutralization: bool = False
    include_st: bool = False
    include_new: bool = True


# ── Backtest Config ──────────────────────────────────────────────────────────

class BtConfig(BaseModel):
    """Parameters specific to factor quantile backtest."""
    rebalance_period: RebalancePeriod = Field(RebalancePeriod.MONTHLY)
    fee_rate: float = Field(0.001, ge=0, le=0.05)
    slippage: float = Field(0.001, ge=0, le=0.05)
    filter_limit_up: bool = True
    portfolio_type: PortfolioType = Field(PortfolioType.LONG_ONLY)


# ── Result Types ─────────────────────────────────────────────────────────────

class ICPoint(BaseModel):
    date: datetime.date
    value: float


class IndustryIC(BaseModel):
    industry: str
    value: float


class TurnoverPoint(BaseModel):
    date: datetime.date
    min_quantile: float
    max_quantile: float


class DecayPoint(BaseModel):
    lag: int
    min_quantile: float
    max_quantile: float


class StockFactorValue(BaseModel):
    symbol: str
    name: str
    value: float


class FactorReport(BaseModel):
    """6-module factor analysis report."""
    ic_series: list[ICPoint] = Field(default_factory=list)
    industry_ic: list[IndustryIC] = Field(default_factory=list)
    turnover: list[TurnoverPoint] = Field(default_factory=list)
    signal_decay: list[DecayPoint] = Field(default_factory=list)
    top20: list[StockFactorValue] = Field(default_factory=list)
    bottom20: list[StockFactorValue] = Field(default_factory=list)
    update_date: datetime.date


class BacktestMetrics(BaseModel):
    total_return: float
    annual_return: float
    sharpe: float
    max_drawdown: float
    alpha: float
    beta: float
    ir: float


class NAVPoint(BaseModel):
    date: datetime.date
    value: float


class BacktestReport(BaseModel):
    """Unified backtest output."""
    nav_series: list[NAVPoint] = Field(default_factory=list)
    benchmark_series: list[NAVPoint] = Field(default_factory=list)
    metrics: BacktestMetrics | None = None
    logs: list[str] = Field(default_factory=list)


# ── Board Types ──────────────────────────────────────────────────────────────

class BoardQuery(BaseModel):
    """Query parameters for factor board."""
    categories: list[str] | None = None  # None = all
    stock_pool: StockPool = StockPool.ZZ500
    period: Literal["3m", "1y", "3y", "10y"] = "3y"
    portfolio_type: PortfolioType = PortfolioType.LONG_ONLY
    fee_config: Literal["none", "commission_stamp", "commission_stamp_slippage"] = "none"
    filter_limit_up: bool = True
    sort_by: str = "ic_mean"
    sort_order: Literal["asc", "desc"] = "desc"
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


class BoardRow(BaseModel):
    """Single row in factor board table."""
    factor_name: str
    category: str
    min_quantile_excess_return: float
    max_quantile_excess_return: float
    min_quantile_turnover: float
    max_quantile_turnover: float
    ic_mean: float
    ir: float


class BoardResponse(BaseModel):
    rows: list[BoardRow]
    total: int
    page: int
    page_size: int


# ── Factor Template ──────────────────────────────────────────────────────────

class TemplateType(str, Enum):
    FINANCIAL = "financial"
    TECHNICAL = "technical"
    CUSTOM_OPERATOR = "custom_operator"
    CUSTOM_BASE = "custom_base"


class FactorTemplate(BaseModel):
    """Template for creating a new factor."""
    id: str
    type: TemplateType
    name: str
    description: str
    preset_expression: str
    preset_params: dict = Field(default_factory=dict)
    category: str


# ── Factor CRUD ──────────────────────────────────────────────────────────────

class FactorCreate(BaseModel):
    name: str
    expression: str
    stock_pool: StockPool = StockPool.HS300
    category: str | None = None
    description: str | None = None
    params: dict = Field(default_factory=dict)


class FactorUpdate(BaseModel):
    name: str | None = None
    expression: str | None = None
    stock_pool: StockPool | None = None
    category: str | None = None
    description: str | None = None
    params: dict | None = None


class FactorResponse(BaseModel):
    id: int
    name: str
    expression: str
    stock_pool: StockPool
    category: str | None
    description: str | None
    params: dict
    created_at: str
    updated_at: str


class ValidateRequest(BaseModel):
    expression: str
    stock_pool: StockPool = StockPool.HS300
    date: datetime.date | None = None


class ValidateResponse(BaseModel):
    valid: bool
    error: str | None = None
    preview_rows: list[dict] | None = None
