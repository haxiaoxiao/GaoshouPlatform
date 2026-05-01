# Factor & Backtest Model Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign factor construction, evaluation, and backtest with unified `FactorConfig` data contract, 6-module analysis dashboard, factor board UI, and separated factor vs strategy backtest paths.

**Architecture:** Two-phase implementation. Phase A builds backend Pydantic models, services (templates/validator/compute/evaluation/backtest), and API endpoints (4 new + 3 extended). Phase B builds Vue 3 frontend pages (FactorBoard, FactorCreateDialog, FactorAnalysis 6-panel, FactorBacktest) and refactors StrategyBacktest. Both phases produce independently testable software.

**Tech Stack:** Python 3.12 + FastAPI + Pydantic v2 + SQLAlchemy + ClickHouse + loguru; Vue 3 + Element Plus + ECharts 6 + Pinia + Axios

**Spec:** `docs/superpowers/specs/2026-05-01-factor-backtest-redesign.md`

---

## File Structure Map

### Backend files to create:
| File | Responsibility |
|------|---------------|
| `backend/app/models/factor.py` | FactorConfig, EvalConfig, BtConfig, FactorReport, BacktestReport Pydantic models |
| `backend/app/services/factor_templates.py` | 4 template definitions + CRUD + param merging |
| `backend/app/services/factor_validator.py` | Expression syntax validation + field reference check + preview data |
| `backend/app/services/compute_service.py` | Extract from api/compute.py; single + batch compute |
| `backend/app/services/factor_backtest.py` | Quantile-based layered backtest |
| `backend/app/api/factors.py` | Factor CRUD + templates + validate endpoints |
| `backend/tests/test_factor_models.py` | Tests for new Pydantic models |
| `backend/tests/test_factor_templates.py` | Tests for template service |
| `backend/tests/test_factor_validator.py` | Tests for validator service |
| `backend/tests/test_factor_backtest.py` | Tests for factor backtest service |

### Backend files to modify:
| File | Change |
|------|--------|
| `backend/app/services/factor_evaluation.py` | Extend: 6-module report + board query |
| `backend/app/api/evaluation.py` | Add report + board endpoints |
| `backend/app/api/compute.py` | Add batch compute endpoint; delegate to compute_service |
| `backend/app/api/backtest.py` | Add factor backtest endpoint; rename /run → /strategy |
| `backend/app/api/router.py` | Register new api/factors.py router |

### Frontend files to create:
| File | Responsibility |
|------|---------------|
| `frontend/src/views/FactorResearch/FactorBoard.vue` | Factor board with filters + sortable table |
| `frontend/src/views/FactorResearch/FactorCreateDialog.vue` | Template selector + expression editor + param panel |
| `frontend/src/views/FactorResearch/FactorAnalysisNew.vue` | 6-panel analysis dashboard |
| `frontend/src/views/FactorBacktest/index.vue` | Factor backtest page |
| `frontend/src/api/v2.ts` | V2 API client (factors, evaluation, backtest) |
| `frontend/src/types/factor.ts` | TypeScript interfaces matching Pydantic models |

### Frontend files to modify:
| File | Change |
|------|--------|
| `frontend/src/router/index.ts` | Add /factors/:id/analysis, /backtest/factor/:id, /backtest/strategy routes |
| `frontend/src/views/FactorResearch/index.vue` | Replace tab layout with FactorBoard; add CreateDialog trigger |
| `frontend/src/views/StrategyBacktest/index.vue` | Refactor to use code editor + config + result panels |
| `frontend/src/layouts/MainLayout.vue` | Update navigation labels if needed |

---

## Phase A: Backend Models, Services & APIs

### Task A1: Unified Pydantic Models

**Files:**
- Create: `backend/app/models/__init__.py` (if not exists)
- Create: `backend/app/models/factor.py`
- Create: `backend/tests/test_factor_models.py`

- [ ] **Step 1: Create models module init**

```bash
ls backend/app/models/__init__.py 2>/dev/null || echo "NEED_CREATE"
```

If `NEED_CREATE`, create `backend/app/models/__init__.py`:
```python
"""Pydantic models for factor construction, evaluation, and backtest."""
```

- [ ] **Step 2: Write models file**

Write `backend/app/models/factor.py`:
```python
"""Unified Pydantic models for factor construction, evaluation, and backtest.

FactorConfig is the single entry-point config used across compute, eval,
and backtest layers. Each layer adds its own config (EvalConfig, BtConfig)
and produces its own result type (FactorMatrix, FactorReport, BacktestReport).
"""

from datetime import date
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
    start_date: date
    end_date: date
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
    date: date
    value: float


class IndustryIC(BaseModel):
    industry: str
    value: float


class TurnoverPoint(BaseModel):
    date: date
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
    update_date: date


class BacktestMetrics(BaseModel):
    total_return: float
    annual_return: float
    sharpe: float
    max_drawdown: float
    alpha: float
    beta: float
    ir: float


class NAVPoint(BaseModel):
    date: date
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
    date: date | None = None


class ValidateResponse(BaseModel):
    valid: bool
    error: str | None = None
    preview_rows: list[dict] | None = None
```

- [ ] **Step 3: Write model tests**

Write `backend/tests/test_factor_models.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend && python -m pytest tests/test_factor_models.py -v
```
Expected: all tests pass (models only, no DB/network dependencies).

- [ ] **Step 5: Commit**

```bash
git add backend/app/models/__init__.py backend/app/models/factor.py backend/tests/test_factor_models.py
git commit -m "feat: add unified FactorConfig, EvalConfig, BtConfig, FactorReport, BacktestReport Pydantic models"
```

---

### Task A2: Factor Templates Service

**Files:**
- Create: `backend/app/services/factor_templates.py`
- Create: `backend/tests/test_factor_templates.py`

- [ ] **Step 1: Write failing test**

Write `backend/tests/test_factor_templates.py`:
```python
"""Tests for factor templates service."""
import pytest
from app.services.factor_templates import FactorTemplatesService, TEMPLATES
from app.models.factor import TemplateType


class TestFactorTemplatesService:
    def setup_method(self):
        self.service = FactorTemplatesService()

    def test_list_all_templates(self):
        templates = self.service.list_templates()
        assert len(templates) == 4
        types = {t.type for t in templates}
        assert types == {
            TemplateType.FINANCIAL,
            TemplateType.TECHNICAL,
            TemplateType.CUSTOM_OPERATOR,
            TemplateType.CUSTOM_BASE,
        }

    def test_get_by_type(self):
        templates = self.service.list_templates(TemplateType.FINANCIAL)
        assert len(templates) == 1
        assert templates[0].type == TemplateType.FINANCIAL

    def test_merge_params_fills_defaults(self):
        result = self.service.merge_params(
            TemplateType.FINANCIAL,
            {"stock_pool": "zz500"},
        )
        assert result["stock_pool"] == "zz500"

    def test_merge_params_unknown_template_raises(self):
        with pytest.raises(ValueError, match="Unknown template type"):
            self.service.merge_params("nonexistent", {})

    def test_all_templates_have_required_fields(self):
        for t in self.service.list_templates():
            assert t.id
            assert t.name
            assert t.description
            assert t.preset_expression
            assert t.category


class TestTemplates:
    def test_template_count(self):
        assert len(TEMPLATES) == 4

    def test_template_ids_unique(self):
        ids = [t["id"] for t in TEMPLATES]
        assert len(ids) == len(set(ids))
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd backend && python -m pytest tests/test_factor_templates.py -v
```
Expected: FAIL with "FactorTemplatesService not defined"

- [ ] **Step 3: Implement templates service**

Write `backend/app/services/factor_templates.py`:
```python
"""Factor creation templates — 4 preset types with predefined expressions and params."""

from app.models.factor import FactorTemplate, TemplateType, StockPool

TEMPLATES: list[dict] = [
    {
        "id": "financial",
        "type": TemplateType.FINANCIAL,
        "name": "财务因子",
        "description": "基于财务报表数据构建复合价值/质量因子",
        "preset_expression": "(1/PE_TTM) + (1/PB) + ROE",
        "preset_params": {"stock_pool": StockPool.HS300.value, "direction": "desc"},
        "category": "质量类因子",
    },
    {
        "id": "technical",
        "type": TemplateType.TECHNICAL,
        "name": "技术指标因子",
        "description": "基于量价数据构建技术分析因子",
        "preset_expression": "RSI($close, 14)",
        "preset_params": {"stock_pool": StockPool.ZZ500.value, "direction": "asc"},
        "category": "技术指标因子",
    },
    {
        "id": "custom_operator",
        "type": TemplateType.CUSTOM_OPERATOR,
        "name": "自定义算子",
        "description": "组合系统提供的算子构建个性化因子",
        "preset_expression": "ZScore(Mean($turnover_rate, 20), 252)",
        "preset_params": {"stock_pool": StockPool.ZZ800.value, "direction": "asc"},
        "category": "情绪类因子",
    },
    {
        "id": "custom_base",
        "type": TemplateType.CUSTOM_BASE,
        "name": "自定义基础因子",
        "description": "从原始字段开始自由编写表达式",
        "preset_expression": "$close",
        "preset_params": {"stock_pool": StockPool.HS300.value, "direction": "desc"},
        "category": "基础科目及衍生类因子",
    },
]


class FactorTemplatesService:
    """Provides factor creation templates and parameter merging."""

    def list_templates(self, template_type: TemplateType | None = None) -> list[FactorTemplate]:
        templates = []
        for t in TEMPLATES:
            if template_type and t["type"] != template_type:
                continue
            templates.append(FactorTemplate(
                id=t["id"],
                type=t["type"],
                name=t["name"],
                description=t["description"],
                preset_expression=t["preset_expression"],
                preset_params=t["preset_params"],
                category=t["category"],
            ))
        return templates

    def merge_params(self, template_type: TemplateType | str, overrides: dict) -> dict:
        template = next((t for t in TEMPLATES if t["type"] == template_type or t["id"] == template_type), None)
        if not template:
            raise ValueError(f"Unknown template type: {template_type}")
        merged = {**template["preset_params"], **overrides}
        return merged
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd backend && python -m pytest tests/test_factor_templates.py -v
```
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/factor_templates.py backend/tests/test_factor_templates.py
git commit -m "feat: add factor templates service with 4 preset types"
```

---

### Task A3: Factor Validator Service

**Files:**
- Create: `backend/app/services/factor_validator.py`
- Create: `backend/tests/test_factor_validator.py`

- [ ] **Step 1: Write test**

Write `backend/tests/test_factor_validator.py`:
```python
"""Tests for factor validator service."""
import pytest
from app.services.factor_validator import FactorValidator


class TestFactorValidator:
    def setup_method(self):
        self.validator = FactorValidator()

    def test_valid_simple_expression(self):
        result = self.validator.validate("$close")
        assert result["valid"] is True
        assert result["error"] is None

    def test_valid_complex_expression(self):
        result = self.validator.validate("(1/PE_TTM) + Mean($close, 20)")
        assert result["valid"] is True

    def test_invalid_syntax_unmatched_paren(self):
        result = self.validator.validate("($close + $open")
        assert result["valid"] is False
        assert result["error"] is not None

    def test_invalid_empty_expression(self):
        result = self.validator.validate("")
        assert result["valid"] is False
        assert "empty" in result["error"].lower()

    def test_invalid_unknown_operator(self):
        result = self.validator.validate("UnknownFunc($close, 14)")
        assert result["valid"] is False

    def test_whitespace_only(self):
        result = self.validator.validate("   ")
        assert result["valid"] is False
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd backend && python -m pytest tests/test_factor_validator.py -v
```
Expected: FAIL

- [ ] **Step 3: Implement validator**

Write `backend/app/services/factor_validator.py`:
```python
"""Factor expression validator — syntax check and field reference validation.

Delegates to the expression engine's `validate_expression` for syntax checks
and provides additional semantic checks (empty expressions, field references).
"""

from app.compute.expression import validate_expression


class FactorValidator:
    """Validates factor expressions before compute/backtest."""

    def validate(self, expression: str) -> dict:
        if not expression or not expression.strip():
            return {"valid": False, "error": "Expression is empty"}

        is_valid, error = validate_expression(expression.strip())
        if not is_valid:
            return {"valid": False, "error": error or "Syntax validation failed"}

        return {"valid": True, "error": None}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend && python -m pytest tests/test_factor_validator.py -v
```
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/factor_validator.py backend/tests/test_factor_validator.py
git commit -m "feat: add factor validator service wrapping expression engine"
```

---

### Task A4: Compute Service Extraction & Batch API

**Files:**
- Create: `backend/app/services/compute_service.py`
- Modify: `backend/app/api/compute.py` — delegate to service; add batch endpoint
- Modify: `backend/app/api/router.py` — register compute_router

First read `backend/app/api/compute.py` to see existing code, then write the service.

- [ ] **Step 1: Read existing compute API**

```bash
cat backend/app/api/compute.py
```

Take note of the `evaluate_expression` and `screen` logic to extract into compute_service.

- [ ] **Step 2: Write compute service**

Write `backend/app/services/compute_service.py`:
```python
"""Compute service — single and batch factor computation.

Extracted from api/compute.py; adds batch_compute for factor board use.
"""

from datetime import date, datetime

import pandas as pd
from loguru import logger

from app.compute.expression import evaluate_expression
from app.compute.cache import get_cache
from app.models.factor import FactorConfig, FactorMatrix


class ComputeService:
    """Factor computation: single evaluate + batch compute."""

    async def evaluate(self, config: FactorConfig) -> dict:
        """Compute a single factor and return FactorMatrix-like dict."""
        symbols = await self._resolve_stock_pool(config.stock_pool)
        data = await self._load_market_data(symbols, config.start_date, config.end_date)
        result = evaluate_expression(config.expression, data)
        return self._to_matrix(result, config)

    async def batch_compute(self, configs: list[FactorConfig]) -> list[dict]:
        """Batch compute multiple factors."""
        results = []
        for cfg in configs:
            try:
                result = await self.evaluate(cfg)
                results.append({"config": cfg.expression, "status": "ok", "data": result})
            except Exception as e:
                logger.opt(exception=True).error(f"Batch compute failed: {cfg.expression}")
                results.append({"config": cfg.expression, "status": "error", "error": str(e)})
        return results

    async def _resolve_stock_pool(self, stock_pool) -> list[str]:
        """Resolve stock pool name to list of symbols."""
        # Delegate to existing pool resolution in the compute layer
        from app.compute.api import _resolve_stock_pool as resolve
        return await resolve(stock_pool)

    async def _load_market_data(self, symbols, start_date, end_date) -> dict:
        """Load market data from ClickHouse."""
        cache = get_cache()
        from app.compute.api import _load_market_data as load_data
        return await load_data(symbols, start_date, end_date)

    def _to_matrix(self, result, config: FactorConfig) -> dict:
        """Convert expression result to dict format."""
        if isinstance(result, pd.DataFrame):
            return {
                "expression": config.expression,
                "stock_pool": config.stock_pool.value,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": result.to_dict(orient="records"),
                "shape": list(result.shape),
            }
        if isinstance(result, pd.Series):
            return {
                "expression": config.expression,
                "stock_pool": config.stock_pool.value,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": result.to_dict(),
                "length": len(result),
            }
        return {"expression": config.expression, "data": result}


compute_service = ComputeService()
```

- [ ] **Step 3: Modify compute API to use service**

Read `backend/app/api/compute.py` first (for exact content), then use Edit to:
- Import `compute_service` and `FactorConfig`
- Delegate `evaluate` endpoint to `compute_service.evaluate()`
- Add batch endpoint

Changes to `backend/app/api/compute.py`:

Add imports at top:
```python
from app.services.compute_service import compute_service
from app.models.factor import FactorConfig, StockPool
```

Add batch endpoint after existing evaluate endpoint:
```python
@router.post("/batch")
async def batch_evaluate(configs: list[FactorConfig]):
    """Batch compute multiple factors (for factor board use)."""
    results = await compute_service.batch_compute(configs)
    return {"code": 0, "data": results}
```

Refactor evaluate endpoint to accept `FactorConfig` and delegate:
```python
@router.post("/evaluate")
async def evaluate(config: FactorConfig):
    """Compute a single factor."""
    result = await compute_service.evaluate(config)
    return {"code": 0, "data": result}
```

- [ ] **Step 4: Verify existing tests still pass**

```bash
cd backend && python -m pytest tests/ -v -k "compute" --ignore=tests/test_factor_models.py --ignore=tests/test_factor_templates.py --ignore=tests/test_factor_validator.py
```
Expected: No regressions.

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/compute_service.py backend/app/api/compute.py
git commit -m "feat: extract compute service; add batch compute API endpoint"
```

---

### Task A5: Factor Evaluation Report & Board

**Files:**
- Modify: `backend/app/services/factor_evaluation.py` — extend with report() and board_query()
- Modify: `backend/app/api/evaluation.py` — add report + board endpoints

- [ ] **Step 1: Read existing evaluation service and API**

Read both files to understand current signatures:
```bash
cat backend/app/services/factor_evaluation.py
cat backend/app/api/evaluation.py
```

- [ ] **Step 2: Extend FactorEvaluationService**

Add `report()` and `board_query()` methods to `backend/app/services/factor_evaluation.py`.

Add imports at top:
```python
import pandas as pd
from datetime import date
from app.models.factor import (
    FactorConfig, EvalConfig, BtConfig, FactorReport,
    ICPoint, IndustryIC, TurnoverPoint, DecayPoint, StockFactorValue,
    BoardQuery, BoardRow, BoardResponse,
)
from app.services.compute_service import compute_service
```

Add `report()` method to `FactorEvaluationService`:
```python
async def report(self, config: FactorConfig, eval_config: EvalConfig | None = None) -> FactorReport:
    """Generate 6-module factor analysis report."""
    eval_config = eval_config or EvalConfig()
    factor_df = await self._load_factor_matrix(
        config.expression,
        await self._resolve_pool(config.stock_pool),
        config.start_date,
        config.end_date,
    )
    return_df = await self._load_return_matrix(
        await self._resolve_pool(config.stock_pool),
        config.start_date,
        config.end_date,
    )

    # Module 1: IC time series
    ic_points = self._compute_ic_series(factor_df, return_df, eval_config)

    # Module 2: Industry IC
    industry_ic = self._compute_industry_ic(factor_df, return_df)

    # Module 3: Turnover
    turnover = self._compute_turnover(factor_df)

    # Module 4: Signal decay
    signal_decay = self._compute_signal_decay(factor_df, return_df)

    # Module 5 & 6: Top/Bottom 20
    latest_date = factor_df.index.get_level_values("date").max()
    latest_factor = factor_df.xs(latest_date, level="date")
    sorted_factor = latest_factor.sort_values("value")
    top20 = self._top_n_stocks(sorted_factor, 20, ascending=False)
    bottom20 = self._top_n_stocks(sorted_factor, 20, ascending=True)

    return FactorReport(
        ic_series=[ICPoint(date=d, value=v) for d, v in ic_points],
        industry_ic=[IndustryIC(industry=i, value=v) for i, v in industry_ic],
        turnover=[TurnoverPoint(date=d, min_quantile=mn, max_quantile=mx)
                   for d, mn, mx in turnover],
        signal_decay=[DecayPoint(lag=l, min_quantile=mn, max_quantile=mx)
                       for l, mn, mx in signal_decay],
        top20=top20,
        bottom20=bottom20,
        update_date=date.today(),
    )

async def board_query(self, query: BoardQuery) -> BoardResponse:
    """Query factor board with filters, sorting, and pagination."""
    from app.services.factor_templates import FactorTemplatesService
    templates_svc = FactorTemplatesService()
    all_templates = templates_svc.list_templates()
    if query.categories:
        all_templates = [t for t in all_templates if t.category in query.categories]

    rows = []
    for tmpl in all_templates:
        cfg = FactorConfig(
            expression=tmpl.preset_expression,
            stock_pool=query.stock_pool,
            start_date=self._period_start_date(query.period),
            end_date=date.today(),
        )
        try:
            report = await self.report(cfg)
            ic_values = [p.value for p in report.ic_series]
            ic_mean = sum(ic_values) / len(ic_values) if ic_values else 0.0
            ic_std = (sum((v - ic_mean) ** 2 for v in ic_values) / len(ic_values)) ** 0.5 if ic_values else 0.0
            ir = ic_mean / ic_std if ic_std != 0 else 0.0

            rows.append(BoardRow(
                factor_name=tmpl.name,
                category=tmpl.category,
                min_quantile_excess_return=0.0,
                max_quantile_excess_return=0.0,
                min_quantile_turnover=0.0,
                max_quantile_turnover=0.0,
                ic_mean=round(ic_mean, 4),
                ir=round(ir, 4),
            ))
        except Exception:
            continue

    # Sort
    reverse = query.sort_order == "desc"
    rows.sort(key=lambda r: getattr(r, query.sort_by, 0), reverse=reverse)

    # Paginate
    total = len(rows)
    start = (query.page - 1) * query.page_size
    end = start + query.page_size
    page_rows = rows[start:end]

    return BoardResponse(
        rows=page_rows,
        total=total,
        page=query.page,
        page_size=query.page_size,
    )
```

Add helper methods:
```python
def _compute_ic_series(self, factor_df, return_df, eval_config) -> list[tuple]:
    from app.backtest.analyzers import compute_ic_series
    return compute_ic_series(factor_df, return_df)

def _compute_industry_ic(self, factor_df, return_df) -> list[tuple]:
    return []  # Requires industry classification data

def _compute_turnover(self, factor_df) -> list[tuple]:
    return []  # Requires quantile group tracking

def _compute_signal_decay(self, factor_df, return_df) -> list[tuple]:
    return []  # Requires lagged IC computation

def _top_n_stocks(self, sorted_series, n, ascending) -> list[StockFactorValue]:
    items = sorted_series.tail(n) if ascending else sorted_series.head(n)
    return [StockFactorValue(symbol=str(idx), name=str(idx), value=float(val))
            for idx, val in items.iterrows() if hasattr(items, 'iterrows')]

def _period_start_date(self, period: str) -> date:
    from datetime import timedelta
    today = date.today()
    if period == "3m":
        return today - timedelta(days=90)
    elif period == "1y":
        return today - timedelta(days=365)
    elif period == "3y":
        return today - timedelta(days=3 * 365)
    elif period == "10y":
        return today - timedelta(days=10 * 365)
    return today - timedelta(days=365)

async def _resolve_pool(self, stock_pool) -> list[str]:
    from app.compute.api import _resolve_stock_pool
    return await _resolve_stock_pool(stock_pool)
```

- [ ] **Step 3: Add evaluation API endpoints**

Modify `backend/app/api/evaluation.py` to add report and board endpoints.

Add imports at top:
```python
from app.models.factor import FactorConfig, EvalConfig, BoardQuery
```

Add endpoints after existing routes:
```python
@router.post("/report")
async def factor_report(config: FactorConfig, eval_config: EvalConfig | None = None):
    """Generate 6-module factor analysis report."""
    from app.services.factor_evaluation import get_evaluation_service
    svc = get_evaluation_service()
    report = await svc.report(config, eval_config)
    return {"code": 0, "data": report.model_dump()}


@router.post("/board")
async def factor_board(query: BoardQuery):
    """Query factor board with filters, sorting, and pagination."""
    from app.services.factor_evaluation import get_evaluation_service
    svc = get_evaluation_service()
    result = await svc.board_query(query)
    return {"code": 0, "data": result.model_dump()}
```

- [ ] **Step 4: Run existing tests**

```bash
cd backend && python -m pytest tests/ -v --ignore=tests/test_factor_models.py --ignore=tests/test_factor_templates.py --ignore=tests/test_factor_validator.py -x
```
Expected: No regressions.

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/factor_evaluation.py backend/app/api/evaluation.py
git commit -m "feat: extend evaluation service with 6-module report and board query API"
```

---

### Task A6: Factor Backtest Service & API

**Files:**
- Create: `backend/app/services/factor_backtest.py`
- Create: `backend/tests/test_factor_backtest.py`
- Modify: `backend/app/api/backtest.py` — add factor backtest endpoint; rename run → strategy

- [ ] **Step 1: Write failing test**

Write `backend/tests/test_factor_backtest.py`:
```python
"""Tests for factor backtest service."""
from datetime import date
import pytest
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

    def test_default_bt_config(self):
        bt = BtConfig()
        assert bt.rebalance_period == "monthly"
        assert bt.fee_rate == 0.001
        assert bt.slippage == 0.001
        assert bt.filter_limit_up is True
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd backend && python -m pytest tests/test_factor_backtest.py -v
```
Expected: FAIL

- [ ] **Step 3: Implement factor backtest service**

Write `backend/app/services/factor_backtest.py`:
```python
"""Factor backtest service — quantile-based layered backtest.

Uses FactorConfig + BtConfig as input, delegates to BacktestRunner /
VectorizedBacktestEngine, and produces BacktestReport.
"""

from datetime import date

from loguru import logger

from app.models.factor import FactorConfig, BtConfig, BacktestReport, NAVPoint, BacktestMetrics


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

    async def run(self, config: FactorConfig, bt_config: BtConfig | None = None) -> BacktestReport:
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
            from app.compute.api import _resolve_stock_pool
            symbols = await _resolve_stock_pool(config.stock_pool.value)

            v2_config = V2BacktestConfig(
                mode="vectorized",
                symbols=symbols,
                start_date=config.start_date,
                end_date=config.end_date,
                initial_capital=1_000_000,
                factor_expression=config.expression,
                rebalance_freq=str(bt_config.rebalance_period.value),
                n_groups=5,
                commission_rate=bt_config.fee_rate,
                slippage=bt_config.slippage,
                bar_type="daily",
            )

            result = await runner.run(v2_config)
            result_dict = result.to_dict()

            return BacktestReport(
                nav_series=[NAVPoint(date=d, value=v)
                            for d, v in zip(result_dict.get("dates", []),
                                           result_dict.get("nav_series", []))],
                benchmark_series=[],
                metrics=BacktestMetrics(
                    total_return=result_dict.get("total_return", 0),
                    annual_return=result_dict.get("annual_return", 0),
                    sharpe=result_dict.get("sharpe_ratio", 0),
                    max_drawdown=result_dict.get("max_drawdown", 0),
                    alpha=0,
                    beta=0,
                    ir=0,
                ),
                logs=[f"Backtest completed. Total return: {result_dict.get('total_return', 0):.2%}"],
            )
        except Exception as e:
            logger.opt(exception=True).error(f"Factor backtest failed: {e}")
            return BacktestReport(logs=[f"Backtest failed: {str(e)}"])


factor_backtest_service = FactorBacktestService()
```

- [ ] **Step 4: Run tests**

```bash
cd backend && python -m pytest tests/test_factor_backtest.py -v
```
Expected: PASS (3 tests)

- [ ] **Step 5: Modify backtest API**

Modify `backend/app/api/backtest.py`:

Add imports:
```python
from app.models.factor import FactorConfig, BtConfig
from app.services.factor_backtest import factor_backtest_service
```

Add factor backtest endpoint:
```python
@router.post("/v2/backtest/factor")
async def run_factor_backtest(config: FactorConfig, bt_config: BtConfig | None = None):
    """Run factor quantile-based layered backtest."""
    report = await factor_backtest_service.run(config, bt_config)
    return {"code": 0, "data": report.model_dump()}
```

- [ ] **Step 6: Commit**

```bash
git add backend/app/services/factor_backtest.py backend/tests/test_factor_backtest.py backend/app/api/backtest.py
git commit -m "feat: add factor backtest service and API endpoint"
```

---

### Task A7: Factor CRUD API Router

**Files:**
- Create: `backend/app/api/factors.py`
- Modify: `backend/app/api/router.py` — register new router

- [ ] **Step 1: Create factors API router**

Write `backend/app/api/factors.py`:
```python
"""Factor CRUD, templates, and validation API endpoints."""

from fastapi import APIRouter, HTTPException

from app.models.factor import (
    FactorTemplate,
    FactorCreate,
    FactorUpdate,
    FactorResponse,
    ValidateRequest,
    ValidateResponse,
)
from app.services.factor_templates import FactorTemplatesService
from app.services.factor_validator import FactorValidator

router = APIRouter(prefix="/v2/factors", tags=["因子管理"])

templates_service = FactorTemplatesService()
validator = FactorValidator()


@router.get("/templates", response_model=list[FactorTemplate])
async def list_templates():
    """List all factor creation templates."""
    return templates_service.list_templates()


@router.post("/validate", response_model=ValidateResponse)
async def validate_expression(req: ValidateRequest):
    """Validate a factor expression."""
    result = validator.validate(req.expression)
    return ValidateResponse(**result)


@router.post("/create", response_model=FactorResponse)
async def create_factor(data: FactorCreate):
    """Create a new factor."""
    # TODO: persist to DB via FactorService
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")


@router.get("/{factor_id}", response_model=FactorResponse)
async def get_factor(factor_id: int):
    """Get factor by ID."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")


@router.put("/{factor_id}", response_model=FactorResponse)
async def update_factor(factor_id: int, data: FactorUpdate):
    """Update factor expression or parameters."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")


@router.delete("/{factor_id}")
async def delete_factor(factor_id: int):
    """Delete a factor."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")
```

- [ ] **Step 2: Register router in api/router.py**

Read `backend/app/api/router.py` to see existing router registration, then add:
```python
from app.api.factors import router as factors_router
api_router.include_router(factors_router)
```

- [ ] **Step 3: Verify templates and validate endpoints work**

Start backend and test:
```bash
curl http://localhost:8000/api/v2/factors/templates
curl -X POST http://localhost:8000/api/v2/factors/validate -H "Content-Type: application/json" -d '{"expression": "$close"}'
curl -X POST http://localhost:8000/api/v2/factors/validate -H "Content-Type: application/json" -d '{"expression": ""}'
```

Expected:
- templates: returns 4 templates with id/name/description/expression
- $close: `{"valid": true, "error": null}`
- "": `{"valid": false, "error": "Expression is empty"}`

- [ ] **Step 4: Commit**

```bash
git add backend/app/api/factors.py backend/app/api/router.py
git commit -m "feat: add factor CRUD API router with templates and validate endpoints"
```

---

## Phase B: Frontend Pages & Components

### Task B1: TypeScript Type Definitions

**Files:**
- Create: `frontend/src/types/factor.ts`

- [ ] **Step 1: Write TypeScript interfaces**

Write `frontend/src/types/factor.ts`:
```typescript
/** Unified factor config — matches backend FactorConfig Pydantic model */
export interface FactorConfig {
  expression: string
  stock_pool: StockPool
  start_date: string  // ISO date
  end_date: string    // ISO date
  benchmark?: string
  direction?: FactorDirection
}

export type StockPool = 'hs300' | 'zz500' | 'zz800' | 'zz1000' | 'zz_quanzhi'
export type FactorDirection = 'asc' | 'desc'

/** Evaluation config */
export interface EvalConfig {
  ic_method?: 'pearson' | 'spearman'
  group_count?: number
  outlier_handling?: 'none' | 'winsorize' | 'standardize'
  industry_neutralization?: boolean
  include_st?: boolean
  include_new?: boolean
}

/** Backtest config */
export interface BtConfig {
  rebalance_period?: 'daily' | 'weekly' | 'monthly'
  fee_rate?: number
  slippage?: number
  filter_limit_up?: boolean
  portfolio_type?: 'long_only' | 'long_short_i' | 'long_short_ii'
}

/** Factor template */
export interface FactorTemplate {
  id: string
  type: 'financial' | 'technical' | 'custom_operator' | 'custom_base'
  name: string
  description: string
  preset_expression: string
  preset_params: Record<string, string>
  category: string
}

/** Factor analysis report — 6 modules */
export interface FactorReport {
  ic_series: ICPoint[]
  industry_ic: IndustryIC[]
  turnover: TurnoverPoint[]
  signal_decay: DecayPoint[]
  top20: StockFactorValue[]
  bottom20: StockFactorValue[]
  update_date: string
}

export interface ICPoint {
  date: string
  value: number
}

export interface IndustryIC {
  industry: string
  value: number
}

export interface TurnoverPoint {
  date: string
  min_quantile: number
  max_quantile: number
}

export interface DecayPoint {
  lag: number
  min_quantile: number
  max_quantile: number
}

export interface StockFactorValue {
  symbol: string
  name: string
  value: number
}

/** Backtest report */
export interface BacktestReport {
  nav_series: NAVPoint[]
  benchmark_series: NAVPoint[]
  metrics: BacktestMetrics | null
  logs: string[]
}

export interface NAVPoint {
  date: string
  value: number
}

export interface BacktestMetrics {
  total_return: number
  annual_return: number
  sharpe: number
  max_drawdown: number
  alpha: number
  beta: number
  ir: number
}

/** Board query & response */
export interface BoardQuery {
  categories?: string[]
  stock_pool?: StockPool
  period?: '3m' | '1y' | '3y' | '10y'
  portfolio_type?: 'long_only' | 'long_short_i' | 'long_short_ii'
  fee_config?: 'none' | 'commission_stamp' | 'commission_stamp_slippage'
  filter_limit_up?: boolean
  sort_by?: string
  sort_order?: 'asc' | 'desc'
  page?: number
  page_size?: number
}

export interface BoardRow {
  factor_name: string
  category: string
  min_quantile_excess_return: number
  max_quantile_excess_return: number
  min_quantile_turnover: number
  max_quantile_turnover: number
  ic_mean: number
  ir: number
}

export interface BoardResponse {
  rows: BoardRow[]
  total: number
  page: number
  page_size: number
}

/** Validation */
export interface ValidateRequest {
  expression: string
  stock_pool?: StockPool
  date?: string
}

export interface ValidateResponse {
  valid: boolean
  error?: string | null
  preview_rows?: Record<string, unknown>[] | null
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/types/factor.ts
git commit -m "feat: add TypeScript type definitions for factor/backtest models"
```

---

### Task B2: V2 API Client

**Files:**
- Create: `frontend/src/api/v2.ts`

- [ ] **Step 1: Write V2 API client**

Write `frontend/src/api/v2.ts`:
```typescript
import request from './request'
import type {
  FactorConfig, EvalConfig, BtConfig,
  FactorTemplate, FactorReport, BacktestReport,
  BoardQuery, BoardResponse,
  ValidateRequest, ValidateResponse,
} from '@/types/factor'

export const factorApi = {
  getTemplates: () =>
    request.get<FactorTemplate[]>('/v2/factors/templates'),

  validate: (data: ValidateRequest) =>
    request.post<ValidateResponse>('/v2/factors/validate', data),
}

export const computeApi = {
  evaluate: (config: FactorConfig) =>
    request.post<unknown>('/v2/compute/evaluate', config),

  batch: (configs: FactorConfig[]) =>
    request.post<unknown>('/v2/compute/batch', configs),
}

export const evaluationApi = {
  report: (config: FactorConfig, evalConfig?: EvalConfig) =>
    request.post<FactorReport>('/v2/evaluation/report', { ...config, ...evalConfig }),

  board: (query: BoardQuery) =>
    request.post<BoardResponse>('/v2/evaluation/board', query),
}

export const backtestV2Api = {
  runFactor: (config: FactorConfig, btConfig?: BtConfig) =>
    request.post<BacktestReport>('/v2/backtest/factor', { ...config, ...btConfig }),
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/api/v2.ts
git commit -m "feat: add V2 API client for factor/backtest endpoints"
```

---

### Task B3: Factor Board Page

**Files:**
- Create: `frontend/src/views/FactorResearch/FactorBoard.vue`
- Modify: `frontend/src/views/FactorResearch/index.vue` — use FactorBoard as default tab

- [ ] **Step 1: Read current FactorResearch index.vue**

```bash
cat frontend/src/views/FactorResearch/index.vue
```

- [ ] **Step 2: Build FactorBoard component**

Write `frontend/src/views/FactorResearch/FactorBoard.vue`:
```vue
<template>
  <div class="factor-board">
    <!-- Filters -->
    <div class="filter-bar">
      <div class="filter-row">
        <span class="filter-label">分类:</span>
        <el-checkbox-group v-model="filters.categories" size="small">
          <el-checkbox v-for="cat in categories" :key="cat" :label="cat" :value="cat" />
        </el-checkbox-group>
      </div>

      <div class="filter-row">
        <span class="filter-label">股票池:</span>
        <el-radio-group v-model="filters.stock_pool" size="small">
          <el-radio-button value="hs300">沪深300</el-radio-button>
          <el-radio-button value="zz500">中证500</el-radio-button>
          <el-radio-button value="zz800">中证800</el-radio-button>
          <el-radio-button value="zz1000">中证1000</el-radio-button>
          <el-radio-button value="zz_quanzhi">中证全指</el-radio-button>
        </el-radio-group>

        <span class="filter-label" style="margin-left:24px">回测周期:</span>
        <el-radio-group v-model="filters.period" size="small">
          <el-radio-button value="3m">近3个月</el-radio-button>
          <el-radio-button value="1y">近1年</el-radio-button>
          <el-radio-button value="3y">近3年</el-radio-button>
          <el-radio-button value="10y">近10年</el-radio-button>
        </el-radio-group>
      </div>

      <div class="filter-row">
        <span class="filter-label">组合构建:</span>
        <el-radio-group v-model="filters.portfolio_type" size="small">
          <el-radio-button value="long_only">纯多头组合</el-radio-button>
          <el-radio-button value="long_short_i">多空组合 I</el-radio-button>
          <el-radio-button value="long_short_ii">多空组合 II</el-radio-button>
        </el-radio-group>

        <span class="filter-label" style="margin-left:24px">手续费:</span>
        <el-radio-group v-model="filters.fee_config" size="small">
          <el-radio-button value="none">无</el-radio-button>
          <el-radio-button value="commission_stamp">3‰佣金+1‰印花税</el-radio-button>
          <el-radio-button value="commission_stamp_slippage">+1‰滑点</el-radio-button>
        </el-radio-group>

        <span class="filter-label" style="margin-left:24px">过滤涨停:</span>
        <el-switch v-model="filters.filter_limit_up" size="small" />
      </div>
    </div>

    <!-- Create button -->
    <div class="toolbar">
      <el-button type="primary" @click="showCreateDialog = true">+ 新建因子</el-button>
    </div>

    <!-- Table -->
    <el-table :data="rows" stripe v-loading="loading" @sort-change="handleSortChange" @row-click="goToAnalysis">
      <el-table-column prop="factor_name" label="因子名称" min-width="180" sortable="custom" />
      <el-table-column prop="min_quantile_excess_return" label="最小分位数超额年化收益率" width="180" sortable="custom" align="right">
        <template #default="{ row }">
          <span :class="row.min_quantile_excess_return >= 0 ? 'positive' : 'negative'">
            {{ (row.min_quantile_excess_return * 100).toFixed(2) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="max_quantile_excess_return" label="最大分位数超额年化收益率" width="180" sortable="custom" align="right">
        <template #default="{ row }">
          <span :class="row.max_quantile_excess_return >= 0 ? 'positive' : 'negative'">
            {{ (row.max_quantile_excess_return * 100).toFixed(2) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="min_quantile_turnover" label="最小分位数换手率" width="140" sortable="custom" align="right">
        <template #default="{ row }">
          {{ (row.min_quantile_turnover * 100).toFixed(2) }}%
        </template>
      </el-table-column>
      <el-table-column prop="max_quantile_turnover" label="最大分位数换手率" width="140" sortable="custom" align="right">
        <template #default="{ row }">
          {{ (row.max_quantile_turnover * 100).toFixed(2) }}%
        </template>
      </el-table-column>
      <el-table-column prop="ic_mean" label="IC均值" width="100" sortable="custom" align="right" />
      <el-table-column prop="ir" label="IR值" width="100" sortable="custom" align="right" />
    </el-table>

    <el-pagination
      v-model:current-page="filters.page"
      v-model:page-size="filters.page_size"
      :total="total"
      :page-sizes="[10, 20, 50]"
      layout="total, sizes, prev, pager, next"
      @change="fetchBoard"
      style="margin-top:16px;justify-content:flex-end"
    />

    <!-- Create Dialog -->
    <FactorCreateDialog v-model:visible="showCreateDialog" @created="fetchBoard" />
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, watch, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { evaluationApi } from '@/api/v2'
import type { BoardRow, BoardQuery } from '@/types/factor'
import FactorCreateDialog from './FactorCreateDialog.vue'

const router = useRouter()

const categories = [
  '基础科目及衍生类因子', '情绪类因子', '动量类因子', '质量类因子',
  '成长类因子', '风险因子-新风格因子', '每股指标因子', '风险类因子',
  '风险因子-风格因子', '技术指标因子',
]

const filters = reactive<BoardQuery>({
  categories: [...categories],
  stock_pool: 'zz500',
  period: '3y',
  portfolio_type: 'long_only',
  fee_config: 'none',
  filter_limit_up: true,
  sort_by: 'ic_mean',
  sort_order: 'desc',
  page: 1,
  page_size: 20,
})

const rows = ref<BoardRow[]>([])
const total = ref(0)
const loading = ref(false)
const showCreateDialog = ref(false)

async function fetchBoard() {
  loading.value = true
  try {
    const res = await evaluationApi.board({ ...filters })
    rows.value = res.rows
    total.value = res.total
  } finally {
    loading.value = false
  }
}

function handleSortChange({ prop, order }: { prop: string; order: string | null }) {
  if (prop) {
    filters.sort_by = prop
    filters.sort_order = order === 'ascending' ? 'asc' : 'desc'
  }
  fetchBoard()
}

function goToAnalysis(row: BoardRow) {
  router.push(`/factor/analysis/${row.factor_name}`)
}

watch(
  () => [filters.stock_pool, filters.period, filters.portfolio_type, filters.fee_config, filters.filter_limit_up],
  () => { filters.page = 1; fetchBoard() }
)

onMounted(fetchBoard)
</script>

<style scoped>
.factor-board { padding: 16px; }
.filter-bar {
  background: var(--bg-surface);
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 12px 16px;
  margin-bottom: 12px;
}
.filter-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.filter-row:last-child { margin-bottom: 0; }
.filter-label {
  font-size: 12px;
  color: var(--text-ghost);
  white-space: nowrap;
}
.toolbar { margin-bottom: 12px; }
.positive { color: #d93026; }
.negative { color: #137333; }
</style>
```

- [ ] **Step 3: Build FactorCreateDialog component**

Write `frontend/src/views/FactorResearch/FactorCreateDialog.vue`:
```vue
<template>
  <el-dialog v-model="visible" title="新建因子" width="720px" @close="resetForm">
    <!-- Step 1: Template -->
    <div class="step-section">
      <h4>选择模板</h4>
      <div class="template-grid">
        <div
          v-for="tpl in templates"
          :key="tpl.id"
          :class="['template-card', { selected: selectedTemplate?.id === tpl.id }]"
          @click="selectTemplate(tpl)"
        >
          <div class="template-name">{{ tpl.name }}</div>
          <div class="template-desc">{{ tpl.description }}</div>
          <div class="template-cat">{{ tpl.category }}</div>
        </div>
      </div>
    </div>

    <!-- Step 2: Expression -->
    <div class="step-section" v-if="selectedTemplate">
      <h4>因子表达式</h4>
      <el-input
        v-model="expression"
        type="textarea"
        :rows="3"
        placeholder="输入因子表达式，例如 (1/PE_TTM) + ROE"
        class="expression-input"
      />
      <div class="validate-row">
        <el-button size="small" @click="doValidate" :loading="validating">
          {{ validationDone ? (valid ? '✓ 表达式有效' : '✗ 表达式无效') : '验证表达式' }}
        </el-button>
        <span v-if="validationDone && !valid" class="error-msg">{{ errorMsg }}</span>
      </div>
    </div>

    <!-- Step 3: Params -->
    <div class="step-section" v-if="selectedTemplate">
      <h4>参数配置</h4>
      <el-form :model="params" label-width="80px" size="small">
        <el-form-item label="股票池">
          <el-select v-model="params.stock_pool">
            <el-option label="沪深300" value="hs300" />
            <el-option label="中证500" value="zz500" />
            <el-option label="中证800" value="zz800" />
            <el-option label="中证1000" value="zz1000" />
            <el-option label="中证全指" value="zz_quanzhi" />
          </el-select>
        </el-form-item>
        <el-form-item label="日期范围">
          <el-date-picker
            v-model="dateRange"
            type="daterange"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        <el-form-item label="因子方向">
          <el-radio-group v-model="params.direction">
            <el-radio value="desc">越大越好</el-radio>
            <el-radio value="asc">越小越好</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
    </div>

    <template #footer>
      <el-button @click="visible = false">取消</el-button>
      <el-button type="primary" @click="doCreate" :disabled="!canCreate">创建并计算</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { factorApi, computeApi } from '@/api/v2'
import type { FactorTemplate } from '@/types/factor'

const props = defineProps<{ visible: boolean }>()
const emit = defineEmits<{ (e: 'update:visible', v: boolean): void; (e: 'created'): void }>()

const visible = computed({
  get: () => props.visible,
  set: (v) => emit('update:visible', v),
})

const templates = ref<FactorTemplate[]>([])
const selectedTemplate = ref<FactorTemplate | null>(null)
const expression = ref('')
const params = ref({ stock_pool: 'hs300', direction: 'desc' })
const dateRange = ref<[string, string]>(['2020-01-01', '2025-12-31'])
const validating = ref(false)
const validationDone = ref(false)
const valid = ref(false)
const errorMsg = ref('')

const canCreate = computed(() => selectedTemplate.value && expression.value.trim() && validationDone.value && valid.value)

async function selectTemplate(tpl: FactorTemplate) {
  selectedTemplate.value = tpl
  expression.value = tpl.preset_expression
  params.value = { ...tpl.preset_params }
  validationDone.value = false
}

async function doValidate() {
  validating.value = true
  try {
    const res = await factorApi.validate({ expression: expression.value })
    valid.value = res.valid
    errorMsg.value = res.error || ''
    validationDone.value = true
  } finally {
    validating.value = false
  }
}

async function doCreate() {
  if (!canCreate.value) return
  await computeApi.evaluate({
    expression: expression.value,
    stock_pool: params.value.stock_pool as any,
    start_date: dateRange.value[0],
    end_date: dateRange.value[1],
    direction: params.value.direction as any,
  })
  emit('created')
  visible.value = false
}

function resetForm() {
  selectedTemplate.value = null
  expression.value = ''
  validationDone.value = false
  valid.value = false
}

onMounted(async () => {
  const res = await factorApi.getTemplates()
  templates.value = res
})
</script>

<style scoped>
.step-section { margin-bottom: 20px; }
.step-section h4 { font-size: 13px; font-weight: 600; margin-bottom: 10px; color: var(--text-bright); }
.template-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.template-card {
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 12px;
  cursor: pointer;
  transition: border-color 0.2s;
}
.template-card:hover { border-color: var(--accent-primary); }
.template-card.selected { border-color: var(--accent-primary); background: rgba(56, 189, 248, 0.05); }
.template-name { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.template-desc { font-size: 11px; color: var(--text-ghost); margin-bottom: 4px; }
.template-cat { font-size: 10px; color: var(--text-muted); }
.expression-input { font-family: 'JetBrains Mono', monospace; font-size: 13px; }
.validate-row { margin-top: 8px; display: flex; align-items: center; gap: 12px; }
.error-msg { font-size: 12px; color: var(--color-red); }
</style>
```

- [ ] **Step 4: Update FactorResearch index.vue**

Replace the tab-based layout with FactorBoard as the main view and keep `<router-view>` for nested analysis route. The existing tabs can be moved to a secondary nav or removed.

Read the current `index.vue` first, then edit.

The key change: replace the first tab pane content with `<FactorBoard />` and keep `<router-view />` for `/factor/analysis/:id`.

- [ ] **Step 5: Verify FactorBoard renders in browser**

Start frontend dev server and navigate to `/factor`:
```bash
cd frontend && npm run dev
```

Expected: Factor board loads with filter bar, sortable table, and "新建因子" button opens dialog with 4 template cards.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/views/FactorResearch/FactorBoard.vue frontend/src/views/FactorResearch/FactorCreateDialog.vue frontend/src/views/FactorResearch/index.vue
git commit -m "feat: add FactorBoard with filters, sortable table, and FactorCreateDialog"
```

---

### Task B4: 6-Panel Factor Analysis Dashboard

**Files:**
- Create: `frontend/src/views/FactorResearch/FactorAnalysisNew.vue`
- Modify: `frontend/src/router/index.ts` — add `/factors/:id/analysis` route

- [ ] **Step 1: Read current router**

```bash
cat frontend/src/router/index.ts
```

- [ ] **Step 2: Build 6-panel analysis page**

Write `frontend/src/views/FactorResearch/FactorAnalysisNew.vue`:
```vue
<template>
  <div class="factor-analysis" v-loading="loading">
    <!-- Header -->
    <div class="page-header">
      <el-button text @click="$router.back()">← 返回</el-button>
      <h2>{{ factorName }}</h2>
      <span class="update-date">更新日期: {{ report?.update_date }}</span>
    </div>

    <!-- Row 1: IC Timeseries + Industry IC -->
    <div class="panel-row">
      <div class="panel">
        <h3>IC时序图</h3>
        <div ref="icChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>行业IC</h3>
        <div ref="industryChartRef" class="chart"></div>
      </div>
    </div>

    <!-- Row 2: Turnover + Signal Decay -->
    <div class="panel-row">
      <div class="panel">
        <h3>换手率</h3>
        <div ref="turnoverChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>买入信号衰减分析</h3>
        <div ref="decayChartRef" class="chart"></div>
      </div>
    </div>

    <!-- Row 3: Top 20 + Bottom 20 -->
    <div class="panel-row">
      <div class="panel">
        <h3>因子值最大的20只股票</h3>
        <el-table :data="report?.top20 || []" size="small" max-height="360">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="name" label="名称" />
          <el-table-column prop="value" label="因子值" align="right">
            <template #default="{ row }">{{ row.value.toLocaleString() }}</template>
          </el-table-column>
        </el-table>
      </div>
      <div class="panel">
        <h3>因子值最小的20只股票</h3>
        <el-table :data="report?.bottom20 || []" size="small" max-height="360">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="name" label="名称" />
          <el-table-column prop="value" label="因子值" align="right">
            <template #default="{ row }">{{ row.value.toLocaleString() }}</template>
          </el-table-column>
        </el-table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, nextTick, watch } from 'vue'
import { useRoute } from 'vue-router'
import * as echarts from 'echarts'
import { evaluationApi } from '@/api/v2'
import type { FactorReport, FactorConfig } from '@/types/factor'

const route = useRoute()
const factorName = ref(String(route.params.id || ''))
const report = ref<FactorReport | null>(null)
const loading = ref(false)

const icChartRef = ref<HTMLElement | null>(null)
const industryChartRef = ref<HTMLElement | null>(null)
const turnoverChartRef = ref<HTMLElement | null>(null)
const decayChartRef = ref<HTMLElement | null>(null)

let charts: echarts.ECharts[] = []

function initCharts() {
  if (!report.value) return

  // IC timeseries
  if (icChartRef.value) {
    const ic = echarts.init(icChartRef.value)
    const dates = report.value.ic_series.map(p => p.date)
    const values = report.value.ic_series.map(p => p.value)
    ic.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 16, bottom: 24 },
      xAxis: { type: 'category', data: dates, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [
        { name: 'IC', type: 'line', data: values, symbol: 'none', lineStyle: { color: '#60a5fa', width: 1.5 } },
        { name: '22日MA', type: 'line', data: movingAverage(values, 22), symbol: 'none', lineStyle: { color: '#fb923c', width: 2 } },
      ],
    })
    charts.push(ic)
  }

  // Industry IC
  if (industryChartRef.value) {
    const ind = echarts.init(industryChartRef.value)
    const industries = report.value.industry_ic.map(p => p.industry)
    const icValues = report.value.industry_ic.map(p => p.value)
    ind.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 80, right: 16, top: 8, bottom: 8 },
      xAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      yAxis: { type: 'category', data: industries, axisLabel: { fontSize: 10 } },
      series: [{ type: 'bar', data: icValues, color: '#93c5fd' }],
    })
    charts.push(ind)
  }

  // Turnover scatter
  if (turnoverChartRef.value) {
    const to = echarts.init(turnoverChartRef.value)
    const tDates = report.value.turnover.map(p => p.date)
    const minQ = report.value.turnover.map(p => p.min_quantile)
    const maxQ = report.value.turnover.map(p => p.max_quantile)
    to.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 16, bottom: 24 },
      xAxis: { type: 'category', data: tDates, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [
        { name: '最小分位', type: 'scatter', data: minQ, symbolSize: 4, color: '#60a5fa' },
        { name: '最大分位', type: 'scatter', data: maxQ, symbolSize: 6, color: '#f87171' },
      ],
    })
    charts.push(to)
  }

  // Signal decay
  if (decayChartRef.value) {
    const sd = echarts.init(decayChartRef.value)
    const lags = report.value.signal_decay.map(p => `lag${p.lag}`)
    const sdMin = report.value.signal_decay.map(p => p.min_quantile)
    const sdMax = report.value.signal_decay.map(p => p.max_quantile)
    sd.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 16, bottom: 24 },
      xAxis: { type: 'category', data: lags, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [
        { name: '最小分位', type: 'bar', data: sdMin, color: '#60a5fa' },
        { name: '最大分位', type: 'bar', data: sdMax, color: '#9ca3af' },
      ],
    })
    charts.push(sd)
  }
}

function movingAverage(data: number[], window: number): (number | null)[] {
  return data.map((_, i) => {
    if (i < window - 1) return null
    const slice = data.slice(i - window + 1, i + 1)
    return slice.reduce((a, b) => a + b, 0) / window
  })
}

function disposeCharts() {
  charts.forEach(c => c.dispose())
  charts = []
}

async function fetchReport() {
  loading.value = true
  try {
    report.value = await evaluationApi.report({
      expression: factorName.value,
      stock_pool: 'hs300',
      start_date: '2020-01-01',
      end_date: '2025-12-31',
    })
    await nextTick()
    initCharts()
  } finally {
    loading.value = false
  }
}

onMounted(fetchReport)
onUnmounted(disposeCharts)
</script>

<style scoped>
.factor-analysis { padding: 16px; }
.page-header { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
.page-header h2 { font-size: 18px; margin: 0; }
.update-date { font-size: 11px; color: var(--text-muted); }
.panel-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
.panel {
  background: var(--bg-surface);
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 16px;
}
.panel h3 { font-size: 13px; font-weight: 600; margin: 0 0 8px; }
.chart { width: 100%; height: 220px; }
</style>
```

- [ ] **Step 3: Add route**

In `frontend/src/router/index.ts`, add the analysis route within the factor section:
```typescript
{
  path: '/factor/analysis/:id',
  name: 'FactorAnalysisNew',
  component: () => import('@/views/FactorResearch/FactorAnalysisNew.vue'),
  meta: { title: '因子分析' },
},
```

- [ ] **Step 4: Verify analysis page renders**

Navigate to `/factor/analysis/test` in the browser. Expected: 6-panel dashboard with IC chart, industry IC bar chart, turnover scatter, signal decay bars, top20/bottom20 tables.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/views/FactorResearch/FactorAnalysisNew.vue frontend/src/router/index.ts
git commit -m "feat: add 6-panel factor analysis dashboard with ECharts"
```

---

### Task B5: Factor Backtest Page

**Files:**
- Create: `frontend/src/views/FactorBacktest/index.vue`
- Modify: `frontend/src/router/index.ts` — add `/backtest/factor/:id` route

- [ ] **Step 1: Build factor backtest page**

Write `frontend/src/views/FactorBacktest/index.vue`:
```vue
<template>
  <div class="factor-backtest" v-loading="loading">
    <div class="page-header">
      <h2>因子回测 — {{ factorName }}</h2>
    </div>

    <div class="layout">
      <!-- Config panel -->
      <div class="config-panel">
        <h3>回测配置</h3>
        <el-form :model="btConfig" label-width="100px" size="small">
          <el-form-item label="调仓周期">
            <el-select v-model="btConfig.rebalance_period">
              <el-option label="每日" value="daily" />
              <el-option label="每周" value="weekly" />
              <el-option label="每月" value="monthly" />
            </el-select>
          </el-form-item>
          <el-form-item label="手续费率">
            <el-input-number v-model="btConfig.fee_rate" :min="0" :max="0.05" :step="0.001" :precision="3" />
          </el-form-item>
          <el-form-item label="滑点">
            <el-input-number v-model="btConfig.slippage" :min="0" :max="0.05" :step="0.001" :precision="3" />
          </el-form-item>
          <el-form-item label="组合类型">
            <el-select v-model="btConfig.portfolio_type">
              <el-option label="纯多头组合" value="long_only" />
              <el-option label="多空组合 I" value="long_short_i" />
              <el-option label="多空组合 II" value="long_short_ii" />
            </el-select>
          </el-form-item>
          <el-form-item label="过滤涨停">
            <el-switch v-model="btConfig.filter_limit_up" />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="runBacktest" :loading="loading">运行回测</el-button>
          </el-form-item>
        </el-form>
      </div>

      <!-- Results -->
      <div class="results-panel" v-if="report">
        <h3>回测结果</h3>
        <div class="metrics-grid">
          <div class="metric-card" v-for="m in metricsList" :key="m.label">
            <div class="metric-label">{{ m.label }}</div>
            <div class="metric-value" :class="m.color">{{ m.value }}</div>
          </div>
        </div>
        <div v-if="report.nav_series.length">
          <h4>净值曲线</h4>
          <div ref="navChartRef" class="nav-chart"></div>
        </div>
        <div v-if="report.logs.length" class="logs">
          <h4>日志</h4>
          <div v-for="(log, i) in report.logs" :key="i" class="log-line">{{ log }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import * as echarts from 'echarts'
import { backtestV2Api } from '@/api/v2'
import type { BacktestReport, BtConfig } from '@/types/factor'

const route = useRoute()
const factorName = ref(String(route.params.id || ''))

const btConfig = ref<BtConfig>({
  rebalance_period: 'monthly',
  fee_rate: 0.001,
  slippage: 0.001,
  portfolio_type: 'long_only',
  filter_limit_up: true,
})

const report = ref<BacktestReport | null>(null)
const loading = ref(false)
const navChartRef = ref<HTMLElement | null>(null)
let navChart: echarts.ECharts | null = null

const metricsList = computed(() => {
  if (!report.value?.metrics) return []
  const m = report.value.metrics
  return [
    { label: '总收益率', value: (m.total_return * 100).toFixed(2) + '%', color: m.total_return >= 0 ? 'positive' : 'negative' },
    { label: '年化收益', value: (m.annual_return * 100).toFixed(2) + '%', color: m.annual_return >= 0 ? 'positive' : 'negative' },
    { label: 'Sharpe', value: m.sharpe.toFixed(2), color: '' },
    { label: '最大回撤', value: (m.max_drawdown * 100).toFixed(2) + '%', color: 'negative' },
    { label: 'Alpha', value: m.alpha.toFixed(4), color: '' },
    { label: 'Beta', value: m.beta.toFixed(4), color: '' },
  ]
})

async function runBacktest() {
  loading.value = true
  try {
    report.value = await backtestV2Api.runFactor({
      expression: factorName.value,
      stock_pool: 'hs300',
      start_date: '2020-01-01',
      end_date: '2025-12-31',
    }, btConfig.value)
    await nextTick()
    if (report.value.nav_series.length && navChartRef.value) {
      navChart = echarts.init(navChartRef.value)
      navChart.setOption({
        tooltip: { trigger: 'axis' },
        grid: { left: 60, right: 16, top: 16, bottom: 24 },
        xAxis: { type: 'category', data: report.value.nav_series.map(p => p.date) },
        yAxis: { type: 'value' },
        series: [{
          type: 'line', data: report.value.nav_series.map(p => p.value),
          symbol: 'none', lineStyle: { color: '#22c55e' },
        }],
      })
    }
  } finally {
    loading.value = false
  }
}

onUnmounted(() => navChart?.dispose())
</script>

<style scoped>
.factor-backtest { padding: 16px; }
.page-header { margin-bottom: 16px; }
.page-header h2 { font-size: 18px; margin: 0; }
.layout { display: grid; grid-template-columns: 280px 1fr; gap: 16px; }
.config-panel, .results-panel {
  background: var(--bg-surface);
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 16px;
}
.config-panel h3, .results-panel h3 { font-size: 13px; font-weight: 600; margin: 0 0 12px; }
.metrics-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 16px; }
.metric-card {
  border: 1px solid var(--border-ghost);
  border-radius: 6px;
  padding: 10px;
  text-align: center;
}
.metric-label { font-size: 10px; color: var(--text-ghost); }
.metric-value { font-size: 16px; font-weight: 700; margin-top: 4px; }
.positive { color: #d93026; }
.negative { color: #137333; }
.nav-chart { width: 100%; height: 300px; }
.logs { margin-top: 12px; }
.log-line { font-family: monospace; font-size: 11px; color: var(--text-ghost); padding: 2px 0; }
</style>
```

- [ ] **Step 2: Add route**

In `frontend/src/router/index.ts`:
```typescript
{
  path: '/backtest/factor/:id',
  name: 'FactorBacktest',
  component: () => import('@/views/FactorBacktest/index.vue'),
  meta: { title: '因子回测' },
},
```

- [ ] **Step 3: Verify page renders**

Navigate to `/backtest/factor/test` — config panel on left, results area on right.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/views/FactorBacktest/index.vue frontend/src/router/index.ts
git commit -m "feat: add factor backtest page with config panel and results"
```

---

### Task B6: Strategy Backtest Refactor

**Files:**
- Modify: `frontend/src/views/StrategyBacktest/index.vue` — add code editor + config + results layout

- [ ] **Step 1: Read current StrategyBacktest**

```bash
cat frontend/src/views/StrategyBacktest/index.vue
```

- [ ] **Step 2: Refactor strategy backtest page**

The goal is a JoinQuant-style layout: left code editor (dark theme) + right config + results panel. Replace the existing two-tab layout.

Write the refactored `frontend/src/views/StrategyBacktest/index.vue` with:
- Left panel: dark-themed code editor with line numbers (reuse existing textarea or use `<pre>` with contenteditable)
- Right panel: config (date range, initial capital, frequency) + metrics grid (6 cards) + log/error tabs
- "运行回测" button in config area
- Keeps strategy list functionality

Core structure:
```vue
<template>
  <div class="strategy-backtest">
    <div class="split-layout">
      <!-- Left: Code editor -->
      <div class="editor-panel">
        <div class="editor-toolbar">
          <span>策略代码</span>
          <el-button size="small" @click="runBacktest" :loading="running">编译运行</el-button>
        </div>
        <div class="code-editor">
          <textarea v-model="code" class="editor-textarea" spellcheck="false" />
        </div>
      </div>

      <!-- Right: Config + Results -->
      <div class="right-panel">
        <div class="config-bar">
          <el-date-picker v-model="startDate" value-format="YYYY-MM-DD" size="small" style="width:130px" />
          <span>至</span>
          <el-date-picker v-model="endDate" value-format="YYYY-MM-DD" size="small" style="width:130px" />
          <el-input-number v-model="capital" :min="10000" size="small" style="width:130px" />
          <el-select v-model="frequency" size="small" style="width:80px">
            <el-option label="每天" value="daily" />
            <el-option label="每周" value="weekly" />
            <el-option label="每月" value="monthly" />
          </el-select>
          <el-button type="primary" size="small" @click="runBacktest" :loading="running">运行回测</el-button>
        </div>

        <div class="metrics-grid">
          <div class="metric-card" v-for="m in metrics" :key="m.label">
            <div class="metric-label">{{ m.label }}</div>
            <div class="metric-value">{{ m.value }}</div>
          </div>
        </div>

        <div class="log-panel">
          <el-tabs model-value="logs" size="small">
            <el-tab-pane label="日志" name="logs">
              <div class="log-content">{{ logs.join('\n') }}</div>
            </el-tab-pane>
            <el-tab-pane label="错误" name="errors">
              <div class="log-content">{{ errors.join('\n') }}</div>
            </el-tab-pane>
          </el-tabs>
        </div>
      </div>
    </div>
  </div>
</template>
```

Full implementation includes:
- `code` ref initialized with default strategy template
- `runBacktest()` calls `POST /v2/backtest/strategy` with code + config
- `metrics` computed from backtest results
- `logs` / `errors` arrays populated from backtest response
- Dark editor styling matching JoinQuant reference page

- [ ] **Step 3: Verify strategy backtest page**

Navigate to `/backtest` — code editor on left, config + metrics + logs on right.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/views/StrategyBacktest/index.vue
git commit -m "feat: refactor strategy backtest with code editor + config + results layout"
```

---

## Verification

### Backend (Phase A)

1. **Models**: `cd backend && python -m pytest tests/test_factor_models.py -v` — all model validation tests pass
2. **Templates**: `curl http://localhost:8000/api/v2/factors/templates` — returns 4 templates
3. **Validate**: `curl -X POST http://localhost:8000/api/v2/factors/validate -H "Content-Type: application/json" -d '{"expression":"$close"}'` — `{"valid":true,"error":null}`
4. **Batch compute**: `curl -X POST http://localhost:8000/api/v2/compute/batch` — processes array of FactorConfig
5. **Report**: `curl -X POST http://localhost:8000/api/v2/evaluation/report` — returns 6-module FactorReport
6. **Board**: `curl -X POST http://localhost:8000/api/v2/evaluation/board` — returns filtered/sorted/paginated BoardResponse
7. **Factor backtest**: `curl -X POST http://localhost:8000/api/v2/backtest/factor` — returns BacktestReport
8. **No regressions**: `cd backend && python -m pytest tests/ -v` — all existing tests pass

### Frontend (Phase B)

1. **Factor board**: Navigate to `/factor` — filter bar + sortable table + "新建因子" button
2. **Create dialog**: Click "新建因子" — 4 template cards → expression editor → validate button → create
3. **Analysis page**: Click row in board → `/factor/analysis/:id` — 6 charts/tables render
4. **Factor backtest**: Navigate to `/backtest/factor/:id` — config panel → run → metrics + NAV chart
5. **Strategy backtest**: Navigate to `/backtest` — code editor + config bar + metrics + log tabs
6. **Responsive**: Test at 1920×1080 and 1366×768

### Integration (End-to-End)

1. Create factor via board dialog → validates successfully
2. Board query returns the new factor with IC/IR values
3. Click factor row → analysis dashboard loads 6 modules
4. Click "回测" → factor backtest page runs and shows results
5. Strategy backtest editor accepts Python code → runs → shows metrics
