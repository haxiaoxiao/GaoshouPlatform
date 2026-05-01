# Factor & Backtest Model Redesign

> **Status:** Design approved — 2026-05-01
> **Context:** Redesign factor construction, factor evaluation, and backtest models with unified data contracts and richer analysis capabilities.

## Background

The current system has three independent layers (compute, evaluation, backtest) with inconsistent interfaces:
- Compute uses expression strings
- Evaluation uses stock_pool
- Backtest uses BacktestConfig
- Results formats are inconsistent across layers

The redesign unifies these via `FactorConfig` and adds factor lifecycle management (create → edit → analyze → browse) and a 6-module analysis dashboard inspired by industry-standard quant platforms.

## Data Types

### FactorConfig — Unified entry for compute, eval, backtest

```python
class FactorConfig(BaseModel):
    expression: str              # e.g. "(1/PE_TTM) + (1/PB) + ROE"
    stock_pool: StockPool        # "hs300" | "zz500" | "zz800" | "zz1000" | "zz_quanzhi"
    start_date: date
    end_date: date
    benchmark: str = "000300.SH"
    direction: Literal["asc", "desc"] = "desc"
```

### EvalConfig — Evaluation parameters

```python
class EvalConfig(BaseModel):
    ic_method: Literal["pearson", "spearman"] = "spearman"
    group_count: int = 5
    outlier_handling: Literal["none", "winsorize", "standardize"] = "winsorize"
    industry_neutralization: bool = False
    include_st: bool = False
    include_new: bool = True
```

### BtConfig — Backtest parameters

```python
class BtConfig(BaseModel):
    rebalance_period: Literal["daily", "weekly", "monthly"] = "monthly"
    fee_rate: float = 0.001
    slippage: float = 0.001
    filter_limit_up: bool = True
    portfolio_type: Literal["long_only", "long_short_i", "long_short_ii"] = "long_only"
```

### FactorReport — 6-module analysis output

```python
class FactorReport(BaseModel):
    ic_series: list[ICPoint]         # date + IC value
    industry_ic: list[IndustryIC]    # industry + IC value
    turnover: list[TurnoverPoint]    # date + min_quantile + max_quantile
    signal_decay: list[DecayPoint]   # lag + min_quantile + max_quantile
    top20: list[StockFactorValue]    # symbol + name + value
    bottom20: list[StockFactorValue] # symbol + name + value
    update_date: date
```

### BacktestReport — Standardized backtest output

```python
class BacktestReport(BaseModel):
    nav_series: list[NAVPoint]
    benchmark_series: list[NAVPoint]
    metrics: BacktestMetrics  # return, annual_return, sharpe, max_dd, alpha, beta, ir
    logs: list[str]
```

## API Endpoints

| Endpoint | Status | Description |
|----------|--------|-------------|
| `GET /v2/factors/templates` | NEW | List 4 factor creation templates |
| `POST /v2/factors/validate` | NEW | Validate expression syntax + preview |
| `POST /v2/factors/create` | NEW | Create factor from template/free |
| `GET /v2/factors/:id` | NEW | Get factor detail |
| `PUT /v2/factors/:id` | NEW | Update factor expression/params |
| `DELETE /v2/factors/:id` | NEW | Delete factor |
| `POST /v2/compute/evaluate` | EXISTING | Compute single factor → FactorMatrix |
| `POST /v2/compute/batch` | NEW | Batch compute factors (board use) |
| `POST /v2/evaluation/report` | EXTEND | Generate 6-module FactorReport |
| `POST /v2/evaluation/board` | NEW | Board query with filters + pagination |
| `POST /v2/backtest/factor` | NEW | Factor quantile-based backtest |
| `POST /v2/backtest/strategy` | RENAME | Strategy backtest (was /run) |

Key distinction: `/v2/backtest/factor` does layered backtest (quantile groups), `/v2/backtest/strategy` does event-driven backtest (handle_data).

## Frontend Routes

| Route | Page | Status |
|-------|------|--------|
| `/factors` | Factor board (filters + table) | REFACTOR |
| `/factors/create` | Factor creation dialog | NEW |
| `/factors/:id` | Factor edit (expression editor + params) | NEW |
| `/factors/:id/analysis` | 6-panel analysis dashboard | NEW |
| `/backtest/strategy` | Strategy backtest (code editor + config + results) | REFACTOR |
| `/backtest/factor/:id` | Factor backtest (config + results) | NEW |

## Component Tree

```
FactorBoard (/factors)
├── CategoryFilter (8 category checkboxes)
├── StockPoolSelector (radio)
├── PeriodSelector (radio)
├── PortfolioTypeSelector (radio)
├── FeeSelector (radio)
├── FilterToggle (limit-up/suspend)
└── FactorTable (7 sortable columns)

FactorCreateDialog (in /factors)
├── TemplateSelector (4 template cards)
├── ExpressionEditor (syntax highlight + validation)
└── ParamPanel (pool/date/direction)

FactorAnalysis (/factors/:id/analysis)
├── ICTimeseriesChart (IC line + 22-day MA)
├── IndustryICChart (horizontal bar chart)
├── TurnoverScatter (min/max quantile scatter)
├── SignalDecayChart (lag 1-10 bar chart)
├── Top20Table (top 20 stocks by factor value)
└── Bottom20Table (bottom 20 stocks by factor value)

StrategyBacktest (/backtest/strategy)
├── CodeEditor (dark theme, line numbers, syntax highlight)
├── BacktestConfigPanel (date/capital/freq/version)
├── MetricsGrid (6 metric cards)
└── LogPanel (log/error tabs)

FactorBacktest (/backtest/factor/:id)
├── BtConfigPanel (rebalance/fee/slippage/portfolio type)
└── FactorBacktestResult (NAV chart + metrics)
```

## Backend Services

| File | Status | Responsibility |
|------|--------|----------------|
| `services/factor_templates.py` | NEW | Template definitions, CRUD, param merging |
| `services/factor_validator.py` | NEW | Expression syntax + field reference validation |
| `services/compute_service.py` | NEW | Extract business logic from api/compute.py |
| `services/factor_evaluation.py` | EXTEND | 6-module report + board query |
| `services/factor_backtest.py` | NEW | Quantile-based layered backtest |
| `api/factors.py` | NEW | Factor CRUD + validation + templates endpoints |
| `api/compute.py` | EXTEND | Add batch compute endpoint |
| `api/evaluation.py` | EXTEND | Add report + board endpoints |
| `api/backtest.py` | EXTEND | Add factor backtest + rename strategy endpoint |
| `models/factor.py` | NEW | FactorConfig, EvalConfig, BtConfig, FactorReport, BacktestReport |

## Design Decisions

1. **Unified FactorConfig** — One config re-used across compute, eval, and backtest layers
2. **Factor vs Strategy Backtest** — Separate endpoints with different models and logic
3. **Factor board is the home page** — `/factors` shows browse view; creation is in-page dialog
4. **6-module analysis is deep-detail** — Accessed from board via row click, routed to `/factors/:id/analysis`
5. **Strategy backtest has code editor** — Full JoinQuant-style editor for Python strategy code
6. **Factor backtest needs only config** — No code, just BtConfig parameters
7. **Charts use ECharts** — Project already depends on ECharts; all visualizations use it
8. **4 factor creation templates** — Financial factor, Technical factor, Custom operator, Custom base factor

## Verification

1. Run backend tests: `cd backend && pytest tests/ -v` — ensure zero regressions
2. Test API flow: Create factor → Validate expression → Compute → Get report → Board query
3. Test backtest flow: Factor backtest (quantile) vs Strategy backtest (event-driven)
4. Frontend: Start dev server at `/factors`, verify board loads, create dialog opens, analysis page renders
5. Test board filters: category checkboxes, stock pool radio, sortable columns
