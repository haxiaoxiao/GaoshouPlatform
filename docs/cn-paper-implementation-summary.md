# 量化研报因子与策略落地摘要

更新时间：2026-05-27

## 范围

本批次把 `E:\Projects\quantpaper` 两个 md 中梳理出的 44 篇研报，按平台现有数据与回测框架落为可追踪清单、可预计算因子、AKQuant 策略模板和 AI/ML 离线实验接口。

不实现逐笔、Tick、订单簿强依赖策略；有分钟线可替代的项只做分钟 K 线代理，并在清单中保留偏差说明。

## 因子清单

新增/接入的研报因子组：

| 因子组 | 用途 | 因子数 |
|---|---|---:|
| `cn_paper_fundamental` | 基本面与低频因子 | 4 |
| `cn_paper_daily_events` | 日频量价与事件因子 | 5 |
| `cn_paper_minute` | 非 Tick 分钟线代理因子 | 2 |
| `cn_paper_style_rotation` | 风格轮动/配置代理因子 | 5 |
| `cn_paper_implemented` | 当前全部已落地非 Tick 研报因子 | 16 |

已落地因子：

| 因子 | 数据频率 | 主要依赖 |
|---|---|---|
| `paper_pb_roe_residual` | monthly | `stock_daily_basic`, `financial_data` |
| `paper_composite_value` | monthly | `stock_daily_basic` |
| `paper_growth_quality_score` | monthly | `financial_data` |
| `paper_financial_health_score` | quarterly | `financial_data` |
| `paper_overnight_turnover_corr` | daily/monthly | `klines_daily`, `stock_daily_basic` |
| `paper_rsi_reversal_score` | daily/monthly | `klines_daily` |
| `paper_new_high_anchor` | daily | `klines_daily` |
| `paper_high_low_volume_event` | weekly | `klines_daily` |
| `paper_reversal_20d` | monthly | `klines_daily` |
| `paper_trend_fund_vwap_ratio` | weekly | `klines_minute` |
| `paper_trend_fund_support` | weekly | `klines_minute` |
| `paper_size_rotation_score` | monthly | `klines_daily`, `stock_daily_basic` |
| `paper_value_growth_rotation_score` | monthly | `klines_daily`, `stock_daily_basic`, `financial_data` |
| `paper_industry_momentum_20d` | monthly | `klines_daily`, `stocks.industry` |
| `paper_defensive_quality_lowvol` | monthly | `klines_daily`, `financial_data` |
| `paper_asset_allocation_proxy` | monthly | `klines_daily` |

## API 与前端

后端接口：

| 接口 | 说明 |
|---|---|
| `GET /api/factor-values/paper-manifest` | 44 篇研报落地清单 |
| `GET /api/factor-values/paper-experiments` | AI/ML 离线实验规格 |
| `POST /api/factor-values/paper-experiments/feature-snapshot` | 从因子缓存生成离线训练特征快照 |
| `POST /api/factor-values/precompute/prepare` | 支持 `cn_paper_*` 因子组依赖检查 |

前端位置：

- 因子研究 -> 因子缓存
- 选择 `研报已落地因子` 或 `研报风格轮动/配置因子` 分组，可查看研报落地清单、状态、等级、调仓频率、验证指标和 AI/ML 离线实验。

## 策略模板

新增 AKQuant 内置模板：

| 模板 key | 说明 | 依赖因子组 |
|---|---|---|
| `cn_paper_factor` | 已落地研报因子月频多因子组合 | `cn_paper_implemented` |
| `cn_paper_style_rotation` | 大小盘、成长价值、行业动量风格轮动组合 | `cn_paper_style_rotation` |
| `cn_paper_defensive_allocation` | 中国版全天候增强的权益防御代理组合 | `cn_paper_style_rotation` |

## 验证

已跑验证命令：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\python.exe -m py_compile app\services\factor_catalog.py app\services\cn_paper_factor_calculator.py app\services\cn_paper_ml_experiment.py app\api\factor_values.py app\backtest\strategies\cn_paper_style_rotation_akquant.py app\backtest\strategies\builtin_templates.py
.\.venv\Scripts\python.exe -m pytest tests\services\test_cn_paper_factor_calculator.py tests\services\test_cn_paper_ml_experiment.py tests\services\test_factor_catalog.py tests\services\test_factor_dependency_sync.py tests\backtest\test_multi_factor_strategy.py -q
.\.venv\Scripts\python.exe -m pytest tests\services\test_factor_pipeline.py tests\services\test_factor_evaluation.py tests\api\test_evaluation_api.py tests\backtest\test_akquant_integration.py -q

cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

补充接口测试覆盖：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\python.exe -m pytest tests\api\test_factor_values_paper.py -q
```

## 后续限制

- 自由现金流、分析师预期、宏观增长/通胀、多资产价格、ETF 赛道标签等仍标记为待数据源。
- Tick-only 与订单簿类研报只保留 backlog，不进入实现。
- AI/深度学习类目前只支持离线实验规格和特征快照；训练、模型版本、复现报告需要在基础因子评估稳定后继续接入。
