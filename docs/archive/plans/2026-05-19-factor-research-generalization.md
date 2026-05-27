# 因子研究模块通用化改造实施文档

## 1. 背景与目标

当前项目里新增的“特征仓库 / Feature Store”能力过于贴合 ID=43 小市值策略，主要问题是：

- 前端默认股票池、日期、指标都围绕 `399101.SZ` 和 `smallcap_*`。
- 后端 `FEATURE_DEFINITIONS` 大量使用 `smallcap_*` 命名。
- “特征仓库”这个名称对量化研究用户不够自然，更像机器学习平台概念。
- 当前模块只覆盖“缓存和预计算”，还没有形成完整的因子研究闭环。
- FactorResearch 相关前端页面存在中文乱码，需要一并修复。

本次改造目标：

- 产品主入口统一叫 **因子研究**。
- 原 Feature Store 对用户改名为 **因子值缓存**。
- 底层工程概念可保留 Feature Store 兼容，但新代码应逐步使用 `FactorValueStore`。
- 建立通用因子研究闭环：**因子定义 → 因子计算 → 因子值缓存 → 因子评估报告 → 因子看板 → 策略复用**。
- ID=43 需要的中间指标作为普通内置因子/指标接入，不再让整个模块围绕该策略设计。

## 2. 总体设计

### 2.1 产品命名

用户可见名称：

| 当前名称 | 新名称 |
|---|---|
| 特征仓库 | 因子值缓存 |
| Feature Store | Factor Value Store |
| Feature Definition | 因子定义 |
| Feature Group | 因子集合 / 预计算集合 |
| Feature Coverage | 因子值覆盖率 |
| Feature Preview | 因子值预览 |

项目代码命名策略：

- 第一阶段不做破坏性重命名，避免影响现有策略和 API。
- 新增推荐服务名：`FactorValueStore`。
- 保留旧 `FeatureValueStore` / `/api/features/*` 作为兼容入口。
- 前端新页面、新 API 调用、新文档统一使用“因子值缓存”。

### 2.2 模块边界

因子研究模块拆成四层：

1. **因子定义层**
   - 管理因子名称、分类、表达式/Python 代码、方向、默认参数、说明。
   - 对应现有 SQLite `factors` 表。

2. **因子计算层**
   - DSL 因子：走现有 Compute Engine / AKQuant expression。
   - Python 因子：走本地可信 Python 执行器。
   - 内置指标：走注册表或专用计算器。

3. **因子值缓存层**
   - 缓存计算结果，便于重复评估、策略复用、覆盖率检查。
   - 第一版继续使用现有 Parquet 目录：`data/parquet/feature_values/`。

4. **因子评估层**
   - 生成 IC、分组收益、行业 IC、换手率、衰减分析、top/bottom 股票列表。
   - 支撑因子看板排序和详情页展示。

## 3. 后端实施方案

### 3.1 因子定义模型

不新增数据库表，继续使用现有 `factors` 表：

- `name`: 因子唯一名称。
- `category`: 因子分类。
- `source`: 可继续使用 `custom` / `builtin`。
- `code`: DSL 表达式或 Python 代码。
- `parameters`: 扩展为统一元数据 JSON。
- `description`: 因子说明。

`parameters` 推荐结构：

```json
{
  "source_type": "dsl",
  "engine": "builtin",
  "kind": "factor",
  "direction": "desc",
  "default_stock_pool": "zz500",
  "default_benchmark": "000905.SH",
  "cache_enabled": true,
  "default_eval_config": {
    "ic_method": "spearman",
    "group_count": 5,
    "rebalance_period": "monthly",
    "outlier_handling": "winsorize",
    "standardize": true,
    "industry_neutralization": false,
    "include_st": false,
    "include_new": true,
    "filter_limit_up": true,
    "filter_limit_down": true,
    "fee_rate": 0,
    "slippage": 0
  }
}
```

枚举约定：

- `source_type`: `dsl | python | builtin`
- `engine`: `builtin | akquant | python`
- `kind`: `factor | indicator | signal | state`
- `direction`: `asc | desc`

### 3.2 FactorValueStore 服务

新增或重构服务层：

- 推荐文件：`backend/app/services/factor_value_store.py`
- 可以先包装现有 `feature_store.py`，避免一次性重命名过大。

必须提供能力：

```python
class FactorValueStore:
    def write(df: pd.DataFrame) -> int: ...
    def load_cross_section(
        factor_name: str,
        trade_date: date,
        symbols: list[str] | None = None,
        as_of_time: str | None = None,
        params: dict | None = None,
    ) -> dict[str, float]: ...

    def coverage(
        factor_name: str,
        start_date: date,
        end_date: date,
        symbols: list[str] | None = None,
        as_of_time: str | None = None,
        params: dict | None = None,
    ) -> dict: ...

    def preview(...) -> list[dict]: ...
```

物理存储第一版继续使用：

```text
data/parquet/feature_values/year=YYYY/month=MM/part-*.parquet
```

逻辑字段保持：

```text
symbol, trade_date, as_of_time, feature_name, params_hash, value, source, created_at
```

新代码中概念映射：

- `feature_name` 物理字段暂时不改。
- 服务层和 API 对外叫 `factor_name`。
- 查询时内部映射 `factor_name -> feature_name`。

### 3.3 API 改造

新增推荐 API，保留旧 API 兼容。

新增前缀：

```text
/api/factor-values/*
```

接口：

```text
GET  /api/factor-values/definitions
GET  /api/factor-values/coverage
GET  /api/factor-values/preview
POST /api/factor-values/precompute
POST /api/factor-values/query
```

兼容要求：

- `/api/features/*` 不删除。
- 旧接口内部转发到新服务。
- 前端新代码只使用 `/api/factor-values/*`。
- 策略旧参数 `enable_feature_store` 暂时保留，后续可新增别名 `enable_factor_value_cache`。

因子评估 API 增强：

```text
POST /api/factors/{factor_id}/preview
POST /api/factors/{factor_id}/precompute
POST /api/factors/{factor_id}/evaluate
```

`evaluate` 请求体：

```json
{
  "stock_pool": "zz500",
  "benchmark": "000905.SH",
  "start_date": "2023-01-01",
  "end_date": "2026-05-06",
  "direction": "desc",
  "group_count": 5,
  "rebalance_period": "monthly",
  "ic_method": "spearman",
  "outlier_handling": "winsorize",
  "standardize": true,
  "industry_neutralization": false,
  "include_st": false,
  "include_new": true,
  "filter_limit_up": true,
  "filter_limit_down": true,
  "fee_rate": 0,
  "slippage": 0,
  "use_cache": true,
  "write_cache": true
}
```

返回体至少包含：

```json
{
  "summary": {
    "ic_mean": 0.0,
    "ic_std": 0.0,
    "icir": 0.0,
    "positive_ic_rate": 0.0,
    "long_short_annual_return": 0.0,
    "max_drawdown": 0.0,
    "turnover": 0.0
  },
  "ic_series": [],
  "quantile_nav": [],
  "industry_ic": [],
  "turnover": [],
  "signal_decay": [],
  "top": [],
  "bottom": []
}
```

### 3.4 DSL 因子执行

DSL 因子继续复用现有能力：

- `backend/app/compute/*`
- `backend/app/services/compute_service.py`
- AKQuant expression engine 如已可用则保留 `engine="akquant"`。

执行流程：

1. 根据股票池解析 symbols。
2. 读取行情/指标数据。
3. 计算表达式。
4. 标准化输出为：

```text
symbol, trade_date, value
```

5. 如 `write_cache=true`，写入 FactorValueStore。

### 3.5 Python 因子执行

第一版采用 **本地可信执行**。

要求 Python 因子代码必须定义：

```python
def compute(data, context):
    """
    data: dict[str, pandas.DataFrame]
    context: dict, contains symbols/start_date/end_date/params/trading_calendar
    return: pandas.DataFrame with columns symbol, trade_date, value
    """
```

执行器建议文件：

```text
backend/app/services/python_factor_runner.py
```

执行规则：

- 第一版可以在后端进程内执行，或用子进程执行。
- 必须设置超时，默认 120 秒。
- 必须捕获异常并返回清晰错误。
- 必须校验返回字段。
- 必须把 `trade_date` 转为 date。
- 必须把 `symbol` 标准化为大写。
- 必须把 `value` 转为 float，无法转换的置为 null 或过滤。
- 第一版不做强沙箱，因为使用场景是个人本地研究。

建议提供给 Python 因子的上下文：

```python
context = {
    "symbols": symbols,
    "start_date": start_date,
    "end_date": end_date,
    "params": params,
    "stock_pool": stock_pool,
    "benchmark": benchmark,
    "trading_calendar": trading_days
}
```

建议提供的数据：

```python
data = {
    "daily": daily_bars,
    "stock_info": stock_info,
    "financial": financial_data,
    "factor_values": optional_cached_values
}
```

### 3.6 通用内置因子定义

把 ID=43 相关的 `smallcap_*` 拆成通用名称。

新增通用因子/指标：

| 通用名称 | 类型 | 说明 |
|---|---|---|
| `market_cap` | indicator | 当日可用市值 |
| `market_cap_rank` | factor | 股票池内市值升序排名 |
| `is_st` | state | ST/退市/名称异常 |
| `is_paused` | state | 停牌或无可用行情 |
| `is_limit_up` | state | 当前价格触及涨停 |
| `is_limit_down` | state | 当前价格触及跌停 |
| `yesterday_limit_up` | state | 前一交易日涨停 |
| `cum_volume_at_time` | indicator | 指定时间累计成交量 |
| `rolling_max_volume` | indicator | N 日最大成交量 |
| `high_volume_ratio` | factor | 当前累计量 / N 日最大量 |
| `high_volume_signal` | signal | 放量卖出信号 |
| `v4gv` | indicator | V4GV 技术指标 |
| `v4gv_signal` | indicator | V4GV 信号线 |
| `macd_positive` | signal | MACD 正向信号 |

兼容别名：

| 旧名称 | 新名称 |
|---|---|
| `smallcap_market_cap` | `market_cap` |
| `smallcap_market_cap_rank` | `market_cap_rank` |
| `smallcap_is_st` | `is_st` |
| `smallcap_is_paused` | `is_paused` |
| `smallcap_is_limit_up` | `is_limit_up` |
| `smallcap_is_limit_down` | `is_limit_down` |
| `smallcap_yesterday_limit_up` | `yesterday_limit_up` |
| `smallcap_v4gv` | `v4gv` |
| `smallcap_v4gv21` | `v4gv_signal` |
| `smallcap_macd_signal` | `macd_positive` |
| `smallcap_indicator_buy_signal` | 可组合表达式 |
| `smallcap_v4gv_dead_cross` | 可组合表达式 |

兼容实现要求：

- 查询旧名称时仍然能返回数据。
- 新 UI 默认只展示通用名称。
- 文档说明旧名称只为策略 43 历史兼容保留。

## 4. 前端实施方案

### 4.1 页面结构

`/factor` 主页面改为：

- 因子看板
- 因子编辑器
- 因子评估报告
- 因子值缓存

建议组件：

```text
frontend/src/views/FactorResearch/FactorBoard.vue
frontend/src/views/FactorResearch/FactorEditor.vue
frontend/src/views/FactorResearch/FactorReport.vue
frontend/src/views/FactorResearch/FactorValueCache.vue
```

当前 `FeatureStore.vue` 可重命名或作为兼容组件封装到 `FactorValueCache.vue`。

### 4.2 中文乱码修复

必须修复以下页面的用户可见乱码：

- `FactorResearch/index.vue`
- `FactorResearch/FactorBoard.vue`
- `FactorResearch/FactorCreateDialog.vue`
- `FactorResearch/FeatureStore.vue`
- `FactorResearch/FactorAnalysisNew.vue`
- `frontend/src/types/factor.ts` 注释可顺手修复
- `backend/app/api/factor.py`、`backend/app/api/evaluation.py` 的中文注释和 summary 可修复，但以不影响逻辑为准

验收标准：

- `npm run build` 无编码报错。
- 浏览器中因子研究页面所有中文正常显示。
- 不出现 `因子`、`特征`、`日期` 这类乱码。

### 4.3 因子编辑器

编辑器必须支持两种模式：

1. DSL 模式
   - 表达式输入。
   - 算子参考。
   - 验证按钮。
   - 预览按钮。
   - 保存按钮。

2. Python 模式
   - 代码编辑框。
   - 固定函数签名提示。
   - 试运行按钮。
   - 保存按钮。
   - 明确提示“本地可信执行”。

保存字段：

```ts
{
  name: string
  source_type: 'dsl' | 'python'
  engine: 'builtin' | 'akquant' | 'python'
  code: string
  category: string
  description?: string
  direction: 'asc' | 'desc'
  default_stock_pool: string
  default_benchmark: string
  cache_enabled: boolean
}
```

### 4.4 因子评估报告页

报告页布局参考用户截图，实现以下区块：

1. 顶部因子详情
   - 因子名称
   - 英文 key
   - 分类
   - 计算公式/代码类型
   - 更新时间
   - 默认参数

2. 参数区
   - 股票池
   - 回测区间
   - 基准
   - 因子方向
   - 调仓周期
   - 分组数
   - IC 计算方法
   - 去极值/标准化
   - 行业中性化
   - 是否过滤 ST、停牌、涨跌停、新股
   - 手续费和滑点

3. 总览指标
   - IC 均值
   - ICIR
   - 年化收益
   - 超额年化收益
   - 最大回撤
   - 胜率
   - 换手率

4. 图表区
   - 分组收益曲线
   - IC 时序图
   - 行业 IC 柱状图
   - 换手率散点/折线
   - 买入信号衰减分析
   - Top/Bottom 股票表格

### 4.5 因子值缓存页面

页面功能：

- 因子选择。
- 股票池选择。
- 日期范围。
- as_of_time。
- 参数 JSON 或常用参数输入。
- 查询覆盖率。
- 预览某日截面。
- 触发预计算。
- 显示最近写入行数、覆盖股票数、覆盖日期数、最早/最晚日期。

页面不应默认展示 ID=43；默认可使用：

- 因子：`market_cap`
- 股票池：`zz500`
- 日期：最近 1 年

## 5. 因子评估计算要求

### 5.1 股票池

第一版支持：

- `hs300`
- `zz500`
- `zz800`
- `zz1000`
- `zz_quanzhi`
- watchlist 分组，如 `watchlist_1`

如果当前系统已有指数成分服务，应优先使用 point-in-time 指数成分。

### 5.2 因子矩阵

统一输出矩阵：

```text
index: trade_date
columns: symbol
value: factor value
```

或等价 long format：

```text
trade_date, symbol, value
```

评估前必须处理：

- 缺失值。
- 非交易日。
- 股票池动态成分。
- 因子方向。
- ST/停牌/涨跌停/新股过滤。

### 5.3 收益矩阵

收益默认使用：

- T 日因子值。
- T+1 开盘或收盘建仓需明确。
- 第一版推荐使用日频 close-to-close 或 open-to-open，但必须在报告参数中显示。
- 不允许隐式使用未来数据。

建议第一版默认：

```text
factor_date = T
forward_return = close(T+1) / close(T) - 1
```

后续可扩展为 open-to-open。

### 5.4 IC

支持：

- Spearman，默认。
- Pearson。

输出：

- 每期 IC。
- IC 均值。
- IC 标准差。
- ICIR。
- IC 为正比例。
- 22 日移动均值。

### 5.5 分组收益

支持：

- 默认 5 组。
- 按因子方向排序。
- 计算每组等权收益。
- 输出最小分位、最大分位、基准、多空组合收益曲线。

### 5.6 行业 IC

行业来源优先：

- SQLite `stocks.industry`
- 后续可扩展 `industry2` / `industry3`

计算方式：

- 每个行业内独立计算 IC。
- 样本数过少的行业跳过。
- 输出行业名、IC 均值、样本数。

### 5.7 换手率

计算：

- 每个调仓期，记录 top quantile 和 bottom quantile 成分。
- 与上一期比较，计算换手。
- 输出每期 top/bottom 换手率。

### 5.8 信号衰减

默认 lag：

```text
1, 3, 5, 10, 20
```

计算：

- 对每个 lag 计算因子与未来 lag 日收益的 IC 或分组收益。
- 输出 min/max quantile 或 IC decay。

## 6. ID=43 策略接入要求

策略 43 不再直接依赖“特征仓库”命名。

改造方向：

- 策略参数保留兼容：
  - `enable_feature_store`
- 新增别名：
  - `enable_factor_value_cache`
- 内部统一调用 `FactorValueStore`。
- `high_volume_signal`、`cum_volume_at_time`、`v4gv` 等都从通用因子值缓存读取。
- 缓存缺失时保持原有 fallback 计算逻辑。

验收：

- 原 43 号策略回测可运行。
- 使用旧参数不报错。
- 使用新参数也能启用缓存。
- 通用因子缓存页面能看到 43 号相关指标，但它们不再以 `smallcap_*` 为主名称。

## 7. 文档更新

需要更新：

- `README.md`
- `AGENTS.md`
- `docs/user-manual.md`
- `docs/feature-store.md`

文档处理：

- `docs/feature-store.md` 建议改名为：
  - `docs/factor-value-store.md`
- 原文件可保留短跳转说明，避免旧链接断掉。

新增说明内容：

- 因子研究模块定位。
- DSL 因子写法。
- Python 因子写法。
- 因子值缓存用途。
- 如何预计算因子。
- 如何评估因子。
- 如何让策略复用因子缓存。
- ID=43 指标如何迁移到通用因子体系。

## 8. 测试计划

### 8.1 后端测试

新增或更新测试：

```text
backend/tests/factors/test_factor_definition.py
backend/tests/factors/test_factor_value_store.py
backend/tests/factors/test_python_factor_runner.py
backend/tests/factors/test_factor_evaluation_report.py
backend/tests/backtest/test_small_cap_factor_cache_compat.py
```

测试场景：

- DSL 因子创建、验证、预览。
- Python 因子创建、试运行、异常、超时。
- FactorValueStore 写入和读取。
- `/api/features/*` 兼容接口仍可用。
- `/api/factor-values/*` 新接口可用。
- 通用名称和 `smallcap_*` 别名都能读取。
- 因子评估报告包含 summary、IC、分组收益、换手率、top/bottom。
- 空数据时返回空报告，不崩溃。
- 43 号策略能通过新旧参数读取缓存。

### 8.2 前端测试

必须运行：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

人工检查：

- 因子研究页面中文正常。
- 因子看板能加载。
- 因子编辑器能保存 DSL 因子。
- 因子编辑器能保存 Python 因子。
- 因子评估报告能渲染图表。
- 因子值缓存页面能查覆盖率和预览。

### 8.3 回归命令

后端建议运行：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_parquet_provider.py -q
```

如果新增 factor tests：

```powershell
.\.venv\Scripts\python.exe -m pytest tests\factors -q
```

## 9. 实施顺序

### Phase 1：命名和兼容层

- 新增 `FactorValueStore` 包装现有 `FeatureValueStore`。
- 新增 `/api/factor-values/*`。
- `/api/features/*` 保持兼容。
- 文档先说明命名变化。
- 不改物理 Parquet 目录。

### Phase 2：因子定义通用化

- 扩展 Factor 创建/更新 payload。
- 支持 `source_type=dsl/python/builtin`。
- 修复 FactorResearch 相关中文乱码。
- 前端因子编辑器支持 DSL/Python 两个模式。

### Phase 3：Python 因子执行器

- 新增本地可信 Python runner。
- 固定 `compute(data, context)` 接口。
- 加超时、异常捕获、返回 schema 校验。
- 接入 preview 和 evaluate。

### Phase 4：评估报告通用化

- 统一评估配置。
- 补齐 IC、分组收益、行业 IC、换手率、信号衰减。
- 前端报告页按截图结构重做。
- 看板读取最近一次评估结果。

### Phase 5：ID=43 指标迁移

- 注册通用指标名称。
- 保留 `smallcap_*` 别名。
- 43 号策略改为通过 `FactorValueStore` 读取。
- 缺失缓存时保留 fallback。

### Phase 6：测试和文档

- 补后端单测。
- 跑前端 build。
- 更新 README、AGENTS、用户手册。
- 更新或重命名 feature-store 文档。

## 10. 验收标准

完成后应满足：

- 用户界面不再出现“特征仓库”作为主功能名称。
- 因子研究模块可以创建 DSL 因子和 Python 因子。
- 因子可以生成完整评估报告。
- 因子值可以预计算、查询覆盖率、预览截面。
- 43 号策略仍能运行，并能复用通用因子值缓存。
- 旧 `/api/features/*` 不破坏。
- 前端中文无乱码。
- `npm run build` 通过。
- AKQuant 相关回归测试通过。
