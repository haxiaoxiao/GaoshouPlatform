# GaoshouPlatform 统一计算层 & 回测引擎设计

日期: 2026-04-30
状态: 已确认

## 背景

基于架构评审报告的结论：当前系统"指标库"与"因子引擎"割裂为两个独立体系，回测引擎为 VeighNa mock 空壳。核心问题：

1. **"裂"** — 21 个指标通过 `IndicatorBase` 独立计算，因子引擎（`factor_engine.py`）只能算 UBL 单一因子，两者无共享计算层
2. **"重"** — `vn_engine.py` 只有 113 行 mock 代码，返回假数据
3. **不可扩展** — 定义新因子需写 Python 类，无声明式表达式能力
4. **无缓存** — 每次请求重新全量计算

## 设计目标

- **统一计算层**：表达式引擎 + 算子注册表 + 三级缓存，替代现有 `indicators/` 和 `factor_engine.py`
- **轻量回测引擎**：自研向量化引擎（因子分组回测）+ 事件驱动引擎（信号触发回测），替代 `vn_engine.py` mock
- **表达式语法**：Qlib 风格，支持 `Mean($close, 5) / Std($close, 20)` 类声明式因子定义
- **迁移策略**：现有 21 个指标全部重构为引擎内置算子，废弃 `IndicatorRegistry`

---

## 第一部分：统一计算层

### 1. 目录结构

```
backend/app/compute/
├── __init__.py
├── expression.py              # 表达式引擎（Tokenizer + Parser + Evaluator）
├── operators/
│   ├── __init__.py            # 算子注册表 + auto_discover
│   ├── registry.py            # OperatorRegistry
│   ├── base.py                # Operator 基类
│   ├── raw_fields.py          # L0: $open, $high, $low, $close, $volume, $amount
│   ├── math_ops.py            # L1: +, -, *, /, Abs, Log, Rank, Delay, Delta
│   ├── rolling_ops.py         # L2: Mean, Std, Max, Min, Sum, Corr, Cov
│   └── ta_ops.py              # L3: RSI, MACD, BBANDS, EMA, SMA, ATR, KDJ...
├── cache.py                   # 三级缓存：内存 LRU → ClickHouse factor_cache → raw data
├── scheduler.py               # 每日盘后预计算常用因子
└── api.py                     # FastAPI router: /api/v2/compute/*
```

### 2. 算子体系（四级）

| 级别 | 类别 | 内容 | 数量 |
|------|------|------|------|
| L0 | 原始字段 | `$open`, `$high`, `$low`, `$close`, `$volume`, `$amount`, `$turnover` | 7 |
| L1 | 数学/统计函数 | `+`, `-`, `*`, `/`, `Abs`, `Log`, `Sign`, `Rank`, `Delay`, `Delta`, `Power`, `Sqrt` | 12 |
| L2 | 滚动窗口函数 | `Mean(x, d)`, `Std(x, d)`, `Max(x, d)`, `Min(x, d)`, `Sum(x, d)`, `Corr(x, y, d)`, `Cov(x, y, d)`, `Slope(x, d)` | 8 |
| L3 | TA-Lib 技术指标 | `SMA`, `EMA`, `RSI`, `MACD`, `BBANDS`, `ATR`, `KDJ`, `OBV`, `CCI`, `WILLR`, `MFI`, `ULTOSC` 等 | ~30 |

现有 21 个 `IndicatorBase` 子类映射为新算子：
- `valuation.py` (pe_ttm, pb, ps_ttm, dividend_yield) → 需基本面数据，保留为独立 L3+ 算子
- `growth.py` (revenue_growth, profit_growth) → 同上
- `quality.py` (roe, debt_ratio, gross_margin) → 同上
- `momentum.py` (return_5d/20d/60d, ma5_slope) → `Delta($close, 5)/Delay($close, 5)`, `Slope(Mean($close, 5), 1)`
- `volatility.py` (volatility_20d, avg_amplitude) → `Std($close, 20) * sqrt(252)`, `Mean($high/$low - 1, 20)`
- `liquidity.py` (turnover_rate, avg_amount_20d, free_float_mv) → 部分从 SQLite 读，保留为混合算子
- `technical.py` (ma5/10/20, rsi_14) → `Mean($close, 5)`, `RSI($close, 14)`
- `theme.py` (business_purity, chain_position, revenue_ratio) → 从 SQLite theme_annotations 读取，保留

### 3. 表达式引擎

**语法规范：**
```
expression  = term (("+" | "-") term)*
term        = factor (("*" | "/") factor)*
factor      = function_call | variable | number | "(" expression ")"
function_call = IDENT "(" arg ("," arg)* ")"
variable    = "$" IDENT
```

**示例表达式：**
```
# 简单因子
($close - $open) / $open                    # 日内涨跌幅

# 滚动窗口
(Mean($close, 5) - Mean($close, 20)) / Std($close, 20)   # 标准化动量

# 嵌套技术指标
RSI($close, 14) - Delay(RSI($close, 14), 1)              # RSI 变动

# 复合因子
Rank(Mean($volume, 20)) + Rank(RSI($close, 14))           # 量+动量综合排名
```

**执行流程：**
1. `Tokenizer.tokenize(expr)` → Token 流
2. `Parser.parse(tokens)` → AST（嵌套 Node 对象）
3. `Evaluator.evaluate(ast, context)` → 递归求值
   - 叶节点：查 L1 缓存 → L2 ClickHouse → L3 原始数据
   - 函数节点：查找 `OperatorRegistry` 调用对应算子
   - 运算符节点：对子节点结果执行运算
4. 结果写回 L1 缓存

**依赖：** 仅用 Python `ast` 标准库做辅助解析，不引入第三方解析库（PEG/PyParsing 等），保持依赖轻量。`ast` 用于将用户表达式字符串转为 AST，自定义 `NodeVisitor` 遍历求值。遇到不支持的语法直接报错并指出位置。

### 4. 缓存策略

| 层级 | 介质 | 生命周期 | 内容 |
|------|------|----------|------|
| L1 | 进程内 `dict` + LRU | 请求级→会话 | 当前计算图的所有中间节点值（Pandas Series/DataFrame） |
| L2 | ClickHouse `factor_cache` | 按交易日持久化 | 预计算的常用因子值，key = `(symbol, trade_date, expression_hash)` |
| L3 | ClickHouse `klines_daily` | 永久 | 原始 K 线 OHLCV |

**缓存 key 规则：** `hash(规范化表达式 + 参数)` 。例如 `RSI(close, 14)` → `sha256("rsi:field=close:period=14")[:16]`。

**L2 预计算：** `scheduler.py` 每日盘后自动计算注册为 `is_precomputed=True` 的因子，写入 `factor_cache` 表。

**L2 新表 DDL：**
```sql
CREATE TABLE factor_cache (
    symbol String,
    trade_date Date,
    expr_hash FixedString(16),
    value Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (expr_hash, trade_date, symbol);
```

### 5. 计算调度器（替代旧 scheduler.py）

```python
class ComputeScheduler:
    """每日盘后预计算"""
    
    async def run_daily_jobs(self):
        """1. 读取已注册的 is_precomputed 算子列表"""
        """2. 获取全市场股票池（A股精选 自选股分组）"""
        """3. 批量求值所有预计算表达式"""
        """4. 结果写入 ClickHouse factor_cache 表"""
        """5. 写入 stock_indicators 表（兼容旧格式）"""
```

### 6. API 设计

所有端点挂在 `/api/v2/compute`：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/operators` | 返回所有可用算子及其签名、分类 |
| POST | `/evaluate` | 计算表达式，返回时间序列或截面值 |
| POST | `/screen` | 基于条件筛选股票 |
| POST | `/batch` | 批量计算多个表达式（用于回测前全量计算） |

**POST `/evaluate` request:**
```json
{
    "expression": "RSI($close, 14)",
    "symbols": ["000001.SZ", "000002.SZ"],
    "start_date": "2025-01-01",
    "end_date": "2025-12-31"
}
```

**POST `/evaluate` response:**
```json
{
    "code": 0,
    "data": {
        "000001.SZ": [
            {"trade_date": "2025-01-02", "value": 55.3},
            {"trade_date": "2025-01-03", "value": 58.1}
        ],
        "000002.SZ": [...]
    },
    "meta": {
        "expression": "RSI($close, 14)",
        "cache_hit": false,
        "compute_time_ms": 45
    }
}
```

**POST `/screen` request:**
```json
{
    "condition": "RSI($close, 14) < 30 AND Mean($volume, 20) > 10000000",
    "universe": "all",
    "trade_date": "2025-12-31",
    "limit": 50
}
```

---

## 第二部分：回测引擎

### 1. 目录结构

```
backend/app/backtest/
├── __init__.py
├── runner.py                  # BacktestRunner 统一入口
├── config.py                  # BacktestConfig, BacktestResult dataclass
├── vectorized.py              # 向量化回测引擎
├── event_driven.py            # 事件驱动回测引擎
├── analyzers.py               # 分析器：收益/回撤/夏普/IC/换手率
└── api.py                     # FastAPI router: /api/v2/backtest/*
```

### 2. 向量化回测引擎

**定位：** 因子分组回测，全市场×多年×月度调仓 < 3 秒。

**输入：**
- `factor_matrix`: DataFrame(index=trade_date, columns=symbol, values=因子值)
- `return_matrix`: DataFrame(同结构, values=下期收益率)
- `config`: 调仓频率(日/周/月)、分组数(N=5/10)、手续费率、初始资金、加权方式(等权/市值加权)

**执行流程（纯 Pandas/NumPy 向量运算）：**
1. 在调仓日截取因子值 → `pd.qcut` 分组 → 每个 symbol 分配组号
2. 取下一期收益矩阵 × 组号 → 组均收益
3. 多空 = 组1 - 组N
4. `cumprod` 累加净值
5. 输出分组净值 + 多空净值 + 指标统计

**性能目标：**
- 3000 只股票 × 5 年 × 月频 → < 3 秒
- 3000 只股票 × 3 年 × 日频 → < 15 秒

**超出向量化能力的场景（回退到事件驱动）：**
- 条件触发的出场规则（止损/止盈）
- 持仓数量限制（最多 N 只）
- 资金比例约束（单票不超过 20%）

### 3. 事件驱动回测引擎

**定位：** 信号触发策略 + 分钟级数据支持。

**输入：**
- K 线数据（日线或分钟线）
- 信号函数（可接受表达式或 Python 函数）
- 仓位/风控规则

**执行流程：**
```
for each bar in chronological order:
    1. 更新持仓市值，检查止损/止盈
    2. 执行信号函数 → 得到买入/卖出信号
    3. 生成订单（以 next_open 成交，含手续费+滑点）
    4. 更新持仓、现金
    5. 记录每日净值快照
```

**性能目标（日线）：** 3000 只股票 × 5 年 → < 30 秒

### 4. 回测分析器

统一输出格式，含：

| 指标 | 计算方式 |
|------|----------|
| 累计收益率 | `(final_value - initial) / initial` |
| 年化收益率 | `(1 + total_return)^(252/n_days) - 1` |
| 年化波动率 | `daily_returns.std() * sqrt(252)` |
| 夏普比率 | `(annual_return - rf) / annual_vol` |
| 最大回撤 | `(cummax - cum) / cummax` 最小值 |
| Calmar 比率 | `annual_return / abs(max_drawdown)` |
| 胜率 | 盈利交易 / 总交易 |
| 盈亏比 | 平均盈利 / 平均亏损 |
| 换手率 | 每期调仓比例均值 |

### 5. 统一入口

```python
@dataclass
class BacktestConfig:
    mode: Literal["vectorized", "event_driven"]
    symbols: list[str]
    start_date: date
    end_date: date
    initial_capital: float = 1_000_000
    
    # 向量化模式
    factor_expression: str | None = None       # 排序因子
    rebalance_freq: str = "monthly"             # daily/weekly/monthly
    n_groups: int = 5
    
    # 事件驱动模式
    buy_condition: str | None = None            # 买入信号表达式
    sell_condition: str | None = None           # 卖出信号表达式
    bar_type: str = "daily"                     # daily/minute
    
    # 通用
    commission_rate: float = 0.0003
    slippage: float = 0.001

class BacktestRunner:
    async def run(self, config: BacktestConfig) -> BacktestResult: ...
    async def get_progress(self, task_id: str) -> dict: ...
```

### 6. API 设计

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/backtest/run` | 提交回测任务，返回 task_id |
| GET | `/backtest/status/{task_id}` | 查询进度 |
| GET | `/backtest/result/{task_id}` | 获取结果（净值曲线 + 指标 + 交易记录） |
| GET | `/backtest/history` | 回测历史列表 |

---

## 第三部分：迁移路径

### 不做破坏性变更

所有新代码放在新目录 `backend/app/compute/` 和 `backend/app/backtest/` 中。旧代码保留不动，通过新 API 路径 `/api/v2/` 暴露。待新系统稳定后逐步废弃旧模块。

### 分四阶段执行

**Phase 1：计算层核心（算子 + 表达式引擎 + 缓存）**
- 新建 `compute/` 目录
- 实现 `OperatorRegistry`、L0-L3 算子
- 实现 `ExpressionEngine`（Tokenizer + Parser + Evaluator）
- 实现三级缓存 + ClickHouse `factor_cache` 表
- 暴露 `/api/v2/compute/*` 端点
- 后端自测：用表达式重算现有 21 个指标，对比结果一致性

**Phase 2：指标迁移**
- 逐个用表达式重写 21 个指标
- 验证 100% 一致性后废弃 `indicators/` 目录
- 实现每日预计算调度器

**Phase 3：向量化回测引擎**
- 新建 `backtest/` 目录
- 实现向量化引擎 + 分析器
- 暴露 `/api/v2/backtest/*` 端点
- 替换 `vn_engine.py` mock

**Phase 4：事件驱动引擎 + 策略接入**
- 实现事件驱动引擎（支持分钟级数据）
- 将趋势资金策略接入事件驱动引擎回测
- 前后端联调：因子编辑器 → 回测 → 结果分析

---

## 第四部分：非功能需求

### 性能
- 单因子全市场（3000股×3年）横截面计算：< 1 秒
- 复合因子（2-3层嵌套）全市场计算：< 3 秒（缓存命中后 < 0.1 秒）
- 向量化回测 3000股×5年月频：< 3 秒
- 事件驱动回测 3000股×5年日频：< 30 秒

### 错误处理
- 表达式语法错误：返回行号 + 错误位置 + 修复建议
- 计算中 NaN/inf：标记但不中断，结果中包含 `null`
- 数据缺失（停牌日）：跳过该 symbol 该日期，记录警告日志
- 超时保护：单次 evaluate 超过 30 秒自动中断，返回已计算部分

### 测试
- 算子单元测试：每个 L2/L3 算子对比 TA-Lib 标准输出
- 表达式引擎测试：100+ 用例覆盖合法/非法语法
- 回测引擎测试：用已知策略（买入持有）验证收益计算正确性
- 迁移一致性测试：新表达式重算旧指标，允许误差 < 1e-6

### 依赖
- `TA-Lib`（C 库 + Python binding）：L3 技术指标算子底层计算
- `pandas-ta`（备选，纯 Python）：部分 TA-Lib 不易安装的环境的 fallback
- 不引入：Celery（回测暂时同步执行，后续再考虑异步）、PyParsing/lark（用标准 ast 模块）
