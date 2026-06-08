# GaoshouPlatform 使用手册

Last updated: 2026-05-25.

本文面向平台使用者和策略调试者，覆盖启动、数据同步、AKQuant 回测、ID=43 小市值策略和常用排错流程。

## 1. 启动平台

后端：

```powershell
cd E:\Projects\GaoshouPlatform\backend
.\.venv\Scripts\activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

前端：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run dev
```

常用地址：

| 地址 | 用途 |
|---|---|
| `http://localhost:5173` | 前端 |
| `http://localhost:8000/docs` | FastAPI Swagger |
| `http://localhost:8000/health` | 健康检查 |

本机桌面脚本或开发会话可能使用 `3500`/`8800` 端口；以启动脚本输出和 `/api/system/health` 为准。

ClickHouse 默认端口：

| 协议 | 端口 |
|---|---:|
| Native | `19000` |
| HTTP | `18123` |

## 2. 数据源使用原则

平台默认主数据源是 miniQMT/xtquant。

| 数据类型 | 首选 | 兜底 |
|---|---|---|
| 实时行情 | miniQMT | 无 |
| 当前股票基础信息 | miniQMT | Tushare / AKShare |
| 在市股票日线 | miniQMT | Tushare / AKShare |
| 退市/历史股票日线 | Tushare | AKShare |
| 指数历史成分 | Tushare `index_weight` | 手工快照 |
| 固定时间点分钟线 | Parquet/DuckDB 或 ClickHouse 已落库分钟线 | miniQMT 本地缓存 |
| 完整历史 1 分钟线 | 本地 JQ 分钟文件 → Parquet `klines_minute` | miniQMT/Indevs 补缺口 |

更详细的数据源经验见 `docs/data-source-cheatsheet.md`。

## 3. 数据同步

常规同步可通过前端数据管理页或 API 执行。

| sync_type | 说明 | 写入位置 |
|---|---|---|
| `stock_info` | 股票基础信息 | SQLite `stocks` |
| `stock_full` | 股票完整信息，含市值/财务 | SQLite `stocks` |
| `financial_data` | 财务数据 | SQLite `financial_data` |
| `kline_daily` | 日 K | Parquet / ClickHouse |
| `kline_minute` | 分钟 K | Parquet / ClickHouse |
| `realtime_mv` | 实时市值 | SQLite `stocks` |

xtquant 是同步阻塞 SDK。后端代码中所有 QMT 调用都应通过 `asyncio.get_running_loop().run_in_executor()` 或 `asyncio.to_thread()` 包装。

不要使用 `download_financial_data`，它可能在 miniQMT 上无限阻塞。财务数据只使用 `download_financial_data2(callback=None)`。

## 4. 固定时间点分钟线流程

对只需要盘中固定时点的策略，不要加载完整分钟线。推荐流程：

1. 使用 miniQMT 主动下载 1 分钟数据。
2. 从本地缓存读取分钟线。
3. 抽取策略需要的时间点，例如 `10:00`、`10:30`、`14:30`、`14:50`。
4. 写入 Parquet `klines_minute_timer` 或 ClickHouse `klines_minute`。
5. 回测使用 `bar_type="minute_timer"`。

如果使用当前默认 Parquet 后端，timer 数据也可以来自公共目录 `E:\Projects\Data\parquet\klines_minute` 或 `E:\Projects\Data\parquet\klines_minute_timer`。当前本地已导入聚宽版全 A 1 分钟线，覆盖 `2005-01-04` 至 `2026-05-15`，因此 ID=43 这类固定时点策略可以直接从 Parquet/DuckDB 抽取 `10:30` 等分钟点。

示例：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\backend\.venv\Scripts\python.exe backend/app/scripts/sync_timer_minute_points.py `
  --index-symbol 399101.SZ `
  --start 20210515 `
  --end 20260508 `
  --times 10:00,10:30,14:30,14:50
```

覆盖率检查：

```text
GET /api/backtest/timer-coverage?index_symbol=399101.SZ&start_date=2021-05-15&end_date=2026-05-08&times=10:00,10:30,14:30,14:50
```

## 5. 因子研究与 Alpha101

因子研究页包含因子值缓存、因子看板、详情页和分析流程。因子值缓存统一走 `/api/factor-values/*`，支持单因子预计算和集合预计算。

Alpha101 因子命名为 `alpha101_001` 到 `alpha101_101`。详情页会展示真实公式和中文解释；如果要批量落库，选择 Alpha101 集合并触发集合预计算。当前 101 个 Alpha 公式已接入宽表批量计算，预计算会复用同一个日线面板，避免每个公式重复构造 groupby 面板。集合任务逐个因子容错，单个公式异常不会中断整组，结果里可以查看 `written_factor_count`、`zero_row_factor_names`、`failed_factor_names` 和 `coverage_ranges`。

使用时注意：

- Alpha101 默认写入 `factor_values`，逻辑主键为 `symbol + trade_date + as_of_time + factor_name + params_hash`。
- 内置 Alpha101 当前使用空参数 hash；因子研究的参数 hash 只用于匹配研究配置，不改变已落库的原始因子值。
- VWAP 已按日线成交量单位自动识别“手/股”并归一到价格口径；`scale()` 已按当日横截面缩放。
- 覆盖率低的因子不要直接比较 IC，例如 `alpha101_097` 这类长链路滚动相关公式在部分股票池里有效样本很少。
- 平均 Rank IC 达到 `0.01` 左右才值得进入候选池，`0.02` 以上再结合 ICIR、分组收益和换手率判断是否可用；稳定负 IC 可以反向使用。

更多公式解读、Alpha02 示例和排查方法见 `docs/alpha101-factor-guide.md`，因子缓存字段和 API 见 `docs/factor-value-store.md`。

## 6. AKQuant 回测

前端回测页选择 AKQuant 引擎后，后端走 `/api/backtest/*`。

常用接口：

| 接口 | 说明 |
|---|---|
| `GET /api/backtest/capabilities` | AKQuant 能力探测 |
| `POST /api/backtest/run` | 运行回测 |
| `POST /api/backtest/optimize/grid` | Grid Search |
| `POST /api/backtest/optimize/walk-forward` | Walk-forward Validation |
| `POST /api/backtest/strategy-params/schema` | 获取策略参数 schema |
| `POST /api/backtest/strategy-params/validate` | 校验策略参数 |

参数原则：

- 日期、初始资金、手续费、滑点、股票池、bar type、timer times 都从前端控制面板/API payload 传入。
- 策略代码读取 `strategy_params`，不要硬编码日期、资金或股票池。
- `daily` 用于纯日线策略。
- `minute_timer` 用于固定时间点盘中策略。
- `minute` 仅用于必须连续处理分钟状态的策略。

## 7. ID=43 小市值策略

推荐设置：

| 参数 | 推荐值 |
|---|---|
| 引擎 | `akquant` |
| 股票池 | 指数池 |
| 指数 | 中小综指 `399101.SZ` |
| bar type | `minute_timer` |
| timer times | 由控制面板传入，例如 `10:00,10:30,14:30,14:50` |

关键原则：

1. 聚宽源码每次调仓使用 `get_index_stocks('399101.XSHE')`，平台应使用 `399101.SZ` 的历史成分快照。
2. 不要用当前自选池 960 只股票静态代替历史指数池。
3. 日线成交价避免未来数据，不要用当天 close 模拟当日成交。
4. 如果只需要固定盘中时点，使用 `minute_timer`，不要跑完整分钟线。
5. 行业集中度、ST、停牌、退市、涨跌停、成交时点是和聚宽对齐时的重点差异点。

运行脚本：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'

# 自动从最早可用 timer 数据起跑

# 年度切片调试

# 对比聚宽日志
```

## 8. 开发验证

后端 AKQuant 集成测试：

```powershell
cd E:\Projects\GaoshouPlatform\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
```

前端构建：

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

当前已知非阻塞警告：

- Vite 可能提示部分 chunk 超过 500 KB。
- Pydantic 可能提示 class-based config deprecated。

## 9. 常见排错

### QMT 分钟线看起来下载了，但平台读不到

分别检查两件事：

1. `download_history_data2(period='1m')` 是否真正下载完成。
2. `get_local_data(..., period='1m', data_dir=...)` 或平台封装是否能从本地目录读回行数。

客户端手动下载成功，不等于脚本读取路径已经指向同一个 `userdata_mini` 目录。

### 中小综指回测日期比预期晚

先查 timer 覆盖率。回测起点应以所需指数成分和所需 timer 分钟点都覆盖的最早日期为准。

### 和聚宽结果差异大

先做年度切片，再逐项对比：

- 调仓日指数成分
- 小市值排序输入
- 行业集中度过滤
- ST/停牌/退市过滤
- 涨跌停是否可成交
- 买卖单成交时点
- 手续费、印花税、过户费和最小佣金
