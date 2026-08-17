# GaoshouPlatform 使用手册

Last updated: 2026-07-20.

本文面向平台使用者和策略调试者，覆盖启动、数据同步、AKQuant 回测、ID=43 小市值策略和常用排错流程。

## 1. 启动平台

生产仓库推荐直接使用统一启动器，它会加载 `.env.local`、启动主 API/同步服务/前端并写入实际前端端口：

```powershell
cd E:\Projects\GaoshouPlatform-prod
.\tools\start-gaoshouplatform.bat --no-pause
```

常用地址：

| 地址 | 用途 |
|---|---|
| `http://127.0.0.1:3511` | 前端；端口冲突时回退到 `3512..3599` |
| `http://127.0.0.1:8800/docs` | FastAPI Swagger |
| `http://127.0.0.1:8800/health` | 主 API 健康检查 |
| `http://127.0.0.1:8810/health` | 同步服务健康检查 |

Prod 标准端口为 `3511/8800/8810`，实际前端端口见 `.runtime/frontend-port.txt`。Dev 使用 `13500/18800/18810`，不要跨环境代理或写入。

## 2. 数据源使用原则

平台默认主数据源是 miniQMT/xtquant。

| 数据类型 | 首选 | 兜底 |
|---|---|---|
| 实时行情 | miniQMT | 无 |
| 当前股票基础信息 | miniQMT | Tushare Relay / 本地快照 |
| 在市股票日线 | miniQMT | Tushare Relay / 本地归档 |
| 退市/历史股票日线 | Tushare Relay / 本地归档 | 无 |
| 指数历史成分 | Tushare `index_weight` | 手工快照 |
| 固定时间点分钟线 | Parquet/DuckDB 已落库分钟线 | miniQMT 本地缓存 |
| 完整历史 1 分钟线 | 本地 JQ 分钟文件 → Parquet `klines_minute` | miniQMT/Indevs 补缺口 |
| JQ 个股资金流 | 本地 Parquet `jq_money_flow_daily` | 后续清洗数据 | 日期字段必须用 `trade_date_1`，不要用空的 `trade_date` |

更详细的数据源经验见 `docs/data-source-cheatsheet.md`。

## 3. 数据同步

常规同步可通过前端数据管理页或 API 执行。

| sync_type | 说明 | 写入位置 |
|---|---|---|
| `stock_info` | 股票基础信息 | SQLite `stocks` |
| `stock_full` | 股票完整信息，含市值/财务 | SQLite `stocks` |
| `financial_data` | 财务数据 | SQLite `financial_data` |
| `kline_daily` | 日 K | Parquet |
| `kline_minute` | 分钟 K | Parquet |
| `realtime_mv` | 实时市值 | SQLite `stocks` |

“日常更新”预设默认使用全市场范围；只有主动切换到“自定义”后才会把股票代码列表发送给同步服务。数据查看页的“最近 / 关注”记录用户实际选择过的最近 10 只股票，并保存在当前浏览器，不再使用固定示例股票。

日线查询默认返回约一个交易年的 250 条。选择更长日期区间后，将 K 线时间轴拖到最左侧会继续加载更早分页，直到覆盖所选区间；切换股票、周期或日期范围会重新从第一页查询。

xtquant 是同步阻塞 SDK。后端代码中所有 QMT 调用都应通过 `asyncio.get_running_loop().run_in_executor()` 或 `asyncio.to_thread()` 包装。

不要使用 `download_financial_data`，它可能在 miniQMT 上无限阻塞。财务数据只使用 `download_financial_data2(callback=None)`。

## 4. 固定时间点分钟线流程

对只需要盘中固定时点的策略，不要加载完整分钟线。推荐流程：

1. 使用 miniQMT 主动下载 1 分钟数据。
2. 从本地缓存读取分钟线。
3. 抽取策略需要的时间点，例如 `10:00`、`10:30`、`14:30`、`14:50`。
4. 写入 Parquet `klines_minute_timer`。
5. 回测使用 `bar_type="minute_timer"`。

当前唯一行情后端是 Parquet/DuckDB，数据根为 `E:\Projects\data\BaiduSyncdisk\parquet`。本地分钟线可直接抽取 `10:30` 等固定时点；覆盖范围以 `/api/backtest/timer-coverage` 的实时结果为准。

数据浏览器和系统摘要均从该数据根读取。`jq_money_flow_daily` 的规范日期字段为 `trade_date_1`；原始 `trade_date` 字段为空，不要接入因子或筛选逻辑。行数和日期覆盖属于动态数据，请在数据浏览器中按需刷新。

示例：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform-prod\backend'
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

运行前先通过 `/api/backtest/index-pools/{index_symbol}` 和 `/api/backtest/timer-coverage` 核对历史成分与 timer 覆盖，再从前端回测页提交。仓库没有独立 yearly debug 脚本，不要引用旧入口。

## 8. 市场雷达

页面入口：`http://127.0.0.1:3511/market-radar`。它用于观察全 A 盈亏分布、上证/深证/中证全指与全 A 中位数趋势、连板生态、交易拥挤度、行业温度和分级预警，不会提交订单。

盘中优先使用 miniQMT 全市场推送，后端每秒聚合；推送超过 5 秒无有效行情时自动切换为每 30 秒批量轮询，并每 60 秒尝试恢复。页面显示 `push`、`polling_30s`、`offline` 或 `closed`，miniQMT 未启动不会阻塞平台和最近日终快照。

使用时先看每个组件的真实数据日期和 `fresh/partial/stale/unavailable` 状态。连板数据必须与目标交易日一致；两融超过 2 个预期交易日会退出拥挤度评分；指数缺价形成断点，不会用零值或旧值延长。高严重度预警立即进入全局通知，确认/忽略状态保存在后端；点击通知会定位到预警证据。

日终任务由同步服务在工作日香港时间 15:20 串行执行。日终快照长期保留，盘中快照保留 90 天。完整指标、规则、接口和排障见 `docs/market-radar.md`。

## 9. 日内做 T

页面入口：`http://127.0.0.1:3511/trade/intraday-t`。

策略股票池固定为利通电子 `603629.SH` 和澜起科技 `688008.SH`，不能从请求中换成其他股票。推荐先将单次做 T 比例保持在 `20%–30%`，底仓输入为 `0` 时由回测资金与现金预留自动分配。页面标记“样本外未晋级”是研究结论，不代表可实盘使用。

回测流程：

1. 选择日期区间并检查两只股票的本地分钟数据覆盖。
2. 设置初始资金、现金预留、底仓数量、入场 Z-Score 和每日最多 T 对。默认只允许 `1.75 <= abs(zscore) < 2.40`、`10:00–10:29` 入场、每日 1 对并冷却 20 分钟。
3. 运行分钟回测，重点检查“相对持有增量”“每股降本”“底仓恢复率”和拒单列表；API 同时返回胜率、盈亏比、最大回撤、最大单日 T 损失以及正/反 T 分方向汇总。
4. 底仓恢复率低于 `100%` 时不要进入模拟盘，应先检查尾盘成交量、涨跌停和参数。

成交口径为信号后下一根分钟线开盘价，并计入双边佣金、最低佣金、卖出印花税、过户费和滑点。当本地 `volume` 为“手”时，服务会根据 `amount / volume / price` 自动换算为股后再检查容量。当日买入不会增加当日可卖数量。新入场使用 SQLite `stock_limit_prices` 精确过滤涨跌停；偏离达到 `stop_z` 时优先止损恢复，上午未平仓在 `11:29` 发出恢复信号，下午在 `14:49` 发出恢复信号；已实现日内亏损达到默认 `45bp` 后停止新开仓，但活动配对仍继续恢复。回测未恢复配对会跨日保留并优先恢复。

10 分钟最低实现波动是实验控件。冻结的两年样本外中，20bp 门控使四折合计从时段门控的 `-5,397.65` 元进一步降为 `-10,551.55` 元，因此默认 `0bp` 表示关闭，不应为了追求历史收益现场调参。

冻结研究覆盖 2024-07-19 至 2026-07-14、223,282 bars、466 个观测交易日：v1 兼容基线 `-88,824.88` 元，当前时段门控 `-5,397.65` 元，20bp 候选 `-10,551.55` 元。候选在 3/3 开发折及最终 holdout 均相对基线改善且无最终未恢复配对，但仍有 2 个名义测试折为负，5bp 与 10bp 滑点压力合计分别为 `-21,516.60`、`-39,226.65` 元，正式结论为 `do_not_promote`。另有 14 个权威交易日没有分钟数据、24 个观测股票日缺少精确涨跌停价，澜起科技缺 2 个观测交易日；解释结果时必须保留这些覆盖警告。

v3 在 2024-07-19 至 2026-03-13 的 398 个完整共同交易日上进一步检查市场残差、波动带、温和方向位移和残差趋势/跳跃。唯一正候选 `0 <= sign(z) * session_return < 100bp` 为 74 对、三折 `+1,038.90` 元；冻结名义信号后，5bp/边压力仅 `+105.37` 元，10bp/边为 `-1,450.49` 元。603629.SH 为负且样本少于 80 对门槛，所有候选仍为 `research_only`。这些字段只存在于离线研究脚本，不是页面或模拟盘的运行参数。

v4 按最终范围仍只研究 `603629.SH` 和 `688008.SH`。在同一 398 日完整面板上，量价在线预测是唯一在 10bp 压力下仍为正的新增门控，但只有 29 对、合计仅 `+43.40` 元，且 603629.SH 为 `-147.52` 元；独立跳跃否决和 5 分钟 Amihud 冲击门控在 10bp 下均为负。48 个折次/变体/压力运行全部恢复，2/5/10bp 使用相同信号账本，2.5% 成交量参与率单独审计。结论固定为 `research_only`：这些历史已参与假设形成，不是样本外证据，也不会自动修改页面、模拟盘或实盘参数。完整结果见 `docs/superpowers/specs/2026-07-20-intraday-t-v4-two-stock-research.md`。

v5 在 v4 量价候选上增加 10:00 大盘情绪。情绪只使用昨日收盘后连板队列、今日盘前精确涨跌停价及当日 09:31-10:00 已完成的 canonical JQ 分钟线；当日收盘结果不参与。连板晋级在板率门控为 19 对，名义/5bp/10bp 分别 `+1,799.38/+1,511.68/+1,032.18` 元，但 603629.SH 仍为 `-67.16` 元；涨跌停广度没有过滤交易，复合情绪为 23 对。分钟 OHLC 的“在板”只代表 10:00 最后价在涨停价，不代表真实封单。结论仍为 `research_only`，完整口径和结果见 `docs/superpowers/specs/2026-07-20-intraday-t-v5-market-sentiment-results.md`。

模拟盘流程：

1. 选择 QMT 账户只读快照，或手工录入现金、底仓与可卖数量。
2. 启动模拟会话；会话和模拟成交持久化到 SQLite。
3. 可手动“评估当前分钟”，也可启动默认 30 秒 Runner。Runner 只生成与记录模拟成交，不提交真实委托；同一会话的手工评估与 Runner 串行执行，同一分钟重复请求幂等。
4. 进程重启后打开页面刷新即可恢复会话；`runner_active=false`、`recoverable=true` 表示需要手工重新启动 Runner。
5. 会话可以跨交易日恢复：进入新交易日时清除前日待成交信号，重置当日配对次数、损益和可卖额度，并将未平配对置为强制恢复；恢复完成前禁止新开仓。持仓未恢复到期初底仓时，停止会话会被拒绝；先完成恢复，再停止或重置账本。

接口前缀为 `/api/intraday-t`。该路由树没有 `submit` 端点，也不调用 `/api/v1/live/orders/submit`。

研究产物位于 `.runtime/intraday-t-v2-research/`、`.runtime/intraday-t-v3-research/`、`.runtime/intraday-t-v4-research/` 和 `.runtime/intraday-t-v5-sentiment-research/`。v5 额外保存 `market_sentiment_daily.csv`，记录涨跌停计数、连板分层分母/观测数和因果标准化特征。复跑命令见根目录 `README.md`；所有研究运行器只读行情与 SQLite 涨跌停价，不会写交易订单。

## 10. 开发验证

后端 AKQuant 集成测试：

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform-prod\backend'
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
```

前端构建：

```powershell
cd E:\Projects\GaoshouPlatform-prod\frontend
npm run build
```

## 11. 安全与并发边界

- 数据浏览器只接受服务端校验的结构化 search、排序和分页，不提供任意 SQL 或自由文本 WHERE。
- Python 因子必须定义 `compute(data, context)`，禁止 import 和常见文件/进程入口，并在超时后可终止的子进程中执行；这不是操作系统级沙箱，只运行可信本地研究代码。
- 用户输入的论坛 URL 在可终止子进程中完成 DNS、HTTP 和 HTML 解析；每次跳转重新验证 public HTTP(S)，并限制代理、编码、类型、响应体和总时限。
- 主 API、同步服务和调度器共享名为 `sync` 的单 worker FIFO，避免多个入口并发写同一数据集。
- Compute cache key 包含表达式、股票池、日期、engine 和 data version；同步完成后同时清理进程缓存与 Redis 缓存。
- `/api/live-trading/orders/submit` 固定返回 410。真实订单只走 `/api/v1/live/orders/submit`，要求 `live_approved` release、有效 control session、匹配的账户掩码、明确确认和 idempotency key。

## 12. 常见排错

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
