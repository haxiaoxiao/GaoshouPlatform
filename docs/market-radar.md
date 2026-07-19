# 市场雷达

Last updated: 2026-07-20.

市场雷达把全市场宽度、核心指数、连板生态、交易拥挤度、行业温度和个股预警放在同一条可追溯链路中。页面入口为 `/market-radar`，接口前缀为 `/api/market-radar`。

它是观察与预警工具，不提交订单，也不会修改任何实盘开关。miniQMT 不可用时，平台仍可展示最近的日终快照。

## 1. 运行模式

| 模式 | 含义 |
|---|---|
| `push` | miniQMT 全市场推送正常，后端每秒聚合一次 |
| `polling_30s` | 推送不可用或超过 5 秒无有效行情，后端每 30 秒调用一次批量行情并每 60 秒尝试恢复推送 |
| `offline` | miniQMT 完全不可用，仅展示最近可用快照 |
| `closed` | 市场休市或实时服务已明确停止 |

浏览器通过 SSE 接收聚合结果，不直接连接 miniQMT。SSE 连续 20 秒没有心跳时，前端进入 30 秒 REST 轮询，并按 5、10、20、40、60 秒的上限退避恢复连接；恢复后先补拉一次 REST 快照，再停止轮询。

默认配置：

```dotenv
MARKET_RADAR_REALTIME_ENABLED=true
MARKET_RADAR_PUSH_STALE_SECONDS=5
MARKET_RADAR_POLL_INTERVAL_SECONDS=30
MARKET_RADAR_RESUBSCRIBE_SECONDS=60
```

实时 feed 复用主 API 进程，不新增端口。日终计算仅由同步服务调度，避免重复订阅和重复计算。

## 2. 数据状态

所有接口都返回 `as_of`、`computed_at`、`status`、`confidence`、`realtime_mode` 和逐来源 `sources`。页面以真实数据日期为准，不把过期值显示成今天，也不把缺失值当成零。

| 状态 | 含义 |
|---|---|
| `fresh` | 数据满足当前模式的新鲜度与覆盖率要求 |
| `partial` | 部分来源或市场缺失，仍可展示有效部分 |
| `stale` | 来源存在但早于要求日期 |
| `unavailable` | 来源不可用或不能形成合法指标 |

主要门槛：

- QMT 推送 tick 不超过 5 秒；全 A 覆盖率至少 80%。
- 30 秒轮询批次不超过 45 秒，并明确标记为 `polling_30s`。
- 日线必须覆盖最近预期交易日。
- 涨跌停和连板必须与目标交易日相同。
- 两融最多滞后 2 个预期交易日，过期后从评分中排除。
- 舆情盘中窗口为 6 小时，日终窗口为 24 小时，并显示来源覆盖数。

北向数据只在存在且新鲜时展示；休市或停止披露不会被解释成零流入。

## 3. 指标口径

### 3.1 全 A 盈亏分布

日终收益使用同一股票连续两个有效交易日的未复权收盘价。排除缺少合法价格、无成交、停牌、重复冲突、上市首日或不能形成连续收益的股票。ST、北交所、创业板和科创板保留，并分别返回样本覆盖。

固定 10 档为：`<=-8%`、`(-8,-6]`、`(-6,-4]`、`(-4,-2]`、`(-2,0)`、`[0,2)`、`[2,4)`、`[4,6)`、`[6,8)`、`>=8%`。精确平盘计入 `[0,2)` 的中性档，但 tooltip 会单独显示平盘数量。

盘中收益使用最新价相对昨收。覆盖率不足 80% 时不会生成完整全市场分布。

### 3.2 指数与市场中位数

页面展示上证指数 `000001.SH`、深证成指 `399001.SZ`、中证全指 `000985.SH` 和全 A 收益中位数。指数缺失或价格冲突时该日为断点，不用零值或旧值延长曲线。

### 3.3 连板生态

优先使用目标交易日的 `tushare_limit_list_d` 和 `tushare_limit_step`，展示涨跌停数、炸板率、最高板、晋级率和连板明细。若梯队缺失但涨跌停明细新鲜，可从连续交易日涨停记录推导，并标记为 `derived`；旧梯队不会覆盖新交易日。

### 3.4 交易拥挤度

拥挤度为 0-100 分，由 120 个交易日的稳健历史分位组成：Top 1% 成交额占比 25%、Top 5% 成交额占比 20%、行业 Top 3 成交额占比 15%、市场成交额相对 20 日均值 15%、高流动性股票相关性 15%、融资余额 5 日变化 10%。

过期的两融分项被排除，其余权重重新归一化；有效原始权重低于 70% 时不输出总分。标签为：`0-30 宽松`、`30-55 正常`、`55-75 拥挤`、`75-90 高拥挤`、`90-100 极端拥挤`。

### 3.5 情绪温度

当前实现使用 `market-radar-emotion-reduced-v1`。接口仍返回四个候选分项的原始值、标准化值、原始/有效权重、贡献、来源日期和排除原因，但 v1 所需的连板历史及若干子分项历史尚未齐备。因此当前结果明确标记为 `partial`，并固定返回 `label = null`；即使 reduced 计算产生数值，也不能按完整情绪温度或阈值标签解释。

目标完整公式的预期组成才是市场宽度 30%、连板生态 25%、流动性与风险偏好 20%、舆情 25%。这些权重描述的是待补齐历史后的目标设计，不代表当前 reduced 版本已实现完整公式或已经启用情绪标签。有效权重不足 70% 时不输出数值；预警热度是按严重度加权的近期事件数，与情绪温度不是同一指标。

## 4. 默认预警

| 范围 | 条件 | 严重度 |
|---|---|---|
| 市场 | 全 A 中位数 `<= -2.5%` 或下跌占比 `>= 80%` | high |
| 市场 | 核心指数 `<= -2%` 或 5 分钟跌幅 `<= -1%` | high |
| 市场 | 跌停数 `>= 30` 且至少为前 5 日中位数 2 倍 | high |
| 市场 | 拥挤度 `>= 80` 且全 A 中位数 `<= -1%` | high |
| 市场 | 情绪温度向下穿过 30 或向上穿过 85 | medium |
| 行业 | 中位数 `<= -2%` 且下跌占比 `>= 80%` | medium |
| 行业 | 成交额占比 20 日 z-score `>= 2.5` 且拥挤度 `>= 80` | medium |
| 持仓股 | 相对昨收 `<= -3% / -5%` | medium / high |
| 持仓股 | 日内高点回撤 `>= 3% / 5%` | medium / high |
| 持仓股 | 20 日量比 `>= 2.5` 且绝对涨跌幅 `>= 2%` | medium |
| 持仓股 | 距跌停 `<= 0.5%` 或涨停开板 | high |
| 自选股 | 跌幅 `<= -7%`、回撤 `>= 5%` 或距跌停 `<= 0.5%` | high |
| 个股舆情 | 负面热度 z-score `>= 2` 且加权情绪 `<= -0.35` | medium |

规则只在依赖字段新鲜且覆盖完整时评估。缺少精确涨跌停价时不会推测制度阈值。

高严重度首次命中立即通知，默认冷却 15 分钟；中严重度连续命中两帧或恶化后通知；连续两帧不再命中后自动 `resolved`。事件可处于 `active`、`acknowledged`、`dismissed`、`resolved`，确认和忽略均持久化到后端。

## 5. API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/market-radar/overview` | 最新摘要、模式、来源和预警计数 |
| GET | `/api/market-radar/breadth?days=15&mode=percent` | 盈亏分布、核心指数和中位数趋势 |
| GET | `/api/market-radar/limit-ladder` | 涨跌停与连板梯队 |
| GET | `/api/market-radar/crowding` | 拥挤度总分和组成项 |
| GET | `/api/market-radar/sectors` | 申万一级行业温度 |
| GET | `/api/market-radar/alerts` | 分页筛选预警事件 |
| GET | `/api/market-radar/alerts/{event_id}` | 查询单个预警事件 |
| POST | `/api/market-radar/alerts/{event_id}/acknowledge` | 确认预警 |
| POST | `/api/market-radar/alerts/{event_id}/dismiss` | 忽略预警 |
| GET | `/api/market-radar/rules` | 分页查询有限类型的个股规则 |
| POST | `/api/market-radar/rules` | 创建有限类型的个股规则 |
| PATCH | `/api/market-radar/rules/{rule_id}` | 修改个股规则 |
| DELETE | `/api/market-radar/rules/{rule_id}` | 删除个股规则 |
| POST | `/api/market-radar/refresh` | 提交盘中或日终重算任务 |
| GET | `/api/market-radar/refresh/{task_id}` | 查询重算任务状态 |
| GET | `/api/market-radar/stream` | SSE：`mode`、`snapshot`、`alert`、`heartbeat` |

`POST /refresh` 不在 HTTP 请求内执行全市场计算。盘中任务由 API 进程串行处理；日终任务代理到同步服务的专用单 worker 队列。两类任务都通过同一个 `GET /refresh/{task_id}` 查询，接口会按任务 ID 路由到对应的进程内状态。

## 6. 调度与保留

同步服务在工作日 `Asia/Hong_Kong 15:20` 提交一次日终计算。同一交易日快照按身份幂等更新；如果依赖同步尚未补齐，会先保存 `partial`，相关数据同步完成后最多触发一次补算，不形成重试循环。

日终快照长期保留。盘中快照保留 90 个自然日，休市后由同一个 radar worker 清理；预警事件不会随快照删除，关联的旧快照 ID 会置空。

## 7. 排障

### 页面显示 `offline`

1. 检查 `GET /api/market-radar/overview` 的 `sources` 和 `realtime_mode`。
2. 检查 miniQMT 是否启动以及 xtquant 数据目录是否可用。
3. miniQMT 是可选依赖；不要因为它未启动而反复重启平台。最近日终快照仍应可读。

### 页面显示 `polling_30s`

这表示后端已自动降级，不是前端故障。检查行情覆盖与 `qmt_realtime` 来源原因；服务会每 60 秒自动尝试恢复推送。

### 图表日期过旧或出现断点

查看组件自己的 `as_of/status/reason`。连板必须严格同日，指数缺价不会用全 A 或旧值替换，两融过期会退出评分。先补齐对应数据源，再提交日终刷新。

### SSE 连接反复重连

确认反向代理没有缓冲 `text/event-stream`，响应应包含 `Cache-Control: no-cache, no-transform` 与 `X-Accel-Buffering: no`。前端会保留最后确认状态并用 30 秒 REST 轮询兜底。

### 日终任务未运行

检查同步服务 `:8810/health`、运行任务状态、交易日历和服务器时区。日终调度只属于同步服务；主 API 不应注册第二份定时任务。

## 8. 验证命令

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m pytest `
  tests/services/test_market_radar_calculator.py `
  tests/services/test_market_radar_store.py `
  tests/services/test_qmt_realtime_feed.py `
  tests/services/test_market_radar_data.py `
  tests/services/test_market_radar_service.py `
  tests/api/test_market_radar_routes.py `
  tests/test_migrations.py -q

cd ..\frontend
npm run test -- --run
npm run build
```
