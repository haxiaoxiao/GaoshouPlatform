# 回测前端增强 — 事件驱动引擎可视化

> **Status:** Design approved | **Date:** 2026-05-02 | **Depends on:** Event-driven engine integration (17a83dd)

## Goal

将新集成的事件驱动回测引擎能力暴露在前端 UI 中：实时事件流、FIFO 持仓追踪、风控反馈、完整成交明细、扩展绩效指标（Sortino/Alpha/Beta/IR/Calmar）。

## Architecture

采用"运行面板实时反馈 + 完成后全屏报告"混合模式。后端在 event loop 每个 tick 将增量数据写入 `_tasks[taskId]`，前端保持 2 秒轮询，返回 delta（新事件 + 持仓快照 + 滚动指标）。完成后通过"查看完整报告"按钮打开全屏 overlay。

```
POST /v2/backtest/run  →  task_id
  ↓ polling (2s interval)
GET /v2/backtest/status/{task_id}
  → {status, progress, live: {events[], positions{}, metrics_snapshot{}, cash, total_value}}
  ↓ on done
GET /v2/backtest/result/{task_id}
  → full BacktestResult with trades, nav_series, metrics
```

## Data Flow

### Backend API Changes

**`GET /v2/backtest/status/{task_id}`** — 扩展响应体：

```json
{
  "status": "running|done|failed",
  "progress": 0.35,
  "live": {
    "current_date": "2024-03-15",
    "events": [
      {"type": "BAR",       "timestamp": "2024-03-15T09:31:00", "symbol": "000300.SH", "data": {"close": 3.58}},
      {"type": "ORDER_PASS","timestamp": "2024-03-15T09:31:01", "order_id": "ord_42", "symbol": "000300.SH", "direction": "buy", "quantity": 200, "price": 3.58},
      {"type": "TRADE",     "timestamp": "2024-03-15T09:31:02", "trade_id": "trd_88", "symbol": "000300.SH", "direction": "buy", "quantity": 200, "price": 3.58, "commission": 2.15},
      {"type": "ORDER_REJECT", "timestamp": "2024-03-15T09:31:01", "order_id": "ord_43", "symbol": "600000.SH", "reason": "资金不足", "detail": "需要15,000 剩余8,500"}
    ],
    "positions": {
      "000300.SH": {"shares": 500, "avg_cost": 12.30, "market_value": 6250, "unrealized_pnl": 100}
    },
    "metrics_snapshot": {"total_return": 0.023, "max_drawdown": -0.015, "sharpe": 1.2},
    "cash": 945000.0,
    "total_value": 1009500.0
  }
}
```

**Backend changes needed:**
- `runner.py`: `_run_event_driven()` 添加 `on_after_trading` 回调，将 snapshot 写入 `_tasks[taskId]`
- `runner.py`: event loop 中每个事件追加到 `_tasks[taskId]["events_buffer"]`
- `api.py`: `/status/{task_id}` 返回 `live` 字段，带 `since` 参数只返回新事件
- `config.py`: `BacktestResult.to_dict()` 已包含 `sharpe/alpha/beta/sortino/calmar`（已 done）

### Frontend Data Flow

1. **Polling loop** (StrategyBacktest): 每 2 秒调用 `/status/{task_id}`，合并 `live.events` 到本地事件列表，更新 `live.positions`，刷新 mini metrics
2. **On completion**: 调用 `/result/{task_id}` 获取完整 BacktestResult，缓存到本地状态
3. **Full report**: 从缓存的 BacktestResult 渲染所有图表和表格，不再次请求

## Components

### 1. Running Panel (现有 tab 内扩展)

**文件:** `frontend/src/views/StrategyBacktest/RunningPanel.vue` (新建，从 index.vue 抽取右侧面板)

**结构:**
```
┌─ Config Bar (日期/资金/频率 只读显示) ──────────┐
├─ Mini Metrics Grid (2×3 = 6 cards) ──────────────┤
│  累计收益 │ 最大回撤 │ 当前仓位                    │
│  持仓市值 │ 累计成交 │ 今日信号                    │
├─ Tabs ───────────────────────────────────────────┤
│  📡 事件流 │ 📊 持仓 │ 📈 指标 │ 📋 日志        │
├─ Tab Content (flex:1, overflow-y:auto) ──────────┤
└─ Bottom: [运行中...] / [📊 查看完整报告] ────────┘
```

**事件流 tab:** 彩色终端风格日志流，不同事件类型用不同颜色：
- `ENGINE_START/END` → `#38bdf8` (cyan)
- `BEFORE/AFTER_TRADING` → `#a78bfa` (violet)
- `BAR` → `#e2e2ea` (white)
- `ORDER_PASS / TRADE` → `#d93026` (A-share red)
- `ORDER_REJECT` → `#137333` (A-share green)
- 系统信息 → `#8888a0` (muted)

**持仓 tab:** 当前持仓表格，列：标的/股数/均价/市价/浮盈/浮盈百分比

**指标 tab:** 实时滚动的 mini 指标面板（与 mini cards 相同指标但有更大字号和趋势指示）

**日志 tab:** 后端 log 输出（已有）

### 2. Full Report Overlay

**文件:** `frontend/src/views/StrategyBacktest/ReportOverlay.vue` (新建)

**触发:** `v-model:visible` 控制，`el-dialog` fullscreen 模式

**结构 — 5 个可滚动 section:**

#### Section 1: 指标总览
- 4×3 = 12 格指标卡
- 指标列表: 总收益率, 年化收益, Sharpe, Sortino, 最大回撤, Calmar, Alpha, Beta, 信息比率, 胜率, 总成交笔数, 最终资金
- A股配色: 正值 `#d93026` (红), 负值 `#137333` (绿), 中性值 `#e2e2ea`

#### Section 2: 净值曲线
- ECharts 面积图: 策略净值线 + 基准净值线(可选) + 回撤红底区域
- X轴: 日期, Y轴左: 净值, Y轴右(可选): 回撤%
- 悬浮 tooltip: 日期/净值/日收益

#### Section 3: 成交明细
- `el-table` 带分页（每页 50 条）
- 列: 日期, 标的, 方向(买/卖用红/绿标签), 价格, 数量, 金额, 手续费

#### Section 4: 持仓 FIFO 分析
- 按标的分组展开，每个标的显示:
  - 当前持仓汇总（总股数/均价/浮盈）
  - 明细 lot 列表: 买入日期, 股数, 成本价, 当前价, 盈亏
  - FIFO 卖出顺序可视化（已平仓的 lots 灰色 + 划线）

#### Section 5: 风控日志
- 被拒订单表格: 时间, 订单ID, 标的, 拒绝原因, 详情
- 底部统计: 共 N 笔被拒 · 资金不足: X · T+1: Y · 价格异常: Z

### 3. StrategyBacktest 重构

**文件:** `frontend/src/views/StrategyBacktest/index.vue` (修改)

将 `backtestRunner` tab 的内容重构为:
- 左侧编辑器（保持不变）
- 右侧 `<RunningPanel>` 组件（新建）
- 底部 `<ReportOverlay>` 组件（新建）

RunningPanel 的 props:
```ts
interface RunningPanelProps {
  taskId: string | null
  running: boolean
  completed: boolean
  liveData: LiveData | null
  fullResult: BacktestResult | null
}
```

## Design Tokens (新增/修改)

### design-system.css 变更

```css
:root {
  /* A股红涨绿跌 — 翻转原有语义色 */
  --color-bull: #d93026;        /* 原是 #22c55e */
  --color-bear: #137333;        /* 原是 #ef4444 */
  --accent-success: #d93026;    /* 原是 #22c55e */
  --accent-danger: #137333;     /* 原是 #ef4444 */

  /* 新增 — 卡片标签文字 */
  --text-label: #b0b0c0;        /* 小卡片标题专用 */
}
```

### Element Plus 覆盖更新

```css
.el-button--success { background: linear-gradient(135deg, #d93026, #c53026); }
.el-button--danger  { background: linear-gradient(135deg, #137333, #0e5c28); }
.el-tag--success { background: rgba(217, 48, 38, 0.15); border-color: rgba(217, 48, 38, 0.3); color: #d93026; }
.el-tag--danger  { background: rgba(19, 115, 51, 0.15); border-color: rgba(19, 115, 51, 0.3); color: #137333; }
```

### 现有页面兼容性

- `FactorBoard.vue` 已使用内联 `.positive { color: #d93026 }` `.negative { color: #137333 }`，不受影响
- Element Plus 组件全局颜色会翻转，需要确认无不良影响区域（如 monitor 页面的 success/danger 标签）

## Files Changed

| File | Action | Description |
|------|--------|-------------|
| `frontend/src/views/StrategyBacktest/RunningPanel.vue` | **New** | 实时运行面板（事件流+持仓+指标+日志） |
| `frontend/src/views/StrategyBacktest/ReportOverlay.vue` | **New** | 全屏回测报告（指标总览+净值图+成交+持仓+风控） |
| `frontend/src/views/StrategyBacktest/index.vue` | Modify | 集成 RunningPanel + ReportOverlay，抽取编辑器逻辑 |
| `frontend/src/styles/design-system.css` | Modify | 翻转 bull/bear 颜色 + 新增 `--text-label` |
| `frontend/src/api/backtest.ts` | Modify | 新增 `getStatus(taskId)` 类型定义 |
| `backend/app/backtest/runner.py` | Modify | event loop 中写入 live data 到 `_tasks` |
| `backend/app/backtest/api.py` | Modify | `/status/{task_id}` 返回 live 字段 |

## Implementation Order

1. **Backend live data** — runner + api 改动（先有数据源）
2. **Design tokens** — A-share 配色翻转（影响全局，先做）
3. **RunningPanel** — 事件流 + 持仓 + mini cards
4. **ReportOverlay** — 5-section 完整报告
5. **StrategyBacktest integration** — 重构 index.vue 集成新组件
6. **Verify** — 跑回测确认端到端

## Self-Review

- ✅ No TBD/TODO placeholders
- ✅ All component props/types defined
- ✅ API response format specified with exact fields
- ✅ Color values are exact (not "dark red" but `#d93026`)
- ✅ A-share convention applied consistently
- ✅ Existing page compatibility assessed
- ✅ Implementation order is logical (backend first, then tokens, then components)
