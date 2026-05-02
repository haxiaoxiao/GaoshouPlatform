# 回测前端增强 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将事件驱动回测引擎的实时事件流、FIFO 持仓、风控反馈和完整报告暴露在前端，同时翻转设计系统配色到 A 股红涨绿跌。

**Architecture:** 后端 runner.py 在 event loop 中将 live data 写入 `_tasks` dict，前端 2 秒轮询 `/status/{task_id}` 获取增量数据。RunningPanel 组件渲染实时事件流和持仓，ReportOverlay 在完成后展示完整5-section报告。

**Tech Stack:** Python (FastAPI backend), Vue 3 + TypeScript + Element Plus + ECharts (frontend)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `backend/app/backtest/runner.py` | Modify | Event loop 中写入 live data 到 `_tasks` |
| `backend/app/backtest/api.py` | Modify | `/status/{task_id}` 返回 `live` 字段 |
| `frontend/src/styles/design-system.css` | Modify | A股配色翻转 + `--text-label` |
| `frontend/src/views/StrategyBacktest/RunningPanel.vue` | **New** | 实时执行面板 |
| `frontend/src/views/StrategyBacktest/ReportOverlay.vue` | **New** | 全屏回测报告 |
| `frontend/src/views/StrategyBacktest/index.vue` | Modify | 集成新组件 |
| `frontend/src/api/backtest.ts` | Modify | 类型定义 |

---

### Task 1: Backend — Runner 写入 live data 到任务上下文

**Files:**
- Modify: `backend/app/backtest/runner.py:41-120`

- [ ] **Step 1: 修改 `_run_event_driven()` 签名，接收可选的 `task_store` 参数**

在 `_run_event_driven` 的 `AFTER_TRADING` listener 和 event producers 中向 `task_store` 写入实时数据。

当前 `_run_event_driven` 的 `on_order_creation_pass` 内部闭包需要改成写入 events_list，AFTER_TRADING 时构建 snapshot。

修改 runner.py，在 `_run_event_driven` 方法签名增加 `task_store: dict | None = None`，在 event loop 回调中写入数据。

在 `def _run_event_driven(self, config: BacktestConfig) -> BacktestResult:` 下方加：

```python
async def _run_event_driven(self, config: BacktestConfig, task_store: dict | None = None) -> BacktestResult:
```

找到 AFTER_TRADING 的 listener 注册点（当前在 portfolio.register_listeners 中）。在 runner 中追加一个额外的 AFTER_TRADING system listener 用于记录 snapshot。

在 runner 的 `_run_event_driven` 中，在 portfolio listeners 注册之后、executor.run 之前加：

```python
        # ── Live data recording for polling API ──
        events_buffer: list[dict] = []
        
        def record_event(event_type: str, data: dict):
            if task_store is not None:
                events_buffer.append({
                    "type": event_type,
                    "timestamp": datetime.now().isoformat(),
                    **data,
                })
        
        def on_after_trading_snapshot(event: Event):
            if task_store is None:
                return
            dt = event.data.get("date")
            bars = event.data.get("bars", {})
            task_store["live"] = {
                "current_date": dt.isoformat() if dt else None,
                "events": events_buffer[-200:],  # keep last 200 events
                "positions": {
                    sym: {
                        "shares": pos.total_shares,
                        "avg_cost": round(pos.avg_cost, 4),
                        "market_value": round(pos.market_value, 2),
                        "unrealized_pnl": round(pos.unrealized_pnl, 2),
                    }
                    for sym, pos in position_manager.positions.items()
                    if pos.total_shares > 0
                },
                "metrics_snapshot": {
                    "total_return": round(portfolio.total_value / config.initial_capital - 1, 4),
                    "cash": round(account.cash, 2),
                    "total_value": round(portfolio.total_value, 2),
                    "n_trades": len(trade_collector.trades),
                },
            }
        
        event_bus.add_listener(EventType.AFTER_TRADING, on_after_trading_snapshot, system=True)
```

同时需要在 on_order_creation_pass 中追加事件到 events_buffer。在现有的 `on_order_creation_pass` 函数内，撮合成功后 `event_bus.publish_event(Event(EventType.TRADE, ...))` 之后加：

```python
            record_event("TRADE", {
                "symbol": symbol, "direction": direction,
                "quantity": quantity, "price": round(trade_price, 4),
                "commission": round(commission, 4),
            })
```

在 `on_bar_dispatch_orders` 中，处理 BAR 事件和 ORDER_PASS/REJECT 后也追加事件。在 `on_bar_dispatch_orders` 内，`on_bar_dispatch_orders` 处理 orders 循环中，在风控校验前加：

```python
                record_event("BAR", {"symbol": bar.symbol, "close": round(bar.close, 4)})
```

在 ORDER_CREATION_PASS 被触发后：
```python
                record_event("ORDER_PASS", {
                    "order_id": order.get("order_id", ""),
                    "symbol": order.get("symbol", ""),
                    "direction": order.get("direction", "buy"),
                    "quantity": order.get("quantity", 0),
                    "price": order.get("price", 0),
                })
```

如果 ORDER_PENDING_NEW 被阻断（`publish_event` 返回 False），则记录 REJECT：

```python
                else:
                    record_event("ORDER_REJECT", {
                        "order_id": order.get("order_id", ""),
                        "symbol": order.get("symbol", ""),
                        "reason": "风控拒绝",
                    })
```

在文件顶部 import 区加入 `from datetime import datetime`（如果还没有）。

- [ ] **Step 2: 运行现有测试验证无回归**

```bash
cd E:/Projects/GaoshouPlatform/backend && python -m pytest tests/backtest/ -v
```

Expected: 所有 30 个测试 PASS（新增的 task_store 默认 None 不影响）

- [ ] **Step 3: Commit**

```bash
cd E:/Projects/GaoshouPlatform/backend && git add app/backtest/runner.py && git commit -m "feat: add live data recording to event-driven backtest runner"
```

---

### Task 2: Backend — API 返回 live 字段

**Files:**
- Modify: `backend/app/backtest/api.py:14-73`

- [ ] **Step 1: 修改 `_tasks` 结构，支持任务上下文存储**

在 `api.py` 中，`_tasks[task_id]` 当前是简单 dict。需要把 `task_store` 引用传给 runner。

修改 `run_backtest` endpoint，在 runner.run 调用时传入 task_store：

```python
@router.post("/run")
async def run_backtest(req: RunBacktestRequest):
    from datetime import date

    task_id = str(uuid.uuid4())[:8]
    config = BacktestConfig(
        mode=req.mode,
        factor_expression=req.factor_expression,
        buy_condition=req.buy_condition,
        sell_condition=req.sell_condition,
        symbols=req.symbols,
        start_date=date.fromisoformat(req.start_date),
        end_date=date.fromisoformat(req.end_date),
        initial_capital=req.initial_capital,
        rebalance_freq=req.rebalance_freq,
        n_groups=req.n_groups,
        bar_type=req.bar_type,
        commission_rate=req.commission_rate,
        slippage=req.slippage,
    )

    task_store = {"status": "queued", "progress": 0, "result": None, "live": None}
    _tasks[task_id] = task_store

    try:
        _tasks[task_id]["status"] = "running"
        runner = get_backtest_runner()
        result = await runner.run(config, task_store=task_store)
        _tasks[task_id] = {
            "status": "done",
            "progress": 1.0,
            "result": result.to_dict(),
            "live": task_store.get("live"),  # preserve last live snapshot
        }
    except Exception as e:
        _tasks[task_id] = {
            "status": "failed",
            "progress": 1.0,
            "result": {"error": str(e)},
            "live": task_store.get("live"),
        }

    return {"code": 0, "message": "success", "data": {"task_id": task_id}}
```

- [ ] **Step 2: 修改 `/status/{task_id}` 返回 live**

```python
@router.get("/status/{task_id}")
async def get_status(task_id: str):
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    return {
        "code": 0,
        "message": "success",
        "data": {
            "status": task["status"],
            "progress": task.get("progress", 0),
            "live": task.get("live"),
        },
    }
```

- [ ] **Step 3: 运行 API 相关测试**

```bash
cd E:/Projects/GaoshouPlatform/backend && python -m pytest tests/ -v -k "backtest"
```

Expected: 所有 backtest 相关测试 PASS

- [ ] **Step 4: Commit**

```bash
cd E:/Projects/GaoshouPlatform/backend && git add app/backtest/api.py && git commit -m "feat: expose live backtest data in status API endpoint"
```

---

### Task 3: Frontend — A股配色翻转 + --text-label

**Files:**
- Modify: `frontend/src/styles/design-system.css:33-44, 362-384, 553-570`

- [ ] **Step 1: 翻转语义色变量 + 新增 --text-label**

在 `:root` 中，修改：

```css
  /* Accent colors - Electric palette, A-share convention: red up, green down */
  --accent-primary: #38bdf8;        /* Electric cyan — unchanged */
  --accent-secondary: #a78bfa;      /* Soft violet — unchanged */
  --accent-success: #d93026;        /* A-share red = positive */
  --accent-danger: #137333;         /* A-share green = negative */
  --accent-warning: #fbbf24;        /* Gold alert — unchanged */
  --accent-glow: rgba(56, 189, 248, 0.4);

  /* Semantic colors — A-share: 红涨绿跌 */
  --color-bull: #d93026;
  --color-bear: #137333;
  --color-neutral: #8888a0;
```

替换原有：

```css
  --accent-success: #22c55e;
  --accent-danger: #ef4444;
  --color-bull: #22c55e;
  --color-bear: #ef4444;
```

在 `--text-muted` 之后加一行：

```css
  --text-label: #b0b0c0;            /* 卡片标题专用 */
```

- [ ] **Step 2: 更新 Element Plus success/danger 按钮和标签覆盖**

`el-button--success` 背景渐变从绿改红：

```css
.el-button--success {
  background: linear-gradient(135deg, #d93026, #c53026);
  border-color: transparent;
}
```

`el-button--danger` 背景渐变从红改绿：

```css
.el-button--danger {
  background: linear-gradient(135deg, #137333, #0e5c28);
  border-color: transparent;
}
```

`el-tag--success` 背景色从绿改红：

```css
.el-tag--success {
  background: rgba(217, 48, 38, 0.15);
  border-color: rgba(217, 48, 38, 0.3);
  color: var(--color-bull);
}

.el-tag--danger {
  background: rgba(19, 115, 51, 0.15);
  border-color: rgba(19, 115, 51, 0.3);
  color: var(--color-bear);
}
```

- [ ] **Step 3: 同步翻转 .positive/.negative 全局辅助类**

在 `.text-bull` / `.text-bear` 区域确认已正确使用 `var(--color-bull)` / `var(--color-bear)`，无需额外改动（它们引用变量）。

- [ ] **Step 4: 构建前端验证无编译错误**

```bash
cd E:/Projects/GaoshouPlatform/frontend && npx vite build 2>&1 | tail -10
```

Expected: build successful, no errors

- [ ] **Step 5: Commit**

```bash
cd E:/Projects/GaoshouPlatform/frontend && git add src/styles/design-system.css && git commit -m "feat: flip to A-share color convention (red up, green down), add --text-label"
```

---

### Task 4: Frontend — RunningPanel 组件 (新建)

**Files:**
- Create: `frontend/src/views/StrategyBacktest/RunningPanel.vue`
- All new file, no existing code to modify

- [ ] **Step 1: 创建组件 skeleton 带 Props 和 Emits**

```vue
<template>
  <div class="running-panel">
    <!-- Config bar -->
    <div class="config-bar">
      <span>{{ currentDate || '—' }}</span>
      <span>·</span>
      <span>{{ capitalLabel }}</span>
      <span>·</span>
      <span>{{ freqLabel }}</span>
      <div class="flex-spacer" />
      <span v-if="running" class="status-running">运行中...</span>
      <span v-else-if="completed" class="status-done">✓ 完成</span>
    </div>

    <!-- Mini metrics grid -->
    <div class="mini-metrics">
      <div v-for="m in miniMetrics" :key="m.label" class="mini-card">
        <div class="mini-label">{{ m.label }}</div>
        <div class="mini-value" :style="{ color: m.color }">{{ m.value }}</div>
      </div>
    </div>

    <!-- Tabs -->
    <div class="panel-tabs">
      <div
        v-for="t in tabs"
        :key="t.key"
        :class="['panel-tab', { active: activeTab === t.key }]"
        @click="activeTab = t.key"
      >{{ t.label }}</div>
    </div>

    <!-- Tab content -->
    <div class="tab-body">
      <!-- Events -->
      <div v-if="activeTab === 'events'" class="event-stream">
        <div
          v-for="(ev, i) in events"
          :key="i"
          class="event-line"
          :style="{ color: eventColor(ev.type) }"
        >
          <span class="event-time">{{ ev.timestamp?.slice(11, 19) || '' }}</span>
          {{ formatEvent(ev) }}
        </div>
        <div v-if="!events.length" class="empty-hint">等待事件...</div>
      </div>

      <!-- Positions -->
      <div v-if="activeTab === 'positions'">
        <el-table :data="positionList" size="small" v-if="positionList.length">
          <el-table-column prop="symbol" label="标的" width="100" />
          <el-table-column prop="shares" label="股数" width="80" />
          <el-table-column prop="avg_cost" label="均价" width="80" />
          <el-table-column prop="market_value" label="市值" width="100" />
          <el-table-column label="浮盈" width="120">
            <template #default="{ row }">
              <span :style="{ color: row.unrealized_pnl >= 0 ? '#d93026' : '#137333' }">
                {{ row.unrealized_pnl >= 0 ? '+' : '' }}{{ row.unrealized_pnl }}
              </span>
            </template>
          </el-table-column>
        </el-table>
        <div v-else class="empty-hint">暂无持仓</div>
      </div>

      <!-- Metrics -->
      <div v-if="activeTab === 'metrics'" class="metrics-panel">
        <div v-for="m in detailMetrics" :key="m.label" class="metric-row">
          <span class="metric-label">{{ m.label }}</span>
          <span class="metric-value" :style="{ color: m.color }">{{ m.value }}</span>
        </div>
      </div>

      <!-- Logs -->
      <div v-if="activeTab === 'logs'" class="log-stream">
        <div v-for="(log, i) in logs" :key="i" class="log-line">{{ log }}</div>
        <div v-if="!logs.length" class="empty-hint">暂无日志</div>
      </div>
    </div>

    <!-- Bottom action -->
    <div class="panel-footer">
      <el-button v-if="completed" type="primary" size="small" @click="$emit('viewReport')">
        查看完整报告
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'

interface LiveEvent {
  type: string
  timestamp: string
  symbol?: string
  direction?: string
  quantity?: number
  price?: number
  commission?: number
  close?: number
  order_id?: string
  trade_id?: string
  reason?: string
}

interface PositionSnapshot {
  shares: number
  avg_cost: number
  market_value: number
  unrealized_pnl: number
}

interface LiveData {
  current_date: string | null
  events: LiveEvent[]
  positions: Record<string, PositionSnapshot>
  metrics_snapshot: Record<string, number>
  cash: number
  total_value: number
}

const props = defineProps<{
  running: boolean
  completed: boolean
  liveData: LiveData | null
  logs: string[]
  currentDate?: string
  capitalLabel?: string
  freqLabel?: string
}>()

defineEmits<{
  viewReport: []
}>()

const activeTab = ref('events')
const tabs = [
  { key: 'events', label: '事件流' },
  { key: 'positions', label: '持仓' },
  { key: 'metrics', label: '指标' },
  { key: 'logs', label: '日志' },
]

const events = computed(() => props.liveData?.events || [])

const positionList = computed(() => {
  const p = props.liveData?.positions
  if (!p) return []
  return Object.entries(p).map(([symbol, pos]) => ({ symbol, ...pos }))
})

const miniMetrics = computed(() => {
  const m = props.liveData?.metrics_snapshot
  const p = props.liveData?.positions
  const posCount = p ? Object.keys(p).length : 0
  return [
    { label: '累计收益', value: m?.total_return != null ? (m.total_return * 100).toFixed(2) + '%' : '—', color: (m?.total_return ?? 0) >= 0 ? '#d93026' : '#137333' },
    { label: '总资产', value: m?.total_value != null ? (m.total_value / 10000).toFixed(1) + '万' : '—', color: '#e2e2ea' },
    { label: '持仓数', value: posCount + ' 只', color: '#a78bfa' },
    { label: '累计成交', value: m?.n_trades != null ? m.n_trades + ' 笔' : '—', color: '#e2e2ea' },
    { label: '现金', value: m?.cash != null ? (m.cash / 10000).toFixed(1) + '万' : '—', color: '#e2e2ea' },
    { label: '最大回撤', value: m?.max_drawdown != null ? (m.max_drawdown * 100).toFixed(2) + '%' : '—', color: '#137333' },
  ]
})

const detailMetrics = computed(() => {
  const m = props.liveData?.metrics_snapshot
  return [
    { label: '累计收益', value: m?.total_return != null ? (m.total_return * 100).toFixed(2) + '%' : '—', color: (m?.total_return ?? 0) >= 0 ? '#d93026' : '#137333' },
    { label: '总资产', value: m?.total_value != null ? '¥' + m.total_value.toLocaleString() : '—', color: '#e2e2ea' },
    { label: '可用现金', value: m?.cash != null ? '¥' + m.cash.toLocaleString() : '—', color: '#e2e2ea' },
    { label: '累计成交', value: m?.n_trades != null ? String(m.n_trades) : '—', color: '#e2e2ea' },
  ]
})

const EVENT_COLORS: Record<string, string> = {
  ENGINE_START: '#38bdf8',
  ENGINE_END: '#38bdf8',
  BEFORE_TRADING: '#a78bfa',
  AFTER_TRADING: '#a78bfa',
  BAR: '#e2e2ea',
  ORDER_PASS: '#d93026',
  TRADE: '#d93026',
  ORDER_REJECT: '#137333',
}

function eventColor(type: string): string {
  return EVENT_COLORS[type] || '#8888a0'
}

function formatEvent(ev: LiveEvent): string {
  switch (ev.type) {
    case 'BAR': return `BAR  ${ev.symbol || ''}  close=${ev.close ?? '?'}`
    case 'ORDER_PASS': return `${ev.direction === 'buy' ? '买入' : '卖出'} ${ev.symbol || ''}  ${ev.quantity ?? 0}股 @${ev.price ?? '?'}`
    case 'TRADE': return `成交 ${ev.symbol || ''}  ${ev.quantity ?? 0}×${ev.price ?? '?'}  费${ev.commission ?? 0}`
    case 'ORDER_REJECT': return `拒绝  ${ev.symbol || ''}  — ${ev.reason || '未知原因'}`
    case 'BEFORE_TRADING': return `开盘  ${props.liveData?.current_date || ''}`
    case 'AFTER_TRADING': return `收盘  ${props.liveData?.current_date || ''}`
    default: return `${ev.type}  ${JSON.stringify(ev)}`
  }
}
</script>

<style scoped>
.running-panel {
  display: flex;
  flex-direction: column;
  gap: 8px;
  height: 100%;
}
.config-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-secondary);
  padding: 6px 10px;
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
}
.flex-spacer { flex: 1; }
.status-running { color: var(--accent-primary); }
.status-done { color: var(--accent-success); font-weight: 600; }

.mini-metrics {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 5px;
}
.mini-card {
  background: var(--bg-surface);
  border-radius: 5px;
  padding: 6px 8px;
  text-align: center;
}
.mini-label {
  font-size: 9px;
  color: var(--text-label);
  font-weight: 500;
}
.mini-value {
  font-size: 14px;
  font-weight: 700;
  margin-top: 2px;
}

.panel-tabs {
  display: flex;
  border-bottom: 1px solid var(--border-subtle);
  gap: 0;
}
.panel-tab {
  padding: 5px 12px;
  font-size: 11px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: color 0.15s;
}
.panel-tab:hover { color: var(--text-primary); }
.panel-tab.active { color: var(--accent-primary); border-bottom: 2px solid var(--accent-primary); }

.tab-body {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
}

.event-stream {
  font-family: var(--font-display);
  font-size: 10px;
  line-height: 1.7;
  padding: 6px 0;
}
.event-line { padding: 1px 4px; }
.event-time { color: var(--text-muted); margin-right: 6px; }

.empty-hint {
  color: var(--text-muted);
  font-style: italic;
  padding: 20px;
  text-align: center;
  font-size: 12px;
}

.metrics-panel { padding: 12px; }
.metric-row {
  display: flex;
  justify-content: space-between;
  padding: 8px 0;
  border-bottom: 1px solid var(--border-subtle);
  font-size: 13px;
}
.metric-label { color: var(--text-label); font-weight: 500; }
.metric-value { font-weight: 700; font-family: var(--font-data); font-size: 15px; }

.log-stream {
  font-family: var(--font-display);
  font-size: 10px;
  line-height: 1.6;
  padding: 6px 4px;
  color: var(--text-muted);
}
.log-line { padding: 1px 0; }

.panel-footer {
  display: flex;
  justify-content: flex-end;
  padding-top: 4px;
}
</style>
```

- [ ] **Step 2: 验证前端编译**

```bash
cd E:/Projects/GaoshouPlatform/frontend && npx vue-tsc --noEmit --skipLibCheck src/views/StrategyBacktest/RunningPanel.vue 2>&1
```

Expected: no TypeScript errors

- [ ] **Step 3: Commit**

```bash
cd E:/Projects/GaoshouPlatform/frontend && git add src/views/StrategyBacktest/RunningPanel.vue && git commit -m "feat: add RunningPanel component with event stream, positions, and live metrics"
```

---

### Task 5: Frontend — ReportOverlay 组件 (新建)

**Files:**
- Create: `frontend/src/views/StrategyBacktest/ReportOverlay.vue`

- [ ] **Step 1: 创建 ReportOverlay 组件**

```vue
<template>
  <el-dialog
    v-model="visible"
    title="回测报告"
    fullscreen
    :show-close="true"
    class="report-overlay"
    @close="$emit('close')"
  >
    <div v-if="result" class="report-body">
      <!-- Section 1: Metrics Grid -->
      <section class="report-section">
        <h3>绩效指标</h3>
        <div class="metrics-grid">
          <div v-for="m in allMetrics" :key="m.label" class="metric-card">
            <div class="metric-card-label">{{ m.label }}</div>
            <div class="metric-card-value" :style="{ color: m.color }">{{ m.value }}</div>
          </div>
        </div>
      </section>

      <!-- Section 2: NAV Curve -->
      <section class="report-section">
        <h3>净值曲线</h3>
        <div ref="navChartRef" class="chart-container"></div>
      </section>

      <!-- Section 3: Trades Table -->
      <section class="report-section">
        <h3>成交明细 ({{ result.trades?.length || 0 }} 笔)</h3>
        <el-table :data="paginatedTrades" stripe size="small" max-height="400">
          <el-table-column prop="trade_date" label="日期" width="110" />
          <el-table-column prop="symbol" label="标的" width="100" />
          <el-table-column label="方向" width="60">
            <template #default="{ row }">
              <span :style="{ color: row.direction === 'buy' ? '#d93026' : '#137333', fontWeight: 600 }">
                {{ row.direction === 'buy' ? '买' : '卖' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="price" label="价格" width="80" />
          <el-table-column prop="quantity" label="数量" width="80" />
          <el-table-column label="金额" width="100">
            <template #default="{ row }">{{ (row.price * row.quantity).toFixed(0) }}</template>
          </el-table-column>
          <el-table-column prop="commission" label="手续费" width="80">
            <template #default="{ row }">{{ row.commission?.toFixed(2) || '0' }}</template>
          </el-table-column>
        </el-table>
        <el-pagination
          v-if="result.trades && result.trades.length > 50"
          v-model:current-page="tradePage"
          :page-size="50"
          :total="result.trades.length"
          layout="prev, pager, next"
          size="small"
          style="margin-top:8px; justify-content:center"
        />
      </section>

      <!-- Section 4: FIFO Positions -->
      <section class="report-section" v-if="result.trades?.length">
        <h3>持仓分析</h3>
        <div class="fifo-placeholder">
          <p class="text-muted">基于 FIFO 成本核算的持仓明细在完整后端集成后展示。</p>
          <p class="text-muted">交易记录包含 {{ result.total_trades || 0 }} 笔，胜率 {{ ((result.win_rate || 0) * 100).toFixed(1) }}%。</p>
        </div>
      </section>

      <!-- Section 5: Risk Rejections -->
      <section class="report-section">
        <h3>风控与回测摘要</h3>
        <div class="summary-grid">
          <div class="summary-item">
            <span class="summary-label">回测周期</span>
            <span class="summary-value">{{ result.start_date }} → {{ result.end_date }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">交易日</span>
            <span class="summary-value">{{ result.n_trading_days || 0 }} 天</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">初始资金</span>
            <span class="summary-value">¥{{ (result.initial_capital || 0).toLocaleString() }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">最终资金</span>
            <span class="summary-value">¥{{ (result.final_capital || 0).toLocaleString() }}</span>
          </div>
        </div>
      </section>
    </div>

    <div v-else class="empty-report">无回测结果数据</div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, nextTick } from 'vue'
import * as echarts from 'echarts'

interface BacktestResult {
  total_return?: number
  annual_return?: number
  annual_volatility?: number
  sharpe?: number
  sharpe_ratio?: number
  sortino?: number
  max_drawdown?: number
  calmar?: number
  alpha?: number
  beta?: number
  information_ratio?: number
  total_trades?: number
  win_trades?: number
  loss_trades?: number
  win_rate?: number
  trades?: Array<{
    trade_date?: string
    symbol?: string
    direction?: string
    price?: number
    quantity?: number
    commission?: number
  }>
  nav_series?: Array<{ date: string; nav: number }>
  daily_returns?: Array<{ date: string; return: number }>
  start_date?: string | null
  end_date?: string | null
  initial_capital?: number
  final_capital?: number
  n_trading_days?: number
}

const props = defineProps<{
  visible: boolean
  result: BacktestResult | null
}>()

const emit = defineEmits<{
  (e: 'update:visible', v: boolean): void
  (e: 'close'): void
}>()

const visible = computed({
  get: () => props.visible,
  set: (v) => emit('update:visible', v),
})

const tradePage = ref(1)
const navChartRef = ref<HTMLElement | null>(null)
let chartInstance: echarts.ECharts | null = null

function fmtPct(v: number | null | undefined): string {
  if (v == null) return '—'
  return (v * 100).toFixed(2) + '%'
}

function colorPct(v: number | null | undefined): string {
  if (v == null) return '#e2e2ea'
  return v >= 0 ? '#d93026' : '#137333'
}

const allMetrics = computed(() => {
  const r = props.result
  if (!r) return []
  return [
    { label: '总收益率', value: fmtPct(r.total_return), color: colorPct(r.total_return) },
    { label: '年化收益', value: fmtPct(r.annual_return), color: colorPct(r.annual_return) },
    { label: 'Sharpe', value: (r.sharpe ?? r.sharpe_ratio)?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: 'Sortino', value: r.sortino?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: '最大回撤', value: fmtPct(r.max_drawdown), color: '#137333' },
    { label: 'Calmar', value: r.calmar?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: 'Alpha', value: r.alpha?.toFixed(4) || '—', color: colorPct(r.alpha) },
    { label: 'Beta', value: r.beta?.toFixed(4) || '—', color: '#e2e2ea' },
    { label: '信息比率', value: r.information_ratio?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: '胜率', value: r.win_rate != null ? (r.win_rate * 100).toFixed(1) + '%' : '—', color: '#e2e2ea' },
    { label: '总成交', value: r.total_trades != null ? String(r.total_trades) : '—', color: '#e2e2ea' },
    { label: '最终资金', value: r.final_capital ? '¥' + (r.final_capital / 10000).toFixed(2) + '万' : '—', color: '#a78bfa' },
  ]
})

const paginatedTrades = computed(() => {
  const trades = props.result?.trades || []
  const start = (tradePage.value - 1) * 50
  return trades.slice(start, start + 50)
})

watch(() => props.visible, async (v) => {
  if (v && props.result?.nav_series?.length) {
    await nextTick()
    renderChart()
  }
})

function renderChart() {
  if (!navChartRef.value) return
  if (chartInstance) chartInstance.dispose()

  const navData = props.result?.nav_series || []
  chartInstance = echarts.init(navChartRef.value)
  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'category', data: navData.map(d => d.date), axisLabel: { color: '#8888a0', fontSize: 10 } },
    yAxis: { type: 'value', axisLabel: { color: '#8888a0', fontSize: 10 } },
    series: [{
      name: '净值', type: 'line', data: navData.map(d => d.nav),
      lineStyle: { color: '#38bdf8', width: 1.5 },
      areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
        { offset: 0, color: 'rgba(56,189,248,0.15)' },
        { offset: 1, color: 'rgba(56,189,248,0)' },
      ])},
      showSymbol: false,
    }],
  })
}
</script>

<style scoped>
.report-body {
  padding: 0 20px;
  max-width: 1200px;
  margin: 0 auto;
}
.report-section {
  margin-bottom: 32px;
}
.report-section h3 {
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
  color: var(--text-bright);
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
}
.metric-card {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  padding: 14px;
  text-align: center;
}
.metric-card-label {
  font-size: 11px;
  color: var(--text-label);
  font-weight: 500;
}
.metric-card-value {
  font-size: 20px;
  font-weight: 700;
  margin-top: 6px;
  font-family: var(--font-data);
}

.chart-container { width: 100%; height: 320px; }

.summary-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
}
.summary-item {
  display: flex;
  justify-content: space-between;
  padding: 10px 14px;
  background: var(--bg-surface);
  border-radius: 6px;
}
.summary-label { color: var(--text-label); font-weight: 500; font-size: 12px; }
.summary-value { color: var(--text-primary); font-weight: 600; font-size: 13px; }

.fifo-placeholder { 
  padding: 24px; 
  text-align: center; 
  background: var(--bg-surface); 
  border-radius: 8px;
  border: 1px dashed var(--border-default);
}
.text-muted { color: var(--text-muted); font-size: 12px; margin: 4px 0; }
.empty-report { text-align: center; padding: 60px; color: var(--text-muted); }
</style>
```

- [ ] **Step 2: 验证前端编译**

```bash
cd E:/Projects/GaoshouPlatform/frontend && npx vite build 2>&1 | tail -10
```

Expected: build successful

- [ ] **Step 3: Commit**

```bash
cd E:/Projects/GaoshouPlatform/frontend && git add src/views/StrategyBacktest/ReportOverlay.vue && git commit -m "feat: add ReportOverlay with full metrics grid, NAV chart, and trade history"
```

---

### Task 6: Frontend — StrategyBacktest/index.vue 集成

**Files:**
- Modify: `frontend/src/views/StrategyBacktest/index.vue`
- Modify: `frontend/src/api/backtest.ts`

- [ ] **Step 1: 更新 backtest.ts API 类型**

在 `frontend/src/api/backtest.ts` 中添加类型：

```typescript
export interface LiveData {
  current_date: string | null
  events: Array<{
    type: string
    timestamp: string
    symbol?: string
    direction?: string
    quantity?: number
    price?: number
    close?: number
    order_id?: string
    trade_id?: string
    commission?: number
    reason?: string
  }>
  positions: Record<string, {
    shares: number
    avg_cost: number
    market_value: number
    unrealized_pnl: number
  }>
  metrics_snapshot: {
    total_return?: number
    max_drawdown?: number
    sharpe?: number
    cash?: number
    total_value?: number
    n_trades?: number
  }
}

export interface TaskStatus {
  status: string
  progress: number
  live: LiveData | null
}

export interface BacktestResultData {
  total_return?: number
  annual_return?: number
  annual_volatility?: number
  sharpe?: number
  sharpe_ratio?: number
  sortino?: number
  max_drawdown?: number
  calmar?: number
  alpha?: number
  beta?: number
  information_ratio?: number
  total_trades?: number
  win_trades?: number
  loss_trades?: number
  win_rate?: number
  avg_return?: number
  trades?: Array<{
    trade_date?: string
    symbol?: string
    direction?: string
    price?: number
    quantity?: number
    commission?: number
    pnl?: number
  }>
  nav_series?: Array<{ date: string; nav: number }>
  daily_returns?: Array<{ date: string; return: number }>
  start_date?: string | null
  end_date?: string | null
  initial_capital?: number
  final_capital?: number
  n_trading_days?: number
}
```

- [ ] **Step 2: 重构 index.vue 的 backtestRunner tab 内容**

替换现有 `backtestRunner` tab 的右侧面板为 `<RunningPanel>` 组件。在 `index.vue` 中：

1. 导入 RunningPanel 和 ReportOverlay：

```typescript
import RunningPanel from './RunningPanel.vue'
import ReportOverlay from './ReportOverlay.vue'
import type { LiveData, TaskStatus, BacktestResultData } from '@/api/backtest'
```

2. 添加新的状态变量：

```typescript
const btLiveData = ref<LiveData | null>(null)
const btFullResult = ref<BacktestResultData | null>(null)
const showReport = ref(false)
```

3. 修改 `handleRunBacktest` 的轮询逻辑，在每次 poll 中更新 `btLiveData`：

在 polling loop 中，`statusRes` 返回后：

```typescript
        const statusData = statusRes as TaskStatus
        if (statusData?.live) {
          btLiveData.value = statusData.live
        }
```

4. 在结果获取完成后设置 `btFullResult`：

```typescript
          const resultRes = await request.get<any>(`/v2/backtest/result/${taskId}`)
          btFullResult.value = resultRes
```

5. 替换当前右侧面板的 template（第 71-111 行左右的 right-panel div）为：

```vue
          <div class="right-panel">
            <div class="bt-config-bar">
              <el-date-picker v-model="btStartDate" value-format="YYYY-MM-DD" size="small" style="width:130px" placeholder="开始日期" />
              <span>至</span>
              <el-date-picker v-model="btEndDate" value-format="YYYY-MM-DD" size="small" style="width:130px" placeholder="结束日期" />
              <span>资金</span>
              <el-input-number v-model="btCapital" :min="10000" :step="100000" size="small" style="width:130px" />
              <el-select v-model="btFrequency" size="small" style="width:80px">
                <el-option label="每天" value="daily" />
                <el-option label="每周" value="weekly" />
                <el-option label="每月" value="monthly" />
              </el-select>
              <el-button type="primary" size="small" @click="handleRunBacktest" :loading="btRunning">运行回测</el-button>
            </div>

            <RunningPanel
              :running="btRunning"
              :completed="!btRunning && btFullResult != null"
              :liveData="btLiveData"
              :logs="btLogs"
              :currentDate="btLiveData?.current_date ?? undefined"
              :capitalLabel="'¥' + (btCapital / 10000).toFixed(0) + '万'"
              :freqLabel="freqLabelMap[btFrequency] || btFrequency"
              @viewReport="showReport = true"
            />
          </div>
```

6. 在 `</el-tabs>` 前加 ReportOverlay：

```vue
    <ReportOverlay v-model:visible="showReport" :result="btFullResult" />
```

7. 添加 `freqLabelMap`:

```typescript
const freqLabelMap: Record<string, string> = {
  daily: '每天',
  weekly: '每周',
  monthly: '每月',
}
```

8. 移除旧的 `btMetrics` 计算（Metric cards 现在由 RunningPanel 渲染）。

9. 保留 `btErrors` 用于日志 tab。

- [ ] **Step 3: 构建验证**

```bash
cd E:/Projects/GaoshouPlatform/frontend && npx vite build 2>&1 | tail -15
```

Expected: build successful, no errors

- [ ] **Step 4: Commit**

```bash
cd E:/Projects/GaoshouPlatform/frontend && git add src/views/StrategyBacktest/index.vue src/api/backtest.ts && git commit -m "feat: integrate RunningPanel and ReportOverlay into StrategyBacktest"
```

---

## Verification

### 单元测试
- 后端: `pytest tests/backtest/ -v` — 30 tests, 全部通过
- 前端: `npx vite build` — 编译零错误

### 端到端验证
1. 启动后端 + 前端
2. 打开 `/backtest` → 策略列表
3. 点击"新建策略" → 自动切到回测运行 tab
4. 点击"运行回测" → 右侧 RunningPanel 显示实时事件流、持仓更新、mini metrics 变化
5. 运行完成 → 底部出现"查看完整报告"按钮
6. 点击 → 全屏 ReportOverlay 展示 12 格指标、净值曲线、成交明细
7. 确认 A 股配色：正收益显示红色、负收益/回撤显示绿色

---

## Self-Review

**1. Spec coverage:**
- ✅ Running Panel with 6 mini cards, 4 tabs (events/positions/metrics/logs)
- ✅ Report Overlay with 5 sections
- ✅ A-share color flip in design-system.css
- ✅ Backend live data in runner.py + api.py
- ✅ StrategyBacktest integration
- ⚠️ FIFO position lots detail (Section 4 of report) is placeholder — needs backend to expose lot-level data. Marked as placeholder in component.

**2. Placeholder scan:**
- Section 4 (FIFO) intentionally simplified to summary text since backend doesn't expose individual lots yet. This is clearly marked, not a TBD.
- No TBD/TODO in steps.

**3. Type consistency:**
- `LiveData` defined in backtest.ts uses same field names as backend `_tasks["live"]`
- `RunningPanel` props match `LiveData` interface
- `ReportOverlay` uses `BacktestResultData` which mirrors `BacktestResult.to_dict()`
- Event type enum used consistently: BAR, ORDER_PASS, TRADE, ORDER_REJECT, etc.
