<template>
  <div class="page-frame live-page">
    <header class="panel-card page-head">
      <div>
        <span class="section-kicker">TRADING GUARDRAILS</span>
        <h2>模拟 / 实盘</h2>
        <p>完美世界 / 昆仑万维网格信号；默认只看信号，真实下单必须显式开启并二次确认。</p>
      </div>
      <div class="actions">
        <el-switch v-model="autoRefresh" active-text="每分钟刷新" />
        <el-button :loading="loading" @click="loadAll">刷新</el-button>
      </div>
    </header>

    <section class="status-band">
      <div>
        <label>行情</label>
        <el-tag :type="status?.quote_connected ? 'success' : 'warning'">
          {{ status?.quote_connected ? '已连接' : '未确认' }}
        </el-tag>
      </div>
      <div>
        <label>交易模块</label>
        <el-tag :type="status?.xttrader_available ? 'success' : 'danger'">
          {{ status?.xttrader_available ? '可用' : '不可用' }}
        </el-tag>
      </div>
      <div>
        <label>账户</label>
        <strong>{{ status?.account_id || '-' }}</strong>
      </div>
      <div>
        <label>下单</label>
        <el-tag :type="status?.order_submit_enabled ? 'danger' : 'info'">
          {{ status?.order_submit_enabled ? '已启用' : '仅信号' }}
        </el-tag>
      </div>
    </section>

    <el-alert
      v-if="status?.order_submit_enabled"
      type="error"
      :closable="false"
      show-icon
      title="真实下单开关已开启：所有委托仍需在卡片内二次确认。"
      class="account-alert"
    />

    <section class="toolbar">
      <div class="field">
        <span>网格间距</span>
        <el-input-number v-model="params.grid_pct" :min="0.005" :max="0.1" :step="0.005" size="small" />
      </div>
      <div class="field">
        <span>中枢窗口</span>
        <el-input-number v-model="params.anchor_window_minutes" :min="30" :max="960" :step="30" size="small" />
      </div>
      <div class="field">
        <span>网格层数</span>
        <el-input-number v-model="params.max_grid_levels" :min="1" :max="20" size="small" />
      </div>
      <div class="field">
        <span>参考资金</span>
        <el-input-number v-model="params.initial_cash" :min="10000" :step="100000" size="small" />
      </div>
    </section>

    <el-alert
      v-if="data?.account?.error || status?.error"
      type="warning"
      :closable="false"
      show-icon
      :title="data?.account?.error || status?.error || ''"
      class="account-alert"
    />

    <el-alert
      v-if="data?.quote_error"
      type="warning"
      :closable="false"
      show-icon
      :title="`行情不可用，已降级为 NO_QUOTE：${data.quote_error}`"
      class="account-alert"
    />

    <section class="account-strip">
      <div>
        <label>账户来源</label>
        <strong>{{ data?.account.source || '-' }}</strong>
      </div>
      <div>
        <label>可用现金</label>
        <strong>{{ formatMoney(data?.account.cash) }}</strong>
      </div>
      <div>
        <label>总资产</label>
        <strong>{{ formatMoney(data?.account.total_asset) }}</strong>
      </div>
      <div>
        <label>市值</label>
        <strong>{{ formatMoney(data?.account.market_value) }}</strong>
      </div>
      <div>
        <label>更新时间</label>
        <strong>{{ data?.timestamp || '-' }}</strong>
      </div>
    </section>

    <div class="signal-grid">
      <article v-for="signal in data?.signals || []" :key="signal.symbol" class="signal-card">
        <div class="card-head">
          <div>
            <h3>{{ signal.name }}</h3>
            <span>{{ signal.symbol }}</span>
          </div>
          <el-tag :type="tagType(signal.action)" size="large">{{ actionText(signal.action) }}</el-tag>
        </div>

        <div class="metrics">
          <div><label>现价</label><strong>{{ formatPrice(signal.current_price) }}</strong></div>
          <div><label>中枢</label><strong>{{ formatPrice(signal.anchor_price) }}</strong></div>
          <div><label>中枢来源</label><strong>{{ sourceText(signal.anchor_source) }}</strong></div>
          <div><label>买点</label><strong>{{ formatPrice(signal.next_buy_price) }}</strong></div>
          <div><label>卖点</label><strong>{{ formatPrice(signal.next_sell_price) }}</strong></div>
          <div><label>持仓</label><strong>{{ formatQty(signal.position_qty) }}</strong></div>
          <div><label>可用</label><strong>{{ formatQty(signal.available_qty) }}</strong></div>
          <div><label>底仓</label><strong>{{ formatQty(signal.base_position_qty) }}</strong></div>
          <div><label>数量</label><strong>{{ formatQty(signal.quantity) }}</strong></div>
        </div>

        <p class="reason">{{ signal.reason }}</p>
        <div class="card-actions" v-if="signal.order_preview">
          <el-button size="small" type="primary" plain @click="previewOrder(signal)">
            委托预览
          </el-button>
          <el-button
            v-if="status?.order_submit_enabled"
            size="small"
            type="danger"
            plain
            @click="submitOrder(signal)"
          >
            确认提交
          </el-button>
        </div>
      </article>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import {
  gridTradingApi,
  type GridSignal,
  type GridSignalsResponse,
  type GridStatus,
} from '@/api/gridTrading'

const params = reactive({
  grid_pct: 0.025,
  anchor_window_minutes: 240,
  max_grid_levels: 6,
  base_position_pct: 0.6,
  grid_sleeve_pct: 0.4,
  anchor_reset_pct: 0.08,
  cash_buffer_pct: 0.05,
  initial_cash: 1_000_000,
})

const status = ref<GridStatus | null>(null)
const data = ref<GridSignalsResponse | null>(null)
const loading = ref(false)
const autoRefresh = ref(true)
const seenSignals = new Set<string>()
let timer: number | undefined

async function loadAll() {
  loading.value = true
  try {
    const [nextStatus, nextSignals] = await Promise.all([
      gridTradingApi.status(),
      gridTradingApi.signals({ params: { ...params } }),
    ])
    status.value = nextStatus
    data.value = nextSignals
    notifyNewSignals(nextSignals.signals)
  } finally {
    loading.value = false
  }
}

function notifyNewSignals(signals: GridSignal[]) {
  for (const signal of signals) {
    if (!['BUY', 'SELL'].includes(signal.action)) continue
    const key = signal.signal_key || `${signal.symbol}:${signal.action}:${signal.quantity}:${signal.timestamp}`
    if (seenSignals.has(key)) continue
    seenSignals.add(key)
    ElMessage({
      type: signal.action === 'BUY' ? 'success' : 'warning',
      message: `${signal.name} ${actionText(signal.action)} ${formatQty(signal.quantity)} 股`,
    })
  }
}

function resetTimer() {
  if (timer) window.clearInterval(timer)
  if (autoRefresh.value) timer = window.setInterval(loadAll, 60_000)
}

async function previewOrder(signal: GridSignal) {
  if (!signal.order_preview) return
  const result = await gridTradingApi.submitPreview(signal.order_preview)
  ElMessageBox.alert(JSON.stringify(result, null, 2), '委托预览', {
    confirmButtonText: '知道了',
  })
}

async function submitOrder(signal: GridSignal) {
  if (!signal.order_preview) return
  await ElMessageBox.confirm(`确认提交 ${signal.name} ${actionText(signal.action)} ${signal.quantity} 股？`, '真实委托确认', {
    type: 'warning',
    confirmButtonText: '确认提交',
    cancelButtonText: '取消',
  })
  const result = await gridTradingApi.submitOrder({ ...signal.order_preview, confirm: true })
  ElMessageBox.alert(JSON.stringify(result, null, 2), '委托结果', {
    confirmButtonText: '知道了',
  })
}

function tagType(action: string) {
  if (action === 'BUY') return 'success'
  if (action === 'SELL') return 'danger'
  if (action === 'NO_QUOTE') return 'warning'
  return 'info'
}

function actionText(action: string) {
  if (action === 'BUY') return '买入'
  if (action === 'SELL') return '卖出'
  if (action === 'NO_QUOTE') return '无行情'
  return '持有'
}

function sourceText(source?: string | null) {
  if (source === 'minute_vwap') return '分钟VWAP'
  if (source === 'minute_last') return '分钟收盘'
  if (source === 'tick_vwap') return 'Tick VWAP'
  if (source === 'last_price_fallback') return '现价兜底'
  return '-'
}

function formatPrice(v?: number | null) {
  return v == null ? '-' : v.toFixed(3)
}

function formatQty(v?: number | null) {
  return v == null ? '-' : Number(v).toLocaleString()
}

function formatMoney(v?: number | null) {
  return v == null ? '-' : Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 })
}

const pageContextBlocks = computed(() => [
  {
    title: 'Trading Guardrails',
    rows: [
      { label: '刷新', value: autoRefresh.value ? '每分钟自动刷新' : '手动刷新' },
      { label: '加载状态', value: loading.value ? '加载中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
      { label: '行情连接', value: status.value?.quote_connected ? '已连接' : '未确认', tone: status.value?.quote_connected ? 'good' : 'warn' },
      { label: '交易模块', value: status.value?.xttrader_available ? '可用' : '不可用', tone: status.value?.xttrader_available ? 'good' : 'bad' },
      { label: '真实下单', value: status.value?.order_submit_enabled ? '已开启' : '仅信号', tone: status.value?.order_submit_enabled ? 'bad' : 'good' },
    ],
  },
  {
    title: 'Signals',
    rows: [
      { label: '账户', value: status.value?.account_id || '-' },
      { label: '信号数', value: `${data.value?.signals?.length || 0} 条` },
      { label: '账户来源', value: data.value?.account?.source || '-' },
      { label: '账户总资产', value: formatMoney(data.value?.account?.total_asset) },
    ],
  },
])

usePageContext(pageContextBlocks)

watch(autoRefresh, resetTimer)

onMounted(() => {
  loadAll().catch((err) => ElMessage.error(err?.message || '网格信号加载失败'))
  resetTimer()
})

onUnmounted(() => {
  if (timer) window.clearInterval(timer)
})
</script>

<style scoped>
.live-page {
  overflow: auto;
}

.page-head {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-4);
  padding: var(--space-5);
}

.page-head h2 {
  margin: var(--space-1) 0 var(--space-2);
  color: var(--text-bright);
  font-size: 22px;
}

.page-head p {
  margin: 0;
  max-width: 760px;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.actions {
  display: flex;
  gap: 12px;
  align-items: center;
}

.status-band,
.toolbar,
.account-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  padding: 12px;
  border: 1px solid var(--border-subtle);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.025), transparent), var(--bg-surface);
  border-radius: 8px;
  box-shadow: var(--shadow-card);
}

.status-band div,
.account-strip div,
.field {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 28px;
}

.status-band label,
.account-strip label,
.field span {
  color: var(--text-muted);
  font-size: 12px;
}

.account-alert {
  margin-bottom: 12px;
}

.signal-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.signal-card {
  border: 1px solid var(--border-subtle);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.025), transparent), var(--bg-surface);
  border-radius: 8px;
  padding: 14px;
  box-shadow: var(--shadow-card);
}

.card-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 14px;
}

.card-head h3 {
  margin: 0;
  font-size: 16px;
}

.card-head span {
  color: var(--text-muted);
  font-size: 12px;
}

.metrics {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.metrics div {
  min-width: 0;
}

.metrics label {
  display: block;
  color: var(--text-muted);
  font-size: 11px;
  margin-bottom: 2px;
}

.metrics strong {
  font-size: 14px;
  font-weight: 600;
}

.reason {
  color: var(--text-muted);
  min-height: 20px;
}

.card-actions {
  display: flex;
  gap: 8px;
}

@media (max-width: 900px) {
  .page-head {
    grid-template-columns: 1fr;
  }

  .signal-grid {
    grid-template-columns: 1fr;
  }

  .metrics {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
