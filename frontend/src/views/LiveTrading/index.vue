<template>
  <div class="page-frame live-page">
    <header class="panel-card page-head">
      <div>
        <span class="section-kicker">LIVE TRADING DESK</span>
        <h2>模拟 / 实盘</h2>
        <p>可配置策略执行台：默认接入 CashAware 稳健版与进攻版，自动交易与真实下单均受独立护栏控制。</p>
      </div>
      <div class="actions">
        <el-segmented v-model="mode" :options="modeOptions" />
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
        <label>真实下单</label>
        <el-tag :type="status?.order_submit_enabled ? 'danger' : 'info'">
          {{ status?.order_submit_enabled ? '开启' : '关闭' }}
        </el-tag>
      </div>
      <div>
        <label>自动实盘</label>
        <el-tag :type="status?.auto_execute_enabled ? 'danger' : 'info'">
          {{ status?.auto_execute_enabled ? '允许' : '禁止' }}
        </el-tag>
      </div>
      <div>
        <label>Runner</label>
        <strong>{{ runnerText }}</strong>
      </div>
    </section>

    <section class="desk-grid">
      <article class="panel-card control-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">STRATEGY PROFILE</span>
            <h3>策略配置</h3>
          </div>
          <el-button size="small" @click="profileDialogOpen = true">新增 Profile</el-button>
        </div>
        <div class="control-body">
          <el-select v-model="selectedProfileKey" filterable placeholder="选择策略 Profile" @change="loadSignals">
            <el-option
              v-for="profile in profiles"
              :key="profile.profile_key"
              :label="profile.display_name"
              :value="profile.profile_key"
            >
              <span>{{ profile.display_name }}</span>
              <small> · ID {{ profile.strategy_id }}</small>
            </el-option>
          </el-select>
          <div class="profile-meta" v-if="selectedProfile">
            <strong>{{ selectedProfile.display_name }}</strong>
            <span>ID {{ selectedProfile.strategy_id }} · {{ selectedProfile.adapter_type }}</span>
            <p>{{ selectedProfile.description || selectedProfile.strategy_name || '-' }}</p>
            <div class="profile-actions">
              <el-switch
                :model-value="selectedProfile.enabled"
                active-text="启用"
                inactive-text="停用"
                @change="toggleProfileEnabled"
              />
              <el-button text size="small" @click="makeDefaultProfile">设为默认</el-button>
            </div>
          </div>
          <div class="param-row">
            <label>交易日</label>
            <el-date-picker v-model="tradeDate" value-format="YYYY-MM-DD" />
          </div>
          <div class="param-row">
            <label>指数池</label>
            <el-input v-model="indexSymbol" />
          </div>
        </div>
      </article>

      <article class="panel-card runner-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RUNNER</span>
            <h3>自动 / 接管</h3>
          </div>
        </div>
        <div class="runner-actions">
          <el-button type="primary" :loading="runnerLoading" @click="startRunner">启动自动</el-button>
          <el-button :loading="runnerLoading" @click="stopRunner">停止</el-button>
          <el-button type="warning" plain :loading="runnerLoading" @click="takeoverRunner">人工接管</el-button>
        </div>
        <div class="runner-state">
          <div><label>状态</label><strong>{{ status?.runner.status || '-' }}</strong></div>
          <div><label>Profile</label><strong>{{ status?.runner.profile_key || '-' }}</strong></div>
          <div><label>最近信号</label><strong>{{ shortHash(status?.runner.last_signal_hash) }}</strong></div>
          <div><label>错误</label><strong>{{ status?.runner.last_error || '-' }}</strong></div>
        </div>
      </article>
    </section>

    <section class="summary-band">
      <div><label>账户来源</label><strong>{{ signalData?.account.source || '-' }}</strong></div>
      <div><label>可用现金</label><strong>{{ formatMoney(signalData?.account.cash) }}</strong></div>
      <div><label>总资产</label><strong>{{ formatMoney(signalData?.account.total_asset) }}</strong></div>
      <div><label>股票池</label><strong>{{ signalData?.universe_size || 0 }}</strong></div>
      <div><label>候选</label><strong>{{ signalData?.candidate_count || 0 }}</strong></div>
      <div><label>订单</label><strong>{{ orderRows.length }}</strong></div>
    </section>

    <el-alert
      v-if="status?.order_submit_enabled"
      type="error"
      :closable="false"
      show-icon
      title="真实下单开关已开启，提交前仍需确认。"
    />
    <el-alert
      v-if="signalData?.quote_error || signalData?.account.error || signalData?.heat_filter_note"
      type="warning"
      :closable="false"
      show-icon
      :title="signalData?.quote_error || signalData?.account.error || signalData?.heat_filter_note || ''"
    />

    <section class="panel-card order-panel">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">ORDER BASKET</span>
          <h3>订单篮子</h3>
        </div>
        <div class="table-actions">
          <span>{{ shortHash(signalData?.signal_hash) }}</span>
          <el-button size="small" :loading="signalsLoading" @click="loadSignals">生成信号</el-button>
          <el-button size="small" type="primary" :disabled="!orderRows.length" @click="submitOrders">提交篮子</el-button>
        </div>
      </div>
      <el-table :data="orderRows" size="small" stripe max-height="340">
        <el-table-column prop="symbol" label="代码" width="110" />
        <el-table-column prop="side" label="方向" width="80">
          <template #default="{ row }">
            <el-tag :type="row.side === 'BUY' ? 'danger' : 'success'" effect="plain">{{ row.side }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="数量" width="160">
          <template #default="{ row }">
            <el-input-number v-model="row.quantity" :min="0" :step="100" size="small" />
          </template>
        </el-table-column>
        <el-table-column prop="reference_price" label="参考价" width="110" />
        <el-table-column prop="remark" label="原因" min-width="220" show-overflow-tooltip />
        <el-table-column label="操作" width="88">
          <template #default="{ $index }">
            <el-button text type="danger" size="small" @click="removeOrder($index)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <section class="lower-grid">
      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">SKIPS</span>
            <h3>跳过订单</h3>
          </div>
        </div>
        <el-table :data="signalData?.skipped_orders || []" size="small" height="260">
          <el-table-column prop="symbol" label="代码" width="110" />
          <el-table-column prop="side" label="方向" width="80" />
          <el-table-column prop="quantity" label="数量" width="100" />
          <el-table-column prop="reason" label="原因" show-overflow-tooltip />
        </el-table>
      </article>

      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">AUDIT</span>
            <h3>订单审计</h3>
          </div>
          <el-button text size="small" @click="loadAudits">刷新</el-button>
        </div>
        <el-table :data="audits" size="small" height="260">
          <el-table-column prop="created_at" label="时间" width="150" />
          <el-table-column prop="profile_key" label="Profile" width="150" show-overflow-tooltip />
          <el-table-column prop="mode" label="模式" width="72" />
          <el-table-column prop="status" label="状态" width="90" />
          <el-table-column prop="skip_reason" label="说明" show-overflow-tooltip />
        </el-table>
      </article>
    </section>

    <el-dialog v-model="profileDialogOpen" title="新增 Live Profile" width="520px">
      <el-form label-width="110px">
        <el-form-item label="策略 ID">
          <el-input-number v-model="newProfile.strategy_id" :min="1" />
        </el-form-item>
        <el-form-item label="Profile Key">
          <el-input v-model="newProfile.profile_key" />
        </el-form-item>
        <el-form-item label="显示名">
          <el-input v-model="newProfile.display_name" />
        </el-form-item>
        <el-form-item label="默认">
          <el-switch v-model="newProfile.is_default" />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="newProfile.enabled" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="profileDialogOpen = false">取消</el-button>
        <el-button type="primary" :loading="profileSaving" @click="createProfile">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import {
  liveTradingApi,
  type LiveOrder,
  type LiveOrderAudit,
  type LiveSignalsResponse,
  type LiveStrategyProfile,
  type LiveTradingMode,
  type LiveTradingStatus,
} from '@/api/liveTrading'

const modeOptions = [
  { label: '模拟', value: 'paper' },
  { label: '实盘', value: 'live' },
]

const mode = ref<LiveTradingMode>('paper')
const tradeDate = ref(new Date().toISOString().slice(0, 10))
const indexSymbol = ref('399101.SZ')
const selectedProfileKey = ref('')
const status = ref<LiveTradingStatus | null>(null)
const profiles = ref<LiveStrategyProfile[]>([])
const signalData = ref<LiveSignalsResponse | null>(null)
const orderRows = ref<LiveOrder[]>([])
const audits = ref<LiveOrderAudit[]>([])
const loading = ref(false)
const signalsLoading = ref(false)
const runnerLoading = ref(false)
const profileDialogOpen = ref(false)
const profileSaving = ref(false)
const newProfile = reactive({
  strategy_id: 62,
  profile_key: '',
  display_name: '',
  enabled: true,
  is_default: false,
})

const selectedProfile = computed(() => profiles.value.find(item => item.profile_key === selectedProfileKey.value) || null)
const runnerText = computed(() => {
  const runner = status.value?.runner
  if (!runner) return '-'
  return runner.takeover ? '人工接管' : runner.status
})

async function loadAll() {
  loading.value = true
  try {
    const [nextStatus, nextProfiles] = await Promise.all([
      liveTradingApi.status(),
      liveTradingApi.profiles(true),
    ])
    status.value = nextStatus
    profiles.value = nextProfiles
    if (!selectedProfileKey.value) {
      selectedProfileKey.value = nextProfiles.find(item => item.is_default)?.profile_key || nextStatus.default_profile || nextProfiles[0]?.profile_key || ''
    }
    await Promise.all([loadSignals(), loadAudits()])
  } finally {
    loading.value = false
  }
}

async function loadSignals() {
  if (!selectedProfileKey.value) return
  signalsLoading.value = true
  try {
    signalData.value = await liveTradingApi.signals({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
    })
    orderRows.value = (signalData.value.orders || []).map(order => ({ ...order }))
  } finally {
    signalsLoading.value = false
  }
}

async function loadAudits() {
  audits.value = await liveTradingApi.audits({
    profile_key: selectedProfileKey.value || undefined,
    limit: 80,
  })
}

async function startRunner() {
  if (!selectedProfileKey.value) return
  if (mode.value === 'live') {
    await ElMessageBox.confirm('确认启动实盘自动交易？', '实盘自动交易确认', {
      type: 'warning',
      confirmButtonText: '确认启动',
      cancelButtonText: '取消',
    })
  }
  runnerLoading.value = true
  try {
    await liveTradingApi.startRunner({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
      interval_seconds: 60,
    })
    ElMessage.success('自动交易已启动')
    status.value = await liveTradingApi.status()
  } finally {
    runnerLoading.value = false
  }
}

async function stopRunner() {
  runnerLoading.value = true
  try {
    await liveTradingApi.stopRunner()
    ElMessage.success('自动交易已停止')
    status.value = await liveTradingApi.status()
  } finally {
    runnerLoading.value = false
  }
}

async function takeoverRunner() {
  await ElMessageBox.confirm('确认人工接管并停止自动提交？', '人工接管', {
    type: 'warning',
    confirmButtonText: '接管',
    cancelButtonText: '取消',
  })
  runnerLoading.value = true
  try {
    await liveTradingApi.takeover('human takeover from UI')
    ElMessage.warning('已切换为人工接管')
    status.value = await liveTradingApi.status()
  } finally {
    runnerLoading.value = false
  }
}

async function submitOrders() {
  const orders = orderRows.value.filter(order => Number(order.quantity || 0) > 0)
  if (!orders.length) {
    ElMessage.info('没有可提交的订单')
    return
  }
  const title = mode.value === 'live' ? '真实委托确认' : '模拟成交确认'
  await ElMessageBox.confirm(`确认提交 ${orders.length} 笔订单？`, title, {
    type: mode.value === 'live' ? 'warning' : 'info',
    confirmButtonText: '确认提交',
    cancelButtonText: '取消',
  })
  const result = await liveTradingApi.submitOrders({
    mode: mode.value,
    orders: orders as unknown as Record<string, unknown>[],
    confirm: true,
  })
  ElMessageBox.alert(JSON.stringify(result, null, 2), '提交结果', {
    confirmButtonText: '知道了',
  })
  await Promise.all([loadSignals(), loadAudits(), liveTradingApi.status().then(next => { status.value = next })])
}

function removeOrder(index: number) {
  orderRows.value.splice(index, 1)
}

async function toggleProfileEnabled(value: string | number | boolean) {
  if (!selectedProfile.value) return
  const updated = await liveTradingApi.updateProfile(selectedProfile.value.profile_key, { enabled: Boolean(value) })
  profiles.value = profiles.value.map(item => item.profile_key === updated.profile_key ? updated : item)
}

async function makeDefaultProfile() {
  if (!selectedProfile.value) return
  const updated = await liveTradingApi.updateProfile(selectedProfile.value.profile_key, { is_default: true })
  profiles.value = profiles.value.map(item => ({ ...item, is_default: item.profile_key === updated.profile_key }))
  ElMessage.success('默认 Profile 已更新')
}

async function createProfile() {
  profileSaving.value = true
  try {
    const created = await liveTradingApi.createProfile({
      strategy_id: newProfile.strategy_id,
      profile_key: newProfile.profile_key,
      display_name: newProfile.display_name,
      enabled: newProfile.enabled,
      is_default: newProfile.is_default,
      adapter_type: 'multi_factor_cash_aware',
      universe_config: { type: 'strategy' },
      execution_policy: {
        allow_auto_trade: true,
        allow_manual_submit: true,
        allow_live_submit: true,
      },
    })
    profiles.value = [created, ...profiles.value.filter(item => item.profile_key !== created.profile_key)]
    selectedProfileKey.value = created.profile_key
    profileDialogOpen.value = false
    ElMessage.success('Profile 已创建')
    await loadSignals()
  } finally {
    profileSaving.value = false
  }
}

function shortHash(value?: string | null) {
  return value ? `${value.slice(0, 8)}…` : '-'
}

function formatMoney(value?: number | null) {
  return value == null ? '-' : Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 })
}

const pageContextBlocks = computed(() => [
  {
    title: 'Live Trading',
    rows: [
      { label: '模式', value: mode.value === 'paper' ? '模拟' : '实盘', tone: mode.value === 'paper' ? 'good' : 'warn' },
      { label: 'Profile', value: selectedProfileKey.value || '-' },
      { label: 'Runner', value: runnerText.value, tone: status.value?.runner.takeover ? 'warn' : 'neutral' },
      { label: '真实下单', value: status.value?.order_submit_enabled ? '开启' : '关闭', tone: status.value?.order_submit_enabled ? 'bad' : 'good' },
    ],
  },
  {
    title: 'Basket',
    rows: [
      { label: '订单', value: `${orderRows.value.length} 笔` },
      { label: '跳过', value: `${signalData.value?.skipped_orders?.length || 0} 笔` },
      { label: '账户', value: signalData.value?.account.source || '-' },
      { label: '信号', value: shortHash(signalData.value?.signal_hash) },
    ],
  },
])

usePageContext(pageContextBlocks)

onMounted(() => {
  loadAll().catch(error => ElMessage.error(error?.message || '实盘交易模块加载失败'))
})
</script>

<style scoped>
.live-page {
  overflow: auto;
}

.page-head,
.desk-grid,
.lower-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: var(--space-4);
  align-items: start;
}

.page-head {
  align-items: center;
  padding: var(--space-5);
}

.page-head h2,
.panel-card__head h3 {
  margin: var(--space-1) 0 var(--space-2);
  color: var(--text-bright);
}

.page-head p,
.profile-meta p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
  line-height: 1.6;
}

.actions,
.table-actions,
.runner-actions,
.profile-actions {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-2);
  align-items: center;
  justify-content: flex-end;
}

.status-band,
.summary-band {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: var(--space-3);
  padding: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: var(--bg-surface);
  box-shadow: var(--shadow-card);
}

.status-band div,
.summary-band div,
.runner-state div {
  min-width: 0;
}

.status-band label,
.summary-band label,
.runner-state label,
.param-row label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.status-band strong,
.summary-band strong,
.runner-state strong {
  color: var(--text-primary);
  font-family: var(--font-data);
}

.desk-grid {
  grid-template-columns: minmax(0, 1.35fr) minmax(360px, 0.65fr);
}

.lower-grid {
  grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
}

.control-body {
  display: grid;
  gap: var(--space-3);
  padding: var(--space-4);
}

.profile-meta {
  display: grid;
  gap: var(--space-2);
  padding: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.03);
}

.profile-meta span,
.table-actions span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.param-row {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr);
  gap: var(--space-2);
  align-items: center;
}

.param-row label {
  margin: 0;
}

.runner-actions,
.runner-state {
  padding: var(--space-4);
}

.runner-actions {
  justify-content: flex-start;
}

.runner-state {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--space-3);
}

.order-panel :deep(.el-input-number) {
  width: 128px;
}

:deep(.el-table) {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: rgba(15, 23, 42, 0.9);
  --el-table-header-text-color: #cbd5e1;
  --el-table-text-color: #dbe4f0;
  --el-table-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-border-color: rgba(148, 163, 184, 0.16);
}

@media (max-width: 1100px) {
  .page-head,
  .desk-grid,
  .lower-grid,
  .status-band,
  .summary-band {
    grid-template-columns: 1fr;
  }

  .actions,
  .table-actions {
    justify-content: flex-start;
  }
}
</style>
