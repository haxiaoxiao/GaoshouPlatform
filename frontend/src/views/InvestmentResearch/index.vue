<template>
  <div class="investment-research">
    <section class="research-command">
      <header class="command-header">
        <div class="command-title">
          <span class="panel-kicker">RESEARCH COMMAND CENTER</span>
          <h2>研究实验室</h2>
          <p>把 Obsidian 里的研究假设接到本地数据、因子预计算、评估和回测动作。</p>
        </div>
        <div class="command-actions">
          <el-input
            v-model="keyword"
            :prefix-icon="Search"
            clearable
            placeholder="搜索假设、研报、因子、备注"
            class="search-input"
          />
          <el-select v-model="statusFilter" clearable placeholder="全部状态" class="status-select">
            <el-option
              v-for="option in statusOptions"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
          <el-button @click="resetFilters">重置</el-button>
          <el-button :icon="Refresh" @click="loadResearch" :loading="loading">
            刷新
          </el-button>
        </div>
      </header>

      <el-alert
        v-if="loadError"
        type="warning"
        class="result-alert"
        :title="`投研清单加载失败：${loadError}`"
      />

      <section class="readiness-strip" aria-label="研究准备度筛选">
        <button
          v-for="stage in readinessStages"
          :key="stage.key"
          class="readiness-cell"
          :class="[
            `readiness-cell--${stage.tone}`,
            { 'readiness-cell--active': readinessFilter === stage.key },
          ]"
          type="button"
          @click="setReadinessFilter(stage.key)"
        >
          <span class="readiness-cell__rank">{{ stage.rank }}</span>
          <span class="readiness-cell__body">
            <strong>{{ stage.label }}</strong>
            <small>{{ stage.hint }}</small>
          </span>
          <span class="readiness-cell__count">{{ stage.count }}</span>
        </button>
      </section>

      <section class="command-grid">
        <article class="queue-panel">
          <div class="panel-header">
            <div>
              <strong>研究队列</strong>
              <span>{{ filteredManifestRows.length }} / {{ paperManifest.length }} 项，按平台可执行动作推进</span>
            </div>
          </div>

          <el-table
            :data="filteredManifestRows"
            v-loading="loading"
            size="small"
            height="100%"
            class="research-table command-table"
            row-key="paper_id"
            highlight-current-row
            :current-row-key="selectedPaperId || undefined"
            @row-click="selectResearchItem"
          >
            <el-table-column prop="paper_id" label="#" width="54" fixed />
            <el-table-column label="研究假设 / 研报" min-width="300" fixed show-overflow-tooltip>
              <template #default="{ row }">
                <div class="hypothesis-cell">
                  <strong>{{ row.title }}</strong>
                  <span>{{ row.strategy_type }} · {{ row.rebalance_frequency || '调仓未知' }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="下一步" width="116">
              <template #default="{ row }">
                <span class="next-action" :class="`next-action--${nextActionTone(row)}`">
                  {{ nextActionLabel(row) }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="116">
              <template #default="{ row }">
                <el-tag :type="paperStatusType(row.landing_status)" size="small" effect="plain">
                  {{ paperStatusLabel(row.landing_status) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="landing_grade" label="等级" width="72" />
            <el-table-column prop="data_frequency" label="频率" width="104" show-overflow-tooltip />
            <el-table-column label="映射因子" min-width="220">
              <template #default="{ row }">
                <div class="tag-list">
                  <el-tag
                    v-for="name in row.factor_names"
                    :key="name"
                    size="small"
                    effect="plain"
                  >
                    <router-link :to="{ name: 'FactorDetail', params: { factorName: name } }">
                      {{ name }}
                    </router-link>
                  </el-tag>
                  <span v-if="!row.factor_names.length">-</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="验证指标" min-width="190" show-overflow-tooltip>
              <template #default="{ row }">
                {{ row.validation_metrics.join(', ') }}
              </template>
            </el-table-column>
            <template #empty>
              <div class="empty-state">
                <strong>没有匹配的研究项</strong>
                <span>清空关键词或状态筛选后再看。</span>
                <el-button size="small" @click="resetFilters">重置筛选</el-button>
              </div>
            </template>
          </el-table>
        </article>

        <aside class="inspector-panel">
          <template v-if="selectedResearchItem">
            <header class="inspector-header">
              <span>SELECTED RESEARCH ITEM</span>
              <h3>{{ selectedResearchItem.title }}</h3>
              <div class="inspector-status">
                <el-tag :type="paperStatusType(selectedResearchItem.landing_status)" effect="plain" size="small">
                  {{ paperStatusLabel(selectedResearchItem.landing_status) }}
                </el-tag>
                <span>{{ selectedResearchItem.landing_grade || '-' }}级</span>
                <span>{{ selectedResearchItem.data_frequency || '-' }}</span>
              </div>
            </header>

            <div class="inspector-body">
              <section class="inspector-section">
                <div class="inspector-section__head">
                  <strong>Obsidian 关联</strong>
                  <a :href="obsidianSearchHref(selectedResearchItem)">打开搜索</a>
                </div>
                <div class="vault-box">
                  <span>Vault</span>
                  <strong>TheLandsBetween / GaoshouPlatform</strong>
                  <small>{{ selectedResearchItem.title }}</small>
                </div>
              </section>

              <section class="inspector-section" v-if="selectedResearchItem.data_dependencies.length || hasDataGap(selectedResearchItem)">
                <div class="inspector-section__head">
                  <strong>数据与缺口</strong>
                  <span :class="hasDataGap(selectedResearchItem) ? 'tone-warn' : 'tone-good'">
                    {{ hasDataGap(selectedResearchItem) ? '需要处理' : '依赖已列明' }}
                  </span>
                </div>
                <div class="data-gap" :class="{ 'data-gap--warn': hasDataGap(selectedResearchItem) }">
                  <span>{{ selectedResearchItem.data_dependencies.length ? selectedResearchItem.data_dependencies.join(' / ') : '未声明依赖' }}</span>
                  <small>{{ dataGapHint(selectedResearchItem) }}</small>
                </div>
              </section>

              <section class="inspector-section">
                <div class="inspector-section__head">
                  <strong>因子与验证</strong>
                  <span>{{ selectedResearchItem.factor_names.length }} 个映射</span>
                </div>
                <div class="factor-link-list" v-if="selectedResearchItem.factor_names.length">
                  <button
                    v-for="name in selectedResearchItem.factor_names"
                    :key="name"
                    type="button"
                    @click="router.push({ name: 'FactorDetail', params: { factorName: name } })"
                  >
                    {{ name }}
                  </button>
                </div>
                <p v-else class="muted-copy">还没有映射到平台因子。</p>
                <div class="metric-list" v-if="selectedResearchItem.validation_metrics.length">
                  <span v-for="metric in selectedResearchItem.validation_metrics" :key="metric">{{ metric }}</span>
                </div>
              </section>

              <section class="inspector-section" v-if="selectedExperiments.length">
                <div class="inspector-section__head">
                  <strong>离线实验</strong>
                  <span>{{ selectedExperiments.length }} 项</span>
                </div>
                <div class="experiment-stack">
                  <div v-for="experiment in selectedExperiments" :key="experiment.name">
                    <strong>{{ experiment.name }}</strong>
                    <span>{{ experiment.status }} · {{ experiment.target_policy }}</span>
                  </div>
                </div>
              </section>

              <section class="inspector-section" v-if="selectedResearchItem.notes || selectedResearchItem.platform_mapping">
                <div class="inspector-section__head">
                  <strong>平台映射 / 备注</strong>
                </div>
                <p v-if="selectedResearchItem.platform_mapping">{{ selectedResearchItem.platform_mapping }}</p>
                <p v-if="selectedResearchItem.notes" class="note-copy">{{ selectedResearchItem.notes }}</p>
              </section>
            </div>

            <footer class="inspector-actions">
              <a class="action-button action-button--primary" :href="obsidianSearchHref(selectedResearchItem)">打开 Obsidian</a>
              <button class="action-button" type="button" @click="router.push('/factor')">进入因子研究</button>
              <button class="action-button" type="button" @click="router.push('/factor/evaluation')">研究评估</button>
              <button class="action-button" type="button" @click="router.push('/backtest')">策略回测</button>
            </footer>
          </template>

          <div v-else class="inspector-empty">
            <strong>选择一条研究项</strong>
            <span>右侧会展示 Obsidian 入口、数据缺口、因子映射和下一步平台动作。</span>
          </div>
        </aside>
      </section>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh, Search } from '@element-plus/icons-vue'
import { usePageContext } from '@/app/pageContext'
import {
  factorValueApi,
  type FactorPaperExperimentSpec,
  type FactorPaperManifestItem,
} from '@/api/factorValues'

const router = useRouter()
const paperManifest = ref<FactorPaperManifestItem[]>([])
const paperExperiments = ref<FactorPaperExperimentSpec[]>([])
const keyword = ref('')
const statusFilter = ref('')
const readinessFilter = ref('')
const loading = ref(false)
const loadError = ref('')
const selectedPaperId = ref<number | null>(null)

const statusLabels: Record<string, string> = {
  implemented: '已实现',
  implemented_template: '已成模板',
  implemented_factor_leg: '已成因子腿',
  backlog_tick: 'Tick-only',
  pending_data: '待数据源',
  pending_research: '待研究',
  partial_data: '部分数据',
  partial_equity_proxy: '权益代理',
  partial_style_rotation: '风格代理',
  partial_daily_proxy: '日频代理',
  partial_minute_proxy: '分钟代理',
  partial_factor_first: '因子优先',
  partial_implemented: '部分实现',
  partial_template: '模板代理',
}

const paperStatusLabel = (status: string) => statusLabels[status] || status

const paperStatusType = (status: string): 'success' | 'warning' | 'danger' | 'info' => {
  if (status === 'implemented' || status.startsWith('implemented')) return 'success'
  if (status === 'backlog_tick' || status.startsWith('partial')) return 'warning'
  if (status === 'pending_data') return 'danger'
  return 'info'
}

const manifestSummary = computed(() => ({
  total: paperManifest.value.length,
  implemented: paperManifest.value.filter(item => item.landing_grade === 'A' || item.factor_names.length > 0).length,
  backlogTick: paperManifest.value.filter(item => item.landing_status === 'backlog_tick').length,
  pendingData: paperManifest.value.filter(item => item.landing_status === 'pending_data').length,
}))

const actionableCount = computed(() => paperManifest.value.filter(item => (
  item.factor_names.length > 0
  || item.landing_status.startsWith('implemented')
  || item.landing_status.startsWith('partial')
)).length)

const isBacktestReady = (item: FactorPaperManifestItem) => (
  item.landing_status.startsWith('implemented')
  || (item.factor_names.length > 0 && item.landing_grade === 'A' && !hasDataGap(item))
)

const isInValidation = (item: FactorPaperManifestItem) => (
  item.landing_status.startsWith('partial')
  || (item.factor_names.length > 0 && !isBacktestReady(item) && !hasDataGap(item))
)

const needsFactorDefinition = (item: FactorPaperManifestItem) => (
  !item.factor_names.length
  && item.landing_status !== 'pending_data'
  && item.landing_status !== 'backlog_tick'
)

const readinessStages = computed(() => {
  const all = paperManifest.value
  return [
    {
      key: 'backtest_ready',
      rank: 'R4',
      label: '可回测',
      hint: '因子映射和口径已就绪',
      tone: 'good',
      count: all.filter(isBacktestReady).length,
    },
    {
      key: 'validating',
      rank: 'R3',
      label: '验证中',
      hint: '已有因子，继续看 IC/OOS',
      tone: 'neutral',
      count: all.filter(isInValidation).length,
    },
    {
      key: 'factor_needed',
      rank: 'R2',
      label: '待定义因子',
      hint: '有假设，缺平台表达',
      tone: 'info',
      count: all.filter(needsFactorDefinition).length,
    },
    {
      key: 'data_needed',
      rank: 'R1',
      label: '待补数据',
      hint: '先处理依赖覆盖',
      tone: 'warn',
      count: all.filter(item => item.landing_status === 'pending_data').length,
    },
    {
      key: 'tick_only',
      rank: 'R0',
      label: 'Tick-only',
      hint: '暂归档或等高频数据',
      tone: 'blocked',
      count: all.filter(item => item.landing_status === 'backlog_tick').length,
    },
  ]
})

const statusOptions = computed(() => {
  const statuses = Array.from(new Set(paperManifest.value.map(item => item.landing_status))).sort()
  return statuses.map(status => ({
    label: paperStatusLabel(status),
    value: status,
  }))
})

const filteredManifestRows = computed(() => {
  const normalizedKeyword = keyword.value.trim().toLowerCase()
  return paperManifest.value.filter(item => {
    if (statusFilter.value && item.landing_status !== statusFilter.value) return false
    if (readinessFilter.value === 'backtest_ready' && !isBacktestReady(item)) return false
    if (readinessFilter.value === 'validating' && !isInValidation(item)) return false
    if (readinessFilter.value === 'factor_needed' && !needsFactorDefinition(item)) return false
    if (readinessFilter.value === 'data_needed' && item.landing_status !== 'pending_data') return false
    if (readinessFilter.value === 'tick_only' && item.landing_status !== 'backlog_tick') return false
    if (!normalizedKeyword) return true
    const haystack = [
      item.title,
      item.strategy_type,
      item.data_frequency,
      item.rebalance_frequency,
      item.landing_status,
      item.landing_grade,
      item.platform_mapping,
      item.notes,
      item.validation_metrics.join(' '),
      item.factor_names.join(' '),
    ].join(' ').toLowerCase()
    return haystack.includes(normalizedKeyword)
  })
})

const selectedResearchItem = computed(() => (
  paperManifest.value.find(item => item.paper_id === selectedPaperId.value)
  || filteredManifestRows.value[0]
  || null
))

const selectedExperiments = computed(() => {
  const selected = selectedResearchItem.value
  if (!selected) return []
  return paperExperiments.value.filter(experiment => (
    experiment.paper_ids.includes(selected.paper_id)
    || experiment.default_factor_names.some(name => selected.factor_names.includes(name))
  ))
})

const resetFilters = () => {
  keyword.value = ''
  statusFilter.value = ''
  readinessFilter.value = ''
}

const selectResearchItem = (row: FactorPaperManifestItem) => {
  selectedPaperId.value = row.paper_id
}

const setReadinessFilter = (key: string) => {
  readinessFilter.value = readinessFilter.value === key ? '' : key
  statusFilter.value = ''
}

const hasDataGap = (item: FactorPaperManifestItem) => (
  item.landing_status === 'pending_data'
  || item.landing_status === 'backlog_tick'
  || item.landing_status.includes('data')
)

const dataGapHint = (item: FactorPaperManifestItem) => {
  if (item.landing_status === 'pending_data') return '需要补齐依赖数据后再进入因子/回测'
  if (item.landing_status === 'backlog_tick') return '依赖 Tick 级数据，当前平台数据可能不足'
  if (item.data_dependencies.length) return '依赖已声明，优先检查本地覆盖范围'
  return '未声明依赖，先回到 Obsidian/研报补充口径'
}

const nextActionLabel = (item: FactorPaperManifestItem) => {
  if (item.landing_status === 'pending_data') return '补数据'
  if (item.landing_status === 'backlog_tick') return '等 Tick'
  if (!item.factor_names.length) return '做因子'
  if (item.landing_status.startsWith('implemented')) return '可回测'
  if (item.landing_status.startsWith('partial')) return '继续验证'
  if (item.landing_status === 'pending_research') return '补证据'
  return '评估'
}

const nextActionTone = (item: FactorPaperManifestItem) => {
  if (item.landing_status === 'pending_data' || item.landing_status === 'backlog_tick') return 'warn'
  if (item.landing_status.startsWith('implemented') || item.factor_names.length) return 'good'
  if (item.landing_status.includes('reject')) return 'bad'
  return 'neutral'
}

const obsidianSearchHref = (item: FactorPaperManifestItem) => (
  `obsidian://search?vault=TheLandsBetween&query=${encodeURIComponent(item.title)}`
)

const formatRequestError = (error: unknown) => error instanceof Error ? error.message : '请求失败'

const loadResearch = async () => {
  loading.value = true
  loadError.value = ''
  try {
    const [nextManifest, nextExperiments] = await Promise.all([
      factorValueApi.paperManifest(),
      factorValueApi.paperExperiments(),
    ])
    paperManifest.value = nextManifest
    paperExperiments.value = nextExperiments
  } catch (error) {
    loadError.value = formatRequestError(error)
  } finally {
    loading.value = false
  }
}

const pageContextBlocks = computed(() => [
  {
    title: 'Research Command',
    rows: [
      { label: '刷新状态', value: loading.value ? '加载中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
      { label: '当前视图', value: 'Command Center' },
      { label: '选中项', value: selectedResearchItem.value?.title || '-' },
      { label: '关键词', value: keyword.value.trim() || '-' },
      { label: '准备度筛选', value: readinessFilter.value || '全部' },
      { label: '状态筛选', value: statusFilter.value || '全部' },
      { label: '错误', value: loadError.value || '无', tone: loadError.value ? 'warn' : 'good' },
    ],
  },
  {
    title: 'Manifest',
    rows: [
      { label: '研报总数', value: `${manifestSummary.value.total}` },
      { label: '已映射', value: `${manifestSummary.value.implemented}` },
      { label: '当前结果', value: `${filteredManifestRows.value.length}` },
      { label: '可推进', value: `${actionableCount.value}` },
      { label: '离线实验', value: `${paperExperiments.value.length}` },
    ],
  },
])

usePageContext(pageContextBlocks)

watch(filteredManifestRows, rows => {
  if (!rows.length) {
    selectedPaperId.value = null
    return
  }
  if (!rows.some(row => row.paper_id === selectedPaperId.value)) {
    selectedPaperId.value = rows[0].paper_id
  }
}, { immediate: true })

onMounted(() => {
  void loadResearch()
})
</script>

<style scoped>
.investment-research {
  height: 100%;
  min-height: 0;
  padding: 12px;
  color: var(--text-primary);
  background:
    linear-gradient(rgba(34, 48, 42, 0.024) 1px, transparent 1px),
    linear-gradient(90deg, rgba(34, 48, 42, 0.02) 1px, transparent 1px),
    linear-gradient(180deg, rgba(253, 251, 247, 0.9), rgba(245, 242, 234, 0.62));
  background-size: 56px 56px, 56px 56px, auto;
}

.research-command {
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 10px;
  overflow: hidden;
}

.command-header {
  display: grid;
  grid-template-columns: minmax(320px, 1fr) minmax(520px, 0.95fr);
  align-items: center;
  gap: 14px;
  padding: 10px 12px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(135deg, rgba(238, 243, 240, 0.84), transparent 42%),
    linear-gradient(180deg, rgba(253, 251, 247, 0.88), rgba(245, 242, 234, 0.72)),
    var(--bg-elevated);
}

.command-title {
  display: flex;
  flex-direction: column;
  gap: 3px;
  min-width: 0;
}

.panel-kicker {
  font-family: var(--font-data);
  font-size: 11px;
  color: var(--accent-primary);
  font-weight: 900;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.command-title h2 {
  margin: 0;
  font-size: 20px;
  line-height: 1.15;
  letter-spacing: 0;
}

.command-title p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 12px;
}

.command-actions {
  display: grid;
  grid-template-columns: minmax(220px, 1fr) 150px auto auto;
  align-items: center;
  gap: 7px;
}

.search-input,
.status-select {
  width: 100%;
}

.result-alert {
  flex-shrink: 0;
}

.readiness-strip {
  display: grid;
  grid-template-columns: repeat(5, minmax(120px, 1fr));
  gap: 6px;
  flex-shrink: 0;
}

.readiness-cell {
  position: relative;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  min-width: 0;
  align-items: center;
  gap: 9px;
  padding: 8px 10px;
  border: 1px solid var(--border-default);
  border-radius: 6px;
  background:
    linear-gradient(180deg, rgba(253, 251, 247, 0.78), rgba(245, 242, 234, 0.58)),
    var(--bg-elevated);
  color: inherit;
  cursor: pointer;
  text-align: left;
  overflow: hidden;
}

.readiness-cell::before {
  content: '';
  position: absolute;
  inset: 0 auto 0 0;
  width: 4px;
  background: var(--readiness-color, var(--accent-primary));
  opacity: 0.85;
}

.readiness-cell:hover,
.readiness-cell--active {
  border-color: rgba(27, 61, 50, 0.28);
  background: var(--bg-hover);
}

.readiness-cell--active {
  box-shadow: inset 0 0 0 1px rgba(27, 61, 50, 0.18);
}

.readiness-cell__rank {
  display: grid;
  width: 28px;
  height: 22px;
  place-items: center;
  border: 1px solid color-mix(in srgb, var(--readiness-color, var(--accent-primary)) 34%, var(--border-subtle));
  border-radius: 4px;
  background: color-mix(in srgb, var(--readiness-color, var(--accent-primary)) 10%, #fdfbf7);
  color: var(--readiness-color, var(--accent-primary));
  font-family: var(--font-data);
  font-size: 10px;
  font-weight: 900;
}

.readiness-cell__body {
  display: grid;
  min-width: 0;
  gap: 2px;
}

.readiness-cell__body strong {
  color: var(--text-bright);
  font-size: 12px;
  line-height: 1.1;
}

.readiness-cell__body small {
  color: var(--text-secondary);
  font-size: 11px;
  line-height: 1.2;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.readiness-cell__count {
  font-family: var(--font-data);
  color: var(--readiness-color, var(--text-bright));
  font-size: 18px;
  font-weight: 800;
}

.readiness-cell--good {
  --readiness-color: #2d6a4f;
}

.readiness-cell--neutral {
  --readiness-color: #355e4f;
}

.readiness-cell--info {
  --readiness-color: #4f665d;
}

.readiness-cell--warn {
  --readiness-color: #9a6a19;
}

.readiness-cell--blocked {
  --readiness-color: #7e5f4f;
}

.command-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(360px, 420px);
  gap: 10px;
  flex: 1;
  min-height: 0;
}

.queue-panel,
.inspector-panel {
  display: flex;
  min-height: 0;
  flex-direction: column;
  overflow: hidden;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: rgba(253, 251, 247, 0.86);
}

.panel-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  padding: 9px 11px;
  border-bottom: 1px solid var(--border-subtle);
  background: rgba(245, 242, 234, 0.72);
}

.panel-header strong {
  display: block;
  color: var(--text-bright);
  font-size: 13px;
}

.panel-header span {
  color: var(--text-secondary);
  font-size: 11px;
}

.command-table {
  flex: 1;
  min-height: 0;
}

.hypothesis-cell {
  display: grid;
  gap: 2px;
  min-width: 0;
}

.hypothesis-cell strong {
  color: var(--text-bright);
  font-size: 12px;
  line-height: 1.3;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.hypothesis-cell span {
  color: var(--text-secondary);
  font-size: 11px;
}

.next-action {
  display: inline-flex;
  min-width: 68px;
  justify-content: center;
  padding: 2px 7px;
  border: 1px solid var(--border-subtle);
  border-radius: 4px;
  font-family: var(--font-data);
  font-size: 11px;
  font-weight: 800;
}

.next-action--good {
  color: #2d6a4f;
  background: #eaf5f0;
  border-color: rgba(45, 106, 79, 0.22);
}

.next-action--warn {
  color: #9a6a19;
  background: #fdf6e6;
  border-color: rgba(154, 106, 25, 0.24);
}

.next-action--bad {
  color: #9d3030;
  background: #fbf1f1;
  border-color: rgba(157, 48, 48, 0.22);
}

.next-action--neutral {
  color: var(--accent-primary);
  background: var(--bg-active);
  border-color: rgba(27, 61, 50, 0.16);
}

.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  min-width: 0;
}

.tag-list span {
  color: var(--text-secondary);
}

.tag-list a {
  color: inherit;
  text-decoration: none;
}

.tag-list a:hover {
  color: var(--accent-primary);
}

.inspector-panel {
  background:
    linear-gradient(180deg, rgba(245, 242, 234, 0.76), rgba(253, 251, 247, 0.82)),
    var(--bg-elevated);
}

.inspector-header {
  display: grid;
  gap: 6px;
  padding: 12px;
  border-bottom: 1px solid var(--border-subtle);
  background: rgba(238, 243, 240, 0.76);
}

.inspector-header > span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: 10px;
  font-weight: 900;
  letter-spacing: 0.08em;
}

.inspector-header h3 {
  margin: 0;
  color: var(--text-bright);
  font-size: 16px;
  line-height: 1.35;
}

.inspector-status {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
}

.inspector-status span {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 11px;
}

.inspector-body {
  display: flex;
  flex: 1;
  min-height: 0;
  flex-direction: column;
  gap: 9px;
  padding: 10px;
  overflow: auto;
}

.inspector-section {
  display: grid;
  gap: 7px;
  padding: 9px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(253, 251, 247, 0.72);
}

.inspector-section__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.inspector-section__head strong {
  color: var(--text-bright);
  font-size: 12px;
}

.inspector-section__head span,
.inspector-section__head a {
  color: var(--text-secondary);
  font-size: 11px;
  text-decoration: none;
}

.inspector-section__head a:hover {
  color: var(--accent-primary);
}

.inspector-section p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 12px;
  line-height: 1.55;
}

.note-copy {
  padding-top: 6px;
  border-top: 1px dashed var(--border-subtle);
}

.vault-box,
.data-gap {
  display: grid;
  gap: 3px;
  padding: 8px;
  border: 1px dashed var(--border-default);
  border-radius: 5px;
  background: var(--bg-primary);
}

.vault-box span,
.data-gap small {
  color: var(--text-secondary);
  font-size: 11px;
}

.vault-box strong,
.data-gap span {
  color: var(--text-bright);
  font-size: 12px;
}

.vault-box small {
  color: var(--text-secondary);
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.data-gap--warn {
  background: #fdf6e6;
  border-color: rgba(154, 106, 25, 0.26);
}

.tone-warn {
  color: #9a6a19 !important;
}

.tone-good {
  color: #2d6a4f !important;
}

.factor-link-list,
.metric-list {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}

.factor-link-list button,
.metric-list span {
  min-width: 0;
  border: 1px solid var(--border-subtle);
  border-radius: 4px;
  background: var(--bg-elevated);
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: 11px;
  padding: 3px 6px;
}

.factor-link-list button {
  cursor: pointer;
}

.factor-link-list button:hover {
  background: var(--bg-hover);
  border-color: rgba(27, 61, 50, 0.28);
}

.metric-list span {
  color: var(--text-secondary);
}

.muted-copy {
  color: var(--text-secondary);
  font-size: 12px;
}

.experiment-stack {
  display: grid;
  gap: 6px;
}

.experiment-stack div {
  display: grid;
  gap: 2px;
  padding: 7px;
  border: 1px solid var(--border-subtle);
  border-radius: 5px;
  background: var(--bg-primary);
}

.experiment-stack strong {
  color: var(--text-bright);
  font-size: 12px;
}

.experiment-stack span {
  color: var(--text-secondary);
  font-size: 11px;
}

.inspector-actions {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 7px;
  padding: 10px;
  border-top: 1px solid var(--border-subtle);
  background: rgba(245, 242, 234, 0.78);
}

.action-button {
  display: inline-flex;
  min-height: 30px;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--border-default);
  border-radius: 5px;
  background: var(--bg-primary);
  color: var(--text-primary);
  cursor: pointer;
  font-size: 12px;
  font-weight: 750;
  text-decoration: none;
}

.action-button:hover {
  background: var(--bg-hover);
  border-color: rgba(27, 61, 50, 0.28);
}

.action-button--primary {
  background: var(--accent-primary);
  border-color: var(--accent-primary);
  color: #fdfbf7;
}

.action-button--primary:hover {
  background: #355e4f;
  color: #fdfbf7;
}

.empty-state,
.inspector-empty {
  display: grid;
  place-items: center;
  gap: 8px;
  min-height: 150px;
  padding: 18px;
  color: var(--text-secondary);
  text-align: center;
}

.empty-state strong,
.inspector-empty strong {
  color: var(--text-bright);
}

.empty-state span,
.inspector-empty span {
  font-size: 12px;
}

:deep(.el-table) {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: var(--bg-elevated);
  --el-table-header-text-color: var(--text-secondary);
  --el-table-text-color: var(--text-primary);
  --el-table-row-hover-bg-color: var(--bg-hover);
  --el-table-border-color: var(--border-subtle);
  font-size: 12px;
}

:deep(.el-table th.el-table__cell) {
  background: rgba(245, 242, 234, 0.95);
  font-size: 11px;
  font-weight: 800;
}

:deep(.el-table td.el-table__cell) {
  background: transparent;
  padding: 5px 0;
}

:deep(.el-table__body tr.current-row > td.el-table__cell),
:deep(.el-table__body tr.hover-row > td.el-table__cell),
:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: var(--bg-hover);
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper) {
  min-height: 30px;
  background: rgba(253, 251, 247, 0.88);
  box-shadow: 0 0 0 1px var(--border-subtle) inset;
}

:deep(.el-button:not(.el-button--primary)) {
  min-height: 30px;
  color: var(--text-primary);
  background: var(--bg-primary);
  border-color: var(--border-default);
}

:deep(.el-tag) {
  color: var(--accent-primary);
  background-color: var(--bg-active);
  background-image: none;
  border-color: rgba(27, 61, 50, 0.18);
}

:deep(.el-tag--success) {
  color: #2d6a4f;
  background: #eaf5f0;
  border-color: rgba(45, 106, 79, 0.22);
}

:deep(.el-tag--warning) {
  color: #9a6a19;
  background: #fdf6e6;
  border-color: rgba(154, 106, 25, 0.24);
}

:deep(.el-tag--danger) {
  color: #9d3030;
  background: #fbf1f1;
  border-color: rgba(157, 48, 48, 0.22);
}

:deep(.el-tag--info) {
  color: var(--text-secondary);
  background: #f2f2ef;
  border-color: rgba(126, 141, 134, 0.22);
}

@media (max-width: 1180px) {
  .command-header {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .command-actions {
    grid-template-columns: 1fr 150px auto auto;
  }

  .command-grid {
    grid-template-columns: 1fr;
    overflow: auto;
  }

  .inspector-panel {
    min-height: 520px;
  }
}

@media (max-width: 760px) {
  .investment-research {
    padding: 10px;
  }

  .command-header {
    padding: 10px;
  }

  .command-actions,
  .readiness-strip,
  .inspector-actions {
    grid-template-columns: 1fr;
  }

  .command-title h2 {
    font-size: 18px;
  }
}
</style>
