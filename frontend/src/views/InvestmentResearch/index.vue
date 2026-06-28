<template>
  <div class="investment-research">
    <section class="research-shell" :class="`research-shell--layout-${layoutMode.toLowerCase()}`">
      <header class="research-header">
        <div class="research-title">
          <span class="panel-kicker">INVESTMENT RESEARCH</span>
          <h2>研究实验室</h2>
          <p>{{ activeLayoutDescription }}</p>
        </div>
        <div class="research-actions">
          <div class="layout-switcher" aria-label="切换研究实验室布局">
            <button
              v-for="option in researchLayoutOptions"
              :key="option.key"
              type="button"
              :class="{ active: layoutMode === option.key }"
              :title="option.hint"
              @click="layoutMode = option.key"
            >
              <span>{{ option.key }}</span>
              {{ option.label }}
            </button>
          </div>
          <el-input
            v-model="keyword"
            :prefix-icon="Search"
            clearable
            placeholder="搜索研报、因子、备注"
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

      <section class="summary-grid">
        <div>
          <span>研报总数</span>
          <strong>{{ manifestSummary.total }}</strong>
        </div>
        <div>
          <span>已映射</span>
          <strong>{{ manifestSummary.implemented }}</strong>
        </div>
        <div>
          <span>Tick-only</span>
          <strong>{{ manifestSummary.backlogTick }}</strong>
        </div>
        <div>
          <span>待数据源</span>
          <strong>{{ manifestSummary.pendingData }}</strong>
        </div>
      </section>

      <section class="research-layout-strip">
        <article v-for="item in researchLayoutNotes" :key="item.title">
          <span>{{ item.kicker }}</span>
          <strong>{{ item.title }}</strong>
          <small>{{ item.description }}</small>
        </article>
      </section>

      <!-- Layout A: 默认双栏笔记本模式 (保留原有展示) -->
      <template v-if="layoutMode === 'A'">
        <section class="research-tool-grid">
          <article class="research-panel research-panel--tools">
            <div class="panel-header">
              <div>
                <strong>研究操作台</strong>
                <span>把想法、证据、实验和复盘拆成可推进的动作</span>
              </div>
            </div>
            <div class="tool-card-grid">
              <a v-for="tool in researchTools" :key="tool.title" class="tool-card" :href="tool.href">
                <span>{{ tool.kicker }}</span>
                <strong>{{ tool.title }}</strong>
                <small>{{ tool.description }}</small>
              </a>
            </div>
          </article>

          <article class="research-panel research-panel--links">
            <div class="panel-header">
              <div>
                <strong>外部链接 / 本地笔记</strong>
                <span>研报、代码、笔记和可视化思维工具放在同一排入口</span>
              </div>
            </div>
            <div class="link-list">
              <a
                v-for="link in externalLinks"
                :key="link.title"
                :href="link.href"
                :target="link.href.startsWith('/') ? undefined : '_blank'"
                rel="noreferrer"
              >
                <span>{{ link.title }}</span>
                <small>{{ link.description }}</small>
              </a>
            </div>
          </article>
        </section>

        <section class="research-panel research-panel--main">
          <div class="panel-header">
            <div>
              <strong>研报落地清单</strong>
              <span>当前显示 {{ filteredManifestRows.length }} / {{ paperManifest.length }} 篇</span>
            </div>
          </div>
          <el-table
            :data="filteredManifestRows"
            v-loading="loading"
            size="small"
            height="100%"
            class="research-table"
          >
            <el-table-column prop="paper_id" label="#" width="58" fixed />
            <el-table-column prop="title" label="研报" min-width="280" show-overflow-tooltip fixed />
            <el-table-column prop="strategy_type" label="类型" width="126" show-overflow-tooltip />
            <el-table-column prop="data_frequency" label="频率" width="104" show-overflow-tooltip />
            <el-table-column prop="rebalance_frequency" label="调仓" width="104" show-overflow-tooltip />
            <el-table-column label="状态" width="130">
              <template #default="{ row }">
                <el-tag :type="paperStatusType(row.landing_status)" size="small" effect="plain">
                  {{ paperStatusLabel(row.landing_status) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="landing_grade" label="等级" width="72" />
            <el-table-column label="已映射因子" min-width="240">
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
            <el-table-column prop="platform_mapping" label="平台映射" min-width="220" show-overflow-tooltip />    
            <el-table-column prop="notes" label="备注" min-width="260" show-overflow-tooltip />
          </el-table>
        </section>

        <section class="research-panel" v-if="paperExperiments.length">
          <div class="panel-header">
            <div>
              <strong>AI/ML 离线实验</strong>
              <span>共 {{ paperExperiments.length }} 个实验规格</span>
            </div>
          </div>
          <el-table :data="paperExperiments" size="small" max-height="260" class="research-table">
            <el-table-column prop="name" label="实验" min-width="220" show-overflow-tooltip />
            <el-table-column label="报告" width="110">
              <template #default="{ row }">{{ row.paper_ids.join(', ') }}</template>
            </el-table-column>
            <el-table-column label="模型族" min-width="180">
              <template #default="{ row }">
                <div class="tag-list">
                  <el-tag v-for="name in row.model_family" :key="name" size="small" effect="plain">
                    {{ name }}
                  </el-tag>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="特征组" min-width="240">
              <template #default="{ row }">
                <div class="tag-list">
                  <el-tag v-for="name in row.feature_groups" :key="name" size="small" effect="plain">
                    {{ name }}
                  </el-tag>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="status" label="状态" width="120" />
            <el-table-column prop="target_policy" label="标签约束" min-width="300" show-overflow-tooltip />       
          </el-table>
        </section>
      </template>

      <!-- Layout B: Kanban Mode -->
      <template v-else-if="layoutMode === 'B'">
        <section class="research-kanban-board">
          <!-- Draft / Pending column -->
          <article class="kanban-column">
            <div class="kanban-column__head">
              <strong>待研究 (Draft)</strong>
              <span>{{ filteredManifestRows.filter(r => r.landing_status === 'pending_research' || r.landing_status === 'pending_data').length }}</span>
            </div>
            <div class="kanban-column__body">
              <div
                v-for="row in filteredManifestRows.filter(r => r.landing_status === 'pending_research' || r.landing_status === 'pending_data')"
                :key="row.paper_id"
                class="kanban-card"
              >
                <strong>{{ row.title }}</strong>
                <small>{{ row.strategy_type }} · {{ paperStatusLabel(row.landing_status) }}</small>
              </div>
            </div>
          </article>
          <!-- Active / In Progress column -->
          <article class="kanban-column">
            <div class="kanban-column__head">
              <strong>进行中 (Active)</strong>
              <span>{{ filteredManifestRows.filter(r => r.landing_status.startsWith('partial')).length }}</span>
            </div>
            <div class="kanban-column__body">
              <div
                v-for="row in filteredManifestRows.filter(r => r.landing_status.startsWith('partial'))"
                :key="row.paper_id"
                class="kanban-card"
              >
                <strong>{{ row.title }}</strong>
                <small>{{ row.strategy_type }} · {{ paperStatusLabel(row.landing_status) }}</small>
                <div class="tag-list" style="margin-top: 6px;">
                  <el-tag v-for="name in row.factor_names" :key="name" size="small" effect="plain">{{ name }}</el-tag>
                </div>
              </div>
            </div>
          </article>
          <!-- Done / Validated column -->
          <article class="kanban-column">
            <div class="kanban-column__head">
              <strong>已验证 (Done)</strong>
              <span>{{ filteredManifestRows.filter(r => r.landing_status.startsWith('implemented')).length }}</span>
            </div>
            <div class="kanban-column__body">
              <div
                v-for="row in filteredManifestRows.filter(r => r.landing_status.startsWith('implemented'))"
                :key="row.paper_id"
                class="kanban-card kanban-card--done"
              >
                <strong>{{ row.title }}</strong>
                <small>{{ row.strategy_type }} · {{ paperStatusLabel(row.landing_status) }}</small>
              </div>
            </div>
          </article>
        </section>
      </template>

      <!-- Layout C: Timeline Mode -->
      <template v-else-if="layoutMode === 'C'">
        <section class="research-timeline-view">
          <div class="timeline-header">
            <strong>实验日志流 (Timeline)</strong>
            <span>将最新状态的研报平铺，强调近期修改动作</span>
          </div>
          <div class="timeline-body">
            <div v-for="row in filteredManifestRows" :key="row.paper_id" class="timeline-item">
              <div class="timeline-marker"></div>
              <div class="timeline-content">
                <strong>{{ row.title }}</strong>
                <span class="timeline-meta">{{ paperStatusLabel(row.landing_status) }} · {{ row.landing_grade }}级 · {{ row.data_frequency }}</span>
                <p v-if="row.notes" class="timeline-note">{{ row.notes }}</p>
                <div class="tag-list" v-if="row.factor_names.length" style="margin-top: 8px;">
                  <span style="font-size: 11px; color: var(--text-muted); margin-right: 4px;">因子映射:</span>
                  <el-tag v-for="name in row.factor_names" :key="name" size="small" effect="plain">{{ name }}</el-tag>
                </div>
              </div>
            </div>
          </div>
        </section>
      </template>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { Refresh, Search } from '@element-plus/icons-vue'
import { usePageContext } from '@/app/pageContext'
import {
  factorValueApi,
  type FactorPaperExperimentSpec,
  type FactorPaperManifestItem,
} from '@/api/factorValues'

const paperManifest = ref<FactorPaperManifestItem[]>([])
const paperExperiments = ref<FactorPaperExperimentSpec[]>([])
const keyword = ref('')
const statusFilter = ref('')
const loading = ref(false)
const loadError = ref('')
const layoutMode = ref<'A' | 'B' | 'C'>('A')

const researchLayoutOptions = [
  { key: 'A' as const, label: '笔记本', hint: '双栏笔记本模式' },
  { key: 'B' as const, label: '看板', hint: 'Draft / In Progress / Validated / Archived' },
  { key: 'C' as const, label: '日历流', hint: '按时间线追踪研究活动' },
]
const activeLayoutDescription = computed(() => {
  if (layoutMode.value === 'B') return '以看板方式推进研究想法，把研报、因子映射、实验和复盘压缩成状态流。'
  if (layoutMode.value === 'C') return '以日志日历流复盘每天的研究活动点，强调证据链和实验结论。'
  return '双栏笔记本模式：左侧研究想法清单，右侧落地清单、证据链和离线实验。'
})
const researchLayoutNotes = computed(() => {
  if (layoutMode.value === 'B') {
    return [
      { kicker: 'DRAFT', title: '待研究', description: '把 pending / partial 的研报沉淀为可证伪假设。' },
      { kicker: 'ACTIVE', title: '进行中', description: '因子映射、数据源补齐和离线实验同屏推进。' },
      { kicker: 'DONE', title: '已验证', description: '已落地因子和策略候选进入评估/回测链路。' },
    ]
  }
  if (layoutMode.value === 'C') {
    return [
      { kicker: 'TODAY', title: '今日活动', description: '筛选、刷新和实验更新都会落在当前研究日志里。' },
      { kicker: 'EVIDENCE', title: '证据链', description: '行情、公告、财务、舆情和研报按日期对齐。' },
      { kicker: 'ARCHIVE', title: '失败复盘', description: '保留被否决假设，防止重复踩坑。' },
    ]
  }
  return [
    { kicker: 'IDEAS', title: '研究想法清单', description: '把假设、状态和落地等级放在左侧快速扫描。' },
    { kicker: 'NOTEBOOK', title: '主笔记区', description: '研报落地清单作为当前笔记主体，保留筛选和表格行为。' },
    { kicker: 'CHAIN', title: '证据链入口', description: '外部笔记、因子定义、因子评估和策略回测一键跳转。' },
  ]
})

const researchTools = [
  {
    kicker: 'IDEA',
    title: '研究假设卡',
    description: '记录信号来源、可证伪条件、适用股票池和预期失败场景。',
    href: 'obsidian://open?vault=GaoshouPlatform&file=Research%2FIdea%20Cards',
  },
  {
    kicker: 'EVIDENCE',
    title: '证据矩阵',
    description: '把行情、公告、财务、舆情和外部研报证据按日期对齐。',
    href: 'obsidian://open?vault=GaoshouPlatform&file=Research%2FEvidence%20Matrix',
  },
  {
    kicker: 'RUNBOOK',
    title: '实验记录',
    description: '沉淀参数、股票池、数据口径、结果截图和失败原因。',
    href: 'obsidian://open?vault=GaoshouPlatform&file=Research%2FExperiment%20Runs',
  },
  {
    kicker: 'ARCHIVE',
    title: '失败复盘',
    description: '保留被否决想法，防止重复踩坑。',
    href: 'obsidian://open?vault=GaoshouPlatform&file=Research%2FFailure%20Archive',
  },
]

const externalLinks = [
  {
    title: 'Obsidian Research Vault',
    description: '打开本地研究笔记库（需要已配置 vault 名称）。',
    href: 'obsidian://open?vault=GaoshouPlatform',
  },
  {
    title: '因子定义',
    description: '进入本平台因子目录、覆盖率和预计算入口。',
    href: '/factor',
  },
  {
    title: '因子评估',
    description: '进入 IC、多空收益、回撤和组合候选看板。',
    href: '/factor/evaluation',
  },
  {
    title: '策略回测',
    description: '把研究假设转为策略代码并运行验证。',
    href: '/backtest',
  },
]

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
    title: 'Research',
    rows: [
      { label: '刷新状态', value: loading.value ? '加载中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
      { label: '布局', value: researchLayoutOptions.find(item => item.key === layoutMode.value)?.label || '笔记本' },
      { label: '关键词', value: keyword.value.trim() || '-' },
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
      { label: '离线实验', value: `${paperExperiments.value.length}` },
    ],
  },
])

usePageContext(pageContextBlocks)

onMounted(() => {
  void loadResearch()
})
</script>

<style scoped>
.investment-research {
  height: 100%;
  min-height: 0;
  padding: 14px;
  color: var(--text-primary);
  background:
    linear-gradient(rgba(34, 48, 42, 0.024) 1px, transparent 1px),
    linear-gradient(90deg, rgba(34, 48, 42, 0.02) 1px, transparent 1px),
    linear-gradient(180deg, rgba(253, 251, 247, 0.9), rgba(245, 242, 234, 0.62));
  background-size: 56px 56px, 56px 56px, auto;
}

.research-shell {
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: hidden;
}

.research-header {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(520px, auto);
  align-items: end;
  gap: 16px;
  padding: 16px 18px;
  border: 1px solid var(--border-default);
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(238, 243, 240, 0.9), transparent 46%),
    linear-gradient(180deg, rgba(253, 251, 247, 0.8), rgba(245, 242, 234, 0.72)),
    var(--bg-elevated);
  box-shadow: var(--shadow-card);
}

.research-title {
  display: flex;
  flex-direction: column;
  gap: 5px;
  min-width: 0;
}

.panel-kicker {
  font-family: var(--font-data);
  font-size: var(--text-xs);
  color: var(--accent-primary);
  font-weight: 900;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.research-title h2 {
  margin: 0;
  font-size: var(--text-2xl);
  line-height: 1.1;
  letter-spacing: 0;
}

.research-title p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.research-actions {
  display: grid;
  grid-template-columns: 1fr minmax(220px, 320px) 150px auto;
  align-items: center;
  gap: 8px;
}

.layout-switcher {
  display: inline-flex;
  gap: 4px;
  justify-self: start;
  padding: 4px;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-full);
  background: rgba(253, 251, 247, 0.78);
}

.layout-switcher button {
  border: 0;
  border-radius: var(--radius-full);
  background: transparent;
  color: var(--text-secondary);
  cursor: pointer;
  font-size: var(--text-xs);
  font-weight: 800;
  padding: 7px 11px;
}

.layout-switcher button span {
  margin-right: 4px;
  font-family: var(--font-data);
}

.layout-switcher button.active {
  background: var(--accent-primary);
  color: #fdfbf7;
}

.search-input,
.status-select {
  width: 100%;
}

.result-alert {
  flex-shrink: 0;
}

.summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(130px, 1fr));
  gap: 8px;
}

.research-tool-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(340px, 0.75fr);
  gap: 12px;
}

.summary-grid > div {
  padding: 11px 12px;
  border: 1px solid var(--border-default);
  border-radius: 14px;
  background:
    linear-gradient(180deg, rgba(238, 243, 240, 0.68), rgba(253, 251, 247, 0.42)),
    var(--bg-elevated);
}

.summary-grid span {
  display: block;
  margin-bottom: 4px;
  color: var(--text-secondary);
  font-size: 12px;
}

.summary-grid strong {
  font-family: var(--font-data);
  color: var(--text-bright);
  font-size: 20px;
  font-weight: 650;
}

.research-layout-strip {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.research-layout-strip article {
  display: grid;
  gap: 4px;
  min-width: 0;
  padding: 12px 14px;
  border: 1px solid var(--border-default);
  border-radius: 14px;
  background:
    linear-gradient(180deg, rgba(238, 243, 240, 0.68), rgba(253, 251, 247, 0.42)),
    var(--bg-surface);
}

.research-layout-strip span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 900;
  letter-spacing: 0.08em;
}

.research-layout-strip strong {
  color: var(--text-bright);
  font-size: var(--text-base);
}

.research-layout-strip small {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.45;
}

.research-panel {
  display: flex;
  min-height: 0;
  flex-direction: column;
  gap: 10px;
  padding: 12px 14px;
  border: 1px solid var(--border-default);
  border-radius: 16px;
  background: rgba(253, 251, 247, 0.78);
  overflow: hidden;
}

.research-panel--main {
  flex: 1;
}

.research-panel--tools,
.research-panel--links {
  flex-shrink: 0;
}

.panel-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.panel-header strong {
  display: block;
  color: var(--text-bright);
  font-size: 14px;
}

.panel-header span {
  color: var(--text-secondary);
  font-size: 12px;
}

.research-table {
  min-height: 0;
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

.tool-card-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.tool-card,
.link-list a {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 0;
  padding: 11px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: 14px;
  color: inherit;
  background: rgba(253, 251, 247, 0.68);
  text-decoration: none;
}

.tool-card:hover,
.link-list a:hover {
  border-color: rgba(27, 61, 50, 0.26);
  background: var(--bg-hover);
}

.tool-card span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: 11px;
}

.tool-card strong,
.link-list span {
  color: var(--text-bright);
  font-size: 13px;
}

.tool-card small,
.link-list small {
  color: var(--text-secondary);
  font-size: 11px;
  line-height: 1.45;
}

.link-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

:deep(.el-table) {
  --el-table-bg-color: var(--bg-primary);
  --el-table-tr-bg-color: var(--bg-primary);
  --el-table-header-bg-color: var(--bg-elevated);
  --el-table-header-text-color: var(--text-secondary);
  --el-table-text-color: var(--text-primary);
  --el-table-row-hover-bg-color: var(--bg-hover);
  --el-table-border-color: var(--border-subtle);
}

:deep(.el-table th.el-table__cell),
:deep(.el-table td.el-table__cell) {
  background: transparent;
}

:deep(.el-table__body tr.current-row > td.el-table__cell),
:deep(.el-table__body tr.hover-row > td.el-table__cell),
:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: var(--bg-hover);
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper) {
  background: rgba(253, 251, 247, 0.86);
  box-shadow: 0 0 0 1px var(--border-subtle) inset;
}

:deep(.el-button:not(.el-button--primary)) {
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
  color: #86efac;
  background: linear-gradient(180deg, rgba(34, 197, 94, 0.16), rgba(34, 197, 94, 0.06));
  border-color: rgba(34, 197, 94, 0.22);
}

:deep(.el-tag--warning) {
  color: #fcd34d;
  background: linear-gradient(180deg, rgba(245, 158, 11, 0.16), rgba(245, 158, 11, 0.06));
  border-color: rgba(245, 158, 11, 0.22);
}

:deep(.el-tag--danger) {
  color: #fca5a5;
  background: linear-gradient(180deg, rgba(239, 68, 68, 0.16), rgba(239, 68, 68, 0.06));
  border-color: rgba(239, 68, 68, 0.22);
}

:deep(.el-tag--info) {
  color: #93c5fd;
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.14), rgba(56, 189, 248, 0.06));
  border-color: rgba(96, 165, 250, 0.22);
}

@media (max-width: 1180px) {
  .research-header {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .research-actions {
    grid-template-columns: 1fr 1fr auto;
  }

  .research-tool-grid,
  .tool-card-grid,
  .research-layout-strip {
    grid-template-columns: 1fr 1fr;
  }
}

@media (max-width: 760px) {
  .investment-research {
    padding: 10px;
  }

  .research-header {
    padding: 14px;
  }

  .research-actions,
  .summary-grid,
  .research-tool-grid,
  .tool-card-grid,
  .link-list,
  .research-layout-strip {
    grid-template-columns: 1fr;
  }

  .research-title h2 {
    font-size: 18px;
  }
}
</style>
