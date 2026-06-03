<template>
  <div class="investment-research">
    <section class="research-shell">
      <header class="research-header">
        <div class="research-title">
          <span class="panel-kicker">INVESTMENT RESEARCH</span>
          <h2>投研工作台</h2>
          <p>跟踪研报落地状态、因子映射和离线实验入口。</p>
        </div>
        <div class="research-actions">
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
        :closable="false"
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
    linear-gradient(135deg, rgba(56, 189, 248, 0.04), transparent 28%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.016), transparent 44%);
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
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: end;
  gap: 16px;
  padding: 16px 18px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(135deg, rgba(56, 189, 248, 0.11), transparent 36%),
    linear-gradient(90deg, rgba(251, 191, 36, 0.055), transparent 24%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)),
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
  font-size: 11px;
  color: var(--accent-primary);
  letter-spacing: 0;
}

.research-title h2 {
  margin: 0;
  font-size: 20px;
  line-height: 1.1;
  letter-spacing: 0;
}

.research-title p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 12px;
}

.research-actions {
  display: grid;
  grid-template-columns: minmax(220px, 320px) 150px auto;
  align-items: center;
  gap: 8px;
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
  border-radius: 8px;
  background:
    linear-gradient(180deg, rgba(56, 189, 248, 0.045), transparent),
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

.research-panel {
  display: flex;
  min-height: 0;
  flex-direction: column;
  gap: 10px;
  padding: 12px 14px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: rgba(15, 15, 19, 0.76);
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
  border-radius: 8px;
  color: inherit;
  background: rgba(10, 14, 20, 0.5);
  text-decoration: none;
}

.tool-card:hover,
.link-list a:hover {
  border-color: rgba(56, 189, 248, 0.38);
  background: rgba(56, 189, 248, 0.08);
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
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: rgba(15, 23, 42, 0.9);
  --el-table-header-text-color: #cbd5e1;
  --el-table-text-color: #dbe4f0;
  --el-table-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-border-color: rgba(148, 163, 184, 0.16);
}

:deep(.el-table th.el-table__cell),
:deep(.el-table td.el-table__cell) {
  background: transparent;
}

:deep(.el-table__body tr.current-row > td.el-table__cell),
:deep(.el-table__body tr.hover-row > td.el-table__cell),
:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: rgba(56, 189, 248, 0.08);
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper) {
  background: rgba(15, 23, 42, 0.76);
  box-shadow: 0 0 0 1px rgba(148, 163, 184, 0.18) inset;
}

:deep(.el-button:not(.el-button--primary)) {
  color: var(--text-primary);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0)), rgba(31, 41, 55, 0.72);
  border-color: rgba(148, 163, 184, 0.18);
}

:deep(.el-tag) {
  color: #dbe4f0;
  background-color: rgba(51, 65, 85, 0.38);
  background-image: linear-gradient(180deg, rgba(148, 163, 184, 0.14), rgba(148, 163, 184, 0.06));
  border-color: rgba(148, 163, 184, 0.24);
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
    grid-template-columns: minmax(0, 1fr) 150px auto;
  }

  .research-tool-grid,
  .tool-card-grid {
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
  .link-list {
    grid-template-columns: 1fr;
  }

  .research-title h2 {
    font-size: 18px;
  }
}
</style>
