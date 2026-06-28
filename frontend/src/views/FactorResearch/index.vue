<template>
  <div class="factor-research">
    <section
      v-if="isShellVisible"
      class="factor-shell"
      :class="[
        `factor-shell--${activeTab}`,
        `factor-shell--layout-${layoutMode.toLowerCase()}`,
      ]"
    >
      <header class="factor-header">
        <div class="factor-title">
          <span class="factor-title__eyebrow">FACTOR LAB / RESEARCH WORKBENCH</span>
          <h2>{{ activeTabLabel }}</h2>
          <p>{{ activeTabDescription }}</p>
        </div>

        <div class="factor-header__right">
          <div class="layout-switcher" aria-label="切换因子页面布局">
            <button
              v-for="option in factorLayoutOptions"
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
          <div class="factor-status">
            <div>
              <span>当前视图</span>
              <strong>{{ activeTabLabel }}</strong>
            </div>
            <div>
              <span>计算前置</span>
              <strong>{{ activeTab === 'board' ? '缓存已落盘' : '可预计算' }}</strong>
            </div>
            <div>
              <span>布局预览</span>
              <strong>{{ activeLayoutLabel }}</strong>
            </div>
          </div>
        </div>
      </header>

      <section class="factor-layout-strip">
        <article v-for="item in factorLayoutNotes" :key="item.title">
          <span>{{ item.kicker }}</span>
          <strong>{{ item.title }}</strong>
          <small>{{ item.description }}</small>
        </article>
      </section>

      <el-tabs v-model="activeTab" class="factor-tabs" @tab-change="handleTabChange">
        <el-tab-pane label="因子定义 / 预计算" name="factor-values" lazy>
          <FactorValueStore v-show="activeTab === 'factor-values'" />
        </el-tab-pane>
        <el-tab-pane label="因子评估看板" name="board" lazy>
          <FactorBoard v-show="activeTab === 'board'" />
        </el-tab-pane>
      </el-tabs>
    </section>
    <router-view v-if="!isShellVisible" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { usePageContext } from '@/app/pageContext'
import FactorBoard from './FactorBoard.vue'
import FactorValueStore from './FactorValueStore.vue'

const route = useRoute()
const router = useRouter()
const activeTab = ref('factor-values')
const layoutMode = ref<'A' | 'B' | 'C'>('A')
const shellRouteNames = new Set(['FactorResearch', 'FactorEvaluation'])
const isShellVisible = computed(() => shellRouteNames.has(String(route.name || '')))
const activeTabLabel = computed(() => activeTab.value === 'board' ? '因子评估' : '因子定义')
const activeTabDescription = computed(() => (
  activeTab.value === 'board'
    ? '评估 IC、ICIR、多空收益、回撤、换手和已计算组合；不在评估页展开表达式编辑器。'
    : '管理因子目录、覆盖率、参数版本和预计算；表达式只在创建或编辑因子时打开。'
))
const factorLayoutOptions = computed(() => (
  activeTab.value === 'board'
    ? [
        { key: 'A' as const, label: '参数 / 图表', hint: '评估参数与图表双栏' },
        { key: 'B' as const, label: '2x2 大板', hint: '多图表网格' },
        { key: 'C' as const, label: '报告流', hint: '历史分析报告流' },
      ]
    : [
        { key: 'A' as const, label: '算子 IDE', hint: '因子树、表达式、预计算三栏' },
        { key: 'B' as const, label: '元数据表', hint: '因子定义大表' },
        { key: 'C' as const, label: '依赖拓扑', hint: '因子依赖链路' },
      ]
))
const activeLayoutLabel = computed(() => factorLayoutOptions.value.find(item => item.key === layoutMode.value)?.label || '算子 IDE')
const factorLayoutNotes = computed(() => {
  if (activeTab.value === 'board') {
    return [
      { kicker: 'SETTINGS', title: layoutMode.value === 'A' ? '评估参数侧栏' : '参数上移', description: '保留现有评估看板，只调整入口和报表信息层级。' },
      { kicker: 'GRAPHICS', title: layoutMode.value === 'B' ? '多图并排' : '报告视口', description: 'IC、多空收益、回撤和换手保持消费缓存的业务语义。' },
      { kicker: 'REPORT', title: layoutMode.value === 'C' ? '论文式报告流' : '样本外审计', description: '突出 OOS、覆盖率和结果可复盘，而不是重新计算因子。' },
    ]
  }
  return [
    { kicker: 'TREE', title: layoutMode.value === 'A' ? '因子树导航' : '定义目录', description: '因子分类、状态和覆盖率作为第一优先级。' },
    { kicker: 'EXPR', title: layoutMode.value === 'C' ? '依赖拓扑' : '表达式工作区', description: '表达式编辑仍只在新建/编辑时打开，避免误改定义。' },
    { kicker: 'CACHE', title: '预计算审计', description: '右侧显示缓存覆盖、落盘范围和待补数据口径。' },
  ]
})

const pageContextBlocks = computed(() => {
  if (!isShellVisible.value) return null
  return [
    {
      title: 'Factor Lab',
      rows: [
        { label: '当前视图', value: activeTabLabel.value },
        { label: '路径', value: route.path },
        { label: '布局', value: activeLayoutLabel.value },
        { label: '外壳状态', value: '主工作台', tone: 'good' },
      ],
    },
    {
      title: 'Mode',
      rows: [
        { label: '预计算', value: activeTab.value === 'board' ? '消费缓存' : '可发起预计算' },
        { label: '表达式入口', value: activeTab.value === 'board' ? '关闭' : '创建/编辑时打开' },
      ],
    },
  ]
})

usePageContext(pageContextBlocks)

const routeToTab = () => {
  if (route.path.startsWith('/factor/evaluation')) return 'board'
  if (route.query.tab === 'board') return 'board'
  return 'factor-values'
}

const handleTabChange = (name: string | number) => {
  const target = name === 'board' ? '/factor/evaluation' : '/factor'
  if (route.path !== target) router.push(target)
}

watch(
  () => [route.path, route.query.tab],
  () => {
    activeTab.value = routeToTab()
  },
  { immediate: true },
)
</script>

<style scoped>
.factor-research {
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

.factor-shell {
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: hidden;
}

.factor-header {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: stretch;
  gap: 16px;
  padding: 16px 18px;
  border: 1px solid var(--border-default);
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(238, 243, 240, 0.9), transparent 46%),
    linear-gradient(180deg, rgba(253, 251, 247, 0.8), rgba(245, 242, 234, 0.72)),
    var(--bg-elevated);
  box-shadow: var(--shadow-card);
  position: relative;
  overflow: hidden;
}

.factor-header::after {
  content: '';
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent, rgba(27, 61, 50, 0.36), transparent);
}

.factor-title {
  display: flex;
  flex-direction: column;
  gap: 5px;
  min-width: 0;
}

.factor-title__eyebrow {
  font-family: var(--font-data);
  font-size: var(--text-xs);
  color: var(--accent-primary);
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.factor-title h2 {
  margin: 0;
  font-size: var(--text-2xl);
  line-height: 1.1;
  letter-spacing: 0;
}

.factor-title p {
  margin: 0;
  max-width: 620px;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.factor-header__right {
  display: grid;
  align-content: space-between;
  gap: 10px;
  min-width: 460px;
}

.layout-switcher {
  justify-self: end;
  display: inline-flex;
  gap: 4px;
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

.factor-status {
  display: grid;
  grid-template-columns: repeat(3, minmax(110px, 1fr));
  gap: 8px;
}

.factor-status > div {
  padding: 10px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: 12px;
  background: rgba(253, 251, 247, 0.64);
  box-shadow: inset 0 1px 0 rgba(253, 251, 247, 0.72);
}

.factor-status span {
  display: block;
  margin-bottom: 4px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.factor-status strong {
  font-family: var(--font-data);
  color: var(--text-bright);
  font-size: var(--text-sm);
  font-weight: 800;
}

.factor-layout-strip {
  flex-shrink: 0;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.factor-layout-strip article {
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

.factor-layout-strip span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 900;
  letter-spacing: 0.08em;
}

.factor-layout-strip strong {
  color: var(--text-bright);
  font-size: var(--text-base);
}

.factor-layout-strip small {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.45;
}

.factor-tabs {
  min-height: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-default);
  border-radius: 16px;
  background: rgba(253, 251, 247, 0.78);
  overflow: hidden;
}

:deep(.el-tabs__header) {
  margin: 0;
  padding: 9px 12px 0;
  background: rgba(245, 242, 234, 0.72);
}

:deep(.el-tabs__nav-wrap::after) {
  height: 1px;
  background: var(--border-subtle);
}

:deep(.el-tabs__item) {
  height: 34px;
  padding: 0 16px;
  color: var(--text-secondary);
  font-weight: 600;
  letter-spacing: 0;
}

:deep(.el-tabs__item.is-active) {
  color: var(--accent-primary);
}

:deep(.el-tabs__active-bar) {
  height: 2px;
  background: var(--accent-primary);
  box-shadow: none;
}

:deep(.el-tabs__content) {
  flex: 1;
  min-height: 0;
  height: auto;
  padding: 12px;
  overflow: auto;
  scroll-padding-top: 12px;
}

:deep(.el-tab-pane) {
  height: 100%;
}

:deep(.el-table) {
  --el-table-bg-color: var(--bg-primary);
  --el-table-tr-bg-color: var(--bg-primary);
  --el-table-header-bg-color: var(--bg-elevated);
  --el-table-row-hover-bg-color: var(--bg-hover);
  --el-table-border-color: var(--border-subtle);
  --el-table-text-color: var(--text-primary);
  --el-table-header-text-color: var(--text-secondary);
  background: var(--bg-primary) !important;
  color: var(--text-primary);
}

:deep(.el-table__inner-wrapper),
:deep(.el-table__body-wrapper),
:deep(.el-table__fixed),
:deep(.el-table__fixed-right),
:deep(.el-table__fixed-body-wrapper),
:deep(.el-table__fixed-right-patch) {
  background: transparent !important;
}

:deep(.el-table th.el-table__cell) {
  background: var(--bg-elevated) !important;
  color: var(--text-secondary);
  border-bottom-color: var(--border-subtle);
}

:deep(.el-table tr),
:deep(.el-table__body tr),
:deep(.el-table__body td.el-table__cell) {
  background: var(--bg-primary) !important;
  color: var(--text-primary);
}

:deep(.el-table--striped .el-table__body tr.el-table__row--striped td.el-table__cell) {
  background: var(--bg-elevated) !important;
}

:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: var(--bg-hover) !important;
}

:deep(.el-table .el-table__cell) {
  border-bottom-color: var(--border-subtle) !important;
}

:deep(.el-table__inner-wrapper::before),
:deep(.el-table__border-left-patch),
:deep(.el-table__border-bottom-patch) {
  background-color: var(--border-subtle) !important;
}

:deep(.el-table .el-table-fixed-column--left),
:deep(.el-table .el-table-fixed-column--right) {
  background: var(--bg-primary) !important;
}

:deep(.el-tag:not(.el-tag--success):not(.el-tag--warning):not(.el-tag--danger):not(.el-tag--info)) {
  color: var(--accent-primary);
  background-color: var(--bg-active);
  border-color: rgba(27, 61, 50, 0.18);
}

@media (max-width: 1280px) {
  .factor-header {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .factor-header__right {
    min-width: 0;
  }

  .layout-switcher {
    justify-self: start;
    flex-wrap: wrap;
  }

  .factor-status {
    min-width: 0;
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .factor-layout-strip {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 900px) {
  .factor-research {
    padding: 10px;
  }

  .factor-header {
    padding: 14px;
  }

  .factor-title h2 {
    font-size: 18px;
  }

  .factor-status {
    grid-template-columns: 1fr;
  }

  :deep(.el-tabs__header) {
    padding-inline: 10px;
  }

  :deep(.el-tabs__content) {
    padding: 10px;
  }
}
</style>
