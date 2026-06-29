<template>
  <div class="factor-research">
    <section
      v-if="isShellVisible"
      class="factor-shell"
      :class="`factor-shell--${activeTab}`"
    >
      <el-tabs v-model="activeTab" class="factor-tabs" @tab-change="handleTabChange">
        <el-tab-pane label="因子研究" name="factor-values" lazy>
          <FactorValueStore v-show="activeTab === 'factor-values'" />
        </el-tab-pane>
        <el-tab-pane label="研究评估" name="board" lazy>
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
const shellRouteNames = new Set(['FactorResearch', 'FactorEvaluation'])
const isShellVisible = computed(() => shellRouteNames.has(String(route.name || '')))
const activeTabLabel = computed(() => activeTab.value === 'board' ? '研究评估' : '因子研究')

const pageContextBlocks = computed(() => {
  if (!isShellVisible.value) return null
  return [
    {
      title: 'Factor Lab',
      rows: [
        { label: '当前视图', value: activeTabLabel.value },
        { label: '路径', value: route.path },
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
  padding: 10px;
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
  gap: 0;
  overflow: hidden;
}

.factor-tabs {
  min-height: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  background: rgba(253, 251, 247, 0.78);
  overflow: hidden;
}

:deep(.el-tabs__header) {
  margin: 0;
  padding: 0 0 8px;
  background: transparent;
}

:deep(.el-tabs__nav-wrap::after) {
  display: none;
}

:deep(.el-tabs__nav-scroll) {
  padding: 0;
}

:deep(.el-tabs__nav) {
  display: grid;
  grid-template-columns: repeat(2, minmax(180px, 1fr));
  gap: 8px;
  float: none;
  width: 100%;
}

:deep(.el-tabs__item) {
  justify-content: center;
  height: 42px;
  padding: 0 18px;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  background: rgba(253, 251, 247, 0.72);
  color: var(--text-secondary);
  font-size: 14px;
  font-weight: 800;
  letter-spacing: 0;
}

:deep(.el-tabs__item.is-active) {
  border-color: rgba(27, 61, 50, 0.38);
  background: var(--accent-primary);
  color: #fdfbf7;
  box-shadow: inset 0 0 0 1px rgba(253, 251, 247, 0.2);
}

:deep(.el-tabs__active-bar) {
  display: none;
}

:deep(.el-tabs__content) {
  flex: 1;
  min-height: 0;
  height: auto;
  padding: 8px;
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
  color: #16352b !important;
  background-color: rgba(27, 61, 50, 0.14) !important;
  border-color: rgba(27, 61, 50, 0.34) !important;
  font-weight: 650;
}

:deep(.el-tag:not(.el-tag--success):not(.el-tag--warning):not(.el-tag--danger):not(.el-tag--info) .el-tag__content) {
  color: #16352b !important;
}

@media (max-width: 900px) {
  .factor-research {
    padding: 10px;
  }

  :deep(.el-tabs__nav) {
    grid-template-columns: 1fr;
  }

  :deep(.el-tabs__header) {
    padding-bottom: 8px;
  }

  :deep(.el-tabs__content) {
    padding: 10px;
  }
}
</style>
