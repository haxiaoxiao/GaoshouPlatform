<template>
  <div class="factor-research">
    <section v-if="isShellVisible" class="factor-shell">
      <header class="factor-header">
        <div class="factor-title">
          <span class="factor-title__eyebrow">FACTOR LAB / RESEARCH WORKBENCH</span>
          <h2>因子研究</h2>
          <p>管理因子缓存、检查覆盖率、预计算特征，并查看因子表现。</p>
        </div>
        <div class="factor-status">
          <div>
            <span>当前视图</span>
            <strong>{{ activeTabLabel }}</strong>
          </div>
          <div>
            <span>数据口径</span>
            <strong>Point-in-time</strong>
          </div>
          <div>
            <span>默认窗口</span>
            <strong>过去一年</strong>
          </div>
        </div>
      </header>

      <el-tabs v-model="activeTab" class="factor-tabs">
        <el-tab-pane label="因子缓存" name="factor-values" lazy>
          <FactorValueStore v-if="activeTab === 'factor-values'" />
        </el-tab-pane>
        <el-tab-pane label="因子看板" name="board" lazy>
          <FactorBoard v-if="activeTab === 'board'" />
        </el-tab-pane>
      </el-tabs>
    </section>
    <router-view v-if="!isShellVisible" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import FactorBoard from './FactorBoard.vue'
import FactorValueStore from './FactorValueStore.vue'

const route = useRoute()
const activeTab = ref('factor-values')
const isShellVisible = computed(() => route.name === 'FactorResearch')
const activeTabLabel = computed(() => activeTab.value === 'board' ? '因子看板' : '因子缓存')

watch(
  () => route.query.tab,
  (tab) => {
    if (tab === 'board' || tab === 'factor-values') activeTab.value = tab
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
    linear-gradient(135deg, rgba(56, 189, 248, 0.045), transparent 26%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.018), transparent 42%);
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
  align-items: end;
  gap: 16px;
  padding: 16px 18px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(135deg, rgba(56, 189, 248, 0.12), transparent 36%),
    linear-gradient(90deg, rgba(251, 191, 36, 0.05), transparent 24%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)),
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
  background: linear-gradient(90deg, transparent, rgba(56, 189, 248, 0.55), transparent);
}

.factor-title {
  display: flex;
  flex-direction: column;
  gap: 5px;
  min-width: 0;
}

.factor-title__eyebrow {
  font-family: var(--font-data);
  font-size: 11px;
  color: var(--accent-primary);
  letter-spacing: 0;
}

.factor-title h2 {
  margin: 0;
  font-size: 20px;
  line-height: 1.1;
  letter-spacing: 0;
}

.factor-title p {
  margin: 0;
  max-width: 620px;
  color: var(--text-secondary);
  font-size: 12px;
}

.factor-status {
  display: grid;
  grid-template-columns: repeat(3, minmax(110px, 1fr));
  gap: 8px;
  min-width: 420px;
}

.factor-status > div {
  padding: 9px 11px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(10, 10, 12, 0.48);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.035);
}

.factor-status span {
  display: block;
  margin-bottom: 4px;
  color: var(--text-muted);
  font-size: 11px;
}

.factor-status strong {
  font-family: var(--font-data);
  color: var(--text-bright);
  font-size: 13px;
  font-weight: 600;
}

.factor-tabs {
  min-height: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: rgba(15, 15, 19, 0.76);
  overflow: hidden;
}

:deep(.el-tabs__header) {
  margin: 0;
  padding: 9px 12px 0;
  background: rgba(10, 10, 12, 0.36);
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
  box-shadow: 0 0 10px rgba(56, 189, 248, 0.45);
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
  --el-table-bg-color: rgba(10, 14, 20, 0.78);
  --el-table-tr-bg-color: rgba(11, 16, 24, 0.72);
  --el-table-header-bg-color: rgba(11, 20, 31, 0.95);
  --el-table-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-border-color: rgba(108, 117, 137, 0.22);
  --el-table-text-color: var(--text-primary);
  --el-table-header-text-color: var(--text-secondary);
  background: rgba(10, 14, 20, 0.78) !important;
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
  background: rgba(11, 20, 31, 0.95) !important;
  color: var(--text-secondary);
  border-bottom-color: rgba(108, 117, 137, 0.24);
}

:deep(.el-table tr),
:deep(.el-table__body tr),
:deep(.el-table__body td.el-table__cell) {
  background: rgba(11, 16, 24, 0.72) !important;
  color: var(--text-primary);
}

:deep(.el-table--striped .el-table__body tr.el-table__row--striped td.el-table__cell) {
  background: rgba(15, 23, 34, 0.82) !important;
}

:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: rgba(56, 189, 248, 0.08) !important;
}

:deep(.el-table .el-table__cell) {
  border-bottom-color: rgba(108, 117, 137, 0.22) !important;
}

:deep(.el-table__inner-wrapper::before),
:deep(.el-table__border-left-patch),
:deep(.el-table__border-bottom-patch) {
  background-color: rgba(108, 117, 137, 0.22) !important;
}

:deep(.el-table .el-table-fixed-column--left),
:deep(.el-table .el-table-fixed-column--right) {
  background: rgba(11, 16, 24, 0.94) !important;
}

:deep(.el-tag:not(.el-tag--success):not(.el-tag--warning):not(.el-tag--danger):not(.el-tag--info)) {
  color: #dbeafe;
  background-color: rgba(56, 189, 248, 0.12);
  border-color: rgba(56, 189, 248, 0.24);
}

@media (max-width: 1280px) {
  .factor-header {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .factor-status {
    min-width: 0;
    grid-template-columns: repeat(3, minmax(0, 1fr));
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
