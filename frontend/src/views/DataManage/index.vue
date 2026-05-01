<template>
  <div class="data-dashboard">
    <!-- Hero Metrics Section -->
    <section class="metrics-hero">
      <div class="metrics-grid">
        <div
          v-for="(metric, index) in heroMetrics"
          :key="metric.key"
          class="metric-card"
          :style="{ animationDelay: `${index * 100}ms` }"
        >
          <div class="metric-card__glow" :style="{ background: metric.glowColor }"></div>
          <div class="metric-card__inner">
            <div class="metric-card__header">
              <span class="metric-card__icon" v-html="metric.icon"></span>
              <span class="metric-card__trend" v-if="metric.trend" :class="metric.trendClass">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path v-if="metric.trend > 0" d="M7 17l5-5 5 5M7 12l5-5 5 5" />
                  <path v-else d="M7 7l5 5 5-5M7 12l5 5 5-5" />
                </svg>
                {{ Math.abs(metric.trend) }}%
              </span>
            </div>
            <div class="metric-card__value">{{ formatNumber(metric.value) }}</div>
            <div class="metric-card__label">{{ metric.label }}</div>
          </div>
        </div>
      </div>
    </section>

    <!-- Main Content -->
    <section class="content-section">
      <!-- Tabs -->
      <div class="tab-navigation">
        <div class="tab-navigation__tabs">
          <button
            v-for="tab in tabs"
            :key="tab.key"
            class="tab-btn"
            :class="{ 'tab-btn--active': activeTab === tab.key }"
            @click="activeTab = tab.key"
          >
            <span class="tab-btn__icon" v-html="tab.icon"></span>
            <span>{{ tab.label }}</span>
          </button>
        </div>
        <div class="tab-navigation__actions">
          <el-button
            type="primary"
            size="small"
            :icon="Refresh"
            :loading="oneClickSyncing"
            @click="handleOneClickSync"
          >
            一键同步
          </el-button>
        </div>
      </div>

      <!-- Tab Content -->
      <div class="tab-content">
        <!-- Stock List -->
        <div v-show="activeTab === 'stockList'" class="panel panel--stock-list">
          <StockList />
        </div>

        <!-- K-Line Query -->
        <div v-show="activeTab === 'klineQuery'" class="panel">
          <KlineQuery />
        </div>

        <!-- Sync Panel -->
        <div v-show="activeTab === 'sync'" class="panel">
          <SyncPanel />
        </div>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import StockList from './StockList.vue'
import SyncPanel from './SyncPanel.vue'
import KlineQuery from './KlineQuery.vue'
import request from '@/api/request'
import { syncApi } from '@/api/sync'

// SVG icons as strings
const icons = {
  database: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>',
  chart: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="m19 9-5 5-4-4-3 3"/></svg>',
  refresh: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M2.5 22v-6h6M2 11.5a10 10 0 0 1 18.8-4.3M22 12.5a10 10 0 0 1-18.8 4.2"/></svg>',
  activity: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
  search: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>',
  barChart: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>'
}

// State
const activeTab = ref('stockList')
const oneClickSyncing = ref(false)

// Hero metrics
const heroMetrics = ref([
  {
    key: 'totalStocks',
    label: '股票总数',
    value: 0,
    icon: icons.database,
    trend: 0,
    trendClass: '',
    glowColor: 'radial-gradient(circle, rgba(56, 189, 248, 0.15) 0%, transparent 70%)'
  },
  {
    key: 'totalKlines',
    label: 'K线数据量',
    value: 0,
    icon: icons.chart,
    trend: 12,
    trendClass: 'trend--up',
    glowColor: 'radial-gradient(circle, rgba(34, 197, 94, 0.15) 0%, transparent 70%)'
  },
  {
    key: 'todaySync',
    label: '今日同步',
    value: 0,
    icon: icons.refresh,
    trend: 0,
    trendClass: '',
    glowColor: 'radial-gradient(circle, rgba(167, 139, 250, 0.15) 0%, transparent 70%)'
  },
  {
    key: 'dataQuality',
    label: '数据质量',
    value: 99.8,
    icon: icons.activity,
    trend: 0.2,
    trendClass: 'trend--up',
    glowColor: 'radial-gradient(circle, rgba(251, 191, 36, 0.15) 0%, transparent 70%)'
  }
])

// Tabs
const tabs = [
  { key: 'stockList', label: '股票列表', icon: icons.search },
  { key: 'klineQuery', label: 'K线查询', icon: icons.barChart },
  { key: 'sync', label: '数据同步', icon: icons.refresh }
]

// 一键同步
const ONE_CLICK_TYPES: { type: SyncType; label: string; daysBack?: number }[] = [
  { type: 'stock_info', label: '股票信息' },
  { type: 'stock_full', label: '股票完整信息' },
  { type: 'kline_daily', label: '日K线', daysBack: 30 },
  { type: 'kline_minute', label: '分钟K线', daysBack: 7 },
  { type: 'realtime_mv', label: '实时市值' },
]

type SyncType = 'stock_info' | 'stock_full' | 'kline_daily' | 'kline_minute' | 'realtime_mv'

const handleOneClickSync = async () => {
  oneClickSyncing.value = true
  activeTab.value = 'sync'  // 切换到同步面板查看进度

  for (const item of ONE_CLICK_TYPES) {
    ElMessage.info(`正在同步 ${item.label}...`)

    try {
      const params: Parameters<typeof syncApi.trigger>[0] = {
        sync_type: item.type,
        failure_strategy: 'skip',
        full_sync: false,
      }

      // K线数据加默认日期范围
      if (item.daysBack) {
        const end = new Date()
        const start = new Date()
        start.setDate(start.getDate() - item.daysBack)
        params.start_date = start.toISOString().slice(0, 10)
        params.end_date = end.toISOString().slice(0, 10)
      }

      await syncApi.trigger(params)
    } catch (error) {
      console.error(`One-click sync failed for ${item.type}:`, error)
      ElMessage.warning(`${item.label} 同步失败，已跳过`)
    }
  }

  oneClickSyncing.value = false
  ElMessage.success('一键同步完成')
}

// Methods
const formatNumber = (num: number): string => {
  if (num >= 100000000) {
    return (num / 100000000).toFixed(1) + '亿'
  }
  if (num >= 10000) {
    return (num / 10000).toFixed(1) + '万'
  }
  return num.toLocaleString()
}

const loadMetrics = async () => {
  try {
    // Load stock count
    const stocksRes = await request.get<{ total: number }>('/data/stocks', { params: { page_size: 1 } })
    heroMetrics.value[0].value = stocksRes.total || 0

    // For demo, set some placeholder values
    heroMetrics.value[1].value = Math.floor(Math.random() * 5000000) + 1000000
    heroMetrics.value[2].value = Math.floor(Math.random() * 500) + 100
  } catch (e) {
    console.error('Failed to load metrics', e)
  }
}

onMounted(() => {
  loadMetrics()
})
</script>

<style scoped>
/* ═══════════════════════════════════════════════════════════════
   DATA DASHBOARD
   ═══════════════════════════════════════════════════════════════ */

.data-dashboard {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: var(--space-5);
  animation: fadeIn var(--duration-slow) var(--ease-out);
}

/* ═══════════════════════════════════════════════════════════════
   HERO METRICS
   ═══════════════════════════════════════════════════════════════ */

.metrics-hero {
  flex-shrink: 0;
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: var(--space-4);
}

@media (max-width: 1200px) {
  .metrics-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 600px) {
  .metrics-grid {
    grid-template-columns: 1fr;
  }
}

.metric-card {
  position: relative;
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  overflow: hidden;
  animation: slideUp var(--duration-slow) var(--ease-out) backwards;
}

.metric-card__glow {
  position: absolute;
  top: -50%;
  right: -50%;
  width: 100%;
  height: 200%;
  opacity: 0.5;
  pointer-events: none;
  transition: opacity var(--duration-normal);
}

.metric-card:hover .metric-card__glow {
  opacity: 0.8;
}

.metric-card__inner {
  position: relative;
  padding: var(--space-5);
}

.metric-card__header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--space-3);
}

.metric-card__icon {
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-surface);
  border-radius: var(--radius-md);
  color: var(--accent-primary);
}

.metric-card__icon :deep(svg) {
  width: 20px;
  height: 20px;
}

.metric-card__trend {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: var(--text-xs);
  font-weight: 600;
  padding: 4px 8px;
  border-radius: var(--radius-full);
}

.metric-card__trend svg {
  width: 12px;
  height: 12px;
}

.metric-card__trend.trend--up {
  background: rgba(34, 197, 94, 0.15);
  color: var(--color-bull);
}

.metric-card__trend.trend--down {
  background: rgba(239, 68, 68, 0.15);
  color: var(--color-bear);
}

.metric-card__value {
  font-family: var(--font-display);
  font-size: var(--text-2xl);
  font-weight: 700;
  color: var(--text-bright);
  margin-bottom: var(--space-1);
  letter-spacing: var(--tracking-tight);
}

.metric-card__label {
  font-size: var(--text-sm);
  color: var(--text-muted);
}

/* ═══════════════════════════════════════════════════════════════
   CONTENT SECTION
   ═══════════════════════════════════════════════════════════════ */

.content-section {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

/* Tab Navigation */
.tab-navigation {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-3) var(--space-4);
  border-bottom: 1px solid var(--border-subtle);
  background: rgba(0, 0, 0, 0.2);
}

.tab-navigation__tabs {
  display: flex;
  gap: var(--space-1);
}

.tab-navigation__actions {
  display: flex;
  align-items: center;
  gap: var(--space-2);
  flex-shrink: 0;
}

.tab-btn {
  display: flex;
  align-items: center;
  gap: var(--space-2);
  padding: var(--space-2) var(--space-4);
  background: transparent;
  border: none;
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  font-size: var(--text-sm);
  font-weight: 500;
  cursor: pointer;
  transition: all var(--duration-normal) var(--ease-out);
}

.tab-btn:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.tab-btn--active {
  background: var(--accent-primary);
  color: var(--bg-void);
}

.tab-btn__icon {
  width: 16px;
  height: 16px;
}

.tab-btn__icon :deep(svg) {
  width: 100%;
  height: 100%;
}

/* Tab Content */
.tab-content {
  flex: 1;
  overflow: hidden;
}

.panel {
  height: 100%;
  overflow: auto;
}

.panel--stock-list :deep(.stock-list-container) {
  padding: var(--space-4);
}
</style>
