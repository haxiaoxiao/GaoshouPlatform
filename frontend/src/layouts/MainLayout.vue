<template>
  <div class="app-shell">
    <!-- Ambient background effects -->
    <div class="ambient-bg">
      <div class="gradient-orb gradient-orb--primary"></div>
      <div class="gradient-orb gradient-orb--secondary"></div>
      <div class="grid-pattern"></div>
    </div>

    <!-- Sidebar -->
    <aside class="sidebar" :class="{ 'sidebar--collapsed': isCollapsed }">
      <!-- Logo -->
      <div class="sidebar__brand">
        <div class="brand-icon">
          <svg viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="2" y="2" width="28" height="28" rx="6" stroke="currentColor" stroke-width="2"/>
            <path d="M8 22L12 14L16 18L20 10L24 16" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <circle cx="24" cy="16" r="2" fill="currentColor"/>
          </svg>
        </div>
        <div class="brand-text" v-if="!isCollapsed">
          <span class="brand-name">GAOSHOU</span>
          <span class="brand-tagline">量化投研平台</span>
        </div>
      </div>

      <!-- Navigation -->
      <nav class="sidebar__nav">
        <div class="nav-section">
          <span class="nav-section__title" v-if="!isCollapsed">核心功能</span>
          <router-link
            v-for="item in mainNavItems"
            :key="item.path"
            :to="item.path"
            class="nav-item"
            :class="{ 'nav-item--active': isActive(item.path) }"
          >
            <span class="nav-item__icon" v-html="item.icon"></span>
            <span class="nav-item__label" v-if="!isCollapsed">{{ item.label }}</span>
            <span class="nav-item__indicator" v-if="item.badge">{{ item.badge }}</span>
          </router-link>
        </div>

        <div class="nav-section" v-if="!isCollapsed">
          <span class="nav-section__title">系统</span>
          <router-link
            v-for="item in systemNavItems"
            :key="item.path"
            :to="item.path"
            class="nav-item"
            :class="{ 'nav-item--active': isActive(item.path) }"
          >
            <span class="nav-item__icon" v-html="item.icon"></span>
            <span class="nav-item__label">{{ item.label }}</span>
          </router-link>
        </div>
      </nav>

      <!-- Sidebar footer -->
      <div class="sidebar__footer" v-if="!isCollapsed">
        <div class="status-indicator">
          <span class="status-dot status-dot--active"></span>
          <span class="status-text">系统运行中</span>
        </div>
        <div class="version-tag">v0.1.0</div>
      </div>

      <!-- Collapse toggle -->
      <button class="sidebar__toggle" @click="toggleSidebar" :title="isCollapsed ? '展开菜单' : '收起菜单'">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path v-if="isCollapsed" d="M9 18l6-6-6-6" stroke-linecap="round" stroke-linejoin="round"/>
          <path v-else d="M15 18l-6-6 6-6" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      </button>
    </aside>

    <!-- Main content area -->
    <main class="main-area">
      <!-- Header -->
      <header class="topbar">
        <div class="topbar__left">
          <h1 class="page-title">{{ pageTitle }}</h1>
          <span class="page-subtitle" v-if="pageSubtitle">{{ pageSubtitle }}</span>
        </div>
        <div class="topbar__right">
          <!-- Search -->
          <div class="global-search">
            <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="11" cy="11" r="8"/>
              <path d="M21 21l-4.35-4.35"/>
            </svg>
            <input
              type="text"
              placeholder="搜索股票、策略..."
              class="search-input"
              @focus="searchFocused = true"
              @blur="searchFocused = false"
              v-model="searchQuery"
            />
            <kbd class="search-kbd" v-if="!searchFocused">⌘K</kbd>
          </div>

          <!-- Quick actions -->
          <div class="quick-actions">
            <button class="action-btn" title="通知">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
                <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
              </svg>
              <span class="action-badge">3</span>
            </button>
            <button class="action-btn" title="设置">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="3"/>
                <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>
              </svg>
            </button>
          </div>

          <!-- User -->
          <div class="user-menu">
            <div class="user-avatar">
              <span>A</span>
            </div>
          </div>
        </div>
      </header>

      <!-- Content -->
      <div class="content-wrapper">
        <router-view v-slot="{ Component }">
          <transition name="page" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </div>
    </main>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()
const isCollapsed = ref(false)
const searchFocused = ref(false)
const searchQuery = ref('')

// SVG icons as strings
const icons = {
  data: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>',
  explorer: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/><path d="M11 8v6"/><path d="M8 11h6"/></svg>',
  watchlist: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>',
  factor: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M18.7 8l-5.1 5.2-2.8-2.7L7 14.3"/></svg>',
  backtest: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18"/><path d="M9 21V9"/></svg>',
  trade: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>',
  trend: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>',
  monitor: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8"/><path d="M12 17v4"/></svg>'
}

// Navigation items
const mainNavItems = [
  { path: '/data', label: '数据管理', icon: icons.data, badge: '' },
  { path: '/explorer', label: '数据浏览器', icon: icons.explorer, badge: '' },
  { path: '/watchlist', label: '自选股', icon: icons.watchlist, badge: '' },
  { path: '/factor', label: '因子研究', icon: icons.factor, badge: '' },
  { path: '/backtest', label: '策略回测', icon: icons.backtest, badge: '' },
  { path: '/trade', label: '实盘交易', icon: icons.trade, badge: '' },
  { path: '/trend-capital', label: '趋势资金策略', icon: icons.trend, badge: '' },
]

const systemNavItems = [
  { path: '/monitor', label: '系统监控', icon: icons.monitor }
]

// Page titles
const pageTitles: Record<string, { title: string; subtitle?: string }> = {
  '/data': { title: '数据管理', subtitle: '股票数据查询与同步' },
  '/explorer': { title: '数据浏览器', subtitle: 'ClickHouse数据查询' },
  '/watchlist': { title: '自选股', subtitle: '分组管理自选股票' },
  '/factor': { title: '因子研究', subtitle: '因子分析与筛选' },
  '/backtest': { title: '策略回测', subtitle: '策略开发与验证' },
  '/trade': { title: '实盘交易', subtitle: '模拟盘与实盘' },
  '/trend-capital': { title: '趋势资金策略', subtitle: '研报十一 · 事件驱动' },
  '/monitor': { title: '系统监控', subtitle: '运行状态与日志' }
}

const pageTitle = computed(() => {
  const info = pageTitles[route.path]
  return info?.title || '高手平台'
})

const pageSubtitle = computed(() => {
  const info = pageTitles[route.path]
  return info?.subtitle || ''
})

const isActive = (path: string) => {
  return route.path.startsWith(path)
}

const toggleSidebar = () => {
  isCollapsed.value = !isCollapsed.value
}
</script>

<style scoped>
/* ═══════════════════════════════════════════════════════════════
   LAYOUT SHELL
   ═══════════════════════════════════════════════════════════════ */

.app-shell {
  display: flex;
  height: 100vh;
  overflow: hidden;
  position: relative;
}

/* Ambient Background */
.ambient-bg {
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: 0;
  overflow: hidden;
}

.gradient-orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(80px);
  opacity: 0.4;
  animation: float 20s ease-in-out infinite;
}

.gradient-orb--primary {
  width: 600px;
  height: 600px;
  background: radial-gradient(circle, rgba(56, 189, 248, 0.15) 0%, transparent 70%);
  top: -200px;
  right: -100px;
  animation-delay: 0s;
}

.gradient-orb--secondary {
  width: 500px;
  height: 500px;
  background: radial-gradient(circle, rgba(167, 139, 250, 0.12) 0%, transparent 70%);
  bottom: -150px;
  left: 20%;
  animation-delay: -10s;
}

@keyframes float {
  0%, 100% { transform: translate(0, 0) scale(1); }
  33% { transform: translate(30px, -30px) scale(1.05); }
  66% { transform: translate(-20px, 20px) scale(0.95); }
}

.grid-pattern {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
  background-size: 60px 60px;
}

/* ═══════════════════════════════════════════════════════════════
   SIDEBAR
   ═══════════════════════════════════════════════════════════════ */

.sidebar {
  width: 240px;
  height: 100vh;
  background: rgba(13, 13, 16, 0.8);
  backdrop-filter: blur(20px);
  border-right: 1px solid var(--border-subtle);
  display: flex;
  flex-direction: column;
  position: relative;
  z-index: var(--z-elevated);
  transition: width var(--duration-slow) var(--ease-out);
}

.sidebar--collapsed {
  width: 72px;
}

/* Brand */
.sidebar__brand {
  display: flex;
  align-items: center;
  gap: var(--space-3);
  padding: var(--space-5);
  border-bottom: 1px solid var(--border-subtle);
  min-height: 72px;
}

.brand-icon {
  width: 36px;
  height: 36px;
  flex-shrink: 0;
  color: var(--accent-primary);
  filter: drop-shadow(0 0 8px var(--accent-glow));
}

.brand-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
  overflow: hidden;
}

.brand-name {
  font-family: var(--font-display);
  font-size: var(--text-sm);
  font-weight: 700;
  letter-spacing: var(--tracking-wider);
  color: var(--text-bright);
}

.brand-tagline {
  font-size: var(--text-xs);
  color: var(--text-muted);
}

/* Navigation */
.sidebar__nav {
  flex: 1;
  overflow-y: auto;
  overflow-x: hidden;
  padding: var(--space-4);
}

.nav-section {
  margin-bottom: var(--space-6);
}

.nav-section__title {
  display: block;
  font-size: var(--text-xs);
  font-weight: 600;
  letter-spacing: var(--tracking-wider);
  color: var(--text-muted);
  text-transform: uppercase;
  padding: 0 var(--space-3);
  margin-bottom: var(--space-2);
}

.nav-item {
  display: flex;
  align-items: center;
  gap: var(--space-3);
  padding: var(--space-3);
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  text-decoration: none;
  font-weight: 500;
  font-size: var(--text-sm);
  transition: all var(--duration-normal) var(--ease-out);
  position: relative;
  margin-bottom: 2px;
}

.nav-item:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.nav-item--active {
  background: linear-gradient(135deg, rgba(56, 189, 248, 0.1) 0%, rgba(167, 139, 250, 0.05) 100%);
  color: var(--accent-primary);
}

.nav-item--active::before {
  content: '';
  position: absolute;
  left: 0;
  top: 50%;
  transform: translateY(-50%);
  width: 3px;
  height: 24px;
  background: var(--accent-primary);
  border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
  box-shadow: 0 0 12px var(--accent-glow);
}

.nav-item__icon {
  width: 20px;
  height: 20px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
}

.nav-item__icon :deep(svg) {
  width: 100%;
  height: 100%;
}

.nav-item__label {
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
}

.nav-item__indicator {
  background: var(--accent-primary);
  color: var(--bg-void);
  font-size: 10px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: var(--radius-full);
  min-width: 18px;
  text-align: center;
}

/* Sidebar Footer */
.sidebar__footer {
  padding: var(--space-4);
  border-top: 1px solid var(--border-subtle);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.status-indicator {
  display: flex;
  align-items: center;
  gap: var(--space-2);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-neutral);
}

.status-dot--active {
  background: var(--color-bull);
  box-shadow: 0 0 8px var(--color-bull);
  animation: pulse 2s ease-in-out infinite;
}

.status-text {
  font-size: var(--text-xs);
  color: var(--text-muted);
}

.version-tag {
  font-size: var(--text-xs);
  color: var(--text-ghost);
  font-family: var(--font-display);
}

/* Collapse Toggle */
.sidebar__toggle {
  position: absolute;
  right: -12px;
  top: 50%;
  transform: translateY(-50%);
  width: 24px;
  height: 24px;
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  color: var(--text-muted);
  transition: all var(--duration-normal);
  z-index: var(--z-elevated);
}

.sidebar__toggle:hover {
  background: var(--bg-hover);
  color: var(--accent-primary);
  border-color: var(--accent-primary);
}

.sidebar__toggle svg {
  width: 14px;
  height: 14px;
}

/* ═══════════════════════════════════════════════════════════════
   MAIN AREA
   ═══════════════════════════════════════════════════════════════ */

.main-area {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  position: relative;
  z-index: var(--z-base);
}

/* Topbar */
.topbar {
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 var(--space-6);
  background: rgba(13, 13, 16, 0.6);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border-subtle);
  position: relative;
  z-index: var(--z-sticky);
}

.topbar__left {
  display: flex;
  align-items: baseline;
  gap: var(--space-3);
}

.page-title {
  font-family: var(--font-display);
  font-size: var(--text-xl);
  font-weight: 600;
  color: var(--text-bright);
  letter-spacing: var(--tracking-tight);
}

.page-subtitle {
  font-size: var(--text-sm);
  color: var(--text-muted);
}

.topbar__right {
  display: flex;
  align-items: center;
  gap: var(--space-4);
}

/* Global Search */
.global-search {
  position: relative;
  width: 280px;
}

.search-icon {
  position: absolute;
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  width: 16px;
  height: 16px;
  color: var(--text-muted);
  pointer-events: none;
  transition: color var(--duration-normal);
}

.search-input {
  width: 100%;
  height: 36px;
  padding: 0 var(--space-10) 0 40px;
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-size: var(--text-sm);
  font-family: var(--font-ui);
  transition: all var(--duration-normal);
}

.search-input::placeholder {
  color: var(--text-muted);
}

.search-input:focus {
  outline: none;
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.1);
}

.search-input:focus + .search-icon,
.global-search:focus-within .search-icon {
  color: var(--accent-primary);
}

.search-kbd {
  position: absolute;
  right: 8px;
  top: 50%;
  transform: translateY(-50%);
  padding: 2px 6px;
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  font-size: var(--text-xs);
  color: var(--text-muted);
  font-family: var(--font-display);
}

/* Quick Actions */
.quick-actions {
  display: flex;
  gap: var(--space-2);
}

.action-btn {
  position: relative;
  width: 36px;
  height: 36px;
  background: transparent;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all var(--duration-normal);
}

.action-btn:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
  border-color: var(--border-default);
}

.action-btn svg {
  width: 18px;
  height: 18px;
}

.action-badge {
  position: absolute;
  top: -4px;
  right: -4px;
  min-width: 16px;
  height: 16px;
  padding: 0 4px;
  background: var(--accent-danger);
  border-radius: var(--radius-full);
  font-size: 10px;
  font-weight: 700;
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
}

/* User Menu */
.user-menu {
  display: flex;
  align-items: center;
}

.user-avatar {
  width: 36px;
  height: 36px;
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  border-radius: var(--radius-md);
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 700;
  font-size: var(--text-sm);
  color: white;
  cursor: pointer;
  transition: transform var(--duration-normal);
}

.user-avatar:hover {
  transform: scale(1.05);
}

/* Content Wrapper */
.content-wrapper {
  flex: 1;
  overflow: hidden;
  padding: var(--space-5);
}

/* Page Transitions */
.page-enter-active,
.page-leave-active {
  transition: opacity var(--duration-slow) var(--ease-out),
              transform var(--duration-slow) var(--ease-out);
}

.page-enter-from {
  opacity: 0;
  transform: translateY(8px);
}

.page-leave-to {
  opacity: 0;
  transform: translateY(-8px);
}
</style>
