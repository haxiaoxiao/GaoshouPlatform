<template>
  <div
    class="app-shell"
    :class="{
      'app-shell--collapsed': isCollapsed,
      'app-shell--context-collapsed': isContextCollapsed,
    }"
  >
    <div class="ambient-bg" aria-hidden="true">
      <div class="gradient-orb gradient-orb--primary"></div>
      <div class="gradient-orb gradient-orb--secondary"></div>
      <div class="grid-pattern"></div>
    </div>

    <aside class="sidebar">
      <div class="sidebar__brand">
        <div class="brand-icon" aria-hidden="true">
          GS
          <span class="brand-env-badge">prod</span>
        </div>
        <div v-if="!isCollapsed" class="brand-text">
          <span class="brand-name">GAOSHOU</span>
          <span class="brand-tagline">Quant Research Cockpit</span>
        </div>
      </div>

      <nav class="sidebar__nav" aria-label="主导航">
        <section v-for="section in navSections" :key="section.key" class="nav-section">
          <span v-if="!isCollapsed" class="nav-section__title">{{ section.label }}</span>
          <router-link
            v-for="item in navItemsForSection(section.key)"
            :key="item.key"
            :to="item.path"
            class="nav-item"
            :class="{ 'nav-item--active': isActive(item) }"
            :title="isCollapsed ? item.label : undefined"
          >
            <span class="nav-item__icon" v-html="item.icon"></span>
            <span v-if="!isCollapsed" class="nav-item__body">
              <span class="nav-item__label">{{ item.label }}</span>
              <span class="nav-item__hint">{{ item.hint }}</span>
            </span>
            <span v-if="!isCollapsed && item.badge" class="nav-item__indicator">{{ item.badge }}</span>
          </router-link>
        </section>
      </nav>

      <div v-if="!isCollapsed" class="sidebar__footer">
        <div>
          <strong>系统运行中</strong>
          <span>Parquet backend · v0.1.0</span>
        </div>
      </div>

      <button
        class="sidebar__toggle"
        type="button"
        :title="isCollapsed ? '展开菜单' : '收起菜单'"
        @click="toggleSidebar"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path v-if="isCollapsed" d="M9 18l6-6-6-6" stroke-linecap="round" stroke-linejoin="round"/>
          <path v-else d="M15 18l-6-6 6-6" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      </button>
    </aside>

    <main class="main-area">
      <header class="topbar">
        <div class="topbar__left">
          <span class="page-kicker">{{ pageKicker }}</span>
          <div class="page-copy">
            <h1 class="page-title">{{ pageTitle }}</h1>
            <span v-if="pageSubtitle" class="page-subtitle">{{ pageSubtitle }}</span>
          </div>
        </div>

        <label class="global-search">
          <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"/>
            <path d="M21 21l-4.35-4.35"/>
          </svg>
          <input
            v-model="searchQuery"
            type="text"
            placeholder="搜索股票、策略、因子..."
            class="search-input"
            @focus="searchFocused = true"
            @blur="searchFocused = false"
          />
          <kbd v-if="!searchFocused" class="search-kbd">Ctrl K</kbd>
        </label>

        <div class="topbar__right">
          <button class="action-btn action-btn--text" type="button" @click="toggleContextRail">
            {{ isContextCollapsed ? '打开上下文' : '收起上下文' }}
          </button>
          <div class="notification-wrapper">
            <button class="action-btn" title="通知" type="button" @click="toggleNotificationPanel">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
                <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
              </svg>
              <span v-if="notificationStore.unreadCount > 0" class="action-badge">{{ notificationStore.unreadCount }}</span>
            </button>
            <NotificationPanel v-if="showNotifications" />
          </div>
          <div class="user-avatar" title="当前用户">A</div>
        </div>
      </header>

      <div class="content-wrapper">
        <router-view v-slot="{ Component }">
          <transition name="page" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </div>

      <StatusBar />
    </main>

    <aside v-if="!isContextCollapsed" class="context-rail" aria-label="当前页面上下文">
      <div class="context-rail__header">
        <div>
          <span class="page-kicker">CONTEXT</span>
          <h2>{{ pageTitle }}上下文</h2>
        </div>
        <span class="context-badge">{{ contextBadge }}</span>
      </div>

      <section
        v-for="block in contextBlocks"
        :key="block.title"
        class="context-card"
        :class="{ 'context-card--actionable': Boolean(block.action) }"
        :role="block.action ? 'button' : undefined"
        :tabindex="block.action ? 0 : undefined"
        @click="handleContextAction(block)"
        @keydown.enter.prevent="handleContextAction(block)"
        @keydown.space.prevent="handleContextAction(block)"
      >
        <h3>{{ block.title }}</h3>
        <div class="context-list">
          <div v-for="row in block.rows" :key="`${block.title}-${row.label}`" class="context-row">
            <span>{{ row.label }}</span>
            <strong :class="row.tone ? `context-value--${row.tone}` : undefined">{{ row.value }}</strong>
          </div>
        </div>
      </section>
    </aside>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import {
  NAV_SECTIONS,
  navItemsForSection,
  resolveNavItem,
  type AppNavItem,
  type ContextBlock,
} from '@/app/navigation'
import { useResolvedPageContext } from '@/app/pageContext'
import { useNotificationStore } from '@/stores/notification'
import NotificationPanel from '@/components/NotificationPanel.vue'
import StatusBar from '@/components/StatusBar.vue'

const notificationStore = useNotificationStore()
const route = useRoute()

const isCollapsed = ref(false)
const isContextCollapsed = ref(false)
const searchFocused = ref(false)
const searchQuery = ref('')
const showNotifications = ref(false)

const navSections = NAV_SECTIONS
const activeNavItem = computed(() => resolveNavItem(route.path))
const fallbackContextBlocks = computed<ContextBlock[]>(() => activeNavItem.value?.context || [])
const resolvedContext = useResolvedPageContext(fallbackContextBlocks)

const pageTitle = computed(() => {
  const title = route.meta.title
  return typeof title === 'string' ? title : activeNavItem.value?.label || '高手平台'
})

const pageSubtitle = computed(() => {
  const subtitle = route.meta.subtitle
  return typeof subtitle === 'string' ? subtitle : activeNavItem.value?.subtitle || ''
})

const pageKicker = computed(() => {
  const kicker = route.meta.kicker
  return typeof kicker === 'string' ? kicker : activeNavItem.value?.kicker || 'GAOSHOU'
})

const contextBlocks = computed<ContextBlock[]>(() => resolvedContext.value.blocks)
const contextBadge = computed(() => resolvedContext.value.isDynamic ? 'Live' : 'Guide')

function toggleSidebar() {
  isCollapsed.value = !isCollapsed.value
}

function toggleContextRail() {
  isContextCollapsed.value = !isContextCollapsed.value
}

function toggleNotificationPanel() {
  showNotifications.value = !showNotifications.value
}

function handleContextAction(block: ContextBlock) {
  if (!block.action) return
  window.dispatchEvent(new CustomEvent('page-context-action', {
    detail: { action: block.action, title: block.title },
  }))
}

function closeNotificationPanel(e: MouseEvent) {
  const target = e.target as HTMLElement
  if (!target.closest('.notification-wrapper') && !target.closest('.notification-panel')) {
    showNotifications.value = false
  }
}

function isActive(item: AppNavItem) {
  return activeNavItem.value?.key === item.key
}

onMounted(() => {
  document.addEventListener('click', closeNotificationPanel)
  notificationStore.startTaskPolling()
})

onUnmounted(() => {
  document.removeEventListener('click', closeNotificationPanel)
  notificationStore.stopTaskPolling()
})
</script>

<style scoped>
.app-shell {
  --sidebar-width: 252px;
  --context-width: 320px;
  display: grid;
  grid-template-columns: var(--sidebar-width) minmax(0, 1fr) var(--context-width);
  height: 100vh;
  overflow: hidden;
  position: relative;
}

.app-shell--collapsed {
  --sidebar-width: 76px;
}

.app-shell--context-collapsed {
  grid-template-columns: var(--sidebar-width) minmax(0, 1fr);
}

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
  opacity: 0.36;
  animation: float 20s ease-in-out infinite;
}

.gradient-orb--primary {
  width: 620px;
  height: 620px;
  background: radial-gradient(circle, rgba(103, 212, 255, 0.14) 0%, transparent 70%);
  top: -220px;
  right: -120px;
}

.gradient-orb--secondary {
  width: 520px;
  height: 520px;
  background: radial-gradient(circle, rgba(231, 185, 79, 0.1) 0%, transparent 70%);
  bottom: -180px;
  left: 18%;
  animation-delay: -10s;
}

.grid-pattern {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
  background-size: 60px 60px;
}

@keyframes float {
  0%, 100% { transform: translate(0, 0) scale(1); }
  33% { transform: translate(30px, -30px) scale(1.05); }
  66% { transform: translate(-20px, 20px) scale(0.95); }
}

.sidebar,
.main-area,
.context-rail {
  position: relative;
  z-index: var(--z-elevated);
}

.sidebar {
  height: 100vh;
  background: rgba(12, 15, 20, 0.94);
  border-right: 1px solid var(--border-subtle);
  backdrop-filter: blur(20px);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.sidebar__brand {
  display: grid;
  grid-template-columns: 40px minmax(0, 1fr);
  gap: var(--space-3);
  align-items: center;
  min-height: 72px;
  padding: var(--space-4);
  border-bottom: 1px solid var(--border-subtle);
}

.app-shell--collapsed .sidebar__brand {
  grid-template-columns: 40px;
  justify-content: center;
  padding-inline: var(--space-3);
}

.brand-icon {
  position: relative;
  width: 40px;
  height: 40px;
  display: grid;
  place-items: center;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-md);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.08), rgba(255, 255, 255, 0.02));
  color: var(--accent-primary);
  font-family: var(--font-display);
  font-size: var(--text-sm);
  font-weight: 800;
  box-shadow: var(--shadow-glow);
}

.brand-env-badge {
  position: absolute;
  right: -10px;
  bottom: -7px;
  min-width: 32px;
  padding: 2px 6px;
  border: 1px solid rgba(248, 113, 113, 0.72);
  border-radius: var(--radius-full);
  background: rgba(127, 29, 29, 0.92);
  color: #fecaca;
  font-family: var(--font-data);
  font-size: 9px;
  font-weight: 900;
  line-height: 1;
  letter-spacing: 0;
  text-align: center;
  text-transform: uppercase;
  box-shadow: 0 0 14px rgba(248, 113, 113, 0.22);
}

.brand-text {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.brand-name {
  font-family: var(--font-display);
  color: var(--text-bright);
  font-size: var(--text-sm);
  font-weight: 800;
  letter-spacing: var(--tracking-wider);
}

.brand-tagline {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.sidebar__nav {
  flex: 1;
  overflow: auto;
  padding: var(--space-4) var(--space-3);
}

.nav-section {
  margin-bottom: var(--space-5);
}

.nav-section__title {
  display: block;
  color: var(--text-ghost);
  font-size: var(--text-xs);
  font-weight: 700;
  letter-spacing: var(--tracking-wider);
  text-transform: uppercase;
  padding: 0 var(--space-3) var(--space-2);
}

.nav-item {
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr) auto;
  gap: var(--space-3);
  align-items: center;
  min-height: 44px;
  margin: 2px 0;
  padding: 7px 10px;
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  text-decoration: none;
  transition: color var(--duration-normal), background var(--duration-normal), box-shadow var(--duration-normal);
  position: relative;
}

.app-shell--collapsed .nav-item {
  grid-template-columns: 30px;
  justify-content: center;
  padding-inline: 8px;
}

.nav-item:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.045);
}

.nav-item--active {
  color: var(--text-bright);
  background: rgba(103, 212, 255, 0.11);
  box-shadow: inset 3px 0 0 var(--accent-primary);
}

.nav-item__icon {
  width: 30px;
  height: 30px;
  display: grid;
  place-items: center;
  border: 1px solid var(--border-default);
  border-radius: 7px;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.07), rgba(255, 255, 255, 0.02));
}

.nav-item__icon :deep(svg) {
  width: 17px;
  height: 17px;
}

.nav-item__body {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.nav-item__label {
  overflow: hidden;
  color: inherit;
  font-size: var(--text-sm);
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.nav-item__hint {
  color: var(--text-ghost);
  font-size: var(--text-xs);
}

.nav-item__indicator,
.context-badge {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-full);
  color: var(--text-secondary);
  font-size: 10px;
  font-weight: 700;
  padding: 2px 7px;
}

.sidebar__footer {
  margin: var(--space-3);
  padding: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.025);
}

.sidebar__footer strong,
.sidebar__footer span {
  display: block;
}

.sidebar__footer strong {
  color: var(--text-primary);
  font-size: var(--text-xs);
  margin-bottom: 3px;
}

.sidebar__footer span {
  color: var(--text-muted);
  font-size: 11px;
}

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
  display: grid;
  place-items: center;
  cursor: pointer;
  color: var(--text-muted);
  transition: all var(--duration-normal);
  z-index: var(--z-sticky);
}

.sidebar__toggle:hover {
  color: var(--accent-primary);
  border-color: var(--accent-primary);
}

.sidebar__toggle svg {
  width: 14px;
  height: 14px;
}

.main-area {
  min-width: 0;
  height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.topbar {
  min-height: 72px;
  display: grid;
  grid-template-columns: minmax(220px, 1fr) minmax(220px, 360px) auto;
  gap: var(--space-3);
  align-items: center;
  padding: var(--space-3) var(--space-5);
  background: rgba(16, 20, 26, 0.82);
  border-bottom: 1px solid var(--border-subtle);
  backdrop-filter: blur(14px);
  box-shadow: var(--shadow-sm);
}

.topbar__left {
  min-width: 0;
  display: grid;
  gap: 4px;
}

.page-kicker {
  color: var(--accent-primary);
  font-family: var(--font-display);
  font-size: var(--text-xs);
  font-weight: 800;
  letter-spacing: var(--tracking-wider);
}

.page-copy {
  min-width: 0;
  display: flex;
  align-items: baseline;
  gap: var(--space-3);
}

.page-title {
  color: var(--text-bright);
  font-family: var(--font-display);
  font-size: var(--text-xl);
  font-weight: 750;
  line-height: 1.1;
  margin: 0;
  white-space: nowrap;
}

.page-subtitle {
  overflow: hidden;
  color: var(--text-muted);
  font-size: var(--text-sm);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.global-search {
  min-width: 0;
  height: 38px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  gap: var(--space-2);
  align-items: center;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(4, 6, 9, 0.45);
  padding: 0 var(--space-3);
  color: var(--text-muted);
}

.search-icon {
  width: 16px;
  height: 16px;
}

.search-input {
  min-width: 0;
  height: 100%;
  border: 0;
  outline: 0;
  color: var(--text-primary);
  background: transparent;
  font-family: var(--font-ui);
  font-size: var(--text-sm);
}

.search-input::placeholder {
  color: var(--text-muted);
}

.search-kbd {
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  background: rgba(255, 255, 255, 0.04);
  color: var(--text-muted);
  font-family: var(--font-display);
  font-size: var(--text-xs);
  padding: 2px 6px;
}

.topbar__right {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--space-2);
}

.action-btn {
  position: relative;
  min-width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.035);
  color: var(--text-secondary);
  cursor: pointer;
  font-family: var(--font-ui);
  transition: all var(--duration-normal);
}

.action-btn--text {
  padding: 0 var(--space-3);
  white-space: nowrap;
}

.action-btn:hover {
  color: var(--text-primary);
  background: var(--bg-hover);
  border-color: var(--border-default);
}

.action-btn svg {
  width: 18px;
  height: 18px;
}

.action-badge {
  position: absolute;
  top: -5px;
  right: -5px;
  min-width: 16px;
  height: 16px;
  display: grid;
  place-items: center;
  border-radius: var(--radius-full);
  background: var(--accent-warning);
  color: #070a0e;
  font-size: 10px;
  font-weight: 800;
}

.notification-wrapper {
  position: relative;
}

.user-avatar {
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  border-radius: var(--radius-md);
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
  color: #071014;
  cursor: default;
  font-weight: 800;
}

.content-wrapper {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  padding: var(--space-5);
}

.context-rail {
  height: 100vh;
  overflow: auto;
  padding: var(--space-4);
  border-left: 1px solid var(--border-subtle);
  background: rgba(16, 20, 26, 0.88);
  backdrop-filter: blur(18px);
}

.context-rail__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--space-3);
  margin-bottom: var(--space-4);
}

.context-rail__header h2 {
  margin: 4px 0 0;
  color: var(--text-bright);
  font-size: var(--text-lg);
}

.context-card {
  padding: var(--space-3);
  margin-bottom: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.025);
}

.context-card--actionable {
  cursor: pointer;
  outline: none;
  transition:
    border-color var(--duration-fast) var(--ease-out),
    background var(--duration-fast) var(--ease-out);
}

.context-card--actionable:hover,
.context-card--actionable:focus-visible {
  border-color: rgba(125, 211, 252, 0.48);
  background: rgba(14, 116, 144, 0.12);
}

.context-card h3 {
  margin: 0 0 var(--space-3);
  color: var(--text-bright);
  font-size: var(--text-sm);
}

.context-list {
  display: grid;
  gap: var(--space-2);
}

.context-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--space-3);
  color: var(--text-muted);
  font-size: 0.78rem;
  line-height: 1.45;
}

.context-row strong {
  color: var(--text-primary);
  font-family: var(--font-data);
  font-size: 0.82rem;
  font-weight: 800;
  letter-spacing: 0.01em;
  line-height: 1.35;
  max-width: 58%;
  overflow-wrap: anywhere;
  text-align: right;
}

.context-value--good { color: var(--status-ready) !important; }
.context-value--warn { color: var(--accent-warning) !important; }
.context-value--bad { color: var(--status-attention) !important; }
.context-value--neutral { color: var(--accent-primary) !important; }

.page-enter-active,
.page-leave-active {
  transition:
    opacity var(--duration-slow) var(--ease-out),
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

@media (max-width: 1360px) {
  .app-shell {
    grid-template-columns: var(--sidebar-width) minmax(0, 1fr);
  }

  .context-rail {
    display: none;
  }
}

@media (max-width: 1024px) {
  .topbar {
    grid-template-columns: minmax(0, 1fr) auto;
  }

  .global-search {
    display: none;
  }
}

@media (max-width: 760px) {
  .app-shell,
  .app-shell--collapsed,
  .app-shell--context-collapsed {
    --sidebar-width: 100%;
    display: grid;
    grid-template-columns: 1fr;
    grid-template-rows: auto minmax(0, 1fr);
  }

  .sidebar {
    height: auto;
    max-height: 42vh;
  }

  .sidebar__toggle {
    display: none;
  }

  .topbar {
    min-height: auto;
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .topbar__right,
  .page-copy {
    justify-content: flex-start;
    flex-wrap: wrap;
  }

  .main-area {
    height: auto;
    min-height: 0;
  }

  .content-wrapper {
    padding: var(--space-3);
  }
}
</style>
