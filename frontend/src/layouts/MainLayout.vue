<template>
  <div
    class="app-shell"
    :class="{
      'app-shell--collapsed': isCollapsed,
    }"
  >
    <div class="ambient-bg" aria-hidden="true">
      <div class="grid-pattern"></div>
    </div>

    <aside class="sidebar">
      <div class="sidebar__brand">
        <div class="brand-icon" aria-hidden="true">
          GS
          <span class="brand-env-badge">{{ envLabel }}</span>
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
} from '@/app/navigation'
import { useNotificationStore } from '@/stores/notification'
import NotificationPanel from '@/components/NotificationPanel.vue'
import StatusBar from '@/components/StatusBar.vue'

const notificationStore = useNotificationStore()
const route = useRoute()

const isCollapsed = ref(false)
const searchFocused = ref(false)
const searchQuery = ref('')
const showNotifications = ref(false)

const navSections = NAV_SECTIONS
const activeNavItem = computed(() => resolveNavItem(route.path))
const envLabel = (import.meta.env.VITE_APP_ENV_LABEL || 'PROD').toString()

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

function toggleSidebar() {
  isCollapsed.value = !isCollapsed.value
}

function toggleNotificationPanel() {
  showNotifications.value = !showNotifications.value
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

const THEME_CLASS_NAMES = [
  'theme-terminal-light',
  'theme-cyber-chalk',
  'theme-ivory-forest',
  'theme-swiss-minimalist',
] as const

type ThemeClassName = typeof THEME_CLASS_NAMES[number]

const DEFAULT_THEME: ThemeClassName = 'theme-ivory-forest'

function applyTheme(theme: ThemeClassName) {
  document.documentElement.classList.remove(...THEME_CLASS_NAMES)
  document.documentElement.classList.add(theme)
}

onMounted(() => {
  document.addEventListener('click', closeNotificationPanel)
  notificationStore.startTaskPolling()
  localStorage.setItem('gs-theme', DEFAULT_THEME)
  applyTheme(DEFAULT_THEME)
})

onUnmounted(() => {
  document.removeEventListener('click', closeNotificationPanel)
  notificationStore.stopTaskPolling()
})
</script>

<style scoped>
.app-shell {
  --sidebar-width: 252px;
  display: grid;
  grid-template-columns: var(--sidebar-width) minmax(0, 1fr);
  height: 100vh;
  overflow: hidden;
  position: relative;
  isolation: isolate;
  background: var(--bg-void);
}

.app-shell--collapsed {
  --sidebar-width: 76px;
}

.ambient-bg {
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: 0;
  overflow: hidden;
}

.grid-pattern {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(34, 48, 42, 0.026) 1px, transparent 1px),
    linear-gradient(90deg, rgba(34, 48, 42, 0.022) 1px, transparent 1px);
  background-size: 72px 72px;
  mask-image: linear-gradient(180deg, rgba(0, 0, 0, 0.64), rgba(0, 0, 0, 0.12));
}

.sidebar,
.main-area {
  position: relative;
  z-index: var(--z-elevated);
}

.sidebar {
  height: 100vh;
  background:
    linear-gradient(180deg, rgba(245, 242, 234, 0.98), rgba(238, 243, 240, 0.9)),
    var(--bg-elevated);
  border-right: 1px solid var(--border-default);
  box-shadow: inset -1px 0 0 rgba(253, 251, 247, 0.62);
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
  background: linear-gradient(145deg, var(--accent-primary), var(--accent-secondary));
  color: #fdfbf7;
  font-family: var(--font-display);
  font-size: var(--text-sm);
  font-weight: 800;
  box-shadow: 0 10px 22px rgba(27, 61, 50, 0.16);
}

.brand-env-badge {
  position: absolute;
  right: -10px;
  bottom: -7px;
  min-width: 32px;
  padding: 2px 6px;
  border: 1px solid rgba(168, 50, 50, 0.28);
  border-radius: var(--radius-full);
  background: var(--status-critical-bg);
  color: var(--status-critical);
  font-family: var(--font-data);
  font-size: 9px;
  font-weight: 900;
  line-height: 1;
  letter-spacing: 0;
  text-align: center;
  text-transform: uppercase;
  box-shadow: 0 6px 14px rgba(168, 50, 50, 0.12);
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
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
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
  background: rgba(235, 231, 220, 0.66);
}

.nav-item--active {
  color: var(--text-bright);
  background: linear-gradient(90deg, rgba(27, 61, 50, 0.12), rgba(238, 243, 240, 0.88));
  box-shadow: inset 3px 0 0 var(--accent-primary);
}

.nav-item__icon {
  width: 30px;
  height: 30px;
  display: grid;
  place-items: center;
  border: 1px solid var(--border-default);
  border-radius: 7px;
  background: linear-gradient(180deg, rgba(253, 251, 247, 0.8), rgba(235, 231, 220, 0.56));
}

.nav-item--active .nav-item__icon {
  border-color: rgba(27, 61, 50, 0.28);
  background: var(--bg-active);
  color: var(--accent-primary);
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
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.nav-item__indicator {
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
  background: rgba(253, 251, 247, 0.6);
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
  background:
    linear-gradient(180deg, rgba(253, 251, 247, 0.94), rgba(245, 242, 234, 0.54)),
    var(--bg-void);
}

.topbar {
  min-height: 72px;
  display: grid;
  grid-template-columns: minmax(220px, 1fr) minmax(220px, 360px) auto;
  gap: var(--space-3);
  align-items: center;
  padding: var(--space-3) var(--space-5);
  background: rgba(253, 251, 247, 0.92);
  border-bottom: 1px solid var(--border-default);
  backdrop-filter: blur(12px);
  box-shadow: 0 8px 22px rgba(34, 48, 42, 0.04);
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
  overflow: hidden;
  text-overflow: ellipsis;
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
  background: rgba(245, 242, 234, 0.74);
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
  background: rgba(238, 243, 240, 0.92);
  color: var(--text-muted);
  font-family: var(--font-display);
  font-size: var(--text-xs);
  padding: 2px 6px;
}

.topbar__right {
  min-width: 0;
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
  background: rgba(253, 251, 247, 0.8);
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
  color: #fdfbf7;
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
  color: #fdfbf7;
  cursor: default;
  font-weight: 800;
}

.content-wrapper {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  padding: var(--space-5);
  background:
    linear-gradient(180deg, rgba(245, 242, 234, 0.48), rgba(253, 251, 247, 0.28));
}

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
  .app-shell--collapsed {
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

  .page-title,
  .page-subtitle {
    white-space: normal;
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
