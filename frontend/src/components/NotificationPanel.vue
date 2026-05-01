<template>
  <div class="notification-panel" @click.stop>
    <div class="panel-header">
      <span class="panel-title">消息通知</span>
      <button
        v-if="store.unreadCount > 0"
        class="mark-all-btn"
        @click="store.markAllRead()"
      >
        全部已读
      </button>
    </div>

    <div class="panel-body">
      <div v-if="store.notifications.length === 0" class="empty-state">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" class="empty-icon">
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
          <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
        </svg>
        <span>暂无通知</span>
      </div>

      <div
        v-for="n in store.notifications"
        :key="n.id"
        class="notification-item"
        :class="{ 'notification-item--unread': !n.read }"
        @click="store.markAsRead(n.id)"
      >
        <div class="notif-dot" :class="`notif-dot--${n.type}`"></div>
        <div class="notif-content">
          <div class="notif-title">{{ n.title }}</div>
          <div class="notif-message">{{ n.message }}</div>
          <div class="notif-time">{{ formatTime(n.time) }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useNotificationStore } from '@/stores/notification'

const store = useNotificationStore()

function formatTime(date: Date): string {
  const d = date instanceof Date ? date : new Date(date)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  if (diff < 60000) return '刚刚'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}分钟前`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}小时前`
  return d.toLocaleDateString('zh-CN', { month: '2-digit', day: '2-digit' })
}
</script>

<style scoped>
.notification-panel {
  position: absolute;
  top: calc(100% + 8px);
  right: -8px;
  width: 360px;
  max-height: 480px;
  background: rgba(18, 18, 22, 0.95);
  backdrop-filter: blur(20px);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  box-shadow: 0 12px 40px rgba(0, 0, 0, 0.5);
  display: flex;
  flex-direction: column;
  z-index: var(--z-dropdown);
  overflow: hidden;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  border-bottom: 1px solid var(--border-subtle);
}

.panel-title {
  font-size: var(--text-sm);
  font-weight: 600;
  color: var(--text-bright);
}

.mark-all-btn {
  background: none;
  border: none;
  color: var(--accent-primary);
  font-size: var(--text-xs);
  cursor: pointer;
  padding: 4px 8px;
  border-radius: var(--radius-sm);
  transition: background var(--duration-normal);
}

.mark-all-btn:hover {
  background: var(--bg-hover);
}

.panel-body {
  flex: 1;
  overflow-y: auto;
  padding: 4px 0;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  padding: 40px 16px;
  color: var(--text-muted);
  font-size: var(--text-sm);
}

.empty-icon {
  width: 32px;
  height: 32px;
  opacity: 0.4;
}

.notification-item {
  display: flex;
  gap: 12px;
  padding: 12px 16px;
  cursor: pointer;
  transition: background var(--duration-normal);
  border-bottom: 1px solid var(--border-subtle);
}

.notification-item:last-child {
  border-bottom: none;
}

.notification-item:hover {
  background: var(--bg-hover);
}

.notification-item--unread {
  background: rgba(56, 189, 248, 0.04);
}

.notif-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-top: 5px;
  flex-shrink: 0;
}

.notif-dot--info {
  background: var(--accent-primary);
}

.notif-dot--success {
  background: var(--color-bull);
}

.notif-dot--warning {
  background: var(--color-warning);
}

.notif-dot--error {
  background: var(--accent-danger);
}

.notif-content {
  flex: 1;
  min-width: 0;
}

.notif-title {
  font-size: var(--text-sm);
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 2px;
}

.notif-message {
  font-size: var(--text-xs);
  color: var(--text-secondary);
  line-height: 1.4;
  margin-bottom: 4px;
}

.notif-time {
  font-size: 11px;
  color: var(--text-ghost);
}
</style>
