<template>
  <div class="status-bar">
    <div class="status-bar__left">
      <!-- 连接状态 -->
      <div class="status-item" :title="connectionTitle">
        <span class="status-led" :class="`status-led--${connectionStatus}`"></span>
        <span class="status-label">{{ connectionLabel }}</span>
      </div>

      <span class="status-divider"></span>

      <!-- 最后同步时间 -->
      <div class="status-item" title="最近一次数据同步时间">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="status-icon">
          <path d="M21 12a9 9 0 1 1-9-9" stroke-linecap="round"/><path d="M21 3v6h-6" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M12 7v5l3 3" stroke-linecap="round"/>
        </svg>
        <span class="status-label">同步: {{ lastSyncTime }}</span>
      </div>

      <span class="status-divider"></span>

      <!-- 指标条数 -->
      <div class="status-item" v-if="dataInfo">
        <span class="status-label">{{ dataInfo }}</span>
      </div>
    </div>

    <div class="status-bar__right">
      <div class="status-item" title="部署版本">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="status-icon">
          <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
        </svg>
        <span class="status-label">v0.1.0</span>
      </div>

      <span class="status-divider"></span>

      <div class="status-item" title="当前系统时间">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="status-icon">
          <circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2" stroke-linecap="round"/>
        </svg>
        <span class="status-label">{{ currentTime }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { syncApi } from '@/api/sync'

type ConnStatus = 'connected' | 'disconnected' | 'checking'

const connectionStatus = ref<ConnStatus>('checking')
const lastSyncEnd = ref<string | null>(null)
const dataInfo = ref('')
const currentTime = ref('')
let clockTimer: ReturnType<typeof setInterval> | null = null
let healthTimer: ReturnType<typeof setInterval> | null = null
let healthCheckOngoing = false

const connectionTitle = computed(() => {
  switch (connectionStatus.value) {
    case 'connected': return '后端服务已连接'
    case 'disconnected': return '后端服务不可用'
    case 'checking': return '正在检测连接...'
  }
})

const connectionLabel = computed(() => {
  switch (connectionStatus.value) {
    case 'connected': return '已连接'
    case 'disconnected': return '断开'
    case 'checking': return '检测中'
  }
})

const lastSyncTime = computed(() => {
  if (!lastSyncEnd.value) return '暂无'
  const d = new Date(lastSyncEnd.value)
  const now = new Date()
  const pad = (n: number) => String(n).padStart(2, '0')
  const time = `${pad(d.getHours())}:${pad(d.getMinutes())}`
  if (d.toDateString() === now.toDateString()) return `今天 ${time}`
  const yesterday = new Date(now)
  yesterday.setDate(yesterday.getDate() - 1)
  if (d.toDateString() === yesterday.toDateString()) return `昨天 ${time}`
  return `${pad(d.getMonth() + 1)}/${pad(d.getDate())} ${time}`
})

function updateClock() {
  const now = new Date()
  const pad = (n: number) => String(n).padStart(2, '0')
  currentTime.value = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
}

async function checkHealth() {
  if (healthCheckOngoing) return
  healthCheckOngoing = true
  try {
    connectionStatus.value = 'checking'
    const status = await syncApi.getStatus()
    connectionStatus.value = 'connected'
    if (status.end_time) lastSyncEnd.value = status.end_time
    dataInfo.value = status.total > 0 ? `已同步 ${status.total} 条` : ''
  } catch {
    connectionStatus.value = 'disconnected'
  } finally {
    healthCheckOngoing = false
  }
}

onMounted(() => {
  updateClock()
  clockTimer = setInterval(updateClock, 1000)

  checkHealth()
  healthTimer = setInterval(checkHealth, 30000)
})

onUnmounted(() => {
  if (clockTimer) clearInterval(clockTimer)
  if (healthTimer) clearInterval(healthTimer)
})
</script>

<style scoped>
.status-bar {
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 12px;
  background: rgba(8, 8, 10, 0.8);
  border-top: 1px solid var(--border-subtle);
  font-size: 11px;
  color: var(--text-ghost);
  flex-shrink: 0;
  z-index: var(--z-elevated);
  user-select: none;
}

.status-bar__left,
.status-bar__right {
  display: flex;
  align-items: center;
  gap: 8px;
}

.status-item {
  display: flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
}

.status-led {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  flex-shrink: 0;
}

.status-led--connected {
  background: var(--color-bull);
  box-shadow: 0 0 6px var(--color-bull);
}

.status-led--disconnected {
  background: var(--accent-danger);
  box-shadow: 0 0 6px var(--accent-danger);
}

.status-led--checking {
  background: var(--color-warning);
  animation: blink 1s ease-in-out infinite;
}

@keyframes blink {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}

.status-icon {
  width: 12px;
  height: 12px;
  flex-shrink: 0;
  opacity: 0.6;
}

.status-label {
  line-height: 1;
}

.status-divider {
  width: 1px;
  height: 12px;
  background: var(--border-subtle);
  flex-shrink: 0;
}
</style>
