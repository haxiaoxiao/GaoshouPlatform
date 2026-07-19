import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { marketRadarApi, type MarketRadarAlert } from '@/api/marketRadar'
import { runtimeTaskApi, type RuntimeTask } from '@/api/runtimeTasks'

export interface Notification {
  id: string
  type: 'info' | 'success' | 'warning' | 'error'
  title: string
  message: string
  time: Date
  read: boolean
  route?: string | null
  taskId?: string
  marketAlertId?: number
  source?: 'task' | 'market' | 'local'
}

const TERMINAL_STATUSES = new Set(['done', 'completed', 'succeeded', 'failed', 'cancelled'])
const POLL_INTERVAL_MS = 5000
const MARKET_ALERT_POLL_INTERVAL_MS = 30_000

export const useNotificationStore = defineStore('notification', () => {
  const notifications = ref<Notification[]>([])
  const seenTaskKeys = ref(new Set<string>())
  const knownRunningTasks = ref(new Set<string>())
  const polling = ref(false)
  let taskTimer: ReturnType<typeof setInterval> | null = null
  let marketAlertTimer: ReturnType<typeof setInterval> | null = null
  let marketAlertPollInFlight = false
  const acknowledgedMarketAlertIds = new Set<number>()

  const unreadCount = computed(() => notifications.value.filter(n => !n.read).length)

  function addNotification(n: Omit<Notification, 'id' | 'time' | 'read'> & { id?: string }) {
    const id = n.id || `${Date.now()}-${Math.random().toString(16).slice(2)}`
    if (notifications.value.some(item => item.id === id)) return
    notifications.value.unshift({
      ...n,
      id,
      time: new Date(),
      read: false,
    })
    notifications.value = notifications.value.slice(0, 80)
  }

  function markAsRead(id: string) {
    const n = notifications.value.find(n => n.id === id)
    if (n) n.read = true
  }

  async function markAllRead() {
    const marketNotifications = notifications.value.filter(
      notification => notification.source === 'market' && !notification.read,
    )
    notifications.value.forEach(notification => {
      if (notification.source !== 'market') notification.read = true
    })
    await Promise.allSettled(
      marketNotifications.map(notification => acknowledgeMarketAlert(notification.id)),
    )
  }

  function addMarketAlert(alert: MarketRadarAlert) {
    if (
      alert.status !== 'active'
      || alert.severity !== 'high'
      || acknowledgedMarketAlertIds.has(alert.id)
    ) return
    const id = `market-alert:${alert.id}`
    const existing = notifications.value.find(notification => notification.id === id)
    if (existing) {
      existing.title = alert.title
      existing.message = alert.explanation
      existing.time = new Date(alert.triggered_at)
      existing.read = false
      existing.route = `/market-radar?alert=${alert.id}`
      return
    }
    notifications.value.unshift({
      id,
      marketAlertId: alert.id,
      source: 'market',
      type: 'error',
      title: alert.title,
      message: alert.explanation,
      time: new Date(alert.triggered_at),
      read: false,
      route: `/market-radar?alert=${alert.id}`,
    })
    notifications.value = notifications.value.slice(0, 80)
  }

  function syncMarketAlerts(alerts: MarketRadarAlert[]) {
    const active = alerts.filter(
      alert => alert.status === 'active'
        && alert.severity === 'high'
        && !acknowledgedMarketAlertIds.has(alert.id),
    )
    const activeIds = new Set(active.map(alert => `market-alert:${alert.id}`))
    notifications.value = notifications.value.filter(
      notification => notification.source !== 'market'
        || (notification.marketAlertId !== undefined
          && acknowledgedMarketAlertIds.has(notification.marketAlertId))
        || activeIds.has(notification.id),
    )
    active.forEach(addMarketAlert)
  }

  async function acknowledgeMarketAlert(notificationId: string) {
    const notification = notifications.value.find(item => item.id === notificationId)
    if (!notification) return
    const marketAlertId = notification.marketAlertId
    if (marketAlertId === undefined) return
    if (acknowledgedMarketAlertIds.has(marketAlertId)) {
      notification.read = true
      return
    }
    const response = await marketRadarApi.acknowledgeAlert(marketAlertId)
    if (response.data.status !== 'acknowledged') {
      throw new Error('市场预警确认状态无效')
    }
    acknowledgedMarketAlertIds.add(marketAlertId)
    notification.read = true
  }

  function notificationFromTask(task: RuntimeTask): Notification {
    const failed = task.status === 'failed'
    const cancelled = task.status === 'cancelled'
    const suffix = cancelled ? '已停止' : failed ? '失败' : '完成'
    const message = cancelled
      ? (task.error || `${task.title} 已停止`)
      : failed
      ? (task.error || `${task.title} 执行失败`)
      : `${task.title} 已执行完成`
    return {
      id: `task:${task.task_id}:${task.status}`,
      taskId: task.task_id,
      source: 'task',
      type: cancelled ? 'warning' : failed ? 'error' : 'success',
      title: `${taskKindLabel(task.kind)}${suffix}`,
      message,
      time: task.finished_at ? new Date(task.finished_at * 1000) : new Date(),
      read: false,
      route: task.result_ref,
    }
  }

  function taskKindLabel(kind: string): string {
    const labels: Record<string, string> = {
      backtest: '回测',
      optimization: '参数优化',
      factor_precompute: '因子计算',
      data_sync: '数据同步',
    }
    return labels[kind] || '任务'
  }

  function handleTask(task: RuntimeTask) {
    if (!TERMINAL_STATUSES.has(task.status)) {
      knownRunningTasks.value.add(task.task_id)
      return
    }

    const key = `${task.task_id}:${task.status}`
    if (seenTaskKeys.value.has(key)) return

    const shouldNotify = knownRunningTasks.value.has(task.task_id) || Date.now() - task.updated_at * 1000 < 15000
    seenTaskKeys.value.add(key)
    knownRunningTasks.value.delete(task.task_id)
    if (!shouldNotify) return

    const notification = notificationFromTask(task)
    addNotification(notification)
    if (task.status === 'cancelled') {
      ElMessage.warning(`${notification.title}: ${notification.message}`)
    } else if (task.status === 'failed') {
      ElMessage.error(`${notification.title}: ${notification.message}`)
    } else {
      ElMessage.success(`${notification.title}: ${notification.message}`)
    }
  }

  async function pollTasks() {
    try {
      const tasks = await runtimeTaskApi.list(true)
      tasks.forEach(handleTask)
    } catch {
      // Notification polling must never interrupt normal work.
    }
  }

  async function pollMarketAlerts() {
    if (marketAlertPollInFlight) return
    marketAlertPollInFlight = true
    try {
      const response = await marketRadarApi.activeHighAlerts()
      syncMarketAlerts(response.data.items)
    } catch {
      // The global panel keeps its last confirmed state during transient API failures.
    } finally {
      marketAlertPollInFlight = false
    }
  }

  function startTaskPolling() {
    if (polling.value) return
    polling.value = true
    void pollTasks()
    void pollMarketAlerts()
    taskTimer = setInterval(pollTasks, POLL_INTERVAL_MS)
    marketAlertTimer = setInterval(pollMarketAlerts, MARKET_ALERT_POLL_INTERVAL_MS)
  }

  function stopTaskPolling() {
    polling.value = false
    if (taskTimer) {
      clearInterval(taskTimer)
      taskTimer = null
    }
    if (marketAlertTimer) {
      clearInterval(marketAlertTimer)
      marketAlertTimer = null
    }
  }

  return {
    notifications,
    unreadCount,
    addNotification,
    addMarketAlert,
    syncMarketAlerts,
    acknowledgeMarketAlert,
    markAsRead,
    markAllRead,
    startTaskPolling,
    stopTaskPolling,
    pollTasks,
    pollMarketAlerts,
  }
})
