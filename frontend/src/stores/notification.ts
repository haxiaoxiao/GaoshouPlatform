import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
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
}

const TERMINAL_STATUSES = new Set(['done', 'completed', 'failed', 'cancelled'])
const POLL_INTERVAL_MS = 5000

export const useNotificationStore = defineStore('notification', () => {
  const notifications = ref<Notification[]>([])
  const seenTaskKeys = ref(new Set<string>())
  const knownRunningTasks = ref(new Set<string>())
  const polling = ref(false)
  let timer: ReturnType<typeof setInterval> | null = null

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

  function markAllRead() {
    notifications.value.forEach(n => { n.read = true })
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

  function startTaskPolling() {
    if (polling.value) return
    polling.value = true
    pollTasks()
    timer = setInterval(pollTasks, POLL_INTERVAL_MS)
  }

  function stopTaskPolling() {
    polling.value = false
    if (timer) {
      clearInterval(timer)
      timer = null
    }
  }

  return {
    notifications,
    unreadCount,
    addNotification,
    markAsRead,
    markAllRead,
    startTaskPolling,
    stopTaskPolling,
    pollTasks,
  }
})
