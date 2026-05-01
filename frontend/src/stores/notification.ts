import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export interface Notification {
  id: number
  type: 'info' | 'success' | 'warning' | 'error'
  title: string
  message: string
  time: Date
  read: boolean
}

export const useNotificationStore = defineStore('notification', () => {
  const notifications = ref<Notification[]>([
    {
      id: 1,
      type: 'info',
      title: '系统启动',
      message: '量化投研平台已成功启动',
      time: new Date(),
      read: false,
    },
    {
      id: 2,
      type: 'success',
      title: '数据同步完成',
      message: '股票基础信息已更新至最新',
      time: new Date(Date.now() - 3600000),
      read: false,
    },
    {
      id: 3,
      type: 'warning',
      title: '数据状态',
      message: '日K线数据截止至2026-04-29，请及时同步',
      time: new Date(Date.now() - 7200000),
      read: true,
    },
  ])

  const unreadCount = computed(() => notifications.value.filter(n => !n.read).length)

  function addNotification(n: Omit<Notification, 'id' | 'time' | 'read'>) {
    notifications.value.unshift({
      ...n,
      id: Date.now(),
      time: new Date(),
      read: false,
    })
  }

  function markAsRead(id: number) {
    const n = notifications.value.find(n => n.id === id)
    if (n) n.read = true
  }

  function markAllRead() {
    notifications.value.forEach(n => { n.read = true })
  }

  return { notifications, unreadCount, addNotification, markAsRead, markAllRead }
})
