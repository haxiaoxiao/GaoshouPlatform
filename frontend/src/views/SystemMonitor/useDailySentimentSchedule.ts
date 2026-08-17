import { ref } from 'vue'

import { syncApi, type DailySentimentSchedule } from '@/api/sync'

export interface DailySentimentScheduleApi {
  getDailySentimentSchedule: () => Promise<DailySentimentSchedule>
  updateDailySentimentSchedule: (enabled: boolean) => Promise<DailySentimentSchedule>
}

export function useDailySentimentSchedule(
  api: DailySentimentScheduleApi = syncApi,
) {
  const schedule = ref<DailySentimentSchedule | null>(null)
  const loading = ref(false)
  const saving = ref(false)

  async function load() {
    loading.value = true
    try {
      schedule.value = await api.getDailySentimentSchedule()
      return schedule.value
    } finally {
      loading.value = false
    }
  }

  async function setEnabled(enabled: boolean) {
    if (saving.value) return schedule.value
    saving.value = true
    try {
      const confirmed = await api.updateDailySentimentSchedule(enabled)
      schedule.value = confirmed
      return confirmed
    } finally {
      saving.value = false
    }
  }

  return {
    schedule,
    loading,
    saving,
    load,
    setEnabled,
  }
}
