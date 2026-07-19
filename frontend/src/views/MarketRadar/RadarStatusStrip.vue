<template>
  <header class="radar-status-strip">
    <div class="radar-title-block">
      <h2>市场趋势雷达</h2>
      <p>把市场宽度、连板生态与风险温度放在同一条时间线上</p>
    </div>
    <div class="radar-transport" aria-label="市场雷达连接状态">
      <span class="transport-dot" :class="`transport-dot--${tone}`" />
      <strong>{{ connectionLabel }}</strong>
      <span>{{ modeLabel }}</span>
      <span v-if="asOf">行情 {{ formatTime(asOf) }}</span>
      <span v-if="computedAt">计算 {{ formatTime(computedAt) }}</span>
      <button class="icon-command" type="button" aria-label="立即刷新市场雷达" title="立即刷新" :disabled="refreshing" @click="$emit('refresh')">
        <el-icon aria-hidden="true"><Refresh /></el-icon>
      </button>
    </div>
  </header>
  <p v-if="error" class="transport-error" role="status">{{ error }}</p>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import type { RadarRealtimeMode } from '@/api/marketRadar'

const props = defineProps<{
  connectionState: string
  realtimeMode: RadarRealtimeMode
  asOf: string | null
  computedAt: string | null
  error: string | null
  refreshing: boolean
}>()

defineEmits<{ refresh: [] }>()

const connectionLabel = computed(() => ({
  live: '实时流已连接',
  fallback_polling: '实时流断开',
  reconnecting: '正在恢复连接',
  connecting: '正在连接',
  stopped: '实时流已停止',
  idle: '等待连接',
}[props.connectionState] || '连接状态未知'))

const modeLabel = computed(() => ({
  push: 'QMT 推送',
  polling_30s: '30 秒轮询',
  offline: '离线快照',
  closed: '休市',
}[props.realtimeMode]))

const tone = computed(() => {
  if (props.connectionState === 'live' && props.realtimeMode === 'push') return 'good'
  if (['fallback_polling', 'reconnecting', 'connecting'].includes(props.connectionState)) return 'warn'
  return 'muted'
})

function formatTime(value: string): string {
  return value.replace('T', ' ').slice(5, 16)
}
</script>
