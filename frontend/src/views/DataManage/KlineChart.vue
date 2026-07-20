<template>
  <div class="kline-shell">
    <div ref="chartRef" class="kline-chart"></div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, shallowRef, watch } from 'vue'
import { useResizeObserver } from '@vueuse/core'
import {
  ColorType,
  CrosshairMode,
  CandlestickSeries,
  HistogramSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from 'lightweight-charts'
import type { KlineDataDisplay } from '@/api/kline'

const props = withDefaults(defineProps<{
  data: KlineDataDisplay[]
  queryKey?: string
}>(), {
  queryKey: '',
})

const emit = defineEmits<{ 'request-older': [] }>()

const chartRef = ref<HTMLDivElement | null>(null)
const chart = shallowRef<IChartApi | null>(null)
const candleSeries = shallowRef<ISeriesApi<'Candlestick'> | null>(null)
const volumeSeries = shallowRef<ISeriesApi<'Histogram'> | null>(null)
let requestOlderArmed = false
let lastDataLength = 0
let lastNewestTime: string | null = null

const sortedData = computed(() =>
  [...(props.data || [])].sort((a, b) => String(a.datetime).localeCompare(String(b.datetime)))
)

const toChartTime = (value: string): Time => {
  const text = String(value || '').trim()
  if (!text) return '' as Time

  const hasClock = text.includes('T') || /\d{2}:\d{2}/.test(text)
  if (!hasClock) {
    return text.slice(0, 10) as Time
  }

  const timestamp = Date.parse(text.includes('T') ? text : text.replace(' ', 'T'))
  if (Number.isNaN(timestamp)) {
    return text.slice(0, 10) as Time
  }
  return Math.floor(timestamp / 1000) as Time
}

const candleData = computed<CandlestickData[]>(() =>
  sortedData.value.map((item) => ({
    time: toChartTime(item.datetime),
    open: Number(item.open),
    high: Number(item.high),
    low: Number(item.low),
    close: Number(item.close),
  }))
)

const volumeData = computed<HistogramData[]>(() =>
  sortedData.value.map((item) => ({
    time: toChartTime(item.datetime),
    value: Number(item.volume || 0),
    color: item.close >= item.open ? 'rgba(239, 68, 68, 0.42)' : 'rgba(16, 185, 129, 0.42)',
  }))
)

const hasIntradayData = computed(() =>
  sortedData.value.some((item) => /\d{2}:\d{2}/.test(String(item.datetime)))
)

const resizeChart = () => {
  if (!chart.value || !chartRef.value) return
  const rect = chartRef.value.getBoundingClientRect()
  chart.value.resize(Math.max(0, Math.floor(rect.width)), Math.max(0, Math.floor(rect.height)))
}

const handleVisibleLogicalRangeChange = (range: { from: number; to: number } | null) => {
  if (!range) return
  if (range.from > 10) {
    requestOlderArmed = true
    return
  }
  if (!requestOlderArmed) return
  requestOlderArmed = false
  emit('request-older')
  queueMicrotask(() => {
    requestOlderArmed = true
  })
}

const updateSeries = (forceFit = false) => {
  if (!candleSeries.value || !volumeSeries.value) return
  const timeScale = chart.value?.timeScale()
  const visibleRange = timeScale?.getVisibleLogicalRange()
  const nextLength = candleData.value.length
  const nextNewestTime = sortedData.value.at(-1)?.datetime || null
  const addedCount = Math.max(0, nextLength - lastDataLength)
  const appendedOlder = !forceFit
    && lastDataLength > 0
    && addedCount > 0
    && nextNewestTime === lastNewestTime
  requestOlderArmed = false
  chart.value?.applyOptions({
    timeScale: {
      timeVisible: hasIntradayData.value,
      secondsVisible: false,
    },
  })
  candleSeries.value.setData(candleData.value)
  volumeSeries.value.setData(volumeData.value)
  if (appendedOlder && visibleRange && timeScale) {
    timeScale.setVisibleLogicalRange({
      from: visibleRange.from + addedCount,
      to: visibleRange.to + addedCount,
    })
  } else {
    timeScale?.fitContent()
  }
  lastDataLength = nextLength
  lastNewestTime = nextNewestTime
  queueMicrotask(() => {
    requestOlderArmed = true
  })
}

const initChart = () => {
  if (!chartRef.value || chart.value) return
  const rect = chartRef.value.getBoundingClientRect()
  const styles = getComputedStyle(document.documentElement)
  const bg = styles.getPropertyValue('--bg-primary').trim() || '#fdfbf7'
  const elevated = styles.getPropertyValue('--bg-elevated').trim() || '#f5f2ea'
  const text = styles.getPropertyValue('--text-secondary').trim() || '#54635c'
  const border = styles.getPropertyValue('--border-default').trim() || '#e5dfd3'
  const bull = styles.getPropertyValue('--accent-danger').trim() || '#a83232'
  const bear = styles.getPropertyValue('--accent-success').trim() || '#2d6a4f'
  const instance = createChart(chartRef.value, {
    width: Math.max(0, Math.floor(rect.width)),
    height: Math.max(0, Math.floor(rect.height)),
    layout: {
      background: { type: ColorType.Solid, color: bg },
      textColor: text,
      fontFamily: 'var(--font-ui), "Microsoft YaHei", sans-serif',
    },
    grid: {
      vertLines: { color: elevated },
      horzLines: { color: elevated },
    },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: {
      borderColor: border,
      scaleMargins: { top: 0.08, bottom: 0.28 },
    },
    timeScale: {
      borderColor: border,
      timeVisible: hasIntradayData.value,
      secondsVisible: false,
    },
  })

  chart.value = instance
  candleSeries.value = instance.addSeries(CandlestickSeries, {
    upColor: bull,
    downColor: bear,
    borderUpColor: bull,
    borderDownColor: bear,
    wickUpColor: bull,
    wickDownColor: bear,
  })
  const volume = instance.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: '',
  })
  volumeSeries.value = volume
  volume.priceScale().applyOptions({
    scaleMargins: { top: 0.76, bottom: 0 },
  })
  instance.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleLogicalRangeChange)
  updateSeries(true)
}

watch(
  () => props.data,
  () => updateSeries(),
  { deep: false }
)

watch(
  () => props.queryKey,
  () => {
    requestOlderArmed = false
    lastDataLength = 0
    lastNewestTime = null
  },
)

onMounted(async () => {
  await nextTick()
  initChart()
})

useResizeObserver(chartRef, resizeChart)

onUnmounted(() => {
  chart.value?.timeScale().unsubscribeVisibleLogicalRangeChange(handleVisibleLogicalRangeChange)
  chart.value?.remove()
  chart.value = null
  candleSeries.value = null
  volumeSeries.value = null
})

defineExpose({
  resize: resizeChart,
})
</script>

<style scoped>
.kline-shell {
  width: 100%;
  height: 520px;
  min-height: 400px;
  overflow: hidden;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-primary);
}

.kline-chart {
  width: 100%;
  height: 100%;
}
</style>
