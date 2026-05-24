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

const props = defineProps<{
  data: KlineDataDisplay[]
}>()

const chartRef = ref<HTMLDivElement | null>(null)
const chart = shallowRef<IChartApi | null>(null)
const candleSeries = shallowRef<ISeriesApi<'Candlestick'> | null>(null)
const volumeSeries = shallowRef<ISeriesApi<'Histogram'> | null>(null)

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

const updateSeries = () => {
  if (!candleSeries.value || !volumeSeries.value) return
  chart.value?.applyOptions({
    timeScale: {
      timeVisible: hasIntradayData.value,
      secondsVisible: false,
    },
  })
  candleSeries.value.setData(candleData.value)
  volumeSeries.value.setData(volumeData.value)
  chart.value?.timeScale().fitContent()
}

const initChart = () => {
  if (!chartRef.value || chart.value) return
  const rect = chartRef.value.getBoundingClientRect()
  const instance = createChart(chartRef.value, {
    width: Math.max(0, Math.floor(rect.width)),
    height: Math.max(0, Math.floor(rect.height)),
    layout: {
      background: { type: ColorType.Solid, color: '#0b0f14' },
      textColor: '#9ca3af',
      fontFamily: 'Inter, "Microsoft YaHei", sans-serif',
    },
    grid: {
      vertLines: { color: 'rgba(148, 163, 184, 0.08)' },
      horzLines: { color: 'rgba(148, 163, 184, 0.08)' },
    },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: {
      borderColor: 'rgba(148, 163, 184, 0.18)',
      scaleMargins: { top: 0.08, bottom: 0.28 },
    },
    timeScale: {
      borderColor: 'rgba(148, 163, 184, 0.18)',
      timeVisible: hasIntradayData.value,
      secondsVisible: false,
    },
  })

  chart.value = instance
  candleSeries.value = instance.addSeries(CandlestickSeries, {
    upColor: '#ef4444',
    downColor: '#10b981',
    borderUpColor: '#ef4444',
    borderDownColor: '#10b981',
    wickUpColor: '#ef4444',
    wickDownColor: '#10b981',
  })
  const volume = instance.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: '',
  })
  volumeSeries.value = volume
  volume.priceScale().applyOptions({
    scaleMargins: { top: 0.76, bottom: 0 },
  })
  updateSeries()
}

watch(
  () => props.data,
  () => updateSeries(),
  { deep: false }
)

onMounted(async () => {
  await nextTick()
  initChart()
})

useResizeObserver(chartRef, resizeChart)

onUnmounted(() => {
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
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 8px;
  background: #0b0f14;
}

.kline-chart {
  width: 100%;
  height: 100%;
}
</style>
