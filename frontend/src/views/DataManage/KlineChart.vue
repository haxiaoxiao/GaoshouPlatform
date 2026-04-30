<template>
  <div ref="chartRef" class="kline-chart"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import type { KlineDataDisplay } from '@/api/kline'

// Props
const props = defineProps<{
  data: KlineDataDisplay[]
}>()

// Refs
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null
let isInitialized = false

// 颜色配置 - 使用更鲜明的颜色
const COLORS = {
  up: '#ef5350',      // 涨 - 红色
  down: '#26a69a',    // 跌 - 绿色
  grid: '#e0e0e0',    // 网格线
  axis: '#616161',    // 坐标轴
  text: '#424242',    // 文字
  background: '#ffffff',
}

// 获取默认配置
const getDefaultOption = (): echarts.EChartsOption => ({
  animation: false,
  backgroundColor: COLORS.background,
  legend: {
    bottom: 10,
    left: 'center',
    data: ['K线', '成交量'],
    textStyle: { color: COLORS.text },
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross',
      lineStyle: { color: '#999' },
    },
    backgroundColor: 'rgba(255, 255, 255, 0.95)',
    borderColor: '#ddd',
    borderWidth: 1,
    textStyle: { color: '#333', fontSize: 13 },
    formatter: (params: unknown) => {
      const items = params as Array<{ seriesName: string; axisValue: string; dataIndex: number }>
      if (!items || items.length === 0) return ''

      const date = items[0].axisValue
      const idx = items[0].dataIndex
      if (idx < 0 || idx >= props.data.length) return ''
      const d = props.data[idx]

      const open = d.open
      const close = d.close
      const high = d.high
      const low = d.low
      const vol = d.volume
      const change = close - open
      const changePercent = open !== 0 ? ((change / open) * 100).toFixed(2) : '0.00'
      const changeColor = change >= 0 ? COLORS.up : COLORS.down

      return `
        <div style="padding: 12px; min-width: 180px;">
          <div style="font-weight: 600; margin-bottom: 10px; font-size: 14px;">${date}</div>
          <div style="display: grid; grid-template-columns: 70px 90px; gap: 6px; font-size: 13px;">
            <span style="color: #666;">开盘:</span><span style="text-align: right; font-weight: 500;">${open.toFixed(2)}</span>
            <span style="color: #666;">收盘:</span><span style="text-align: right; font-weight: 600; color: ${changeColor};">${close.toFixed(2)}</span>
            <span style="color: #666;">最高:</span><span style="text-align: right; font-weight: 500;">${high.toFixed(2)}</span>
            <span style="color: #666;">最低:</span><span style="text-align: right; font-weight: 500;">${low.toFixed(2)}</span>
            <span style="color: #666;">涨跌:</span><span style="text-align: right; font-weight: 600; color: ${changeColor};">${change >= 0 ? '+' : ''}${changePercent}%</span>
            <span style="color: #666;">成交量:</span><span style="text-align: right;">${formatVolume(vol)}</span>
          </div>
        </div>
      `
    },
  },
  axisPointer: {
    link: [{ xAxisIndex: 'all' }],
  },
  grid: [
    {
      left: '8%',
      right: '4%',
      top: '8%',
      height: '52%',
    },
    {
      left: '8%',
      right: '4%',
      top: '68%',
      height: '18%',
    },
  ],
  xAxis: [
    {
      type: 'category',
      data: [],
      boundaryGap: true,
      axisLine: { lineStyle: { color: COLORS.axis } },
      axisTick: { lineStyle: { color: COLORS.axis } },
      axisLabel: {
        color: COLORS.text,
        formatter: (value: string) => {
          if (value.length >= 10) return value.substring(5)
          return value
        },
      },
      splitLine: { show: false },
    },
    {
      type: 'category',
      gridIndex: 1,
      data: [],
      boundaryGap: true,
      axisLine: { lineStyle: { color: COLORS.axis } },
      axisTick: { show: false },
      splitLine: { show: false },
      axisLabel: { show: false },
    },
  ],
  yAxis: [
    {
      type: 'value',
      scale: true,
      axisLine: { show: true, lineStyle: { color: COLORS.axis } },
      axisTick: { show: true, lineStyle: { color: COLORS.axis } },
      axisLabel: {
        color: COLORS.text,
        formatter: (value: number) => value.toFixed(2),
      },
      splitLine: { lineStyle: { color: COLORS.grid, type: 'dashed' } },
      splitArea: { show: false },
    },
    {
      type: 'value',
      gridIndex: 1,
      scale: true,
      axisLine: { show: true, lineStyle: { color: COLORS.axis } },
      axisTick: { show: false },
      axisLabel: {
        color: COLORS.text,
        formatter: (value: number) => formatVolume(value),
      },
      splitLine: { lineStyle: { color: COLORS.grid, type: 'dashed' } },
    },
  ],
  dataZoom: [
    {
      type: 'inside',
      xAxisIndex: [0, 1],
      start: 0,
      end: 100,
    },
    {
      show: true,
      xAxisIndex: [0, 1],
      type: 'slider',
      bottom: 0,
      start: 0,
      end: 100,
      height: 24,
      borderColor: '#ccc',
      fillerColor: 'rgba(64, 158, 255, 0.2)',
      handleStyle: { color: '#409eff' },
      textStyle: { color: COLORS.text },
    },
  ],
  series: [
    {
      name: 'K线',
      type: 'candlestick',
      data: [],
      itemStyle: {
        color: COLORS.up,
        color0: COLORS.down,
        borderColor: COLORS.up,
        borderColor0: COLORS.down,
      },
      barWidth: '60%',
    },
    {
      name: '成交量',
      type: 'bar',
      xAxisIndex: 1,
      yAxisIndex: 1,
      data: [],
      barWidth: '60%',
      itemStyle: {
        color: (params: { dataIndex: number }) => {
          if (!props.data[params.dataIndex]) return '#bdbdbd'
          const item = props.data[params.dataIndex]
          return item.close >= item.open ? COLORS.up : COLORS.down
        },
      },
    },
  ],
})

// 初始化图表
const initChart = () => {
  if (!chartRef.value) return

  // 销毁旧实例
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }

  chartInstance = echarts.init(chartRef.value, undefined, { renderer: 'canvas' })
  chartInstance.setOption(getDefaultOption())
  isInitialized = true
}

// 更新图表数据 - 关键修复：始终重置dataZoom
const updateChart = () => {
  if (!chartInstance || !chartRef.value) {
    initChart()
  }

  if (!chartInstance) return

  // 如果没有数据，清空图表
  if (!props.data || props.data.length === 0) {
    chartInstance.setOption({
      xAxis: [{ data: [] }, { data: [] }],
      series: [{ data: [] }, { data: [] }],
    })
    return
  }

  const dates = props.data.map((item) => item.datetime)
  const klineData = props.data.map((item) => [
    item.open,
    item.close,
    item.low,
    item.high,
  ])
  const volumeData = props.data.map((item) => item.volume)

  // 关键：使用 notMerge: true 完全重置图表状态
  // 这样可以确保 dataZoom 和其他状态被正确重置
  chartInstance.setOption({
    xAxis: [
      { data: dates },
      { data: dates },
    ],
    series: [
      { data: klineData },
      { data: volumeData },
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: 0,
        end: 100,
      },
      {
        show: true,
        xAxisIndex: [0, 1],
        type: 'slider',
        bottom: 0,
        start: 0,
        end: 100,
        height: 24,
      },
    ],
  }, { notMerge: false, lazyUpdate: true })

  // 强制重新渲染
  chartInstance.resize()
}

// 格式化成交量
const formatVolume = (volume: number): string => {
  if (volume >= 100000000) {
    return (volume / 100000000).toFixed(2) + '亿'
  }
  if (volume >= 10000) {
    return (volume / 10000).toFixed(2) + '万'
  }
  return volume.toString()
}

// 处理窗口大小变化
const handleResize = () => {
  chartInstance?.resize()
}

// 监听数据变化
watch(
  () => props.data,
  (newData) => {
    // 使用 nextTick 确保 DOM 已更新
    nextTick(() => {
      if (!isInitialized) {
        initChart()
      }
      updateChart()
    })
  },
  { deep: true, immediate: false }
)

// 生命周期
onMounted(() => {
  initChart()
  if (props.data && props.data.length > 0) {
    updateChart()
  }
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
  isInitialized = false
})

// 暴露方法供父组件调用
defineExpose({
  resize: handleResize,
})
</script>

<style scoped>
.kline-chart {
  width: 100%;
  height: 520px;
  min-height: 400px;
  background: #ffffff;
  border-radius: 8px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1);
}
</style>
