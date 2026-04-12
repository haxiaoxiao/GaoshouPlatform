<template>
  <div ref="chartRef" class="kline-chart"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import type { KlineData } from '@/api/kline'

// Props
const props = defineProps<{
  data: KlineData[]
}>()

// Refs
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

// 初始化图表
const initChart = () => {
  if (!chartRef.value) return

  chartInstance = echarts.init(chartRef.value)

  const option: echarts.EChartsOption = {
    animation: false,
    legend: {
      bottom: 10,
      left: 'center',
      data: ['K线', '成交量'],
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross',
      },
      backgroundColor: 'rgba(255, 255, 255, 0.9)',
      borderColor: '#ccc',
      borderWidth: 1,
      textStyle: {
        color: '#333',
      },
      formatter: (params: unknown) => {
        const items = params as Array<{ seriesName: string; data: unknown; axisValue: string }>
        if (!items || items.length === 0) return ''

        const date = items[0].axisValue
        const klineItem = items.find((item) => item.seriesName === 'K线')
        const volumeItem = items.find((item) => item.seriesName === '成交量')

        const klineData = klineItem?.data as [number, number, number, number] | undefined
        const volumeData = volumeItem?.data as number | undefined

        if (!klineData) return ''

        const [open, close, low, high] = klineData
        const change = close - open
        const changePercent = ((change / open) * 100).toFixed(2)
        const changeColor = change >= 0 ? '#f56c6c' : '#67c23a'

        return `
          <div style="padding: 8px;">
            <div style="font-weight: bold; margin-bottom: 8px;">${date}</div>
            <div style="display: grid; grid-template-columns: 60px 80px; gap: 4px;">
              <span>开盘:</span><span style="text-align: right;">${open.toFixed(2)}</span>
              <span>收盘:</span><span style="text-align: right; color: ${changeColor};">${close.toFixed(2)}</span>
              <span>最高:</span><span style="text-align: right;">${high.toFixed(2)}</span>
              <span>最低:</span><span style="text-align: right;">${low.toFixed(2)}</span>
              <span>涨跌:</span><span style="text-align: right; color: ${changeColor};">${change >= 0 ? '+' : ''}${changePercent}%</span>
              <span>成交量:</span><span style="text-align: right;">${formatVolume(volumeData || 0)}</span>
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
        left: '10%',
        right: '8%',
        top: '5%',
        height: '55%',
      },
      {
        left: '10%',
        right: '8%',
        top: '68%',
        height: '20%',
      },
    ],
    xAxis: [
      {
        type: 'category',
        data: [],
        boundaryGap: false,
        axisLine: { onZero: false },
        splitLine: { show: false },
        min: 'dataMin',
        max: 'dataMax',
        axisLabel: {
          formatter: (value: string) => {
            // 格式化日期显示
            if (value.length >= 10) {
              return value.substring(5) // 只显示月-日
            }
            return value
          },
        },
      },
      {
        type: 'category',
        gridIndex: 1,
        data: [],
        boundaryGap: false,
        axisLine: { onZero: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        min: 'dataMin',
        max: 'dataMax',
      },
    ],
    yAxis: [
      {
        scale: true,
        splitArea: {
          show: true,
        },
        axisLabel: {
          formatter: (value: number) => value.toFixed(2),
        },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: {
          formatter: (value: number) => formatVolume(value),
        },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: 50,
        end: 100,
      },
      {
        show: true,
        xAxisIndex: [0, 1],
        type: 'slider',
        bottom: 0,
        start: 50,
        end: 100,
        height: 20,
      },
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: [],
        itemStyle: {
          color: '#f56c6c', // 阳线颜色
          color0: '#67c23a', // 阴线颜色
          borderColor: '#f56c6c',
          borderColor0: '#67c23a',
        },
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: [],
        itemStyle: {
          color: (params: { dataIndex: number }) => {
            if (!props.data[params.dataIndex]) return '#909399'
            const item = props.data[params.dataIndex]
            return item.close >= item.open ? '#f56c6c' : '#67c23a'
          },
        },
      },
    ],
  }

  chartInstance.setOption(option)
}

// 更新图表数据
const updateChart = () => {
  if (!chartInstance || !props.data.length) return

  const dates = props.data.map((item) => item.datetime)
  const klineData = props.data.map((item) => [
    item.open,
    item.close,
    item.low,
    item.high,
  ])
  const volumeData = props.data.map((item) => item.volume)

  chartInstance.setOption({
    xAxis: [
      { data: dates },
      { data: dates },
    ],
    series: [
      { data: klineData },
      { data: volumeData },
    ],
  })
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
  () => {
    nextTick(() => {
      updateChart()
    })
  },
  { deep: true }
)

// 生命周期
onMounted(() => {
  initChart()
  updateChart()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chartInstance?.dispose()
  chartInstance = null
})

// 暴露方法供父组件调用
defineExpose({
  resize: handleResize,
})
</script>

<style scoped>
.kline-chart {
  width: 100%;
  height: 500px;
  min-height: 400px;
}
</style>
