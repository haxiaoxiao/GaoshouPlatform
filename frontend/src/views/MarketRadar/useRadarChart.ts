import { nextTick, onBeforeUnmount, watch, type Ref } from 'vue'

import { init, type ECharts, type EChartsOption } from '@/lib/echarts'

export function useRadarChart(
  element: Ref<HTMLElement | null>,
  option: () => EChartsOption,
  dependencies: ReadonlyArray<() => unknown>,
): void {
  let chart: ECharts | null = null
  let chartElement: HTMLElement | null = null
  let observer: ResizeObserver | null = null
  let frame: number | null = null

  const disposeChart = () => {
    observer?.disconnect()
    observer = null
    chart?.dispose()
    chart = null
    chartElement = null
  }

  const attachChart = (current: HTMLElement): ECharts => {
    if (chart && chartElement === current) return chart
    disposeChart()
    chartElement = current
    const nextChart = init(current)
    chart = nextChart
    if (typeof ResizeObserver !== 'undefined') {
      observer = new ResizeObserver(resize)
      observer.observe(current)
    }
    return nextChart
  }

  const render = async () => {
    await nextTick()
    const current = element.value
    if (!current) return
    const activeChart = attachChart(current)
    activeChart.setOption(option(), { notMerge: false, lazyUpdate: true })
  }

  const resize = () => {
    if (frame !== null) cancelAnimationFrame(frame)
    frame = requestAnimationFrame(() => {
      frame = null
      chart?.resize()
    })
  }

  watch(element, current => {
    if (current) void render()
    else disposeChart()
  }, { flush: 'post' })
  watch(dependencies, () => void render(), { deep: true })

  onBeforeUnmount(() => {
    if (frame !== null) cancelAnimationFrame(frame)
    disposeChart()
  })
}
