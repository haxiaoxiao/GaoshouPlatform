/** @vitest-environment jsdom */

import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, ref } from 'vue'
import { beforeEach, expect, it, vi } from 'vitest'

const charts = vi.hoisted(() => [] as Array<{
  setOption: ReturnType<typeof vi.fn>
  resize: ReturnType<typeof vi.fn>
  dispose: ReturnType<typeof vi.fn>
}>)

vi.mock('@/lib/echarts', () => ({
  init: vi.fn(() => {
    const chart = { setOption: vi.fn(), resize: vi.fn(), dispose: vi.fn() }
    charts.push(chart)
    return chart
  }),
}))

import { useRadarChart } from './useRadarChart'

class FakeResizeObserver {
  static instances: FakeResizeObserver[] = []
  observe = vi.fn()
  disconnect = vi.fn()

  constructor(_callback: ResizeObserverCallback) {
    FakeResizeObserver.instances.push(this)
  }
}

const Host = defineComponent({
  props: { show: Boolean, value: { type: Number, required: true } },
  setup(props) {
    const element = ref<HTMLElement | null>(null)
    useRadarChart(element, () => ({ series: [{ data: [props.value] }] }), [() => props.value])
    return { element }
  },
  template: '<div><div v-if="show" ref="element" class="chart-host" /></div>',
})

beforeEach(() => {
  charts.length = 0
  FakeResizeObserver.instances = []
  vi.stubGlobal('ResizeObserver', FakeResizeObserver)
})

it('observes a chart element that appears after mount and rebuilds after replacement', async () => {
  const wrapper = mount(Host, { props: { show: false, value: 1 } })
  await flushPromises()
  expect(charts).toHaveLength(0)

  await wrapper.setProps({ show: true })
  await flushPromises()
  expect(charts).toHaveLength(1)
  expect(FakeResizeObserver.instances[0].observe).toHaveBeenCalledOnce()

  await wrapper.setProps({ show: false })
  await flushPromises()
  expect(charts[0].dispose).toHaveBeenCalledOnce()
  expect(FakeResizeObserver.instances[0].disconnect).toHaveBeenCalledOnce()

  await wrapper.setProps({ show: true, value: 2 })
  await flushPromises()
  expect(charts).toHaveLength(2)
  expect(FakeResizeObserver.instances[1].observe).toHaveBeenCalledOnce()
  expect(charts[1].setOption).toHaveBeenCalled()

  wrapper.unmount()
  expect(charts[1].dispose).toHaveBeenCalledOnce()
})
