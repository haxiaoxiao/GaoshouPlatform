/** @vitest-environment jsdom */

import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  capabilities: vi.fn(),
  coverage: vi.fn(),
  runBacktest: vi.fn(),
  startPaper: vi.fn(),
  paperStatus: vi.fn(),
  evaluatePaper: vi.fn(),
  startPaperRunner: vi.fn(),
  stopPaperRunner: vi.fn(),
  paperTrades: vi.fn(),
  stopPaper: vi.fn(),
  resetPaper: vi.fn(),
}))

vi.mock('@/api/intradayT', () => ({ intradayTApi: api }))
vi.mock('@/lib/echarts', () => ({
  init: () => ({ setOption: vi.fn(), resize: vi.fn(), dispose: vi.fn() }),
}))

import IntradayTPage from './index.vue'

const capabilities = {
  symbols: [
    { symbol: '603629.SH', name: '利通电子', board: 'MAIN' },
    { symbol: '688008.SH', name: '澜起科技', board: 'STAR' },
  ],
  modes: ['backtest', 'paper'],
  real_order_submit_enabled: false,
  defaults: {
    strategy: {
      warmup_bars: 30,
      volatility_window: 30,
      fast_ema_span: 10,
      slow_ema_span: 30,
      vwap_slope_bars: 5,
      entry_z: 1.75,
      max_entry_z: 2.4,
      exit_z: 0.25,
      stop_z: 3,
      realized_vol_window: 10,
      min_realized_vol_bps: 0,
      max_trade_fraction: 0.25,
      max_pairs_per_day: 1,
      cooldown_minutes: 20,
      edge_buffer_bps: 12,
      max_daily_loss_bps: 45,
    },
    cost: {
      commission_rate: 0.0003,
      min_commission: 5,
      stamp_duty_rate: 0.0005,
      transfer_fee_rate: 0.00001,
      slippage_bps: 2,
    },
    initial_capital: 1_000_000,
    cash_buffer_fraction: 0.3,
    max_bar_volume_fraction: 0.05,
  },
  risk_controls: {
    next_bar_fill: true,
    t_plus_one_sellable_inventory: true,
    lunch_restore_time: '11:29',
    force_restore_time: '14:49',
    max_daily_loss_bps: 45,
    max_trade_fraction: 0.3,
    simulated_only: true,
  },
}

const backtestResult = {
  symbols: ['603629.SH', '688008.SH'],
  period: {
    start: '2026-07-01T09:31:00',
    end: '2026-07-08T15:00:00',
    trade_days: 6,
    common_trade_days: 6,
    symbol_trade_days: { '603629.SH': 6, '688008.SH': 6 },
    missing_observed_days: { '603629.SH': [], '688008.SH': [] },
    bars: 2890,
  },
  parameters: {},
  metrics: {
    initial_capital: 1_000_000,
    final_equity: 1_000_500,
    passive_final_equity: 1_000_000,
    incremental_pnl: 500,
    incremental_return: 0.0005,
    cost_reduction_per_share: 0.16,
    completed_pairs: 4,
    entry_count: 4,
    restoration_failures: 0,
    restoration_rate: 1,
    open_pairs_at_end: 0,
    total_fees: 88,
    rejection_count: 1,
  },
  equity_curve: [
    { trade_date: '2026-07-08', equity: 1_000_500, passive_equity: 1_000_000, incremental_pnl: 500 },
  ],
  symbol_summaries: [],
  trades: [],
  rejections: [],
  data_quality: {
    limit_prices: {
      mode: 'fail_closed',
      missing_symbol_days: [],
    },
  },
}

describe('intraday T workbench', () => {
  beforeEach(() => {
    Object.values(api).forEach(mock => mock.mockReset())
    api.capabilities.mockResolvedValue(capabilities)
    api.coverage.mockResolvedValue({ requested: {}, coverage: [] })
    api.paperStatus.mockRejectedValue(new Error('paper session not found'))
    api.runBacktest.mockResolvedValue(backtestResult)
    api.startPaper.mockResolvedValue({
      session_id: 'it-test',
      status: 'RUNNING',
      states: {},
      pending: {},
      real_order_submit_enabled: false,
    })
    api.startPaperRunner.mockResolvedValue({
      session_id: 'it-test',
      status: 'RUNNING',
      runner_active: true,
      states: {},
      pending: {},
      real_order_submit_enabled: false,
    })
    api.paperTrades.mockResolvedValue([])
  })

  it('loads the fixed universe, runs a backtest, and starts a simulated session', async () => {
    const wrapper = mount(IntradayTPage)
    await flushPromises()

    expect(wrapper.text()).toContain('利通电子')
    expect(wrapper.text()).toContain('澜起科技')
    expect(wrapper.text()).toContain('仅模拟')
    expect(wrapper.find('[data-testid="real-order-submit"]').exists()).toBe(false)

    await wrapper.get('[data-testid="run-backtest"]').trigger('click')
    await flushPromises()
    expect(api.runBacktest).toHaveBeenCalledOnce()
    expect(wrapper.text()).toContain('+¥500.00')

    await wrapper.get('[data-testid="start-paper"]').trigger('click')
    await flushPromises()
    expect(api.startPaper).toHaveBeenCalledOnce()
    expect(wrapper.text()).toContain('it-test')

    const runnerButton = wrapper.findAll('button').find(button => button.text().includes('启动 Runner'))
    expect(runnerButton).toBeDefined()
    await runnerButton!.trigger('click')
    await flushPromises()
    expect(api.startPaperRunner).toHaveBeenCalledWith('it-test', 30)
  })

  it('sends the safety gates while presenting the volatility gate as unpromoted', async () => {
    const wrapper = mount(IntradayTPage)
    await flushPromises()

    const riskSummary = wrapper.get('[data-testid="v2-risk-summary"]')
    expect(riskSummary.text()).toContain('v2 候选门控')
    expect(riskSummary.text()).toContain('Z 1.75–2.40')
    expect(riskSummary.text()).toContain('最低波动门控关闭')
    expect(riskSummary.text()).toContain('10:00–10:29')
    expect(riskSummary.text()).toContain('样本外未晋级')
    expect(wrapper.text()).toContain('极端 Z 上限')
    expect(wrapper.text()).toContain('波动窗口')
    expect(wrapper.text()).toContain('最低实现波动')
    expect(wrapper.find('[data-testid="real-order-submit"]').exists()).toBe(false)

    await wrapper.get('[data-testid="run-backtest"]').trigger('click')
    await flushPromises()
    expect(api.runBacktest).toHaveBeenCalledWith(expect.objectContaining({
      strategy: expect.objectContaining({
        entry_z: 1.75,
        max_entry_z: 2.4,
        realized_vol_window: 10,
        min_realized_vol_bps: 0,
      }),
    }))

    await wrapper.get('[data-testid="start-paper"]').trigger('click')
    await flushPromises()
    expect(api.startPaper).toHaveBeenCalledWith(expect.objectContaining({
      strategy: expect.objectContaining({
        entry_z: 1.75,
        max_entry_z: 2.4,
        realized_vol_window: 10,
        min_realized_vol_bps: 0,
      }),
    }))
  })

  it('uses a scrolling page root and keeps real order submission unavailable', async () => {
    const wrapper = mount(IntradayTPage)
    await flushPromises()

    expect(wrapper.get('main').classes()).toContain('page-scroll')
    expect(wrapper.find('[data-testid="real-order-submit"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('样本外未晋级')
  })

  it('falls back to safe v2 defaults when rolling capabilities omit new fields', async () => {
    const rollingCapabilities = structuredClone(capabilities)
    const rollingStrategy = rollingCapabilities.defaults.strategy as Record<string, number>
    delete rollingStrategy.max_entry_z
    delete rollingStrategy.realized_vol_window
    delete rollingStrategy.min_realized_vol_bps
    delete rollingStrategy.max_pairs_per_day
    delete rollingStrategy.cooldown_minutes
    api.capabilities.mockResolvedValueOnce(rollingCapabilities)

    const wrapper = mount(IntradayTPage)
    await flushPromises()

    expect(wrapper.get('[data-testid="v2-risk-summary"]').text()).toContain('Z 1.75–2.40')
    expect(wrapper.get('[data-testid="v2-risk-summary"]').text()).toContain('最低波动门控关闭')

    await wrapper.get('[data-testid="run-backtest"]').trigger('click')
    await flushPromises()
    expect(api.runBacktest).toHaveBeenCalledWith(expect.objectContaining({
      strategy: expect.objectContaining({
        max_entry_z: 2.4,
        realized_vol_window: 10,
        min_realized_vol_bps: 0,
        max_pairs_per_day: 1,
        cooldown_minutes: 20,
      }),
    }))
  })

  it('normalizes cleared numeric controls instead of rendering or submitting null values', async () => {
    const wrapper = mount(IntradayTPage)
    await flushPromises()
    const vm = wrapper.vm as unknown as {
      form: Record<string, number | null>
      $nextTick: () => Promise<void>
    }

    vm.form.entryZ = null
    vm.form.maxEntryZ = null
    vm.form.realizedVolWindow = null
    vm.form.minRealizedVolBps = null
    vm.form.maxPairs = null
    vm.form.cooldownMinutes = null
    await vm.$nextTick()

    expect(wrapper.get('[data-testid="v2-risk-summary"]').text()).toContain('Z 1.75–2.40')
    expect(wrapper.get('[data-testid="v2-risk-summary"]').text()).toContain('最低波动门控关闭')
    await wrapper.get('[data-testid="run-backtest"]').trigger('click')
    await flushPromises()
    expect(api.runBacktest).toHaveBeenCalledWith(expect.objectContaining({
      strategy: expect.objectContaining({
        entry_z: 1.75,
        max_entry_z: 2.4,
        realized_vol_window: 10,
        min_realized_vol_bps: 0,
        max_pairs_per_day: 1,
        cooldown_minutes: 20,
      }),
    }))
  })

  it('clears stale coverage and results when the date range changes', async () => {
    api.coverage.mockResolvedValueOnce({
      requested: { start_date: '2026-01-01', end_date: '2026-07-20' },
      coverage: [
        { symbol: '603629.SH', name: '利通电子', bars: 100, trade_days: 1, start: null, end: null },
        { symbol: '688008.SH', name: '澜起科技', bars: 100, trade_days: 1, start: null, end: null },
      ],
    })
    const wrapper = mount(IntradayTPage)
    await flushPromises()
    await wrapper.get('[data-testid="run-backtest"]').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('+¥500.00')
    expect(wrapper.text()).toContain('分钟数据就绪')

    const picker = wrapper.getComponent({ name: 'ElDatePicker' })
    picker.vm.$emit('update:modelValue', ['2026-06-01', '2026-06-30'])
    await flushPromises()

    expect(wrapper.text()).toContain('等待覆盖检查')
    expect(wrapper.text()).not.toContain('+¥500.00')
  })

  it('warns that missing exact limit prices fail closed for the affected days', async () => {
    api.runBacktest.mockResolvedValueOnce({
      ...backtestResult,
      data_quality: {
        limit_prices: {
          mode: 'fail_closed',
          missing_symbol_days: ['688008.SH|2026-07-14'],
        },
      },
    })
    const wrapper = mount(IntradayTPage)
    await flushPromises()

    await wrapper.get('[data-testid="run-backtest"]').trigger('click')
    await flushPromises()

    const warning = wrapper.get('[data-testid="limit-price-warning"]')
    expect(warning.text()).toContain('涨跌停价缺失')
    expect(warning.text()).toContain('fail-closed')
    expect(warning.text()).toContain('缺失日禁止入场')
    expect(warning.text()).toContain('688008.SH · 2026-07-14')
  })
})
