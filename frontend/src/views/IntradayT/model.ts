import type { IntradayTDirection, IntradayTEquityPoint, IntradayTSide } from '@/api/intradayT'

export function marketDateString(value: Date, timeZone = 'Asia/Shanghai'): string {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(value).map(part => [part.type, part.value]),
  )
  const { year, month, day } = parts
  if (!year || !month || !day) throw new Error('Unable to format market date')
  return `${year}-${month}-${day}`
}

export function formatCurrency(value: number, signed = false): string {
  const sign = value < 0 ? '-' : signed && value > 0 ? '+' : ''
  const formatted = Math.abs(value).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
  return `${sign}¥${formatted}`
}

export function formatPercent(value: number, signed = false): string {
  const sign = value < 0 ? '-' : signed && value > 0 ? '+' : ''
  return `${sign}${Math.abs(value * 100).toFixed(2)}%`
}

export function tradeSideLabel(side: IntradayTSide): string {
  return side === 'BUY' ? '买入' : '卖出'
}

export function directionLabel(direction: IntradayTDirection): string {
  return direction === 'POSITIVE' ? '正 T' : '反 T'
}

export function stateLabel(state: string): string {
  const labels: Record<string, string> = {
    READY: '可交易',
    POSITIVE_T_OPEN: '正 T 待恢复',
    REVERSE_T_OPEN: '反 T 待恢复',
    FORCE_RESTORE: '强制恢复',
    RESTORED: '已恢复',
    LOCKED: '已锁定',
  }
  return labels[state] || state
}

export function reasonLabel(reason: string): string {
  const labels: Record<string, string> = {
    mean_reversion_entry: '均值回归入场',
    mean_reversion_exit: '均值回归恢复',
    force_restore: '时点强制恢复',
    risk_restore: '风险止损恢复',
    volume_cap: '成交量容量不足',
    limit_up: '涨停无法买入',
    limit_down: '跌停无法卖出',
    insufficient_cash: '现金不足',
  }
  return labels[reason] || reason
}

export function buildEquitySeries(points: IntradayTEquityPoint[]) {
  return {
    dates: points.map(point => point.trade_date.slice(5)),
    strategy: points.map(point => point.equity),
    passive: points.map(point => point.passive_equity),
    incremental: points.map(point => point.incremental_pnl),
  }
}
