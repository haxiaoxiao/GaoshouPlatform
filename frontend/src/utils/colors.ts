export const MARKET_UP_COLOR = '#d93026'
export const MARKET_DOWN_COLOR = '#137333'
export const MARKET_UP_CSS = 'var(--market-up)'
export const MARKET_DOWN_CSS = 'var(--market-down)'
export const STATUS_READY_COLOR = '#16a34a'
export const STATUS_ATTENTION_COLOR = '#ef4444'
export const STATUS_READY_CSS = 'var(--status-ready)'
export const STATUS_ATTENTION_CSS = 'var(--status-attention)'
export const NEUTRAL_COLOR = '#8888a0'
export const NEUTRAL_CSS = 'var(--color-neutral)'

export function marketValueColor(value: number | null | undefined): string {
  if (value == null) return NEUTRAL_CSS
  if (value > 0) return MARKET_UP_CSS
  if (value < 0) return MARKET_DOWN_CSS
  return NEUTRAL_CSS
}

export function marketValueColorHex(value: number | null | undefined): string {
  if (value == null) return NEUTRAL_COLOR
  if (value > 0) return MARKET_UP_COLOR
  if (value < 0) return MARKET_DOWN_COLOR
  return NEUTRAL_COLOR
}

export function marketDirectionColor(direction: string | null | undefined): string {
  const normalized = String(direction || '').toLowerCase()
  if (normalized === 'buy') return MARKET_UP_CSS
  if (normalized === 'sell') return MARKET_DOWN_CSS
  return NEUTRAL_CSS
}
