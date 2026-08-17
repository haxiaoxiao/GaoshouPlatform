import request, { type RequestConfig } from './request'

export type RadarFreshness = 'fresh' | 'partial' | 'stale' | 'unavailable'
export type RadarRealtimeMode = 'push' | 'polling_30s' | 'offline' | 'closed'
export type MarketAlertStatus = 'active' | 'acknowledged' | 'dismissed' | 'resolved'
export type MarketAlertSeverity = 'low' | 'medium' | 'high'
export type MarketAlertScope = 'market' | 'sector' | 'symbol' | 'data'

export interface MarketRadarSource {
  name: string
  as_of: string | null
  status: string
  reason?: string | null
  [key: string]: unknown
}

export interface MarketRadarEnvelope<T> {
  as_of: string | null
  computed_at: string
  status: RadarFreshness
  confidence: number
  realtime_mode: RadarRealtimeMode
  sources: MarketRadarSource[]
  data: T
}

export interface MarketRadarOverview {
  alert_counts?: Record<string, number>
  emotion?: Record<string, unknown>
  crowding?: Record<string, unknown>
  [key: string]: unknown
}

export interface MarketRadarAlert {
  id: number
  rule_id: number
  snapshot_id: number | null
  scope: MarketAlertScope
  subject: string
  direction: string
  severity: MarketAlertSeverity
  status: MarketAlertStatus
  title: string
  explanation: string
  dedupe_key: string
  evidence: Record<string, unknown>
  triggered_at: string
  last_seen_at: string
  acknowledged_at: string | null
  dismissed_at: string | null
  resolved_at: string | null
  last_notified_at: string | null
  occurrence_count: number
  clear_streak: number
}

export interface MarketRadarAlertPage {
  items: MarketRadarAlert[]
  total: number
  page: number
  page_size: number
}

export type MarketRadarStreamEvent<T extends object> = T & {
  schema_version: 1
  event_id: string
  sequence: number
  occurred_at: string
}

export interface MarketRadarRule {
  id: number
  rule_key: string
  version: number
  scope: 'symbol'
  subject: string
  title: string
  direction: 'up' | 'down' | 'either'
  rule_type: 'metric_threshold'
  parameters: {
    metric: string
    operator: 'lte' | 'gte' | 'abs_gte'
    threshold: number
  }
  severity: MarketAlertSeverity
  cooldown_seconds: number
  enabled: boolean
  source: 'system' | 'user'
  created_at: string
  updated_at: string
}

export interface MarketRadarRulePage {
  items: MarketRadarRule[]
  total: number
}

export interface MarketRadarRefreshResult {
  task_id: string
  kind: 'market_radar_refresh'
  status: 'queued' | 'running'
  refresh_kind: 'intraday' | 'eod'
  trade_date: string | null
}

export type MarketRadarRuleCreate = Omit<
  MarketRadarRule,
  'id' | 'version' | 'source' | 'created_at' | 'updated_at'
>

export type MarketRadarRulePatch = Partial<Pick<
  MarketRadarRule,
  'subject' | 'title' | 'direction' | 'parameters' | 'severity' | 'cooldown_seconds' | 'enabled'
>>

interface AlertQuery {
  status?: MarketAlertStatus
  severity?: MarketAlertSeverity
  scope?: MarketAlertScope
  subject?: string
  start_at?: string
  end_at?: string
  page?: number
  page_size?: number
}

const withDate = (tradeDate?: string) => tradeDate ? { trade_date: tradeDate } : undefined

export const marketRadarApi = {
  overview: (config?: RequestConfig) =>
    request.get<MarketRadarEnvelope<MarketRadarOverview>>('/market-radar/overview', config),
  breadth: (params: { days?: number; mode?: 'percent' | 'count' } = {}) =>
    request.get<MarketRadarEnvelope<Record<string, unknown>>>('/market-radar/breadth', { params }),
  limitLadder: (tradeDate?: string) =>
    request.get<MarketRadarEnvelope<Record<string, unknown>>>('/market-radar/limit-ladder', {
      params: withDate(tradeDate),
    }),
  crowding: (params: {
    scope?: 'market' | 'sector' | 'symbol'
    subject?: string
  } = {}) => request.get<MarketRadarEnvelope<Record<string, unknown>>>(
    '/market-radar/crowding',
    { params },
  ),
  sectors: (tradeDate?: string) =>
    request.get<MarketRadarEnvelope<Record<string, unknown>>>('/market-radar/sectors', {
      params: withDate(tradeDate),
    }),
  alerts: (params: AlertQuery = {}, config?: RequestConfig) =>
    request.get<MarketRadarEnvelope<MarketRadarAlertPage>>('/market-radar/alerts', {
      ...config,
      params,
    }),
  activeHighAlerts: (config?: RequestConfig) =>
    request.get<MarketRadarEnvelope<MarketRadarAlertPage>>('/market-radar/alerts', {
      ...config,
      params: { status: 'active', severity: 'high', page: 1, page_size: 100 },
      notifyError: false,
    }),
  alert: (eventId: number) =>
    request.get<MarketRadarEnvelope<MarketRadarAlert>>(`/market-radar/alerts/${eventId}`),
  acknowledgeAlert: (eventId: number) =>
    request.post<MarketRadarEnvelope<MarketRadarAlert>>(
      `/market-radar/alerts/${eventId}/acknowledge`,
    ),
  dismissAlert: (eventId: number) =>
    request.post<MarketRadarEnvelope<MarketRadarAlert>>(`/market-radar/alerts/${eventId}/dismiss`),
  rules: (includeDisabled = false) =>
    request.get<MarketRadarEnvelope<MarketRadarRulePage>>('/market-radar/rules', {
      params: { include_disabled: includeDisabled },
    }),
  createRule: (payload: MarketRadarRuleCreate) =>
    request.post<MarketRadarEnvelope<MarketRadarRule>>('/market-radar/rules', payload),
  patchRule: (ruleId: number, payload: MarketRadarRulePatch) =>
    request.patch<MarketRadarEnvelope<MarketRadarRule>>(`/market-radar/rules/${ruleId}`, payload),
  deleteRule: (ruleId: number) =>
    request.delete<MarketRadarEnvelope<MarketRadarRule>>(`/market-radar/rules/${ruleId}`),
  refresh: (payload: { kind: 'intraday'; trade_date?: never } | { kind: 'eod'; trade_date: string }) =>
    request.post<MarketRadarEnvelope<MarketRadarRefreshResult>>('/market-radar/refresh', payload),
  streamUrl: () => '/api/market-radar/stream',
}
