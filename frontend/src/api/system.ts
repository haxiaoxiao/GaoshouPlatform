import request from './request'

export interface LlmEndpoint {
  id: string
  name: string
  api_base: string
  api_key_hint: string
  model: string
  priority: number
  enabled: boolean
  consecutive_failures: number
  cooldown_until: string | null
  last_success_at: string | null
  last_failure_at: string | null
  last_error: string | null
  created_at: string
  updated_at: string
}

export interface LlmEndpointCreatePayload {
  name: string
  api_base: string
  api_key: string
  model: string
  enabled: boolean
}

export interface LlmEndpointUpdatePayload {
  name?: string
  api_base?: string
  api_key?: string
  model?: string
  enabled?: boolean
}

export interface LlmEndpointDraft {
  name: string
  api_base: string
  api_key: string
  model: string
  enabled: boolean
}

export interface LlmEndpointTestResult {
  status: 'ok' | 'error'
  latency_ms: number
  model: string | null
  error: string | null
}

export interface LlmGatewaySummary {
  readiness: 'ready' | 'degraded' | 'blocked'
  enabled: number
  total: number
  primary: string | null
  recentError: string | null
}

export type LlmEndpointLoadState = 'loading' | 'ready' | 'error'

export interface LlmGatewayView {
  readiness: LlmGatewaySummary['readiness'] | 'unknown' | 'error'
  enabled: number | null
  total: number | null
  primary: string | null
  recentError: string | null
}

export function buildLlmEndpointUpdate(current: LlmEndpoint, draft: LlmEndpointDraft): LlmEndpointUpdatePayload {
  const apiBase = draft.api_base.trim()
  const apiKey = draft.api_key.trim()
  const payload: LlmEndpointUpdatePayload = {
    name: draft.name.trim(),
    api_base: apiBase,
    model: draft.model.trim(),
    enabled: draft.enabled,
  }
  if (apiKey || apiBase !== current.api_base) payload.api_key = apiKey
  return payload
}

export function moveLlmEndpointIds(ids: string[], endpointId: string, offset: -1 | 1): string[] {
  const currentIndex = ids.indexOf(endpointId)
  const nextIndex = currentIndex + offset
  if (currentIndex < 0 || nextIndex < 0 || nextIndex >= ids.length) return [...ids]
  const next = [...ids]
  ;[next[currentIndex], next[nextIndex]] = [next[nextIndex], next[currentIndex]]
  return next
}

export function sanitizeRecentEndpointError(error: string | null | undefined): string | null {
  if (!error) return null
  const compact = error
    .replace(/\b(?:sk|key|token)-[A-Za-z0-9._-]{6,}\b/gi, '[redacted]')
    .replace(/\s+/g, ' ')
    .trim()
  return compact.length > 160 ? `${compact.slice(0, 160)}...` : compact
}

export function isLlmEndpointCooldownActive(endpoint: Pick<LlmEndpoint, 'cooldown_until'>, now = Date.now()): boolean {
  if (!endpoint.cooldown_until) return false
  const cooldownUntil = Date.parse(endpoint.cooldown_until)
  return Number.isFinite(cooldownUntil) && cooldownUntil > now
}

export function summarizeLlmEndpoints(endpoints: LlmEndpoint[]): LlmGatewaySummary {
  const ordered = [...endpoints].sort((a, b) => a.priority - b.priority)
  const enabled = ordered.filter(endpoint => endpoint.enabled)
  const latestFailure = ordered
    .filter(endpoint => endpoint.last_error && endpoint.last_failure_at)
    .sort((a, b) => String(b.last_failure_at).localeCompare(String(a.last_failure_at)))[0]
  const unhealthy = enabled.some(endpoint => endpoint.consecutive_failures > 0 || isLlmEndpointCooldownActive(endpoint))
  return {
    readiness: enabled.length === 0 ? 'blocked' : unhealthy ? 'degraded' : 'ready',
    enabled: enabled.length,
    total: ordered.length,
    primary: enabled[0]?.name || null,
    recentError: sanitizeRecentEndpointError(latestFailure?.last_error),
  }
}

export function getLlmGatewayView(endpoints: LlmEndpoint[], loadState: LlmEndpointLoadState): LlmGatewayView {
  if (loadState !== 'ready' && endpoints.length === 0) {
    return { readiness: 'unknown', enabled: null, total: null, primary: null, recentError: null }
  }
  const summary = summarizeLlmEndpoints(endpoints)
  return { ...summary, readiness: loadState === 'error' ? 'error' : summary.readiness }
}

export interface SystemStatus {
  status: string
  database: string
  market_data_backend?: string
  parquet_data_dir?: string
  data_dir?: string
  dev_data_mode?: DevDataMode
  [key: string]: unknown
}

export interface DataSummaryItem {
  key: string
  label: string
  source: string
  latest_date: string | null
  latest_datetime?: string | null
  row_count: number | null
  row_count_estimated: boolean
  status: 'good' | 'stale' | 'missing' | 'error' | string
  status_text: string
  storage?: string
  dataset?: string | null
  date_column?: string | null
  min_date?: string | null
  error?: string | null
  notes?: string | null
}

export interface DataSummary {
  generated_at: string
  overall_status: 'good' | 'degraded' | 'error' | string
  market_data_backend: string
  parquet_data_dir: string
  data_dir?: string
  dev_data_mode?: DevDataMode
  items: DataSummaryItem[]
  by_key: Record<string, DataSummaryItem>
}

export interface DevDataMode {
  enabled: boolean
  environment: 'dev' | 'prod' | string
  use_prod_data: boolean
  active_data_dir: string
  active_database_url: string
  active_parquet_data_dir: string
  dev_local_data_dir: string
  dev_prod_data_dir: string
  warning: string | null
  updated_at?: string | null
}

export interface LiveTradingGuardrails {
  enable_order_submit: boolean
  auto_execute_enabled: boolean
  env_file: string
  env_values: Record<string, string | null>
  requires_restart: boolean
  confirm_text: string
  updated_at: string
}

export type DatasetReadinessStatus = 'ready' | 'stale' | 'missing' | 'invalid'

export interface DatasetReadinessItem {
  dataset: string
  status: DatasetReadinessStatus
  max_date: string | null
  age_days?: number | null
  reason?: string | null
  row_count?: number | null
  file_count?: number | null
  source?: string
  schema_hash?: string | null
}

export interface PlatformReadiness {
  as_of: string
  environment: 'research' | 'paper' | 'live'
  overall_status: 'ready' | 'degraded' | 'invalid'
  datasets: Record<string, DatasetReadinessItem>
  trading: {
    order_submit_enabled: boolean
    auto_execute_enabled: boolean
    control_secret_configured: boolean
  }
}

export interface ReadinessContext {
  environmentLabel: string
  dataDate: string
  readinessLabel: string
  orderSubmitLabel: string
  orderSubmitRisk: boolean
}

export function formatReadinessContext(payload: PlatformReadiness): ReadinessContext {
  const dailyDate = payload.datasets.klines_daily?.max_date
  const fallbackDate = Object.values(payload.datasets)
    .map(item => item.max_date)
    .filter((value): value is string => Boolean(value))
    .sort()
    .at(-1)
  const readinessLabels: Record<PlatformReadiness['overall_status'], string> = {
    ready: '数据就绪',
    degraded: '数据降级',
    invalid: '数据异常',
  }

  return {
    environmentLabel: payload.environment.toUpperCase(),
    dataDate: dailyDate || fallbackDate || '未知',
    readinessLabel: readinessLabels[payload.overall_status],
    orderSubmitLabel: payload.trading.order_submit_enabled ? '真实下单开启' : '真实下单关闭',
    orderSubmitRisk: payload.trading.order_submit_enabled,
  }
}

let readinessCache: PlatformReadiness | null = null
let readinessCacheUntil = 0
let readinessRequest: Promise<PlatformReadiness> | null = null

export function getPlatformReadiness(force = false): Promise<PlatformReadiness> {
  if (!force && readinessCache && Date.now() < readinessCacheUntil) {
    return Promise.resolve(readinessCache)
  }
  if (!force && readinessRequest) return readinessRequest
  readinessRequest = request.get<PlatformReadiness>('/v1/readiness')
    .then(payload => {
      readinessCache = payload
      readinessCacheUntil = Date.now() + 30_000
      return payload
    })
    .finally(() => {
      readinessRequest = null
    })
  return readinessRequest
}

export const systemApi = {
  getStatus: () => request.get<SystemStatus>('/system/status'),

  healthCheck: () => request.get<{ status: string }>('/system/health'),

  dataSummary: () => request.get<DataSummary>('/system/data-summary'),

  readiness: (force = false) => getPlatformReadiness(force),

  getDevDataMode: () => request.get<DevDataMode>('/system/dev-data-mode'),

  setDevDataMode: (payload: { use_prod_data: boolean; acknowledge_warning?: boolean }) =>
    request.put<DevDataMode>('/system/dev-data-mode', payload),

  getLiveTradingGuardrails: () =>
    request.get<LiveTradingGuardrails>('/system/live-trading-guardrails'),

  setLiveTradingGuardrails: (payload: {
    enable_order_submit: boolean
    auto_execute_enabled: boolean
    acknowledge_risk?: boolean
    confirm_text?: string | null
  }) => request.put<LiveTradingGuardrails>('/system/live-trading-guardrails', payload),

  listLlmEndpoints: () => request.get<LlmEndpoint[]>('/system/llm-endpoints'),

  createLlmEndpoint: (payload: LlmEndpointCreatePayload) =>
    request.post<LlmEndpoint>('/system/llm-endpoints', payload),

  updateLlmEndpoint: (endpointId: string, payload: LlmEndpointUpdatePayload) =>
    request.patch<LlmEndpoint>(`/system/llm-endpoints/${endpointId}`, payload),

  deleteLlmEndpoint: (endpointId: string) =>
    request.delete<void>(`/system/llm-endpoints/${endpointId}`),

  reorderLlmEndpoints: (endpointIds: string[]) =>
    request.post<LlmEndpoint[]>('/system/llm-endpoints/reorder', { endpoint_ids: endpointIds }),

  testLlmEndpoint: (endpointId: string) =>
    request.post<LlmEndpointTestResult>(`/system/llm-endpoints/${endpointId}/test`),
}
