import request from './request'

export interface LlmEndpoint {
  id: string
  name: string
  api_base: string
  api_key_hint: string
  model: string
  provider: string
  review_model: string | null
  wire_api: 'responses' | 'chat_completions'
  reasoning_effort: 'none' | 'minimal' | 'low' | 'medium' | 'high' | 'xhigh' | null
  disable_response_storage: boolean
  requires_openai_auth: boolean
  config: LlmEndpointConfig
  preserved_fields: string[]
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
  config: LlmEndpointConfig
  enabled: boolean
}

export interface LlmEndpointUpdatePayload {
  config?: LlmEndpointConfig
  enabled?: boolean
}

export type LlmJsonValue = string | number | boolean | null | LlmJsonValue[] | { [key: string]: LlmJsonValue }
export type LlmEndpointConfig = { [key: string]: LlmJsonValue }

export interface LlmEndpointConfigPreview {
  provider: string
  name: string
  model: string
  reviewModel: string | null
  apiBase: string
  wireApi: string
  reasoningEffort: string | null
  disableResponseStorage: boolean
  requiresOpenaiAuth: boolean
}

export const LLM_API_KEY_PLACEHOLDER = '__GAOSHOU_STORED_SECRET__'
const FAKE_API_KEY = 'replace-with-your-api-key'
const LLM_RUNTIME_ROOT_FIELDS = new Set([
  'OPENAI_API_KEY',
  'disable_response_storage',
  'env',
  'model',
  'model_provider',
  'model_providers',
  'model_reasoning_effort',
  'review_model',
])
const LLM_RUNTIME_PROVIDER_FIELDS = new Set(['base_url', 'name', 'requires_openai_auth', 'wire_api'])

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

export function parseLlmEndpointConfig(text: string): { config: LlmEndpointConfig } {
  let value: unknown
  try {
    value = JSON.parse(text)
  } catch {
    throw new Error('Configuration must contain valid JSON.')
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Configuration must be a JSON object.')
  }
  return { config: value as LlmEndpointConfig }
}

export function formatLlmEndpointConfig(text: string): string {
  return JSON.stringify(parseLlmEndpointConfig(text).config, null, 2)
}

export function createLlmEndpointTemplate(): string {
  return JSON.stringify({
    model_provider: 'openai',
    model: 'gpt-5',
    review_model: 'gpt-5',
    model_reasoning_effort: 'medium',
    disable_response_storage: true,
    model_providers: {
      openai: {
        name: 'OpenAI',
        base_url: 'https://api.openai.com/v1',
        wire_api: 'responses',
        requires_openai_auth: true,
      },
    },
    env: { OPENAI_API_KEY: FAKE_API_KEY },
  }, null, 2)
}

export function sanitizedLlmEndpointConfig(endpoint: Pick<LlmEndpoint, 'config'>): string {
  return JSON.stringify(endpoint.config, null, 2)
}

function objectValue(value: LlmJsonValue | undefined): Record<string, LlmJsonValue> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {}
}

export function previewLlmEndpointConfig(config: LlmEndpointConfig): LlmEndpointConfigPreview {
  const provider = typeof config.model_provider === 'string' ? config.model_provider : ''
  const selected = objectValue(objectValue(config.model_providers)[provider])
  return {
    provider,
    name: typeof selected.name === 'string' ? selected.name : provider,
    model: typeof config.model === 'string' ? config.model : '',
    reviewModel: typeof config.review_model === 'string' ? config.review_model : null,
    apiBase: typeof selected.base_url === 'string' ? selected.base_url : '',
    wireApi: typeof selected.wire_api === 'string' ? selected.wire_api : 'chat_completions',
    reasoningEffort: typeof config.model_reasoning_effort === 'string' ? config.model_reasoning_effort : null,
    disableResponseStorage: config.disable_response_storage === true,
    requiresOpenaiAuth: selected.requires_openai_auth === true,
  }
}

export function getLlmEndpointPreservedFields(config: LlmEndpointConfig): string[] {
  const provider = typeof config.model_provider === 'string' ? config.model_provider : ''
  const preserved = Object.keys(config).filter(key => !LLM_RUNTIME_ROOT_FIELDS.has(key))
  const env = objectValue(config.env)
  preserved.push(...Object.keys(env).filter(key => key !== 'OPENAI_API_KEY').map(key => `env.${key}`))

  const providers = objectValue(config.model_providers)
  preserved.push(...Object.keys(providers).filter(key => key !== provider).map(key => `model_providers.${key}`))
  const selected = objectValue(providers[provider])
  preserved.push(...Object.keys(selected)
    .filter(key => !LLM_RUNTIME_PROVIDER_FIELDS.has(key))
    .map(key => `model_providers.${provider}.${key}`))
  return preserved.sort()
}

export function getLlmEndpointConfigWarnings(config: LlmEndpointConfig): string[] {
  const preservedFields = getLlmEndpointPreservedFields(config)
  if (!preservedFields.length) return []
  return [`These fields are preserved but not interpreted by the gateway: ${preservedFields.join(', ')}`]
}

export function shouldConfirmLlmTemplateReset(text: string): boolean {
  try {
    return formatLlmEndpointConfig(text) !== createLlmEndpointTemplate()
  } catch {
    return Boolean(text.trim())
  }
}

export function buildLlmEndpointCreate(config: LlmEndpointConfig, enabled: boolean): LlmEndpointCreatePayload {
  return { config, enabled }
}

export function buildLlmEndpointUpdate(config: LlmEndpointConfig, enabled: boolean): LlmEndpointUpdatePayload {
  return { config, enabled }
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
