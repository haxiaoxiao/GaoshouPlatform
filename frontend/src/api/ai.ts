import request from './request'

export interface AIChatMessage {
  role: 'system' | 'user' | 'assistant' | 'tool'
  content: string
}

export interface AIActionCard {
  tool_name: string
  title: string
  description: string
  arguments: Record<string, unknown>
  risk_level: 'read' | 'write' | 'danger'
  requires_confirmation: boolean
  route?: string | null
}

export interface AITraceNode {
  name: string
  status: string
  detail: string | null
}

export interface AITraceToolCall {
  tool_name: string
  title: string
  arguments: Record<string, unknown>
  risk_level: 'read' | 'write' | 'danger'
  reason: string
  status: string
  summary?: string | null
}

export interface AITrace {
  note: string
  source: string
  confidence?: number | null
  error?: string | null
  clarification?: string | null
  context: Record<string, unknown>
  nodes: AITraceNode[]
  tool_calls: AITraceToolCall[]
}

export interface AIChatResponse {
  message: AIChatMessage
  actions: AIActionCard[]
  executed_tools: Array<{
    tool_name: string
    status: 'ok' | 'error' | 'needs_confirmation'
    summary: string
    task_id: string | null
    result_ref: string | null
    error: string | null
  }>
  trace: AITrace
  artifact_id: string | null
  model: string | null
  offline: boolean
}

export interface AIToolDefinition {
  name: string
  title: string
  description: string
  category: string
  risk_level: 'read' | 'write' | 'danger'
  requires_confirmation: boolean
  input_schema: Record<string, unknown>
  output_schema: Record<string, unknown>
}

export interface AIToolExecutionResponse {
  tool_name: string
  status: 'ok' | 'error' | 'needs_confirmation'
  summary: string
  result: unknown
  artifact_id: string | null
  task_id: string | null
  result_ref: string | null
  error: string | null
}

export interface AIStatus {
  enabled: boolean
  gateway: {
    available: boolean
    configured: boolean
    provider: string
    model: string
    api_key_env: string
    api_key_configured: boolean
    timeout_seconds: number
    max_tokens: number
    error: string | null
  }
  tool_count: number
  artifact_store: Record<string, unknown>
  decisions: string[]
}

export interface AIConfig {
  enabled: boolean
  provider: string
  model: string
  base_url: string | null
  api_key_env: string
  api_key_configured: boolean
  api_key_masked: string | null
  api_key_source: string | null
  api_key_warning: string | null
  env_file: string
  requires_restart: boolean
  updated_at: string
  gateway: AIStatus['gateway']
}

export interface AIConfigUpdate {
  enabled?: boolean
  provider?: string
  model?: string
  base_url?: string
  api_key_env?: string
  api_key?: string
  clear_api_key?: boolean
}

export const aiApi = {
  status: () => request.get<AIStatus>('/ai/status'),
  config: () => request.get<AIConfig>('/ai/config'),
  updateConfig: (payload: AIConfigUpdate) => request.put<AIConfig>('/ai/config', payload),
  tools: () => request.get<AIToolDefinition[]>('/ai/tools'),
  chat: (payload: {
    messages: AIChatMessage[]
    page_context?: Record<string, unknown>
    auto_execute?: boolean
  }) => request.post<AIChatResponse>('/ai/chat', payload),
  executeTool: (toolName: string, payload: { arguments?: Record<string, unknown>; confirmed?: boolean }) =>
    request.post<AIToolExecutionResponse>(`/ai/tools/${encodeURIComponent(toolName)}/execute`, payload),
}
