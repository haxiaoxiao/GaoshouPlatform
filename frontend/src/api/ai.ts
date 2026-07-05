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
  workflow?: {
    workflow_name?: string | null
    status?: string | null
    summary?: string | null
    nodes?: Array<{
      name: string
      title?: string | null
      status: string
      detail?: string | null
      tool_name?: string | null
      arguments?: Record<string, unknown>
    }>
    pending_tools?: Array<Record<string, unknown>>
  } | null
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
  answer?: {
    mode?: string | null
    error?: string | null
  }
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

export interface AIWorkflowDefinition {
  name: string
  title: string
  description: string
  category: string
  nodes: Array<{
    name: string
    title: string
    status: string
    detail: string | null
    tool_name: string | null
    arguments: Record<string, unknown>
  }>
  input_schema: Record<string, unknown>
}

export interface AIWorkflowRunRequest {
  command?: string | null
  messages?: AIChatMessage[]
  page_context?: Record<string, unknown> | null
  arguments?: Record<string, unknown>
  auto_execute?: boolean
  dry_run?: boolean
  confirmed?: boolean
}

export interface AIWorkflowRunResponse {
  workflow_name: string
  status: 'planned' | 'completed' | 'needs_confirmation' | 'error'
  summary: string
  nodes: Array<{
    name: string
    title: string
    status: string
    detail: string | null
    tool_name: string | null
    arguments: Record<string, unknown>
  }>
  tool_results: Array<Record<string, unknown>>
  pending_tools: Array<Record<string, unknown>>
  result: Record<string, unknown>
  artifact_id: string | null
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

export interface AIRecentArtifact {
  artifact_id: string | null
  kind: string | null
  status: string | null
  created_at: string | null
  input_summary: string
  reply_preview: string
  route_source: string
  answer_mode: string
  tool_call_count: number
  executed_count: number
  error: string | null
}

export interface AIArtifact {
  artifact_id: string
  kind: string
  status: string
  input_summary: string | null
  tool_calls: Array<Record<string, unknown>>
  result_ref: string | null
  key_outputs: Record<string, unknown>
  error: string | null
  created_at: string | null
  updated_at: string | null
}

export interface AIDiagnostics {
  health: {
    status: string
    ready: boolean
    warnings: string[]
  }
  gateway: AIStatus['gateway'] & {
    enabled: boolean
  }
  manifest: {
    schema_version: string | null
    generated_at: string | null
    tool_count: number
    workflow_count: number
    categories: Record<string, number>
    risk_levels: Record<string, number>
    confirmation_required: number
    workflows: Array<{
      name: string | null
      title: string | null
      category: string | null
      node_count: number
    }>
    http: {
      chat?: string | null
      manifest?: string | null
      workflows?: string | null
      workflow_run_template?: string | null
      execute_template?: string | null
    }
    mcp_stdio: {
      command?: string | null
      args: string[]
    }
  }
  artifacts: {
    sample_limit: number
    sampled: number
    status_counts: Record<string, number>
    kind_counts: Record<string, number>
    latest: AIRecentArtifact | null
    recent: AIRecentArtifact[]
  }
  routing: {
    source_counts: Record<string, number>
    pending_confirmation_count: number
  }
  answers: {
    mode_counts: Record<string, number>
    error_count: number
  }
  tools: {
    status_counts: Record<string, number>
    top_tools: Array<{ tool_name: string; count: number }>
    recent_failures: Array<{
      artifact_id: string | null
      created_at: string | null
      tool_name: string
      status: string
      summary: string
      error: string
    }>
  }
}

export const aiApi = {
  status: () => request.get<AIStatus>('/ai/status'),
  config: () => request.get<AIConfig>('/ai/config'),
  diagnostics: (limit = 50) => request.get<AIDiagnostics>('/ai/diagnostics', { params: { limit } }),
  updateConfig: (payload: AIConfigUpdate) => request.put<AIConfig>('/ai/config', payload),
  workflows: () => request.get<AIWorkflowDefinition[]>('/ai/workflows'),
  runWorkflow: (workflowName: string, payload: AIWorkflowRunRequest) =>
    request.post<AIWorkflowRunResponse>(`/ai/workflows/${encodeURIComponent(workflowName)}/run`, payload),
  tools: () => request.get<AIToolDefinition[]>('/ai/tools'),
  chat: (payload: {
    messages: AIChatMessage[]
    page_context?: Record<string, unknown>
    auto_execute?: boolean
  }) => request.post<AIChatResponse>('/ai/chat', payload),
  executeTool: (toolName: string, payload: { arguments?: Record<string, unknown>; confirmed?: boolean }) =>
    request.post<AIToolExecutionResponse>(`/ai/tools/${encodeURIComponent(toolName)}/execute`, payload),
  artifacts: (params?: { kind?: string; limit?: number }) =>
    request.get<AIArtifact[]>('/ai/artifacts', { params }),
}
