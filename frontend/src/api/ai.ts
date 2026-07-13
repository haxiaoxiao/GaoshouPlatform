import request from '@/api/request'

export interface AIStatus {
  configured: boolean
  state: 'ready' | 'blocked'
  api_base: string
  model: string | null
  reason: string | null
  retention_days: number
}

export interface AIApproval {
  approval_id: string
  tool: string
  arguments: Record<string, unknown>
  status?: 'pending' | 'running' | 'completed' | 'rejected' | 'failed'
  result?: unknown
}

export interface AIMessage {
  role: 'user' | 'assistant' | 'tool'
  content: string
  approvals?: AIApproval[]
  tool_results?: Array<Record<string, unknown>>
}

export interface AIConversation {
  id: string
  title: string
  messages: AIMessage[]
  context: Record<string, unknown>
  expires_at: string
}

export const aiApi = {
  status: () => request.get<AIStatus>('/ai/status'),
  conversations: () => request.get<AIConversation[]>('/ai/conversations'),
  createConversation: (title: string, context: Record<string, unknown>) =>
    request.post<AIConversation>('/ai/conversations', { title, context }),
  conversation: (id: string) => request.get<AIConversation>(`/ai/conversations/${id}`),
  chat: (conversationId: string, message: string, context: Record<string, unknown>) =>
    request.post<{ conversation_id: string; message: AIMessage; approvals: AIApproval[] }>('/ai/chat', {
      conversation_id: conversationId,
      message,
      context,
    }),
  confirm: (approvalId: string) => request.post(`/ai/approvals/${approvalId}/confirm`),
  reject: (approvalId: string) => request.post(`/ai/approvals/${approvalId}/reject`),
}
