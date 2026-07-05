<template>
  <teleport to="body">
    <div v-if="visible" class="copilot-layer" @click.self="emit('close')">
      <aside class="copilot-drawer" aria-label="AI Copilot">
        <header class="copilot-header">
          <div>
            <span class="copilot-kicker">AI NATIVE</span>
            <h2>Copilot</h2>
          </div>
          <button class="icon-btn" type="button" title="关闭" @click="emit('close')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M18 6L6 18" stroke-linecap="round"/>
              <path d="M6 6l12 12" stroke-linecap="round"/>
            </svg>
          </button>
        </header>

        <div class="copilot-status">
          <span :class="['status-dot', status?.gateway.configured ? 'status-dot--ok' : 'status-dot--warn']"></span>
          <span>{{ statusLabel }}</span>
          <small v-if="status">{{ status.tool_count }} tools</small>
        </div>

        <section ref="messageListRef" class="message-list">
          <div v-if="messages.length === 0" class="empty-copy">
            <strong>平台助手待命</strong>
            <span>可以从状态、数据、任务、股票和回测开始。</span>
          </div>
          <article v-for="(message, index) in messages" :key="index" :class="['message', `message--${message.role}`]">
            <span class="message-role">{{ message.role === 'user' ? '我' : 'Copilot' }}</span>
            <p>{{ message.content }}</p>
            <details v-if="message.role === 'assistant' && message.trace" class="trace-panel" open>
              <summary>
                <span>可审计思路</span>
                <small>{{ traceSourceLabel(message.trace.source) }}</small>
              </summary>
              <div class="trace-note">{{ message.trace.note }}</div>
              <div class="trace-grid">
                <div v-for="node in message.trace.nodes" :key="`${node.name}-${node.status}-${node.detail || ''}`" class="trace-node">
                  <strong>{{ node.name }}</strong>
                  <span>{{ node.status }}</span>
                  <small v-if="node.detail">{{ node.detail }}</small>
                </div>
              </div>
              <div v-if="message.trace.tool_calls.length" class="trace-tools">
                <div v-for="tool in message.trace.tool_calls" :key="tool.tool_name" class="trace-tool">
                  <div>
                    <strong>{{ tool.tool_name }}</strong>
                    <span>{{ tool.reason }}</span>
                  </div>
                  <small>{{ tool.status }} · {{ formatTraceArgs(tool.arguments) }}</small>
                  <div v-if="tool.workflow?.nodes?.length" class="workflow-node-list">
                    <span class="workflow-node-list__title">
                      {{ tool.workflow.workflow_name || 'Workflow' }} · {{ tool.workflow.status || tool.status }}
                    </span>
                    <div
                      v-for="node in tool.workflow.nodes"
                      :key="`${tool.tool_name}-${node.name}-${node.status}`"
                      class="workflow-node"
                    >
                      <strong>{{ node.title || node.name }}</strong>
                      <small>{{ node.status }}<template v-if="node.detail"> · {{ node.detail }}</template></small>
                    </div>
                  </div>
                </div>
              </div>
            </details>
          </article>
        </section>

        <section v-if="actions.length > 0" class="action-list">
          <article v-for="action in actions" :key="action.tool_name" class="action-card">
            <div class="action-card__copy">
              <strong>{{ action.title }}</strong>
              <span>{{ action.description }}</span>
            </div>
            <div class="action-card__meta">
              <span :class="['risk-pill', `risk-pill--${action.risk_level}`]">{{ riskLabel(action.risk_level) }}</span>
              <button
                class="run-btn"
                type="button"
                :disabled="runningTool === action.tool_name"
                @click="runAction(action, confirmingTool === action.tool_name)"
              >
                {{ actionButtonLabel(action) }}
              </button>
            </div>
          </article>
        </section>

        <section v-if="lastResult" class="result-box">
          <div class="result-box__header">
            <strong>{{ lastResult.summary }}</strong>
            <button v-if="lastResult.result_ref" class="text-btn" type="button" @click="goResult(lastResult.result_ref)">打开</button>
          </div>
          <pre v-if="resultPreview">{{ resultPreview }}</pre>
        </section>

        <form class="composer" @submit.prevent="sendMessage">
          <textarea
            v-model="draft"
            rows="2"
            placeholder="问平台状态、数据覆盖、600519 快照..."
            :disabled="sending"
            @keydown.enter.exact.prevent="sendMessage"
          ></textarea>
          <button class="send-btn" type="submit" :disabled="sending || !draft.trim()">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M22 2L11 13" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M22 2l-7 20-4-9-9-4 20-7z" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </button>
        </form>
      </aside>
    </div>
  </teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { aiApi, type AIActionCard, type AIChatMessage, type AIStatus, type AITrace, type AIToolExecutionResponse } from '@/api/ai'

const props = defineProps<{
  visible: boolean
}>()

const emit = defineEmits<{
  close: []
}>()

const route = useRoute()
const router = useRouter()

type CopilotMessage = AIChatMessage & { trace?: AITrace | null }

const status = ref<AIStatus | null>(null)
const messages = ref<CopilotMessage[]>([])
const actions = ref<AIActionCard[]>([])
const draft = ref('')
const sending = ref(false)
const runningTool = ref('')
const confirmingTool = ref('')
const lastResult = ref<AIToolExecutionResponse | null>(null)
const messageListRef = ref<HTMLElement | null>(null)

const statusLabel = computed(() => {
  if (!status.value) return '检查中'
  if (!status.value.gateway.available) return 'LiteLLM 未安装'
  if (!status.value.gateway.configured) return `${status.value.gateway.model} 未配置`
  return `${status.value.gateway.model} 就绪`
})

const resultPreview = computed(() => {
  if (!lastResult.value?.result) return ''
  try {
    return JSON.stringify(lastResult.value.result, null, 2).slice(0, 1200)
  } catch {
    return String(lastResult.value.result).slice(0, 1200)
  }
})

watch(
  () => props.visible,
  async (visible) => {
    if (!visible) return
    await loadStatus()
    await nextTick()
    scrollToBottom()
  },
)

async function loadStatus() {
  try {
    status.value = await aiApi.status()
  } catch {
    status.value = null
  }
}

async function sendMessage() {
  const text = draft.value.trim()
  if (!text || sending.value) return
  const userMessage: AIChatMessage = { role: 'user', content: text }
  messages.value.push(userMessage)
  draft.value = ''
  sending.value = true
  actions.value = []
  lastResult.value = null
  try {
    const response = await aiApi.chat({
      messages: messages.value.map((message) => ({ role: message.role, content: message.content })),
      page_context: {
        path: route.path,
        name: String(route.name || ''),
        query: route.query,
      },
      auto_execute: true,
    })
    messages.value.push({ ...response.message, trace: response.trace })
    actions.value = response.actions
  } catch (error) {
    messages.value.push({
      role: 'assistant',
      content: error instanceof Error ? error.message : '请求失败',
    })
  } finally {
    sending.value = false
    await nextTick()
    scrollToBottom()
  }
}

async function runAction(action: AIActionCard, confirmed: boolean) {
  if (action.requires_confirmation && !confirmed) {
    confirmingTool.value = action.tool_name
    messages.value.push({ role: 'assistant', content: `${action.title} 需要确认。` })
    await nextTick()
    scrollToBottom()
    return
  }
  runningTool.value = action.tool_name
  confirmingTool.value = ''
  try {
    const response = await aiApi.executeTool(action.tool_name, {
      arguments: action.arguments,
      confirmed: action.requires_confirmation ? true : confirmed,
    })
    lastResult.value = response
    messages.value.push({ role: 'assistant', content: response.summary })
  } catch (error) {
    messages.value.push({
      role: 'assistant',
      content: error instanceof Error ? error.message : '工具执行失败',
    })
  } finally {
    runningTool.value = ''
    await nextTick()
    scrollToBottom()
  }
}

function actionButtonLabel(action: AIActionCard) {
  if (runningTool.value === action.tool_name) return '执行中'
  if (action.requires_confirmation && confirmingTool.value === action.tool_name) return '确认执行'
  return action.requires_confirmation ? '确认' : '执行'
}

function riskLabel(risk: AIActionCard['risk_level']) {
  if (risk === 'danger') return '高风险'
  if (risk === 'write') return '写入'
  return '只读'
}

function traceSourceLabel(source: string) {
  if (source === 'llm') return 'LLM 路由'
  if (source === 'llm+fallback') return 'LLM + 兜底'
  return '本地兜底'
}

function formatTraceArgs(args: Record<string, unknown>) {
  try {
    const text = JSON.stringify(args)
    return text.length > 90 ? `${text.slice(0, 90)}...` : text
  } catch {
    return '{}'
  }
}

function goResult(resultRef: string) {
  router.push(resultRef)
  emit('close')
}

function scrollToBottom() {
  if (messageListRef.value) {
    messageListRef.value.scrollTop = messageListRef.value.scrollHeight
  }
}
</script>

<style scoped>
.copilot-layer {
  position: fixed;
  inset: 0;
  z-index: 2000;
  display: flex;
  justify-content: flex-end;
  background: rgba(6, 8, 12, 0.24);
}

.copilot-drawer {
  width: min(440px, calc(100vw - 24px));
  height: 100vh;
  display: grid;
  grid-template-rows: auto auto minmax(0, 1fr) auto auto;
  gap: 12px;
  padding: 18px;
  background: var(--bg-surface);
  border-left: 1px solid var(--border-subtle);
  box-shadow: -18px 0 48px rgba(0, 0, 0, 0.28);
}

.copilot-header,
.result-box__header,
.action-card__meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.copilot-kicker {
  display: block;
  color: var(--text-muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0;
  margin-bottom: 4px;
}

.copilot-header h2 {
  margin: 0;
  color: var(--text-bright);
  font-size: 22px;
}

.icon-btn,
.send-btn {
  width: 38px;
  height: 38px;
  display: inline-grid;
  place-items: center;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--bg-elevated);
  color: var(--text-primary);
  cursor: pointer;
}

.icon-btn svg,
.send-btn svg {
  width: 18px;
  height: 18px;
}

.copilot-status {
  min-height: 34px;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  color: var(--text-secondary);
  font-size: 13px;
}

.copilot-status small {
  margin-left: auto;
  color: var(--text-muted);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: var(--color-warning);
}

.status-dot--ok {
  background: var(--status-ready);
}

.status-dot--warn {
  background: var(--color-warning);
}

.message-list {
  min-height: 0;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding-right: 2px;
}

.empty-copy {
  display: grid;
  gap: 6px;
  padding: 18px;
  border: 1px dashed var(--border-subtle);
  border-radius: 8px;
  color: var(--text-secondary);
}

.empty-copy strong {
  color: var(--text-primary);
}

.message {
  display: grid;
  gap: 4px;
  max-width: 92%;
}

.message--user {
  align-self: flex-end;
}

.message-role {
  font-size: 11px;
  color: var(--text-muted);
}

.message p {
  margin: 0;
  padding: 10px 12px;
  border-radius: 8px;
  background: var(--bg-elevated);
  color: var(--text-primary);
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-word;
}

.message--user p {
  background: rgba(56, 189, 248, 0.14);
}

.trace-panel {
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: rgba(15, 23, 42, 0.28);
  color: var(--text-secondary);
  font-size: 12px;
}

.trace-panel summary {
  min-height: 32px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 0 10px;
  cursor: pointer;
  color: var(--text-primary);
}

.trace-panel summary small {
  color: var(--text-muted);
}

.trace-note {
  padding: 0 10px 8px;
  color: var(--text-muted);
  line-height: 1.4;
}

.trace-grid,
.trace-tools {
  display: grid;
  gap: 6px;
  padding: 0 10px 10px;
}

.trace-node,
.trace-tool {
  display: grid;
  gap: 3px;
  padding: 8px;
  border-radius: 8px;
  background: var(--bg-elevated);
}

.trace-node {
  grid-template-columns: auto auto minmax(0, 1fr);
  align-items: center;
}

.trace-node strong,
.trace-tool strong {
  color: var(--text-primary);
  font-size: 12px;
}

.trace-node span {
  color: var(--status-ready);
}

.trace-node small,
.trace-tool small,
.trace-tool span {
  color: var(--text-muted);
  line-height: 1.35;
  word-break: break-word;
}

.trace-tool div {
  display: grid;
  gap: 3px;
}

.workflow-node-list {
  display: grid;
  gap: 5px;
  margin-top: 4px;
  padding-top: 6px;
  border-top: 1px solid var(--border-subtle);
}

.workflow-node-list__title {
  color: var(--text-secondary);
  font-size: 11px;
  font-weight: 800;
}

.workflow-node {
  display: grid;
  grid-template-columns: minmax(76px, 0.55fr) minmax(0, 1fr);
  gap: 8px;
  align-items: start;
}

.workflow-node strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: 11px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.workflow-node small {
  min-width: 0;
  white-space: normal;
}

.action-list {
  display: grid;
  gap: 8px;
  max-height: 220px;
  overflow-y: auto;
}

.action-card,
.result-box {
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.action-card {
  display: grid;
  gap: 10px;
  padding: 12px;
}

.action-card__copy {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.action-card__copy strong {
  color: var(--text-primary);
  font-size: 14px;
}

.action-card__copy span {
  color: var(--text-secondary);
  font-size: 12px;
  line-height: 1.4;
}

.risk-pill {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 0 8px;
  border-radius: 999px;
  color: var(--text-secondary);
  background: var(--bg-muted);
  font-size: 12px;
}

.risk-pill--write {
  color: var(--color-warning);
}

.risk-pill--danger {
  color: var(--status-attention);
}

.run-btn,
.text-btn {
  min-height: 28px;
  padding: 0 12px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--bg-surface);
  color: var(--text-primary);
  cursor: pointer;
}

.run-btn:disabled,
.send-btn:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.result-box {
  display: grid;
  gap: 8px;
  padding: 12px;
  max-height: 180px;
  overflow: auto;
}

.result-box strong {
  color: var(--text-primary);
  font-size: 13px;
}

.result-box pre {
  margin: 0;
  color: var(--text-secondary);
  font-size: 11px;
  line-height: 1.45;
  white-space: pre-wrap;
  word-break: break-word;
}

.composer {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 42px;
  gap: 10px;
  align-items: end;
}

.composer textarea {
  width: 100%;
  min-height: 42px;
  max-height: 120px;
  resize: vertical;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--bg-elevated);
  color: var(--text-primary);
  padding: 10px 12px;
  line-height: 1.45;
  font: inherit;
}
</style>
