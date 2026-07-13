<template>
  <transition name="copilot">
    <aside v-if="open" class="copilot" aria-label="AI Copilot">
      <header class="copilot__header">
        <div>
          <strong>Gaoshou Copilot</strong>
          <span>{{ statusText }}</span>
        </div>
        <el-button :icon="Close" circle text title="关闭" @click="$emit('close')" />
      </header>

      <div class="copilot__toolbar">
        <el-select v-model="conversationId" placeholder="选择会话" @change="loadConversation">
          <el-option v-for="item in conversations" :key="item.id" :label="item.title" :value="item.id" />
        </el-select>
        <el-button :icon="Plus" circle title="新会话" @click="createConversation" />
      </div>

      <div ref="messagePane" class="copilot__messages">
        <div v-if="status && !status.configured" class="copilot__blocked">
          {{ status.reason }}
        </div>
        <article v-for="(message, index) in messages" :key="index" :class="['copilot-message', `copilot-message--${message.role}`]">
          <span>{{ message.role === 'user' ? '我' : 'AI' }}</span>
          <p>{{ message.content }}</p>
          <div v-for="approval in message.approvals || []" :key="approval.approval_id" class="approval">
            <strong>{{ approval.tool }}</strong>
            <pre>{{ JSON.stringify(approval.arguments, null, 2) }}</pre>
            <div v-if="!approval.status || approval.status === 'pending'">
              <el-button size="small" @click="rejectApproval(approval)">拒绝</el-button>
              <el-button type="primary" size="small" @click="confirmApproval(approval)">确认执行</el-button>
            </div>
            <span v-else class="approval__status">{{ approvalStatusText(approval.status) }}</span>
          </div>
        </article>
        <div v-if="sending" class="copilot__thinking">正在分析...</div>
      </div>

      <footer class="copilot__composer">
        <el-input
          v-model="draft"
          type="textarea"
          :rows="3"
          resize="none"
          placeholder="询问股票、因子、回测或舆情"
          :disabled="!status?.configured || sending"
          @keydown.ctrl.enter.prevent="send"
        />
        <el-button type="primary" :icon="Promotion" :loading="sending" :disabled="!draft.trim()" @click="send">
          发送
        </el-button>
      </footer>
    </aside>
  </transition>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { Close, Plus, Promotion } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { aiApi, type AIApproval, type AIConversation, type AIMessage, type AIStatus } from '@/api/ai'

const props = defineProps<{ open: boolean }>()
defineEmits<{ close: [] }>()
const route = useRoute()
const status = ref<AIStatus | null>(null)
const conversations = ref<AIConversation[]>([])
const conversationId = ref('')
const messages = ref<AIMessage[]>([])
const draft = ref('')
const sending = ref(false)
const messagePane = ref<HTMLElement | null>(null)

const statusText = computed(() => status.value?.configured ? status.value.model || '已配置' : '未配置')
const pageContext = computed(() => ({ route: route.path, params: route.params, query: route.query }))

async function initialize() {
  status.value = await aiApi.status()
  conversations.value = await aiApi.conversations()
  if (conversations.value.length) {
    conversationId.value = conversations.value[0].id
    await loadConversation()
  } else if (status.value.configured) {
    await createConversation()
  }
}

async function createConversation() {
  const row = await aiApi.createConversation(`研究会话 ${new Date().toLocaleDateString()}`, pageContext.value)
  conversations.value.unshift(row)
  conversationId.value = row.id
  messages.value = []
}

async function loadConversation() {
  if (!conversationId.value) return
  const row = await aiApi.conversation(conversationId.value)
  messages.value = row.messages || []
  await scrollBottom()
}

async function send() {
  const content = draft.value.trim()
  if (!content || sending.value) return
  if (!conversationId.value) await createConversation()
  messages.value.push({ role: 'user', content })
  draft.value = ''
  sending.value = true
  await scrollBottom()
  try {
    const response = await aiApi.chat(conversationId.value, content, pageContext.value)
    messages.value.push(response.message)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : 'AI 请求失败')
  } finally {
    sending.value = false
    await scrollBottom()
  }
}

async function confirmApproval(approval: AIApproval) {
  await aiApi.confirm(approval.approval_id)
  ElMessage.success('操作已执行')
  await loadConversation()
}

async function rejectApproval(approval: AIApproval) {
  await aiApi.reject(approval.approval_id)
  ElMessage.info('操作已拒绝')
  await loadConversation()
}

function approvalStatusText(status: AIApproval['status']) {
  return ({ running: '执行中', completed: '已执行', rejected: '已拒绝', failed: '执行失败' } as const)[status as 'running' | 'completed' | 'rejected' | 'failed'] || ''
}

async function scrollBottom() {
  await nextTick()
  if (messagePane.value) messagePane.value.scrollTop = messagePane.value.scrollHeight
}

watch(() => props.open, value => {
  if (value && !status.value) void initialize()
})
</script>

<style scoped>
.copilot { position: fixed; z-index: 1200; top: 0; right: 0; bottom: 0; width: min(440px, 100vw); display: grid; grid-template-rows: auto auto minmax(0, 1fr) auto; background: var(--bg-elevated); border-left: 1px solid var(--border-subtle); box-shadow: -16px 0 36px rgba(20, 28, 24, .16); }
.copilot__header, .copilot__toolbar, .copilot__composer { padding: 14px 16px; border-bottom: 1px solid var(--border-subtle); }
.copilot__header { display: flex; justify-content: space-between; align-items: center; }
.copilot__header div { display: grid; gap: 3px; }
.copilot__header span { color: var(--text-muted); font-size: 12px; }
.copilot__toolbar { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; }
.copilot__messages { overflow: auto; padding: 16px; }
.copilot-message { margin-bottom: 16px; }
.copilot-message > span { display: block; margin-bottom: 5px; color: var(--text-muted); font-size: 11px; }
.copilot-message p { margin: 0; padding: 10px 12px; white-space: pre-wrap; line-height: 1.55; background: var(--bg-surface); border-left: 3px solid var(--accent-primary); }
.copilot-message--user p { border-left-color: var(--accent-warning); }
.approval { margin-top: 8px; padding: 10px; border: 1px solid var(--border-strong); background: var(--bg-surface); }
.approval pre { max-height: 140px; overflow: auto; font-size: 11px; white-space: pre-wrap; }
.approval > div { display: flex; justify-content: flex-end; gap: 8px; }
.approval__status { display: block; color: var(--text-muted); font-size: 12px; text-align: right; }
.copilot__composer { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; border-top: 1px solid var(--border-subtle); border-bottom: 0; }
.copilot__blocked { padding: 12px; color: var(--status-warning); background: var(--bg-surface); }
.copilot__thinking { color: var(--text-muted); font-size: 13px; }
.copilot-enter-active, .copilot-leave-active { transition: transform .2s ease; }
.copilot-enter-from, .copilot-leave-to { transform: translateX(100%); }
</style>
