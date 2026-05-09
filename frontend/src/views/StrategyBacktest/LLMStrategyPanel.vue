<template>
  <div class="llm-strategy-panel">
    <el-tabs v-model="activeTab" class="llm-tabs">
      <!-- Tab 1: 代码转换 -->
      <el-tab-pane label="代码转换" name="convert">
        <div class="llm-section">
          <p class="llm-desc">将任意量化策略代码（RQAlpha / Backtrader / VNPY）转换为 AKQuant 可运行格式</p>
          <el-input v-model="convertInput" type="textarea" :rows="10"
            placeholder="粘贴任意格式的策略代码..."
            class="llm-textarea" />
          <el-button type="primary" :loading="converting" @click="handleConvert" style="margin-top:12px">
            {{ converting ? '转换中...' : '转换为 AKQuant 代码' }}
          </el-button>
          <div v-if="convertResult" class="llm-result">
            <div class="llm-result-header">
              <span>转换结果</span>
              <el-button size="small" text @click="emit('code-generated', convertResult)">应用到编辑器</el-button>
            </div>
            <pre class="llm-code">{{ convertResult }}</pre>
          </div>
          <div v-if="convertError" class="llm-error">{{ convertError }}</div>
        </div>
      </el-tab-pane>

      <!-- Tab 2: 研报对话生成 -->
      <el-tab-pane label="研报生成" name="report">
        <div class="llm-section">
          <p class="llm-desc">上传研报，通过多轮对话确认策略逻辑，生成可运行的 AKQuant 代码</p>

          <!-- Step 1: Upload -->
          <div v-if="!chatSessionId" class="upload-step">
            <el-upload drag accept=".pdf,.txt,.md" :auto-upload="false"
              :on-change="handleFileChange" :limit="1" :file-list="fileList">
              <el-icon class="el-icon--upload"><UploadFilled /></el-icon>
              <div class="el-upload__text">拖拽或点击上传研报</div>
              <template #tip><div class="el-upload__tip">PDF / TXT / Markdown</div></template>
            </el-upload>
            <el-button type="primary" :loading="creating" @click="handleCreateSession"
              :disabled="!uploadFile" style="margin-top:12px">
              {{ creating ? '分析中...' : '开始分析研报' }}
            </el-button>
          </div>

          <!-- Step 2: Chat -->
          <div v-else class="chat-step">
            <div class="chat-header">
              <span>会话: {{ chatSessionId }}</span>
              <el-button size="small" text @click="resetChat">新对话</el-button>
            </div>
            <div class="chat-messages" ref="chatRef">
              <div v-for="(msg, i) in chatMessages" :key="i" :class="['chat-msg', msg.role]">
                <div class="chat-role">{{ msg.role === 'user' ? '我' : 'AI' }}</div>
                <div class="chat-text">{{ msg.content }}</div>
              </div>
              <div v-if="chatting" class="chat-msg assistant">
                <div class="chat-role">AI</div>
                <div class="chat-text typing">思考中...</div>
              </div>
            </div>
            <div class="chat-input-row">
              <el-input v-model="chatInput" placeholder="输入确认/修改意见..."
                @keyup.enter="handleChatSend" :disabled="chatting" />
              <el-button type="primary" @click="handleChatSend" :loading="chatting">发送</el-button>
            </div>
            <div v-if="generatedCode" class="llm-result" style="margin-top:12px">
              <div class="llm-result-header">
                <span>生成的策略代码</span>
                <el-button size="small" text @click="emit('code-generated', generatedCode)">应用到编辑器</el-button>
              </div>
              <pre class="llm-code">{{ generatedCode }}</pre>
            </div>
          </div>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, nextTick } from 'vue'
import { UploadFilled } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'

defineProps<{ engine: string }>()
const emit = defineEmits<{ 'code-generated': [code: string] }>()

// ── Tab ──
const activeTab = ref('convert')

// ── 代码转换 ──
const convertInput = ref('')
const convertResult = ref('')
const convertError = ref('')
const converting = ref(false)

const handleConvert = async () => {
  if (!convertInput.value.trim()) { ElMessage.warning('请先输入代码'); return }
  converting.value = true; convertError.value = ''; convertResult.value = ''
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>('/strategy/convert-to-akquant', {
      source_code: convertInput.value,
    }, { timeout: 120000 })
    convertResult.value = res?.code || res?.data?.code || ''
  } catch (e: any) {
    convertError.value = '转换失败: ' + (e?.message || '未知错误')
  } finally { converting.value = false }
}

// ── 研报对话 ──
const chatSessionId = ref('')
const uploadFile = ref<File | null>(null)
const fileList = ref<any[]>([])
const creating = ref(false)
const chatMessages = ref<{ role: string; content: string }[]>([])
const chatInput = ref('')
const chatting = ref(false)
const generatedCode = ref('')
const chatRef = ref<HTMLElement | null>(null)

const handleFileChange = (file: any) => {
  uploadFile.value = file?.raw || file
}

const handleCreateSession = async () => {
  if (!uploadFile.value) { ElMessage.warning('请先上传研报'); return }
  creating.value = true
  try {
    const { default: request } = await import('@/api/request')
    const form = new FormData()
    form.append('file', uploadFile.value)
    const res = await request.post<any>('/strategy/chat-session', form, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 120000,
    })
    chatSessionId.value = res?.session_id || res?.data?.session_id || ''
    const reply = res?.reply || res?.data?.reply || '分析完成'
    const code = res?.code || res?.data?.code || ''
    chatMessages.value = [{ role: 'assistant', content: reply }]
    if (code) generatedCode.value = code
    await nextTick(); scrollChat()
  } catch (e: any) {
    ElMessage.error('创建会话失败: ' + (e?.message || '未知错误'))
  } finally { creating.value = false }
}

const handleChatSend = async () => {
  const msg = chatInput.value.trim()
  if (!msg) return
  chatMessages.value.push({ role: 'user', content: msg })
  chatInput.value = ''
  chatting.value = true
  await nextTick(); scrollChat()
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>(`/strategy/chat-session/${chatSessionId.value}/send`, {
      message: msg,
    }, { timeout: 120000 })
    const reply = res?.reply || res?.data?.reply || ''
    chatMessages.value.push({ role: 'assistant', content: reply })
    const code = res?.code || res?.data?.code || ''
    if (code) generatedCode.value = code
    await nextTick(); scrollChat()
  } catch (e: any) {
    chatMessages.value.push({ role: 'assistant', content: '错误: ' + (e?.message || '未知') })
  } finally { chatting.value = false }
}

const scrollChat = () => {
  if (chatRef.value) chatRef.value.scrollTop = chatRef.value.scrollHeight
}

const resetChat = () => {
  chatSessionId.value = ''
  chatMessages.value = []
  generatedCode.value = ''
  uploadFile.value = null
  fileList.value = []
}
</script>

<style scoped>
.llm-strategy-panel { height: 100%; display: flex; flex-direction: column; }
.llm-tabs { flex: 1; display: flex; flex-direction: column; }
.llm-tabs :deep(.el-tabs__content) { flex: 1; overflow-y: auto; }
.llm-section { padding: 8px 0; }
.llm-desc { color: #999; font-size: 13px; margin-bottom: 12px; }
.llm-textarea { font-family: 'Courier New', monospace; font-size: 13px; }
.llm-result { margin-top: 12px; border: 1px solid #333; border-radius: 6px; overflow: hidden; }
.llm-result-header { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: #1a1a2e; }
.llm-code { padding: 12px; margin: 0; background: #0d1117; color: #c9d1d9; font-size: 12px; max-height: 300px; overflow: auto; white-space: pre-wrap; }
.llm-error { margin-top: 8px; color: #f56c6c; font-size: 13px; }
.upload-step { text-align: center; }
.chat-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; font-size: 12px; color: #888; }
.chat-messages { max-height: 320px; overflow-y: auto; padding: 8px; background: #1a1a2e; border-radius: 6px; margin-bottom: 8px; }
.chat-msg { margin-bottom: 10px; }
.chat-msg.user .chat-text { background: #1a3a5c; }
.chat-msg.assistant .chat-text { background: #2d1a3a; }
.chat-role { font-size: 11px; color: #888; margin-bottom: 2px; }
.chat-text { padding: 8px 12px; border-radius: 8px; font-size: 13px; white-space: pre-wrap; }
.chat-text.typing { color: #888; font-style: italic; }
.chat-input-row { display: flex; gap: 8px; }
</style>
