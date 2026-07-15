<template>
  <el-dialog
    :model-value="modelValue"
    title="AI Gateway endpoints"
    width="min(920px, calc(100vw - 32px))"
    class="llm-endpoint-dialog"
    destroy-on-close
    @open="loadEndpoints"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div class="endpoint-toolbar">
      <div>
        <strong>Provider routing</strong>
        <small>Enabled endpoints are attempted in priority order.</small>
      </div>
      <div class="endpoint-toolbar__actions">
        <el-button :icon="Refresh" :loading="loading" @click="loadEndpoints">Reload</el-button>
        <el-button type="primary" :icon="CirclePlus" :disabled="mutationsDisabled" @click="openCreate">Add endpoint</el-button>
      </div>
    </div>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <el-alert
      v-if="loadState === 'error' && endpoints.length"
      title="Showing stale endpoint data. Editing is disabled until reload succeeds."
      type="warning"
      show-icon
      :closable="false"
    />
    <div v-loading="loading" class="endpoint-list" aria-live="polite">
      <el-empty v-if="!loading && !error && !endpoints.length" description="No AI endpoints configured" />
      <article
        v-for="(endpoint, index) in endpoints"
        :key="endpoint.id"
        class="endpoint-row"
        :class="{ 'endpoint-row--stale': mutationsDisabled }"
      >
        <div class="endpoint-priority">{{ index + 1 }}</div>
        <div class="endpoint-main">
          <div class="endpoint-title">
            <strong>{{ endpoint.name }}</strong>
            <el-tag :type="endpoint.enabled ? 'success' : 'info'" effect="plain" size="small">
              {{ endpoint.enabled ? 'enabled' : 'disabled' }}
            </el-tag>
            <el-tag v-if="isLlmEndpointCooldownActive(endpoint)" type="warning" effect="plain" size="small">cooldown</el-tag>
          </div>
          <span>{{ endpoint.model }}</span>
          <small>{{ endpoint.api_base }} · key {{ endpoint.api_key_hint }}</small>
          <small v-if="endpoint.last_error" class="endpoint-error">{{ endpoint.last_error }}</small>
        </div>
        <div class="endpoint-health">
          <span>{{ healthLabel(endpoint) }}</span>
          <small>{{ healthTime(endpoint) }}</small>
        </div>
        <div class="endpoint-actions">
          <el-tooltip content="Enable or disable">
            <el-switch
              :model-value="endpoint.enabled"
              :aria-label="`${endpoint.enabled ? 'Disable' : 'Enable'} ${endpoint.name}`"
              :loading="busyId === endpoint.id && busyAction === 'toggle'"
              :disabled="mutationsDisabled"
              @change="toggleEndpoint(endpoint, Boolean($event))"
            />
          </el-tooltip>
          <el-tooltip content="Move up">
            <el-button :icon="ArrowUp" :aria-label="`Move ${endpoint.name} up`" circle :disabled="index === 0 || mutationsDisabled" @click="move(endpoint.id, -1)" />
          </el-tooltip>
          <el-tooltip content="Move down">
            <el-button :icon="ArrowDown" :aria-label="`Move ${endpoint.name} down`" circle :disabled="index === endpoints.length - 1 || mutationsDisabled" @click="move(endpoint.id, 1)" />
          </el-tooltip>
          <el-tooltip content="Test connection">
            <el-button
              :icon="Connection"
              :aria-label="`Test ${endpoint.name}`"
              circle
              :loading="busyId === endpoint.id && busyAction === 'test'"
              :disabled="mutationsDisabled"
              @click="testEndpoint(endpoint)"
            />
          </el-tooltip>
          <el-tooltip content="Edit endpoint">
            <el-button :icon="Edit" :aria-label="`Edit ${endpoint.name}`" circle :disabled="mutationsDisabled" @click="openEdit(endpoint)" />
          </el-tooltip>
          <el-tooltip content="Delete endpoint">
            <el-button :icon="Delete" :aria-label="`Delete ${endpoint.name}`" circle type="danger" plain :disabled="mutationsDisabled" @click="remove(endpoint)" />
          </el-tooltip>
        </div>
      </article>
    </div>

    <el-dialog
      v-model="editorOpen"
      :title="editing ? 'Edit endpoint' : 'Add endpoint'"
      width="min(820px, calc(100vw - 32px))"
      class="llm-json-editor-dialog"
      append-to-body
      destroy-on-close
    >
      <el-alert v-if="editorError" :title="editorError" type="error" show-icon :closable="false" />
      <div class="json-editor-layout">
        <section class="json-editor-main" aria-label="Endpoint JSON configuration">
          <div class="json-editor-heading">
            <div>
              <strong>Configuration JSON</strong>
              <small>The API key is encrypted at rest and is never shown after saving.</small>
            </div>
            <el-button :icon="MagicStick" :disabled="saving" @click="formatConfig">Format JSON</el-button>
          </div>
          <CodeEditor v-model="configText" language="json" :readonly="saving" :min-height="360" />
          <small v-if="editing" class="key-hint">Stored key: {{ editing.api_key_hint }}. Keep {{ LLM_API_KEY_PLACEHOLDER }} to preserve it.</small>
        </section>

        <aside class="config-preview" aria-live="polite" aria-label="Extracted endpoint preview">
          <strong>Extracted preview</strong>
          <template v-if="preview">
            <dl>
              <dt>Provider</dt><dd>{{ preview.provider || 'Not set' }}</dd>
              <dt>Name</dt><dd>{{ preview.name || 'Not set' }}</dd>
              <dt>Model</dt><dd>{{ preview.model || 'Not set' }}</dd>
              <dt>Review model</dt><dd>{{ preview.reviewModel || 'None' }}</dd>
              <dt>API base</dt><dd>{{ preview.apiBase || 'Not set' }}</dd>
              <dt>Wire API</dt><dd>{{ preview.wireApi }}</dd>
              <dt>Reasoning</dt><dd>{{ preview.reasoningEffort || 'Default' }}</dd>
              <dt>Storage</dt><dd>{{ preview.disableResponseStorage ? 'Disabled' : 'Enabled' }}</dd>
              <dt>OpenAI auth</dt><dd>{{ preview.requiresOpenaiAuth ? 'Required' : 'Not required' }}</dd>
            </dl>
          </template>
          <small v-else>Preview appears when the JSON is valid.</small>
          <el-alert
            v-for="warning in preservedWarnings"
            :key="warning"
            :title="warning"
            type="warning"
            :closable="false"
          />
          <div class="enabled-control">
            <span>Enabled</span>
            <el-switch v-model="editorEnabled" aria-label="Enable endpoint" />
          </div>
        </aside>
      </div>
      <template #footer>
        <el-button :disabled="saving" @click="editorOpen = false">Cancel</el-button>
        <el-button type="primary" :loading="saving" :disabled="mutationsDisabled" @click="save">Save endpoint</el-button>
      </template>
    </el-dialog>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { ArrowDown, ArrowUp, CirclePlus, Connection, Delete, Edit, MagicStick, Refresh } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import CodeEditor from '@/components/CodeEditor.vue'
import {
  LLM_API_KEY_PLACEHOLDER,
  buildLlmEndpointCreate,
  buildLlmEndpointUpdate,
  createLlmEndpointTemplate,
  formatLlmEndpointConfig,
  getLlmEndpointConfigWarnings,
  isLlmEndpointCooldownActive,
  moveLlmEndpointIds,
  parseLlmEndpointConfig,
  previewLlmEndpointConfig,
  sanitizedLlmEndpointConfig,
  systemApi,
  type LlmEndpoint,
} from '@/api/system'

defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  changed: [endpoints: LlmEndpoint[]]
}>()

const endpoints = ref<LlmEndpoint[]>([])
const loading = ref(false)
const loadState = ref<'loading' | 'ready' | 'error'>('loading')
const error = ref('')
const editorOpen = ref(false)
const editorError = ref('')
const editing = ref<LlmEndpoint | null>(null)
const saving = ref(false)
const busyId = ref('')
const busyAction = ref('')
const configText = ref('')
const editorEnabled = ref(true)
const mutationsDisabled = computed(() => loading.value || loadState.value !== 'ready' || Boolean(busyId.value))
const parsedConfig = computed(() => {
  try { return parseLlmEndpointConfig(configText.value).config } catch { return null }
})
const preview = computed(() => parsedConfig.value ? previewLlmEndpointConfig(parsedConfig.value) : null)
const preservedWarnings = computed(() => editing.value ? getLlmEndpointConfigWarnings(editing.value) : [])

function errorDetail(reason: unknown): string {
  if (reason && typeof reason === 'object' && 'response' in reason) {
    const response = (reason as { response?: { data?: { detail?: unknown } } }).response
    if (typeof response?.data?.detail === 'string') return response.data.detail
  }
  return reason instanceof Error ? reason.message : String(reason)
}

async function loadEndpoints(): Promise<boolean> {
  loading.value = true
  error.value = ''
  try {
    endpoints.value = await systemApi.listLlmEndpoints()
    loadState.value = 'ready'
    return true
  } catch (reason) {
    loadState.value = 'error'
    error.value = `Failed to load endpoints: ${errorDetail(reason)}`
    return false
  } finally {
    loading.value = false
  }
}

function openCreate() {
  if (mutationsDisabled.value) return
  editing.value = null
  configText.value = createLlmEndpointTemplate()
  editorEnabled.value = true
  editorError.value = ''
  editorOpen.value = true
}

function openEdit(endpoint: LlmEndpoint) {
  if (mutationsDisabled.value) return
  editing.value = endpoint
  configText.value = sanitizedLlmEndpointConfig(endpoint)
  editorEnabled.value = endpoint.enabled
  editorError.value = ''
  editorOpen.value = true
}

async function save() {
  if (mutationsDisabled.value) return
  editorError.value = ''
  let config
  try {
    config = parseLlmEndpointConfig(configText.value).config
  } catch (reason) {
    editorError.value = errorDetail(reason)
    return
  }
  saving.value = true
  try {
    if (editing.value) {
      await systemApi.updateLlmEndpoint(editing.value.id, buildLlmEndpointUpdate(config, editorEnabled.value))
    } else {
      await systemApi.createLlmEndpoint(buildLlmEndpointCreate(config, editorEnabled.value))
    }
    editorOpen.value = false
    ElMessage.success(editing.value ? 'Endpoint updated' : 'Endpoint added')
    await refreshAfterMutation()
  } catch (reason) {
    editorError.value = errorDetail(reason)
  } finally {
    saving.value = false
  }
}

function formatConfig() {
  editorError.value = ''
  try {
    configText.value = formatLlmEndpointConfig(configText.value)
  } catch (reason) {
    editorError.value = errorDetail(reason)
  }
}

async function refreshAfterMutation() {
  if (await loadEndpoints()) emit('changed', endpoints.value)
}

async function runMutation(endpointId: string, action: string, operation: () => Promise<unknown>) {
  if (mutationsDisabled.value) return
  busyId.value = endpointId
  busyAction.value = action
  error.value = ''
  try {
    await operation()
    await refreshAfterMutation()
  } catch (reason) {
    error.value = errorDetail(reason)
  } finally {
    busyId.value = ''
    busyAction.value = ''
  }
}

async function toggleEndpoint(endpoint: LlmEndpoint, enabled: boolean) {
  await runMutation(endpoint.id, 'toggle', () => systemApi.updateLlmEndpoint(endpoint.id, { enabled }))
}

async function move(endpointId: string, offset: -1 | 1) {
  const endpointIds = moveLlmEndpointIds(endpoints.value.map(endpoint => endpoint.id), endpointId, offset)
  await runMutation(endpointId, 'move', () => systemApi.reorderLlmEndpoints(endpointIds))
}

async function testEndpoint(endpoint: LlmEndpoint) {
  if (mutationsDisabled.value) return
  busyId.value = endpoint.id
  busyAction.value = 'test'
  error.value = ''
  try {
    const result = await systemApi.testLlmEndpoint(endpoint.id)
    if (result.status === 'ok') ElMessage.success(`Connection ready in ${result.latency_ms} ms`)
    else error.value = result.error || 'Connection test failed.'
    await refreshAfterMutation()
  } catch (reason) {
    error.value = errorDetail(reason)
  } finally {
    busyId.value = ''
    busyAction.value = ''
  }
}

async function remove(endpoint: LlmEndpoint) {
  if (mutationsDisabled.value) return
  const confirmed = await ElMessageBox.confirm(
    `Delete endpoint “${endpoint.name}”? This cannot be undone.`,
    'Delete endpoint',
    { confirmButtonText: 'Delete', cancelButtonText: 'Cancel', type: 'warning' },
  ).catch(() => false)
  if (!confirmed) return
  await runMutation(endpoint.id, 'delete', () => systemApi.deleteLlmEndpoint(endpoint.id))
}

function healthLabel(endpoint: LlmEndpoint): string {
  if (!endpoint.enabled) return 'Disabled'
  if (isLlmEndpointCooldownActive(endpoint)) return 'Cooling down'
  if (endpoint.consecutive_failures > 0) return `${endpoint.consecutive_failures} failures`
  return endpoint.last_success_at ? 'Healthy' : 'Not tested'
}

function healthTime(endpoint: LlmEndpoint): string {
  const value = endpoint.last_failure_at || endpoint.last_success_at
  return value ? new Date(value).toLocaleString() : 'No result'
}
</script>

<style scoped>
.endpoint-toolbar,
.endpoint-row,
.endpoint-title,
.endpoint-actions {
  display: flex;
  align-items: center;
}

.endpoint-toolbar {
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 16px;
}

.endpoint-toolbar div,
.endpoint-main {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.endpoint-toolbar .endpoint-toolbar__actions {
  display: flex;
  grid-auto-flow: column;
  gap: 8px;
}

.endpoint-toolbar small,
.endpoint-main small,
.endpoint-health small,
.el-form-item small {
  color: var(--text-muted);
}

.endpoint-list {
  min-height: 180px;
}

.endpoint-row {
  display: grid;
  grid-template-columns: 32px minmax(220px, 1fr) 120px auto;
  gap: 14px;
  padding: 14px 0;
  border-bottom: 1px solid var(--border-subtle);
}

.endpoint-row--stale {
  opacity: 0.68;
}

.endpoint-priority {
  color: var(--text-muted);
  font-variant-numeric: tabular-nums;
}

.endpoint-title,
.endpoint-actions {
  gap: 8px;
}

.endpoint-main > span,
.endpoint-main > small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.endpoint-error {
  color: var(--accent-danger) !important;
}

.endpoint-health {
  display: grid;
  gap: 4px;
}

.json-editor-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 220px;
  gap: 16px;
  min-height: 470px;
}

.json-editor-main,
.config-preview,
.json-editor-heading > div {
  display: grid;
  align-content: start;
  gap: 10px;
  min-width: 0;
}

.json-editor-heading,
.enabled-control {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.json-editor-heading small,
.key-hint,
.config-preview > small {
  color: var(--text-muted);
}

.config-preview {
  padding-left: 16px;
  border-left: 1px solid var(--border-subtle);
}

.config-preview dl {
  display: grid;
  grid-template-columns: 82px minmax(0, 1fr);
  gap: 7px 8px;
  margin: 0;
  font-size: 12px;
}

.config-preview dt {
  color: var(--text-muted);
}

.config-preview dd {
  min-width: 0;
  margin: 0;
  overflow-wrap: anywhere;
}

.enabled-control {
  margin-top: 6px;
  padding-top: 12px;
  border-top: 1px solid var(--border-subtle);
}

:global(.llm-json-editor-dialog .el-dialog__body) {
  min-height: 500px;
  max-height: calc(100vh - 180px);
  overflow: auto;
}

@media (max-width: 760px) {
  .endpoint-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .endpoint-row {
    grid-template-columns: 24px minmax(0, 1fr);
  }

  .endpoint-health,
  .endpoint-actions {
    grid-column: 2;
  }

  .endpoint-actions {
    flex-wrap: wrap;
  }

  .json-editor-layout {
    grid-template-columns: 1fr;
    min-height: 0;
  }

  .config-preview {
    padding: 14px 0 0;
    border-top: 1px solid var(--border-subtle);
    border-left: 0;
  }

  .json-editor-heading {
    align-items: flex-start;
    flex-direction: column;
  }

  :global(.llm-json-editor-dialog .el-dialog__body) {
    min-height: 0;
  }
}
</style>
