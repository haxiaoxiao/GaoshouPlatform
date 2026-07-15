import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import {
  LLM_API_KEY_PLACEHOLDER,
  buildLlmEndpointCreate,
  buildLlmEndpointUpdate,
  createLlmEndpointTemplate,
  formatLlmEndpointConfig,
  getLlmEndpointConfigWarnings,
  getLlmEndpointPreservedFields,
  shouldConfirmLlmTemplateReset,
  parseLlmEndpointConfig,
  previewLlmEndpointConfig,
  sanitizedLlmEndpointConfig,
  isLlmEndpointCooldownActive,
  getLlmGatewayView,
  moveLlmEndpointIds,
  sanitizeRecentEndpointError,
  summarizeLlmEndpoints,
  type LlmEndpoint,
} from '@/api/system'
const sourceRoot = fileURLToPath(new URL('..', import.meta.url))
const readSource = (path: string) => readFileSync(`${sourceRoot}/${path}`, 'utf-8')

const endpoint = (overrides: Partial<LlmEndpoint> = {}): LlmEndpoint => ({
  id: 'primary',
  name: 'Primary',
  api_base: 'https://llm.example.test/v1',
  api_key_hint: '********1234',
  model: 'provider/model',
  provider: 'provider',
  review_model: null,
  wire_api: 'responses',
  reasoning_effort: 'medium',
  disable_response_storage: true,
  requires_openai_auth: true,
  config: {
    model_provider: 'provider',
    model: 'provider/model',
    model_providers: {
      provider: {
        name: 'Primary',
        base_url: 'https://llm.example.test/v1',
        wire_api: 'responses',
        requires_openai_auth: true,
      },
    },
    env: { OPENAI_API_KEY: LLM_API_KEY_PLACEHOLDER },
  },
  preserved_fields: [],
  priority: 0,
  enabled: true,
  consecutive_failures: 0,
  cooldown_until: null,
  last_success_at: '2026-07-14T01:00:00Z',
  last_failure_at: null,
  last_error: null,
  created_at: '2026-07-14T00:00:00Z',
  updated_at: '2026-07-14T01:00:00Z',
  ...overrides,
})

describe('LLM endpoint JSON configuration', () => {
  it('creates a safe template with an obviously fake key', () => {
    const text = createLlmEndpointTemplate()
    expect(text).toContain('replace-with-your-api-key')
    expect(text).not.toContain(LLM_API_KEY_PLACEHOLDER)
    expect(parseLlmEndpointConfig(text).config).toMatchObject({
      model_provider: 'openai',
      env: { OPENAI_API_KEY: 'replace-with-your-api-key' },
    })
  })

  it('rejects invalid JSON and non-object JSON with useful local errors', () => {
    expect(() => parseLlmEndpointConfig('{')).toThrow('valid JSON')
    expect(() => parseLlmEndpointConfig('[]')).toThrow('JSON object')
  })

  it('formats valid JSON deterministically', () => {
    expect(formatLlmEndpointConfig('{"model":"gpt-5","model_provider":"openai"}'))
      .toBe('{\n  "model": "gpt-5",\n  "model_provider": "openai"\n}')
  })

  it('loads sanitized edit JSON with the stored-secret placeholder', () => {
    const text = sanitizedLlmEndpointConfig(endpoint())
    expect(text).toContain(LLM_API_KEY_PLACEHOLDER)
    expect(text).not.toContain('********1234')
  })

  it('builds JSON-only create and update payloads', () => {
    const config = parseLlmEndpointConfig(createLlmEndpointTemplate()).config
    expect(buildLlmEndpointCreate(config, true)).toEqual({ config, enabled: true })
    expect(buildLlmEndpointUpdate(config, false)).toEqual({ config, enabled: false })
  })

  it('extracts a preview and derives preserved fields from the current JSON', () => {
    const current = endpoint()
    expect(previewLlmEndpointConfig(current.config)).toEqual({
      provider: 'provider',
      name: 'Primary',
      model: 'provider/model',
      reviewModel: null,
      apiBase: 'https://llm.example.test/v1',
      wireApi: 'responses',
      reasoningEffort: null,
      disableResponseStorage: false,
      requiresOpenaiAuth: true,
    })
    current.config.custom_root = true
    current.config.network_access = 'enabled'
    current.config.windows_wsl_setup_acknowledged = true
    current.config.env = { OPENAI_API_KEY: LLM_API_KEY_PLACEHOLDER, REGION: 'us' }
    current.config.model_providers = {
      provider: {
        name: 'Primary',
        base_url: 'https://llm.example.test/v1',
        wire_api: 'responses',
        requires_openai_auth: true,
        vendor_option: 'kept',
      },
      backup: { base_url: 'https://backup.example.test/v1' },
    }
    expect(getLlmEndpointPreservedFields(current.config)).toEqual([
      'custom_root',
      'env.REGION',
      'model_providers.backup',
      'model_providers.provider.vendor_option',
      'network_access',
      'windows_wsl_setup_acknowledged',
    ])
    expect(getLlmEndpointConfigWarnings(current.config)).toEqual([
      'These fields are preserved but not interpreted by the gateway: custom_root, env.REGION, model_providers.backup, model_providers.provider.vendor_option, network_access, windows_wsl_setup_acknowledged',
    ])

    delete current.config.custom_root
    expect(getLlmEndpointConfigWarnings(current.config)[0]).not.toContain('custom_root')
  })

  it('requires confirmation only when reset would discard non-template content', () => {
    const template = createLlmEndpointTemplate()
    expect(shouldConfirmLlmTemplateReset(template)).toBe(false)
    expect(shouldConfirmLlmTemplateReset(`${template}\n`)).toBe(false)
    expect(shouldConfirmLlmTemplateReset(sanitizedLlmEndpointConfig(endpoint()))).toBe(true)
    expect(shouldConfirmLlmTemplateReset('{"custom":true}')).toBe(true)
  })

  it('never models or reads a plaintext API key', () => {
    const current = endpoint()
    expect('api_key' in current).toBe(false)
    const apiSource = readSource('api/system.ts')
    const readType = apiSource.slice(apiSource.indexOf('export interface LlmEndpoint {'), apiSource.indexOf('export interface LlmEndpointCreatePayload'))
    expect(readType).not.toMatch(/\bapi_key\s*:/)
  })
})

describe('LLM endpoint operations', () => {
  it('treats only a future cooldown timestamp as active', () => {
    const now = Date.parse('2026-07-14T12:00:00Z')
    expect(isLlmEndpointCooldownActive(endpoint({ cooldown_until: '2026-07-14T12:00:01Z' }), now)).toBe(true)
    expect(isLlmEndpointCooldownActive(endpoint({ cooldown_until: '2026-07-14T11:59:59Z' }), now)).toBe(false)
    expect(isLlmEndpointCooldownActive(endpoint({ cooldown_until: null }), now)).toBe(false)
  })

  it('distinguishes an initial load failure from a configured block and preserves stale data', () => {
    expect(getLlmGatewayView([], 'error')).toMatchObject({ readiness: 'unknown', enabled: null, total: null })
    expect(getLlmGatewayView([endpoint()], 'error')).toMatchObject({
      readiness: 'error',
      enabled: 1,
      total: 1,
      primary: 'Primary',
    })
    expect(getLlmGatewayView([], 'ready')).toMatchObject({ readiness: 'blocked', enabled: 0, total: 0 })
  })

  it('moves endpoints one position without crossing list boundaries', () => {
    const ids = ['a', 'b', 'c']
    expect(moveLlmEndpointIds(ids, 'b', -1)).toEqual(['b', 'a', 'c'])
    expect(moveLlmEndpointIds(ids, 'b', 1)).toEqual(['a', 'c', 'b'])
    expect(moveLlmEndpointIds(ids, 'a', -1)).toEqual(ids)
    expect(moveLlmEndpointIds(ids, 'c', 1)).toEqual(ids)
  })

  it('summarizes readiness, counts and primary endpoint by priority', () => {
    expect(summarizeLlmEndpoints([
      endpoint({ id: 'backup', name: 'Backup', priority: 1 }),
      endpoint({ id: 'primary', name: 'Primary', priority: 0 }),
      endpoint({ id: 'off', name: 'Off', priority: 2, enabled: false }),
    ])).toEqual({
      readiness: 'ready',
      enabled: 2,
      total: 3,
      primary: 'Primary',
      recentError: null,
    })
  })

  it('reports degraded readiness and sanitizes a recent error for the dock', () => {
    const secret = 'sk-should-never-render'
    const summary = summarizeLlmEndpoints([
      endpoint({
        consecutive_failures: 2,
        last_failure_at: '2026-07-14T02:00:00Z',
        last_error: `401 bearer ${secret}\nprovider details`,
      }),
    ])
    expect(summary.readiness).toBe('degraded')
    expect(summary.recentError).toBe('401 bearer [redacted] provider details')
    expect(summary.recentError).not.toContain(secret)
    expect(sanitizeRecentEndpointError('x'.repeat(240)).length).toBeLessThanOrEqual(163)
  })
})

describe('LLM endpoint source contracts', () => {
  it('routes all six endpoint methods through the shared request wrapper', () => {
    const apiSource = readSource('api/system.ts')
    expect(apiSource).not.toContain("import axios from 'axios'")
    expect(apiSource).toMatch(/listLlmEndpoints:[\s\S]*request\.get<LlmEndpoint\[]>\('\/system\/llm-endpoints'\)/)
    expect(apiSource).toMatch(/createLlmEndpoint:[\s\S]*request\.post<LlmEndpoint>\('\/system\/llm-endpoints', payload\)/)
    expect(apiSource).toMatch(/updateLlmEndpoint:[\s\S]*request\.patch<LlmEndpoint>\(`\/system\/llm-endpoints\/\$\{endpointId\}`, payload\)/)
    expect(apiSource).toMatch(/deleteLlmEndpoint:[\s\S]*request\.delete<void>\(`\/system\/llm-endpoints\/\$\{endpointId\}`\)/)
    expect(apiSource).toMatch(/reorderLlmEndpoints:[\s\S]*request\.post<LlmEndpoint\[]>\('\/system\/llm-endpoints\/reorder'/)
    expect(apiSource).toMatch(/testLlmEndpoint:[\s\S]*request\.post<LlmEndpointTestResult>\(`\/system\/llm-endpoints\/\$\{endpointId\}\/test`\)/)
    expect(readSource('api/request.ts')).toMatch(/patch:\s*<T>[\s\S]*instance\.patch\(url, data, config\)/)
  })

  it('mounts the manager and retains its security and operation controls', () => {
    const monitorSource = readSource('views/SystemMonitor/index.vue')
    const managerSource = readSource('components/LLMEndpointManager.vue')
    expect(monitorSource).toContain("import LLMEndpointManager from '@/components/LLMEndpointManager.vue'")
    expect(monitorSource).toContain('<LLMEndpointManager')
    expect(monitorSource).toContain("llmEndpointLoadState === 'loading'")
    expect(managerSource).toContain("import CodeEditor from '@/components/CodeEditor.vue'")
    expect(managerSource).toContain('language="json"')
    expect(managerSource).toContain('aria-label="LLM endpoint configuration JSON"')
    expect(managerSource).toContain('@click="formatConfig"')
    expect(managerSource).toContain('@click="validateConfig"')
    expect(managerSource).toContain('updateConfigInsights(config)')
    expect(managerSource).toContain('@click="resetToTemplate"')
    expect(managerSource).toContain("ElMessage.success('Configuration JSON is valid.')")
    expect(managerSource).toContain("ElMessageBox.confirm(")
    expect(managerSource).toContain('The API key is encrypted at rest and is never shown after saving.')
    expect(managerSource).toContain('endpoint.api_key_hint')
    expect(managerSource).toContain('preservedWarnings')
    expect(managerSource).toContain('v-if="!loading && !error && !endpoints.length"')
    expect(managerSource).toContain("loadState === 'error' && endpoints.length")
    expect(managerSource).toContain(':class="{ \'endpoint-row--stale\': mutationsDisabled }"')
    expect(managerSource).toContain("const mutationsDisabled = computed(() => loading.value || loadState.value !== 'ready' || Boolean(busyId.value))")
    expect(managerSource).toContain(':disabled="mutationsDisabled"')
    expect(managerSource).toContain('@click="loadEndpoints">Reload</el-button>')
    expect(managerSource).toContain('<el-switch')
    expect(managerSource).toContain(':aria-label="`${endpoint.enabled ? \'Disable\' : \'Enable\'} ${endpoint.name}`"')
    expect(managerSource).toContain('@click="move(endpoint.id, -1)"')
    expect(managerSource).toContain('@click="move(endpoint.id, 1)"')
    expect(managerSource).toContain('@click="testEndpoint(endpoint)"')
    expect(managerSource).toContain(':aria-label="`Move ${endpoint.name} up`"')
    expect(managerSource).toContain(':aria-label="`Move ${endpoint.name} down`"')
    expect(managerSource).toContain(':aria-label="`Test ${endpoint.name}`"')
    expect(managerSource).toContain(':aria-label="`Edit ${endpoint.name}`"')
    expect(managerSource).toContain(':aria-label="`Delete ${endpoint.name}`"')
    expect(managerSource).toContain('ElMessageBox.confirm')
    expect(managerSource).toContain('CirclePlus')
    expect(managerSource).not.toMatch(/\bPlus\b/)
  })

  it('passes an accessible name through to the actual CodeMirror textbox', () => {
    const editorSource = readSource('components/CodeEditor.vue')
    expect(editorSource).toContain('ariaLabel?: string')
    expect(editorSource).toContain("EditorView.contentAttributes.of({ 'aria-label': props.ariaLabel })")
    expect(editorSource).toContain('contentAttributesCompartment.reconfigure')
  })
})
