import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import {
  buildLlmEndpointUpdate,
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

describe('LLM endpoint edit payloads', () => {
  it('never models a readable API key and omits a blank key when destination is unchanged', () => {
    const current = endpoint()
    expect('api_key' in current).toBe(false)

    expect(buildLlmEndpointUpdate(current, {
      name: 'Primary renamed',
      api_base: current.api_base,
      api_key: '',
      model: current.model,
      enabled: current.enabled,
    })).toEqual({
      name: 'Primary renamed',
      api_base: current.api_base,
      model: current.model,
      enabled: true,
    })
  })

  it('sends a blank key when the destination changes so backend validation is visible', () => {
    const current = endpoint()
    expect(buildLlmEndpointUpdate(current, {
      name: current.name,
      api_base: 'https://other.example.test/v1',
      api_key: '   ',
      model: current.model,
      enabled: true,
    })).toMatchObject({
      api_base: 'https://other.example.test/v1',
      api_key: '',
    })
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
    expect(managerSource).toContain('type="password"')
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
})
