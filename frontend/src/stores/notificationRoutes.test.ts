import { describe, expect, it } from 'vitest'

import { resolveNotificationRoute } from './notificationRoutes'

describe('notification result route resolver', () => {
  it('maps backend AI references to the valid Copilot UI route', () => {
    expect(resolveNotificationRoute('/api/ai/runs/run-42'))
      .toBe('/home?copilot=1&ai_run=run-42')
    expect(resolveNotificationRoute('/api/ai/conversations/conversation-7'))
      .toBe('/home?copilot=1&conversation_id=conversation-7')
  })

  it('keeps allowlisted frontend result routes', () => {
    expect(resolveNotificationRoute('/backtest?task_id=task-1'))
      .toBe('/backtest?task_id=task-1')
    expect(resolveNotificationRoute('/factor/detail/value_score'))
      .toBe('/factor/detail/value_score')
    expect(resolveNotificationRoute('/market-radar?alert=42'))
      .toBe('/market-radar?alert=42')
  })

  it.each([
    'https://example.com/phishing',
    '//example.com/phishing',
    '/api/system/config',
    '/unknown/path',
    '/unknown/../home',
    '/api/ai/runs/run%2Fsecret',
    'backtest:1',
    '',
    null,
  ])('rejects external, backend-only, or unknown references: %s', reference => {
    expect(resolveNotificationRoute(reference)).toBeUndefined()
  })
})
