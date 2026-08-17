const FRONTEND_RESULT_ROUTES = [
  /^\/home$/,
  /^\/data(?:\/sync)?$/,
  /^\/explorer$/,
  /^\/watchlist$/,
  /^\/stock\/[^/]+$/,
  /^\/factor(?:\/evaluation|\/analysis-new\/[^/]+|\/detail\/[^/]+)?$/,
  /^\/research$/,
  /^\/backtest(?:\/factor\/[^/]+|\/optimization\/[^/]+)?$/,
  /^\/market-radar$/,
  /^\/trade$/,
  /^\/monitor$/,
  /^\/docs$/,
]

function encodedSegment(value: string): string | undefined {
  try {
    const decoded = decodeURIComponent(value)
    return /^[A-Za-z0-9._:-]{1,200}$/.test(decoded)
      ? encodeURIComponent(decoded)
      : undefined
  } catch {
    return undefined
  }
}

function containsPathTraversal(resultRef: string): boolean {
  const rawPath = resultRef.split(/[?#]/, 1)[0]
  return rawPath.split('/').some(segment => {
    try {
      const decoded = decodeURIComponent(segment)
      return decoded === '.' || decoded === '..'
    } catch {
      return true
    }
  })
}

export function resolveNotificationRoute(resultRef: string | null | undefined): string | undefined {
  if (
    !resultRef
    || !resultRef.startsWith('/')
    || resultRef.startsWith('//')
    || resultRef.includes('\\')
    || containsPathTraversal(resultRef)
  ) {
    return undefined
  }

  let url: URL
  try {
    url = new URL(resultRef, 'https://gaoshou.local')
  } catch {
    return undefined
  }
  if (url.origin !== 'https://gaoshou.local') return undefined

  const aiRun = url.pathname.match(/^\/api\/ai\/runs\/([^/]+)$/)
  if (aiRun) {
    const runId = encodedSegment(aiRun[1])
    return runId ? `/home?copilot=1&ai_run=${runId}` : undefined
  }

  const aiConversation = url.pathname.match(/^\/api\/ai\/conversations\/([^/]+)$/)
  if (aiConversation) {
    const conversationId = encodedSegment(aiConversation[1])
    return conversationId ? `/home?copilot=1&conversation_id=${conversationId}` : undefined
  }

  if (!FRONTEND_RESULT_ROUTES.some(pattern => pattern.test(url.pathname))) {
    return undefined
  }
  return `${url.pathname}${url.search}`
}
