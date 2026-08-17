export interface RequestScopeToken {
  readonly scope: string
  readonly scopeVersion: number
  readonly requestVersion: number
}

export interface ScopeToken {
  readonly scope: string
  readonly scopeVersion: number
}

export function createScopedRequestGate(initialScope = '') {
  let scope = initialScope
  let scopeVersion = 0
  let requestVersion = 0

  function setScope(nextScope: string) {
    if (nextScope === scope) return
    scope = nextScope
    scopeVersion += 1
    requestVersion = 0
  }

  function captureScope(): ScopeToken {
    return { scope, scopeVersion }
  }

  function begin(nextScope: string): RequestScopeToken {
    setScope(nextScope)
    requestVersion += 1
    return { scope, scopeVersion, requestVersion }
  }

  function isScopeCurrent(token: ScopeToken): boolean {
    return token.scope === scope && token.scopeVersion === scopeVersion
  }

  function isLatest(token: RequestScopeToken): boolean {
    return isScopeCurrent(token) && token.requestVersion === requestVersion
  }

  function commit(token: RequestScopeToken, apply: () => void): boolean {
    if (!isLatest(token)) return false
    apply()
    return true
  }

  return {
    begin,
    captureScope,
    commit,
    isLatest,
    isScopeCurrent,
    setScope,
  }
}
