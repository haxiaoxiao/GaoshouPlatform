export class PollingAbortedError extends Error {
  constructor() {
    super('Polling aborted')
    this.name = 'PollingAbortedError'
  }
}

export class PollingTimeoutError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'PollingTimeoutError'
  }
}

export const isPollingAborted = (error: unknown): error is PollingAbortedError =>
  error instanceof PollingAbortedError

const assertNotAborted = (signal?: AbortSignal) => {
  if (signal?.aborted) throw new PollingAbortedError()
}

export const pollingDelay = (milliseconds: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    assertNotAborted(signal)
    const timer = window.setTimeout(() => {
      signal?.removeEventListener('abort', abort)
      resolve()
    }, milliseconds)
    const abort = () => {
      window.clearTimeout(timer)
      reject(new PollingAbortedError())
    }
    signal?.addEventListener('abort', abort, { once: true })
  })

interface PollUntilOptions<T> {
  request: () => Promise<T>
  isTerminal: (value: T) => boolean
  onValue?: (value: T) => void | Promise<void>
  intervalMs: number
  timeoutMs: number
  timeoutMessage: string
  signal?: AbortSignal
  now?: () => number
  wait?: (milliseconds: number, signal?: AbortSignal) => Promise<void>
}

export async function pollUntil<T>(options: PollUntilOptions<T>): Promise<T> {
  const now = options.now || Date.now
  const wait = options.wait || pollingDelay
  const deadline = now() + options.timeoutMs

  for (;;) {
    assertNotAborted(options.signal)
    const value = await options.request()
    assertNotAborted(options.signal)
    await options.onValue?.(value)
    if (options.isTerminal(value)) return value

    const remaining = deadline - now()
    if (remaining <= 0) throw new PollingTimeoutError(options.timeoutMessage)
    await wait(Math.min(options.intervalMs, remaining), options.signal)
  }
}
