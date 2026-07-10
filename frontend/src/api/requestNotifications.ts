export type RequestErrorNotifier = (message: string) => void

const defaultNotifier: RequestErrorNotifier = message => {
  console.error(`[API Request Error] ${message}`)
}

let requestErrorNotifier = defaultNotifier

export function setRequestErrorNotifier(notifier?: RequestErrorNotifier) {
  requestErrorNotifier = notifier ?? defaultNotifier
}

export function notifyRequestError(message: string) {
  requestErrorNotifier(message)
}
