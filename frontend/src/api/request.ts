import axios, {
  type AxiosInstance,
  type AxiosRequestConfig,
  type AxiosResponse,
} from 'axios'
import { notifyRequestError } from './requestNotifications'

export interface RequestConfig<D = unknown> extends AxiosRequestConfig<D> {
  notifyError?: boolean
}

interface ResponseLike {
  data: unknown
  config?: RequestConfig
}

interface RequestErrorLike {
  message?: string
  config?: RequestConfig
  response?: {
    data?: unknown
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object'
}

function errorMessage(error: RequestErrorLike): string {
  const responseData = error.response?.data
  const envelopeMessage = isRecord(responseData)
    && typeof responseData.code === 'number'
    && responseData.code !== 0
    && typeof responseData.message === 'string'
    && responseData.message.trim()
    ? responseData.message
    : undefined
  const detail = isRecord(responseData) ? responseData.detail : undefined
  return envelopeMessage || (typeof detail === 'string' && detail.trim()
    ? detail
    : error.message || '请求失败')
}

function notificationsEnabled(config?: RequestConfig): boolean {
  return config?.notifyError !== false
}

export function handleResponse(response: ResponseLike): unknown {
  const backendData = response.data
  if (!isRecord(backendData) || typeof backendData.code !== 'number') {
    return backendData
  }

  if (backendData.code === 0) {
    return backendData.data
  }

  const message = typeof backendData.message === 'string' && backendData.message.trim()
    ? backendData.message
    : '请求失败'
  if (notificationsEnabled(response.config)) {
    notifyRequestError(message)
  }
  throw new Error(message)
}

export function handleResponseError(error: unknown): Promise<never> {
  const requestError = isRecord(error) ? error as RequestErrorLike : {}
  const responseData = requestError.response?.data
  const isBusinessEnvelope = isRecord(responseData)
    && typeof responseData.code === 'number'
    && responseData.code !== 0
  const message = errorMessage(requestError)
  if (notificationsEnabled(requestError.config)) {
    notifyRequestError(message)
  }
  return Promise.reject(isBusinessEnvelope ? new Error(message, { cause: error }) : error)
}

const instance: AxiosInstance = axios.create({
  baseURL: '/api',
  timeout: 120_000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// 响应拦截器 - 解包 response.data，并解包后端的 { code, message, data } 包装
instance.interceptors.response.use(
  (response: AxiosResponse) => handleResponse(response) as AxiosResponse,
  handleResponseError,
)

// 包装请求方法，返回正确的类型（拦截器已经解包了 data）
const request = {
  get: <T>(url: string, config?: RequestConfig): Promise<T> =>
    instance.get(url, config) as Promise<T>,

  post: <T>(url: string, data?: unknown, config?: RequestConfig): Promise<T> =>
    instance.post(url, data, config) as Promise<T>,

  put: <T>(url: string, data?: unknown, config?: RequestConfig): Promise<T> =>
    instance.put(url, data, config) as Promise<T>,

  patch: <T>(url: string, data?: unknown, config?: RequestConfig): Promise<T> =>
    instance.patch(url, data, config) as Promise<T>,

  delete: <T>(url: string, config?: RequestConfig): Promise<T> =>
    instance.delete(url, config) as Promise<T>,
}

export default request
