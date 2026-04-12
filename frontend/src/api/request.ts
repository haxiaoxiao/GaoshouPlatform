import axios, { type AxiosInstance, type AxiosResponse } from 'axios'
import { ElMessage } from 'element-plus'

const instance: AxiosInstance = axios.create({
  baseURL: '/api',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// 响应拦截器 - 解包 response.data
instance.interceptors.response.use(
  (response: AxiosResponse) => {
    return response.data
  },
  (error) => {
    const message = error.response?.data?.detail || error.message || '请求失败'
    ElMessage.error(message)
    return Promise.reject(error)
  }
)

// 包装请求方法，返回正确的类型（拦截器已经解包了 data）
const request = {
  get: <T>(url: string, config?: object): Promise<T> =>
    instance.get(url, config) as Promise<T>,

  post: <T>(url: string, data?: unknown, config?: object): Promise<T> =>
    instance.post(url, data, config) as Promise<T>,

  put: <T>(url: string, data?: unknown, config?: object): Promise<T> =>
    instance.put(url, data, config) as Promise<T>,

  delete: <T>(url: string, config?: object): Promise<T> =>
    instance.delete(url, config) as Promise<T>,
}

export default request
